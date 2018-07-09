#!/usr/bin/env python

############################################################
# Copyright (C) 2018 Citrix Systems Inc.
# Unauthorized copying or use of this file is prohibited.
############################################################

import argparse
import errno
import os
from multiprocessing import Process
import shutil
import stat
import subprocess
import tempfile
import XenAPI

BACKUP_VDI_SIZE = 10 * 1024 * 1024 * 1024
RESTIC_URL = ('https://github.com/restic/restic/releases/download/'
              'v0.9.1/restic_0.9.1_linux_amd64.bz2')
RESTIC_EXEC = 'restic_0.9.1_linux_amd64'


def runcmd(cmd_args, error=True, expRc=0):
    os.environ["RESTIC_PASSWORD"] = "none"
    print(cmd_args)
    proc = subprocess.Popen(
        cmd_args,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        close_fds=True)
    stdout, stderr = proc.communicate()
    if error and proc.returncode != expRc:
        raise Exception('{} exitted with non-zero code {}: {}'.format(
            " ".join(cmd_args), proc.returncode, stderr))
    print(stdout)
    print(stderr)
    return stdout, stderr, proc.returncode


def get_restic():
    if not os.path.exists('/usr/bin/restic'):
        tempdir = tempfile.mkdtemp()
        runcmd(['wget',
                RESTIC_URL,
                '-O', os.path.join(tempdir, 'restic.bz2')])
        runcmd(['bzip2', '-d', os.path.join(tempdir, 'restic.bz2')])
        shutil.move(os.path.join(
            tempdir, 'restic_0.9.1_linux_amd64'), '/usr/bin/restic')
        os.rmdir(tempdir)
    os.chmod('/usr/bin/restic', stat.S_IREAD | stat.S_IWRITE | stat.S_IEXEC)


class Backup_Share():
    def __init__(self):
        return None

    def _retry_device_exists(self, function, config, devicenumberfield):
        devicenumber = 0
        config[devicenumberfield] = str(devicenumber)
        while True:
            try:
                ref = function(config)
                return ref
            except XenAPI.Failure as failure:
                if ((failure.details[0] != 'DEVICE_ALREADY_EXISTS' or
                     devicenumber > 20)):
                    raise failure
                devicenumber = devicenumber + 1
                config[devicenumberfield] = str(devicenumber)

    def get_this_host_uuid(self):
        # ToDo: There must be a better way that also works with plugins?!?
        uuid = None
        filehandler = open("/etc/xensource-inventory", 'r')
        try:
            for line in filehandler.readlines():
                if line.startswith("INSTALLATION_UUID"):
                    uuid = line.split("'")[1]
                    break
        finally:
            filehandler.close()
        return uuid

    def get_this_host_ref(self):
        host_uuid = self.get_this_host_uuid()
        host_ref = self._session.xenapi.host.get_by_uuid(host_uuid)
        return host_ref

    def get_this_control_vm_ref(self):
        vms = self._session.xenapi.VM.get_all_records()
        this_host_ref = self.get_this_host_ref()
        for vm_ref, vm in vms.iteritems():
            if vm['is_control_domain'] and vm['resident_on'] == this_host_ref:
                return vm_ref
        raise Exception("No control VM found")

    def get_other_config(self):
        return self._session.xenapi.host.get_other_config(
                self.get_this_host_ref())

    def update_other_config(self, key, value):
        other_config = self.get_other_config()
        other_config[key] = value
        self._session.xenapi.host.set_other_config(self.get_this_host_ref(),
                                                   other_config)

    def get_existing_backup_vdi(self):
        other_config = self.get_other_config()
        vdi_ref = None
        if "backup_vdi_uuid" in other_config:
            vdi_ref = self._session.xenapi.VDI.get_by_uuid(
                other_config["backup_vdi_uuid"])
        return vdi_ref


    def create_backup_vdi(self):
        sr_ref_to_use = None
        for sr_ref in self._session.xenapi.SR.get_all():
            type = self._session.xenapi.SR.get_record(sr_ref).get('type')
            if type in ['ext', 'lvm']:
                # ToDO: check local pbd availiable
                sr_ref_to_use = sr_ref
        if not sr_ref_to_use:
            raise Exception("No SR for backups found")
        vdiconf = {
            'name_label': 'Dom0 Backup',
            'name_description': '',
            'SR': sr_ref_to_use,
            'virtual_size': str(BACKUP_VDI_SIZE),
            'type': 'user',
            'sharable': False,
            'read_only': False,
            'other_config': {}}
        vdi_ref = self._session.xenapi.VDI.create(vdiconf)
        vdi_uuid = self._session.xenapi.VDI.get_uuid(vdi_ref)
        self.update_other_config("backup_vdi_uuid", vdi_uuid)
        return vdi_ref

    def create_and_plug_backup_vbd(self, vdi_ref):
        vbdconf = {'VDI': vdi_ref,
                   'VM': self.get_this_control_vm_ref(),
                   'userdevice': '1',
                   'type': 'Disk',
                   'mode': 'rw',
                   'bootable': False,
                   'empty': False,
                   'other_config': {},
                   'qos_algorithm_type': '',
                   'qos_algorithm_params': {},
                   }
        self.vbd_ref = self._retry_device_exists(
                self._session.xenapi.VBD.create, vbdconf, 'userdevice')
        self._session.xenapi.VBD.plug(self.vbd_ref)

    def __enter__(self):
        self._session = XenAPI.xapi_local()
        self._session.xenapi.login_with_password("root", "")
        other_config = self.get_other_config()
        vdi_ref = self.get_existing_backup_vdi()
        new_vdi = False
        if not vdi_ref:
            new_vdi = True
            vdi_ref = self.create_backup_vdi()
        self.create_and_plug_backup_vbd(vdi_ref)
        device = os.path.join(
            "/dev/", self._session.xenapi.VBD.get_device(self.vbd_ref))
        if new_vdi:
            runcmd(['mkfs.ext4', device])
        try:
            os.mkdir('/srv/restic-repo')
        except OSError as exc:
            if exc.errno != errno.EEXIST:
                raise
        if not os.path.ismount("/srv/restic-repo"):
            # Check the filesystem, just in case. -f required
            # for later offline resize2fs.
            runcmd(['e2fsck', '-f', '-p', device], error=False)
            # Try to resize, just in case.
            runcmd(['resize2fs', device])
            runcmd(['mount', device, "/srv/restic-repo"])
        if new_vdi:
            init_backup_repo()

    def __exit__(self, type, value, traceback):
        runcmd(['umount', '/srv/restic-repo'])
        self._session.xenapi.VBD.unplug(self.vbd_ref)
        self._session.xenapi.VBD.destroy(self.vbd_ref)
        self._session.xenapi.session.logout()


def init_backup_repo():
    runcmd(['restic', 'init', '--repo', '/srv/restic-repo'])


def do_backup():
    runcmd(['restic', '-r', '/srv/restic-repo', '--verbose', 'backup',
            '--one-file-system', '/'])
    runcmd(['restic', '-r', '/srv/restic-repo',
            'forget', '--keep-last', '1', '--prune'])


def do_restore():
    runcmd(['init', '1'])
    runcmd(['restic', '-r', '/srv/restic-repo',
            'restore', 'latest', '--target', '/'])
    runcmd(['shutdown', '-r', 'now'])


def do_restore_process():
    p = Process(target=do_restore)
    p.daemon = True
    p.start()
    # avoid tidying up during exit
    os._exit(0)


def do_status():
    runcmd(['restic', '-r', '/srv/restic-repo', 'snapshots'])


def main():
    argparser = argparse.ArgumentParser()
    subparsers = argparser.add_subparsers(
        help='sub-command help', dest='command')
    subparsers.add_parser('backup', help='Make a backup')
    subparsers.add_parser('restore', help='Restore to backup')
    subparsers.add_parser('status', help='Show the status')
    args = argparser.parse_args()

    get_restic()
    with Backup_Share():
        if args.command == "backup":
            do_backup()
        elif args.command == "restore":
            do_restore_process()
        elif args.command == "status":
            do_status()


if __name__ == "__main__":
    main()
