"""

Implements DiskMapHelper class, a helper to map a list of block mounted
on Juju to a list of folder paths, fs-types and their options.

Given that Juju only support passing a list of block devices rather than
a list of filesystems using "juju storage" feature, this Helper class
bridges this gap by listening to storage events and mapping them to a
list of folderpaths - fs-type - options received.

DiskMapHelper counts with its own set of events that should be emitted
every time the folder map has been updated.

Every charm using DiskMapHelper should have an option that captures the
folder path map, for example:

data-folders:
  type: string
  default: ''
  description: ...

That option is used by the operator as follows:

data-folders: |
  - /data:
    - fs-type: ext4
    - options: ''
  - /test:
    - fs-type: ext4
    - options: ''
...

If your charm needs to manipulate more than one type of storage, then
create one DiskMapHelper per storage and one config as above as well.

IMPORTANT:
Changing the config folders post-install hook will result in little
changes because: (1) afaiu juju runs storage-attached hooks only before
install; and (2) the config itself will not clean up any existing mounts


Ensure the user and group specified exists prior to calling this class.

"""

import yaml
import os
import shutil
import copy
import logging
import subprocess
import pwd
import grp

import json

from ops.framework import Object, StoredState
from charmhelpers.core.host import mount
from charmhelpers.contrib.storage.linux.utils import is_device_mounted
from charmhelpers.contrib.storage.linux.lvm import (
    create_lvm_physical_volume,
    create_lvm_volume_group,
    create_logical_volume,
    list_logical_volumes,
    is_lvm_physical_volume,
    extend_logical_volume_by_device,
    list_lvm_volume_group
)

logger = logging.getLogger(__name__)


class DiskMapHelperDeviceNotDefined(Exception):
    def __init__(self, param):
        super().__init__(
            "Device not found: {}".format(param))


class DiskMapHelperPVAlreadyTakenForLVM(Exception):
    def __init__(self, d, lvm):
        super().__init__(
            "Device already a PV {} and under LVM: {}".format(
                d, lvm))


def manage_lvm(device, dirname):
    """Add the device to the VG corresponding to the dirname.

    In this case, logical volume name will be dirname with /
    replaced by _ symbols. If logical volume already exists,
    then, device is added as a new physical device and the entire
    logical volume is resized.

    If logical volume name does not exist, then prepare the
    physical device and add to the newly created volume.
    """
    lv_name = "lvm" + dirname.replace("/", "_")
    vg_name = "vg" + lv_name
    pv = device
    if list_lvm_volume_group(device):
        raise DiskMapHelperPVAlreadyTakenForLVM(device)
    if not is_lvm_physical_volume(device):
        # This is not a PV yet, create one
        create_lvm_physical_volume(device)
    if lv_name not in list_logical_volumes():
        # lv_name is always associated 1:1 with vg_name
        # If one does not exist, so we need to create both.
        create_lvm_volume_group(vg_name, pv)
        create_logical_volume(lv_name, vg_name)
        return lv_name
    # Now extend the lv_name:
    extend_logical_volume_by_device(lv_name, pv)
    # TODO: find FS already present and resize it as well
    return "/dev/{}/{}".format(vg_name, lv_name)


def create_dir(data_log_dev,
               data_log_dir,
               data_log_fs,
               user,
               group,
               fs_options=None):

    # # In some clouds, such as Azure, the location shown is
    # # actually a symlink. To clear this out, run:
    # data_dev = os.path.realpath(data_log_dev)
    if is_device_mounted(data_dev):
        logger.warning("Data device {} already mounted".format(
            data_dev))
        return
    lvm = None
    try:
        lvm = manage_lvm(data_dev, data_log_dir)
        if not lvm:
            return
    except DiskMapHelperPVAlreadyTakenForLVM:
        # Ignore this exception for now, it means there was
        # a rerun of disk_map.
        return

    if len(data_log_dir or "") == 0:
        logger.warning("Data log dir config empty")
        return
    if len(lvm or "") == 0:
        raise DiskMapHelperDeviceNotDefined(lvm)
    os.makedirs(data_log_dir, 0o750, exist_ok=True)
    shutil.chown(data_log_dir,
                 user=user,
                 group=group)
    logger.debug("Data device: mkfs -t {}".format(data_log_fs))
    cmd = ["mkfs", "-t", data_log_fs, lvm]
    opts = fs_options
    if "uid" not in fs_options:
        if len(opts) > 0:
            opts = opts + ","
        opts = opts + "uid={}".format(pwd.getpwnam(user).pw_uid)
    if "gid" not in fs_options:
        if len(opts) > 0:
            opts = opts + ","
        opts = opts + "gid={}".format(grp.getgrnam(group).gr_gid)
    subprocess.check_call(cmd)
    logger.debug("mount {} to {} with options {} and fs {}".format(
        data_log_dir, lvm, opts, data_log_fs))
    mount(lvm, data_log_dir,
          options=opts,
          persist=True, filesystem=data_log_fs)


class DiskMapHelperPathAlreadyExistsError(Exception):
    def __init__(self, path):
        super().__init__("Path already exists: {}".format(path))


class DiskMapHelperPathTooManyParamsError(Exception):
    def __init__(self, param):
        super().__init__(
            "Set only fs-type and option parameters: {}".format(param))


class DiskMapHelperPathMissingParamError(Exception):
    def __init__(self, param):
        super().__init__(
            "Missing parameter: {}".format(param))


class DiskMapHelper(Object):
    state = StoredState()

    def _check_folder_map(self, fm):
        if not yaml.load(fm):
            # If the folder map is empty, we can just inform it
            return False
        # The yaml should return a dict in the format of:
        # [... {"folderpath": []} ...]
        # Now, check if any missing parameters:
        for el in yaml.load(fm):
            for path in el.keys():
                # The value of el[path] is also a list with dicts

                # Now, check if there is more than two options:
                # - fs-type
                # - options
                if len(el[path]) > 2 or len(el[path]) == 0:
                    raise DiskMapHelperPathTooManyParamsError("path")
                # Check if fs-type and option is present:
                option_present = False
                fs_type_present = False
                for i in el[path]:
                    if "options" == str(list(i.keys())[0]):
                        option_present = True
                    if "fs-type" == str(list(i.keys())[0]):
                        fs_type_present = True
                if not fs_type_present:
                    raise DiskMapHelperPathMissingParamError("fs-type")
                if not option_present:
                    raise DiskMapHelperPathMissingParamError("options")
        return True

    def __init__(self, charm, folder_map, storage_name,
                 user, group):
        """Receives the parameter and sets the map initial state

        Arguments:
        - charm: CharmBase object corresponding to the charm
        - folder_map: string containing the yaml-format of the foldermap
        - storage_name: name used in the metadata.yaml for the storage
        """
        super().__init__(charm, storage_name)

        self.framework.observe(
            charm.on[storage_name].storage_attached,
            self.on_storage_attached)
        self.framework.observe(
            charm.on[storage_name].storage_detaching,
            self.on_storage_detaching)
        # Dict containing a map of disks > folders already dealt with
        self.state.set_default(disk2folder="{}")
        self.charm = charm
        self.storage_name = storage_name
        self.user = user
        self.group = group
        if self._check_folder_map(folder_map):
            self.state.set_default(foldermap=folder_map)
        else:
            self.state.set_default(foldermap="{}")
        # Disks are attached at install only
        self.state.set_default(are_disks_attached=False)

    @property
    def disk2folder(self):
        return json.loads(self.state.disk2folder)

    @disk2folder.setter
    def disk2folder(self, d):
        """Sets the disk2folder from a dict to the string value"""
        self.state.disk2folder = json.dumps(d)

    @property
    def foldermap(self):
        # Receives a list in the form of:
        # [... {"folderpath": []} ...]
        # and returns an organized dict with {... "folderpath": {} ...}
        result = {}
        folder_map = yaml.load(self.state.foldermap)
        for f in folder_map:
            # this is a single-element dict in the format:
            # {folderpath: [{fs-type: aa}, {options: bb}] }
            folder = list(f.keys())[0]
            # content is a list of dicts
            content = f[folder]
            """
            Now building a dictionary that is free of original
            in the format of:
            {
                "/test": {
                    "fs-type": ... ,
                    "options": ...
                },
                ...
            }
            """
            result[folder] = {
                **copy.deepcopy(content[0]),
                **copy.deepcopy(content[1])
            }
        return result

    @foldermap.setter
    def foldermap(self, folder_map):
        # IMPORTANT:
        # Only new folders will actually be considered in this class.
        if self._check_folder_map(folder_map):
            self.state.foldermap = folder_map

    def used_folders(self):
        return copy.deepcopy(list(self.disk2folder.values()))

    def on_storage_attached(self, event):
        logger.info("Attached disk: {}, nothing done".format(event))

    def attach_disks(self):
        """Checks for the storage available and cross with disk2folder
        dict. If disk is present there, move to the next until an non
        used disk is found.
        """
        if self.state.are_disks_attached:
            # This has been previously arranged, move on
            return
        d2f = self.disk2folder
        # Iterate over each disk available and add mount points
        # If no folder is available anymore, disks will be silently ignored
        for s in self.charm.model.storages[self.storage_name]:
            # Convert PosixPath to string
            dk = str(s.location)
            if dk not in d2f.keys():
                # Found unused disk
                # Now, select a unused mount point from foldermap
                foldermap = self.foldermap
                for f, args in foldermap.items():
                    # Check if this has been already allocated:
                    if f not in d2f.values():
                        if os.path.exists(f):
                            # It has, continue to the next folder
                            continue
                        # This folder has not been implemented yet, create it
                        create_dir(dk, f, foldermap[f]["fs-type"],
                                   self.user, self.group,
                                   fs_options=foldermap[f]["options"])
                        # Storage location comes as PosixPath,
                        # convert it to str()
                        d2f[dk] = f
                        self.disk2folder = d2f
                        # Finished finding the folder, for this disk, we
                        # can break the foldermap loop and get the next disk
                        break
        self.state.are_disks_attached = True

    def on_storage_detaching(self, event):
        # There is nothing so far to do with detaching
        # if we remove its entry from disk2folder, the folder may
        # end up relocated to another disk, which can also confuse
        # the application.
        # Log and leave it.
        logger.info("Detached disk: {}, nothing done".format(event))
