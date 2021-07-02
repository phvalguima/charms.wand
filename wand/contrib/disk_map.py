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

"""

import yaml
import os
import shutil
import copy
import logging
import subprocess

import json

from ops.framework import Object, StoredState
from charmhelpers.core.host import mount

logger = logging.getLogger(__name__)


def create_dir(data_log_dev,
               data_log_dir,
               data_log_fs,
               user,
               group,
               fs_options=None):

    if len(data_log_dir or "") == 0:
        logger.warning("Data log dir config empty")
        return
    os.makedirs(data_log_dir, 0o750, exist_ok=True)
    shutil.chown(data_log_dir,
                 user=user,
                 group=group)
    dev, fs = None, None
    if len(data_log_dev or "") == 0:
        logger.warning("Data log device not found, using rootfs instead")
    else:
        for k, v in data_log_dev:
            fs = k
            dev = v
        logger.info("Data device: mkfs -t {}".format(fs))
        cmd = ["mkfs", "-t", fs, dev]
        subprocess.check_call(cmd)
        mount(dev, data_log_dir,
              options=data_log_fs,
              persist=True, filesystem=fs)


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
                if os.path.exists(path):
                    raise DiskMapHelperPathAlreadyExistsError("path")
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
            self.state.set_default(foldermap=folder_map)

    def used_folders(self):
        return copy.deepcopy(list(self.disk2folder.values()))

    def on_storage_attached(self, event):
        """Checks for the storage available and cross with disk2folder
        dict. If disk is present there, move to the next until an non
        used disk is found.

        event: CharmEvent, will not be used
        """
        d2f = self.disk2folder
        # Iterate over each disk available and add mount points
        # If no folder is available anymore, disks will be silently ignored
        for s in self.charm.model.storages[self.storage_name]:
            if s.location not in d2f.keys():
                # Found unused disk
                # Now, select a unused mount point from foldermap
                foldermap = self.foldermap
                for f, args in foldermap.items():
                    # Check if this has been already allocated:
                    if f not in d2f.values():
                        # This folder has not been implemented yet, create it
                        create_dir(s.location, f, foldermap[f],
                                   self.user, self.group,
                                   fs_options=foldermap[f]["options"])
                        # Storage location comes as PosixPath, convert it to str()
                        d2f[str(s.location)] = f
                        self.disk2folder = d2f

    def on_storage_detaching(self, event):
        # There is nothing so far to do with detaching
        # if we remove its entry from disk2folder, the folder may
        # end up relocated to another disk, which can also confuse
        # the application.
        # Log an leave it.
        logger.info("Detached disk: {}, nothing done".format(event))
        pass
