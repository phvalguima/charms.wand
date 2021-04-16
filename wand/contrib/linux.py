import os
import pathlib
import subprocess

__all__= [
    "LinuxError",
    "getUserAndGroupOfFolder",
    "getCurrentUserAndGroup",
    "userAdd",
    "groupAdd"
]

class LinuxError(Exception):
    def __init__(self, message):
        super().__init__(self.message)


def getUserAndGroupOfFolder(folderpath):
    f = pathlib.Path(folderpath)
    return f.owner(), f.group()


def getCurrentUserAndGroup():
    folder = os.getenv("HOME")
    return getUserAndGroupOfFolder(folder)


def userAdd(username,
            password=None,
            group=None,
            group_list=None,
            uid=None,
            home=None,
            system=None,
            shell=None):

    cmd = ["useradd"]
    if uid:
        cmd += ["-u", uid]
# TODO(pguimaraes): create groupExists call
#    if group and not groupExists(group):
#        raise LinuxError("userAdd: failed to find defined group")
    if group:
        cmd += ["-g", group]
    elif group_list:
        cmd += ["-G", ",".join(group_list)]
    if home:
        cmd += ["-d", home]
    else:
        cmd += ["-M"]
    if shell:
        cmd += ["-s", shell]
    if system:
        cmd += ["-r"]
    cmd += [username]
    return subprocess.check_call(cmd)


def groupAdd(groupname,
             system=None,
             gid=None):
    cmd = ["groupadd"]
    if gid:
        cmd += ["-g", gid]
    if system:
        cmd += ["-r"]
    cmd += [groupname]
    return subprocess.check_call(cmd)
