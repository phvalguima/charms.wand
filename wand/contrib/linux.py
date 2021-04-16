import os
import pathlib
import subprocess
import pwd
import grp

__all__ = [
    "LinuxError",
    "LinuxUserDoesNotExistError",
    "LinuxGroupDoesNotExistError",
    "LinuxGroupAlreadyExistsError",
    "LinuxUserAlreadyExistsError",
    "getUserAndGroupOfFolder",
    "getCurrentUserAndGroup",
    "userAdd",
    "groupAdd"
]


class LinuxError(Exception):
    def __init__(self, message):
        super().__init__(self.message)


class LinuxUserDoesNotExistError(Exception):
    def __init__(self, user):
        super().__init__(
            "User {} does not exist.".format(user))


class LinuxUserAlreadyExistsError(Exception):
    def __init__(self, user):
        super().__init__(
            "User {} already exists".format(user))


class LinuxGroupDoesNotExistError(Exception):
    def __init__(self, group):
        super().__init__(
            "Group {} does not exist.".format(group))


class LinuxGroupAlreadyExistsError(Exception):
    def __init__(self, group):
        super().__init__(
            "Group {} already exists.".format(group))


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

    alreadyExists = True
    try:
        pwd.getpwnam(username)
    except KeyError:
        alreadyExists = False
    if alreadyExists:
        raise LinuxUserAlreadyExistsError(username)

    if group:
        try:
            grp.getgrnam(group)
        except KeyError:
            raise LinuxGroupDoesNotExistError(group)

    cmd = ["useradd"]
    if uid:
        cmd += ["-u", uid]
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
    alreadyExists = True
    try:
        grp.getgrnam(groupname)
    except KeyError:
        alreadyExists = False
    if alreadyExists:
        raise LinuxGroupAlreadyExistsError(groupname)

    cmd = ["groupadd"]
    if gid:
        cmd += ["-g", gid]
    if system:
        cmd += ["-r"]
    cmd += [groupname]
    return subprocess.check_call(cmd)
