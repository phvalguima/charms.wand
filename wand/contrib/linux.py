import os
import pathlib
import subprocess
import pwd
import grp
from python_hosts import Hosts, HostsEntry

from charmhelpers.contrib.network import ip

__all__ = [
    "LinuxError",
    "LinuxUserDoesNotExistError",
    "LinuxGroupDoesNotExistError",
    "LinuxGroupAlreadyExistsError",
    "LinuxUserAlreadyExistsError",
    "getUserAndGroupOfFolder",
    "getCurrentUserAndGroup",
    "userAdd",
    "groupAdd",
    "fixMaybeLocalhost",
    "get_hostname"
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


# The issue: generally deployed hosts come with:
# 127.0.0.1 <server-name>
# That forces inter-cluster to open only to localhost interface
# if server.X=<server-name>
# To resolve that, any entries for hostnames parameter will be removed
# and readded with the correct IP address.
def fixMaybeLocalhost(hosts_path="/etc/hosts",
                      hostname=None,
                      IP=None):
    hosts = Hosts(path=hosts_path)
    removed_hosts = []
    # Consider cases where it is added both node.maas and node
    for h in [hostname.split(".")[0], hostname]:
        r = hosts.remove_all_matching(name=h)
        if r:
            removed_hosts += [str(el) for el in r]
    hosts.add([HostsEntry(entry_type='ipv4',
                          address=IP, names=[hostname])])
    # Check if localhost exists, if not, set it to 127.0.0.1
    if len(hosts.find_all_matching(name="localhost")) == 0:
        # Set localhost
        hosts.add([HostsEntry(entry_type='ipv4',
                              address='127.0.0.1', names=["localhost"])])
    hosts.write()
    return removed_hosts


def get_hostname(ipaddr):
    if not ipaddr:
        return
    h = ip.get_hostname(ipaddr)
    if h:
        fixMaybeLocalhost(hostname=h, IP=ipaddr)
    return h
