import os
import pathlib


def getUserAndGroupOfFolder(folderpath):
    f = pathlib.Path(folderpath)
    return f.owner(), f.group()


def getCurrentUserAndGroup():
    folder = os.getenv("HOME")
    return getUserAndGroupOfFolder(folder)
