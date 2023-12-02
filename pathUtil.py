import os


def getPathName(path):
    pathName = path.replace("\\", "/").split("/")[1:]
    # remove .json in last element
    pathName[-1] = pathName[-1].split(".")[0]
    return pathName
