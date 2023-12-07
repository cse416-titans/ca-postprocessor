import os

def getPathName(path):
    pathName = path.replace("\\", "/").split("/")[1:]
    # remove .json in last element
    pathName[-1] = pathName[-1].split(".")[0]
    return pathName

def getClusterSetPathName(path):
    pathName = "/".join(path.replace("\\", "/").split("/")[1:4])
    return pathName

def getPlanPathName(path):
    pathArr = path.replace("\\", "/").split("/")[1:]
    pathName = pathArr[0:2]
    pathName.append(pathArr[-1])
    pathName[-1] = pathName[-1].split(".")[0]
    return pathName
