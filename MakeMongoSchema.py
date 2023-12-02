import json

import pymongo
import os

from pathUtil import getPathName

DB_ADDR = "mongodb://localhost:27017/"
DB_NAME = "Titans"

myClient = pymongo.MongoClient(DB_ADDR)
mydb = myClient[DB_NAME]


def make_plan(name, path):
    planPathName = getPathName(path)
    clusterPathName = planPathName[:-1]

    planId = ":".join(planPathName)
    clusterId = ":".join(clusterPathName)

    # TODO opportunity districts compute
    # Voting Splits remanipulation?
    with open(path, "r") as file:
        json_data = json.load(file)
        features = json_data["features"]
        republic = 0
        democrat = 0
        for feature in features:
            properties = feature["properties"]
            dem = properties["Democratic"]
            rep = properties["Republic"]
            if dem > rep:
                democrat += 1
            else:
                republic += 1
        plan = {
            "_id": planId,
            "name": name,
            "clusterId": clusterId,
            "democrat": democrat,
            "republic": republic,
            "numOfAAOpp": 0,
            "numOfWhiteOpp": 0,
            "numOfAsianOpp": 0,
            "numOfHispanicOpp": 0,
            "availability": True,
            "geoJson": json_data,
        }
        return plan


def make_cluster(plans, name, path):
    clusterPathName = getPathName(path)
    clusterSetPathName = clusterPathName[:-1]

    clusterId = ":".join(clusterPathName)
    clusterSetId = ":".join(clusterSetPathName)

    # TODO distBtwPlans, avgClusterBoundary Copmutation
    refs = []
    total_dem = 0
    total_rep = 0
    total_aa_opp = 0
    total_asian_opp = 0
    total_hispanic_opp = 0
    total_white_opp = 0

    for plan in plans:
        """
        total_dem += plan["Democratic"]
        total_rep += plan["Republic"]
        total_aa_opp = plan["numOfAAOpp"]
        total_asian_opp = plan["numOfAsianOpp"]
        total_white_opp = plan["numOfWhiteOpp"]
        total_hispanic_opp = plan["numOfHispanicOpp"]
        """
        ref = {"$ref": "DistrictPlans", "$id": plan["_id"]}
        refs.append(ref)

    cluster = {
        "_id": clusterId,  # root_
        "name": name,
        "clusterSetId": clusterSetId,
        "numOfPlans": len(plans),
        "avgClusterBoundary": None,
        # "distBtwPlans": None,
        "avgDemocrat": 0,
        "avgRepublic": 0,
        "avgNumOfAAOpp": 0,
        "avgNumOfWhiteOpp": 0,
        "avgNumOfAsianOpp": 0,
        "avgNumOfHispanicOpp": 0,
        "plans": refs,
    }
    return cluster


def make_clusterSet(clusters, name, path):
    # TODO distanceMeasureId, clusterSeperationInde, and clusterQualituIndex Computation

    clusterSetPathName = getPathName(path)
    ensemblePathName = clusterSetPathName[:-1]

    clusterSetId = ":".join(clusterSetPathName)
    ensembleId = ":".join(ensemblePathName)

    numOfPlans = 0

    refs = []

    for cluster in clusters:
        ref = {"$ref": "Clusters", "$id": cluster["_id"]}
        refs.append(ref)
        numOfPlans += cluster["numOfPlans"]

    set = {
        "_id": clusterSetId,
        "name": name,
        "numOfClusters": len(clusters),
        "numOfPlans": numOfPlans,
        "ensembleId": ensembleId,
        "distanceMeasureId": None,
        "clusterSeperationIndex": None,
        "clusterQualityIndex": None,
        "copmuteTime": None,
        "clusters": refs,
    }
    return set


def make_ensemble(size, sets, name, path):
    ensemblePathName = getPathName(path)
    statePathName = ensemblePathName[:-1]

    ensembleId = ":".join(ensemblePathName)
    stateId = ":".join(statePathName)

    refs = []

    for set in sets:
        ref = {"$ref": "ClusterSets", "$id": set["_id"]}
        refs.append(ref)
    ensemble = {
        "_id": ensembleId,
        "name": name,
        "stateId": stateId,
        "size": size,
        "clusterSets": refs,
    }
    return ensemble


def make_state(ensembles, name):
    refs = []
    if name == "AZ":
        center = {"Lat": 34.048927, "Lng": -111.093735}
        statePlan = {"$ref": "DistrictPlans", "$id": "curr_AZ"}
    elif name == "LA":
        center = {"Lat": 30.391830, "Lng": -92.329102}
        statePlan = {"$ref": "DistrictPlans", "$id": "curr_LA"}
    else:
        center = {"Lat": 39.876019, "Lng": -117.224121}
        statePlan = {"$ref": "DistrictPlans", "$id": "curr_NV"}

    for ensemble in ensembles:
        ref = {"$ref": "Ensembles", "$id": ensemble["_id"]}
        refs.append(ref)
    state = {
        "_id": name,
        "name": name,
        "center": center,
        "statePlan": statePlan,
        "ensembles": refs,
    }
    return state


if __name__ == "__main__":
    print("hello")
