import json

import pymongo

DB_ADDR = "mongodb://localhost:27017/"
DB_NAME = "Titans"

myClient = pymongo.MongoClient(DB_ADDR)
mydb = myClient[DB_NAME]


def make_plan(path, name, root):
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
            "_id": name,
            "name": name,
            "clusterId": root,
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


def make_cluster(plans, name, root):
    # TODO distBtwPlans, avgClusterBoundary Copmutation
    refs = []
    total_dem = 0
    total_rep = 0
    total_aa_opp = 0
    total_asian_opp = 0
    total_hispanic_opp = 0
    total_white_opp = 0

    for plan in plans:
        total_dem += plan["democrat"]
        total_rep += plan["republic"]
        total_aa_opp = plan["numOfAAOpp"]
        total_asian_opp = plan["numOfAsianOpp"]
        total_white_opp = plan["numOfWhiteOpp"]
        total_hispanic_opp = plan["numOfHispanicOpp"]
        ref = {"$ref": "DistrictPlans", "$id": plan["_id"]}
        refs.append(ref)
    cluster = {
        "_id": name,
        "name": name,
        "clusterSetId": root,
        "numOfPlans": len(plans),
        "avgClusterBoundary": None,
        # "distBtwPlans": None,
        "avgDemocrat": total_dem / len(plans),
        "avgRepublic": total_rep / len(plans),
        "avgNumOfAAOpp": total_aa_opp / len(plans),
        "avgNumOfWhiteOpp": total_white_opp / len(plans),
        "avgNumOfAsianOpp": total_asian_opp / len(plans),
        "avgNumOfHispanicOpp": total_hispanic_opp / len(plans),
        "plans": refs,
    }
    return cluster


def make_clusterSet(clusters, name, root):
    # TODO distanceMeasureId, clusterSeperationInde, and clusterQualituIndex Computation
    refs = []

    for cluster in clusters:
        ref = {"$ref": "Clusters", "$id": cluster["_id"]}
        refs.append(ref)
    set = {
        "_id": name,
        "name": name,
        "ensembleId": root,
        "distanceMeasureId": None,
        "clusterSeperationIndex": None,
        "clusterQualityIndex": None,
        "copmuteTime": None,
        "clusters": refs,
    }
    return set


def make_ensemble(size, sets, name, root):
    refs = []

    for set in sets:
        ref = {"$ref": "ClusterSets", "$id": set["_id"]}
        refs.append(ref)
    ensemble = {
        "_id": name,
        "name": name,
        "stateId": root,
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
