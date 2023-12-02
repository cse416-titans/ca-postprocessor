import json

import pymongo
import os

import random
import pandas as pd

from pathUtil import getPathName

DB_ADDR = "mongodb://localhost:27017/"
DB_NAME = "Titans"

myClient = pymongo.MongoClient(DB_ADDR)
mydb = myClient[DB_NAME]

OPP_THRESHOLD = 0.5


def make_plan(name, coordinate, path):
    planPathName = getPathName(path)
    clusterPathName = planPathName[:-1]

    planId = ":".join(planPathName)
    clusterId = ":".join(clusterPathName)

    # by 50% change set availability to false
    if random.randint(0, 1) == 0:
        availability = False
    else:
        availability = True

    # open plan geojson file in path as pandas dataframe
    with open(path, "r") as file:
        json_data = json.load(file)
        features = json_data["features"]

        republicanVotes = []
        democraticVotes = []
        republicanSplit = []
        democraticSplit = []

        aAOpps = []
        whiteOpps = []
        asianOpps = []
        hispanicOpps = []

        whitePercentages = []
        aAPercentages = []
        asianPercentages = []
        hispanicPercentages = []
        indianPercentages = []

        districtIdx = 0
        for feature in features:
            districtIdx += 1
            properties = feature["properties"]

            dem = properties["Democratic"]
            rep = properties["Republic"]

            democraticVotes.append(dem)
            republicanVotes.append(rep)

            totalPopulation = properties["Total_Population"]
            whitePopulationRatio = properties["White"] / totalPopulation
            aaPopulationRatio = properties["Black"] / totalPopulation
            asianPopulationRatio = properties["Asian"] / totalPopulation
            hispanicPopulationRatio = properties["Hispanic"] / totalPopulation
            indianPopulationRatio = properties["American_Indian"] / totalPopulation

            whitePercentages.append(whitePopulationRatio)
            aAPercentages.append(aaPopulationRatio)
            asianPercentages.append(asianPopulationRatio)
            hispanicPercentages.append(hispanicPopulationRatio)
            indianPercentages.append(indianPopulationRatio)

            if dem > rep:
                democraticSplit.append(districtIdx)
            else:
                republicanSplit.append(districtIdx)
            if whitePopulationRatio > OPP_THRESHOLD:
                whiteOpps.append(districtIdx)
            if aaPopulationRatio > OPP_THRESHOLD:
                aAOpps.append(districtIdx)
            if asianPopulationRatio > OPP_THRESHOLD:
                asianOpps.append(districtIdx)
            if hispanicPopulationRatio > OPP_THRESHOLD:
                hispanicOpps.append(districtIdx)

        numOfAAOpp = len(aAOpps)
        numOfWhiteOpp = len(whiteOpps)
        numOfAsianOpp = len(asianOpps)
        numOfHispanicOpp = len(hispanicOpps)

        avgWhitePercentage = sum(whitePercentages) / len(whitePercentages)
        avgAAPercentage = sum(aAPercentages) / len(aAPercentages)
        avgAsianPercentage = sum(asianPercentages) / len(asianPercentages)
        avgHispanicPercentage = sum(hispanicPercentages) / len(hispanicPercentages)
        avgIndianPercentage = sum(indianPercentages) / len(indianPercentages)

        plan = {
            "_id": planId,
            "name": name,
            "clusterId": clusterId,
            "coordinates": coordinate,  # in MDS plot using i-th distance measure (in clusterSet i)
            "totalPopulation": totalPopulation,  # total population across state
            "totalDemocraticVotes": sum(
                democraticVotes
            ),  # total democratic votes across state
            "totalRepublicanVotes": sum(
                republicanVotes
            ),  # total republican votes across state
            "democraticVotes": democraticVotes,  # democratic votes in each district
            "republicanVotes": republicanVotes,  # republican votes in each district
            "democraticSplit": democraticSplit,  # districts that voted for democratic party
            "republicanSplit": republicanSplit,  # districts that voted for republican party
            "numOfAAOpp": numOfAAOpp,  # number of districts that have AA population ratio > 0.5
            "numOfWhiteOpp": numOfWhiteOpp,  # number of districts that have white population ratio > 0.5
            "numOfAsianOpp": numOfAsianOpp,  # number of districts that have asian population ratio > 0.5
            "numOfHispanicOpp": numOfHispanicOpp,  # number of districts that have hispanic population ratio > 0.5
            "aAOpps": aAOpps,  # districts that have AA population ratio > 0.5
            "whiteOpps": whiteOpps,  # districts that have white population ratio > 0.5
            "asianOpps": asianOpps,  # districts that have asian population ratio > 0.5
            "hispanicOpps": hispanicOpps,  # districts that have hispanic population ratio > 0.5
            "whitePercentages": whitePercentages,  # white population ratio in each district
            "aAPercentages": aAPercentages,  # AA population ratio in each district
            "asianPercentages": asianPercentages,  # asian population ratio in each district
            "hispanicPercentages": hispanicPercentages,  # hispanic population ratio in each district
            "indianPercentages": indianPercentages,  # indian population ratio in each district
            "avgWhitePercentage": avgWhitePercentage,  # average white population ratio across districts
            "avgAAPercentage": avgAAPercentage,  # average AA population ratio across districts
            "avgAsianPercentage": avgAsianPercentage,  # average asian population ratio across districts
            "avgHispanicPercentage": avgHispanicPercentage,  # average hispanic population ratio across districts
            "avgIndianPercentage": avgIndianPercentage,  # average indian population ratio across districts
            "availability": availability,  # if plan is available for download
            "geoJson": json_data if availability == True else None,  # geojson file
        }

        return plan


def make_cluster(plans, name, coordinate, planDistances, path):
    clusterPathName = getPathName(path)
    clusterSetPathName = clusterPathName[:-1]

    clusterId = ":".join(clusterPathName)
    clusterSetId = ":".join(clusterSetPathName)

    if len(planDistances) < 2:
        avgPlanDistance = 0
    else:
        avgPlanDistance = sum(planDistances) / len(planDistances)

    refs = []

    # sort plans by ethic group ratio, and then voting ratio
    plans = sorted(
        plans,
        key=lambda x: (
            x["numOfWhiteOpp"],
            x["numOfAAOpp"],
            x["numOfAsianOpp"],
            x["numOfHispanicOpp"],
            x["democraticSplit"],
        ),
    )

    # filter only ones that available flag is true
    filteredPlans = list(filter(lambda x: x["availability"] == True, plans))

    # pick middle plan as representative plan
    if len(filteredPlans) == 0:
        avgPlan = plans[len(plans) // 2]
    else:
        avgPlan = filteredPlans[len(filteredPlans) // 2]

    avgPlanRef = {"$ref": "DistrictPlans", "$id": avgPlan["_id"]}
    democraticSplits = []
    republicanSplits = []
    whitePercentages = []
    aAPercentages = []
    asianPercentages = []
    hispanicPercentages = []
    indianPercentages = []

    for plan in plans:
        ref = {"$ref": "DistrictPlans", "$id": plan["_id"]}
        refs.append(ref)
        democraticSplits.append(plan["democraticSplit"])
        republicanSplits.append(plan["republicanSplit"])
        whitePercentages.append(plan["whitePercentages"])
        aAPercentages.append(plan["aAPercentages"])
        asianPercentages.append(plan["asianPercentages"])
        hispanicPercentages.append(plan["hispanicPercentages"])
        indianPercentages.append(plan["indianPercentages"])

    cluster = {
        "_id": clusterId,
        "name": name,
        "clusterSetId": clusterSetId,
        "coordinate": coordinate,  # coordinate in MDS plot using i-th distance measure (in clusterSet i)
        "numOfPlans": len(plans),  # number of plans in the cluster
        "planDistances": planDistances,  # distances between each pair of plans in the cluster
        "avgPlanDistance": avgPlanDistance,  # average pairwise distance of plans in the cluster
        "avgClusterBoundary": None,  # average cluster boundary
        "avgPlan": avgPlanRef,  # average plan in the cluster
        "whitePercentages": whitePercentages,  # white population ratio in each district of each plan (2-D array)
        "aAPercentages": aAPercentages,  # AA population ratio in each district of each plan (2-D array)
        "asianPercentages": asianPercentages,  # asian population ratio in each district of each plan (2-D array)
        "hispanicPercentages": hispanicPercentages,  # hispanic population ratio in each district of each plan (2-D array)
        "indianPercentages": indianPercentages,  # indian population ratio in each district of each plan (2-D array)
        "plans": refs,
    }

    return cluster


def make_clusterSet(clusters, name, clusterDistances, path):
    # TODO distanceMeasureId, clusterSeperationInde, and clusterQualituIndex Computation

    clusterSetPathName = getPathName(path)
    ensemblePathName = clusterSetPathName[:-1]

    clusterSetId = ":".join(clusterSetPathName)
    ensembleId = ":".join(ensemblePathName)

    numOfPlans = 0
    clusterSizes = []
    planDistances = []
    refs = []

    if len(clusterDistances) < 2:
        avgClusterDistance = 0
    else:
        avgClusterDistance = sum(clusterDistances) / len(clusterDistances)

    for cluster in clusters:
        ref = {"$ref": "Clusters", "$id": cluster["_id"]}
        refs.append(ref)
        numOfPlans += cluster["numOfPlans"]
        clusterSizes.append(cluster["numOfPlans"])
        planDistances += cluster["planDistances"]

    if len(planDistances) < 2:
        avgPlanDistance = 0
    else:
        avgPlanDistance = sum(planDistances) / len(planDistances)

    clusterSet = {
        "_id": clusterSetId,
        "name": name,
        "ensembleId": ensembleId,
        "numOfClusters": len(clusters),  # number of clusters in the cluster set
        "numOfPlans": numOfPlans,  # number of plans in the cluster set
        "clusterDistances": clusterDistances,  # distances between each pair of clusters in the cluster set
        "avgClusterDistance": avgClusterDistance,  # average pairwise distance of clusters in the cluster set
        "planDistances": planDistances,  # distances between each pair of plans in the cluster set
        "avgPlanDistance": avgPlanDistance,  # average pairwise distance of plans in the cluster set
        "clusterSizes": clusterSizes,  # number of plans in each cluster in the cluster set
        "avgClusterSize": sum(clusterSizes)
        / len(clusterSizes),  # average cluster size in the cluster set
        "copmuteTime": random.randint(0, 1000),  # TODO compute time
        "clusters": refs,
    }

    return clusterSet


def make_ensemble(clusterSets, name, path):
    ensemblePathName = getPathName(path)
    statePathName = ensemblePathName[:-1]

    ensembleId = ":".join(ensemblePathName)
    stateId = ":".join(statePathName)

    numOfPlans = 0
    refs = []

    for clusterSet in clusterSets:
        ref = {"$ref": "ClusterSets", "$id": clusterSet["_id"]}
        refs.append(ref)
        numOfPlans += clusterSet["numOfPlans"]

    ensemble = {
        "_id": ensembleId,
        "name": name,
        "stateId": stateId,
        "numOfPlans": numOfPlans,  # number of plans in the ensemble
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
        "statePlan": statePlan,  # enacted district plan
        "ensembles": refs,
    }

    return state


if __name__ == "__main__":
    print("hello")