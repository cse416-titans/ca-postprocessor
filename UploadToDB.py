import json
import os
import pymongo
import pandas as pd
import math
from MakeMongoDocument import *
from pathUtil import getPathName, getClusterSetPathName, getPlanPathName

DB_SOURCE = "./"
DB_ADDR = "mongodb://localhost:27017/"
DB_NAME = "Titans"

myClient = pymongo.MongoClient(DB_ADDR)
mydb = myClient[DB_NAME]

NOT_TO_ADD = [
    "district_list",
    "districts",
    "districts_reassigned",
    "plots",
    "plots_reassigned",
    "summary",
    "units",
]


plans_collection = mydb["DistrictPlans"]

for planPath in ["az_curr.json", "la_curr.json", "nv_curr.json"]:
    print("planPath: ", planPath)

    availability = True

    # open plan geojson file in path as pandas dataframe
    with open(planPath, "r") as file:
        json_data = json.load(file)
        features = json_data["features"]

        totalPopulation = 0

        republicanVotes = []
        democraticVotes = []
        republicanSplit = []
        democraticSplit = []

        aAOpps = []
        whiteOpps = []
        asianOpps = []
        hispanicOpps = []
        majMinDistricts = []
        competitiveDistricts = []

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

            districtTotalPopulation = properties["Total_Population"]
            whitePopulationRatio = properties["White"] / districtTotalPopulation
            aaPopulationRatio = properties["Black"] / districtTotalPopulation
            asianPopulationRatio = properties["Asian"] / districtTotalPopulation
            hispanicPopulationRatio = properties["Hispanic"] / districtTotalPopulation
            indianPopulationRatio = (
                properties["American_Indian"] / districtTotalPopulation
            )

            totalPopulation += districtTotalPopulation

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
            if whitePopulationRatio < OPP_THRESHOLD:
                majMinDistricts.append(districtIdx)
            if aaPopulationRatio > OPP_THRESHOLD:
                aAOpps.append(districtIdx)
            if asianPopulationRatio > OPP_THRESHOLD:
                asianOpps.append(districtIdx)
            if hispanicPopulationRatio > OPP_THRESHOLD:
                hispanicOpps.append(districtIdx)
            if (
                abs(
                    whitePopulationRatio
                    - (
                        aaPopulationRatio
                        + hispanicPopulationRatio
                        + asianPopulationRatio
                        + indianPopulationRatio
                    )
                )
                < 0.1
            ):
                competitiveDistricts.append(districtIdx)

        numOfAAOpp = len(aAOpps)
        numOfWhiteOpp = len(whiteOpps)
        numOfAsianOpp = len(asianOpps)
        numOfHispanicOpp = len(hispanicOpps)
        numOfMajMinDistricts = len(majMinDistricts)
        numOfCompetitiveDistricts = len(competitiveDistricts)

        avgWhitePercentage = sum(whitePercentages) / len(whitePercentages)
        avgAAPercentage = sum(aAPercentages) / len(aAPercentages)
        avgAsianPercentage = sum(asianPercentages) / len(asianPercentages)
        avgHispanicPercentage = sum(hispanicPercentages) / len(hispanicPercentages)
        avgIndianPercentage = sum(indianPercentages) / len(indianPercentages)

        planId = planPath
        name = planPath

        print(numOfCompetitiveDistricts)
        print(numOfMajMinDistricts)

        plan = {
            "_id": planId,
            "name": name,
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
            "numOfMajMinDistricts": numOfMajMinDistricts,  # number of districts that have white population ratio < 0.5
            "numOfCompetitiveDistricts": numOfCompetitiveDistricts,  # number of districts that have white population ratio < 0.5
            "majMinDistricts": majMinDistricts,  # districts that have white population ratio < 0.5
            "competitiveDistricts": competitiveDistricts,  # districts that have white population ratio < 0.5
            "geoJson": json_data if availability == True else None,  # geojson file
        }

        plans_collection.insert_one(plan)


def make_summary(root, path):
    name = os.path.splitext(os.path.basename(path))[0]
    list = []

    if name in NOT_TO_ADD:
        return

    if os.path.isdir(path):
        file_list = os.listdir(path)
        for file in file_list:
            make_summary(name, os.path.join(path, file))
            list.append(file)

        if "ensemble" in name:
            clusterSets = []
            sets_collection = mydb["ClusterSets"]
            for file in file_list:
                if file in NOT_TO_ADD:
                    continue
                clusterSetId = ":".join(getPathName(path + "/" + file))

                clusterSet = sets_collection.find_one({"_id": clusterSetId})
                clusterSets.append(clusterSet)

            ensemble = make_ensemble(clusterSets, name, path)
            ensembles_collection = mydb["Ensembles"]
            try:
                ensembles_collection.insert_one(ensemble)
            except Exception as e:
                print(e)
            print("Ensemble inserted: ", name)

        elif "clusterSet" in name:
            clusters = []
            clusters_collection = mydb["Clusters"]
            for file in file_list:
                if not os.path.isdir(path + "/" + file):  # ignore image files
                    continue
                clusterId = ":".join(getPathName(path + "/" + file))

                cluster = clusters_collection.find_one({"_id": clusterId})
                clusters.append(cluster)

            # read cluster-centers-summary.csv into dataframe and retireve all pairwise distances between clusters
            df = pd.read_csv(
                f"{getClusterSetPathName(path)}/cluster-centers-summary.csv",
            )
            df_plan = pd.read_csv(
                f"{getClusterSetPathName(path)}/distance-matrix-summary.csv",
            )

            clusterDistances = []
            for i in range(len(clusters)):
                avgPlanIdx1 = int(
                    clusters_collection.find_one({"_id": clusters[i]["_id"]})[
                        "avgPlanId"
                    ].split("-")[-1]
                )
                for j in range(i + 1, len(clusters)):
                    if i >= j:
                        continue
                    avgPlanIdx2 = int(
                        clusters_collection.find_one({"_id": clusters[j]["_id"]})[
                            "avgPlanId"
                        ].split("-")[-1]
                    )
                    # print("avgPlanIdx1: ", avgPlanIdx1)
                    # print("avgPlanIdx2: ", avgPlanIdx2)
                    dist = float(df_plan.iloc[avgPlanIdx1 - 1, avgPlanIdx2 - 1])
                    # print(type(dist))
                    clusterDistances.append(dist)

            set = make_clusterSet(clusters, name, clusterDistances, path)
            sets_collection = mydb["ClusterSets"]
            try:
                sets_collection.insert_one(set)
            except Exception as e:
                print(e)
            print("ClusterSet inserted: ", name)

        elif "cluster" in name:
            planIdxs = []
            plans = []
            plans_collection = mydb["DistrictPlans"]
            for file in file_list:
                planId = ":".join(getPlanPathName(path + "/" + file))
                planIdxs.append(int(planId.split(":")[-1].split("-")[-1]) - 1)
                plan = plans_collection.find_one({"_id": planId})
                plans.append(plan)

            clusterIdx = int(name.split("-")[-1])
            clusterSetPathName = getClusterSetPathName(path)
            clusterSetIdx = int(clusterSetPathName.split("-")[-1])

            # find xy coordinate of the cluster in plan-mds-coordinate.csv for optimal transport (do the same for other distance measures)
            df = pd.read_csv(
                f"{getClusterSetPathName(path)}/cluster-centers-summary.csv",
            )
            x = df.iloc[clusterIdx - 1, 1]
            y = df.iloc[clusterIdx - 1, 2]
            coordinate = (x, y)

            # read plan-mds-coordinates-summary.csv into dataframe and retireve all pairwise distances between plans in the cluster
            df = pd.read_csv(
                f"{getClusterSetPathName(path)}/distance-matrix-summary.csv",
            )

            planDistances = []
            # print("planIdxs: ", planIdxs)
            for i in range(len(df)):
                if not i in planIdxs:
                    continue
                for j in range(i + 2, len(df)):
                    if not j in planIdxs:
                        continue
                    if i == j:
                        continue
                    planDistances.append(df.iloc[i, j])

            cluster = make_cluster(plans, name, coordinate, planDistances, path)
            clusters_collection = mydb["Clusters"]
            try:
                clusters_collection.insert_one(cluster)
            except Exception as e:
                print(e)
            print("Cluster inserted: ", name)

        elif name in ["LA", "NV"]:  # states
            ensembles = []
            ensembles_collection = mydb["Ensembles"]
            for file in file_list:
                ensembleId = ":".join(getPathName(path + "/" + file))

                ensemble = ensembles_collection.find_one({"_id": ensembleId})
                ensembles.append(ensemble)
            state = make_state(ensembles, name)
            states_collection = mydb["States"]
            try:
                states_collection.insert_one(state)
            except Exception as e:
                print(e)
            print("State inserted: ", name)
        else:
            return

    # If it is a json file, make it into an object and save it inside DB
    else:
        fileName = os.path.splitext(path)
        if not os.path.isfile(path):
            return
        if fileName[1] != ".json":
            return
        if fileName[0].replace("\\", "/").split("/")[-1].split("-")[-1] == "summary":
            return

        plans_collection = mydb["DistrictPlans"]
        planId = ":".join(getPlanPathName(path))
        existing_plan = plans_collection.find_one({"_id": planId})

        if not existing_plan:
            planIdx = int(name.split("-")[-1]) - 1

            # find xy coordinate of the plan in plan-mds-coordinate.csv for optimal transport (do the same for other distance measures)
            df = pd.read_csv(
                f"{getClusterSetPathName(path)}/plan-mds-coordinates-summary.csv",
            )
            x = df.iloc[planIdx, 1]
            y = df.iloc[planIdx, 2]
            coordinate = (x, y)

            plan = make_plan(name, coordinate, path)
            try:
                plans_collection.insert_one(plan)
            except Exception as e:
                print(e)
            print("Plan inserted: ", name)


"""
make_summary(
    None,
    DB_SOURCE,
)
"""
