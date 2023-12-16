import json
import os
import pymongo
import pandas as pd
import math
from MakeMongoDocument import *
from pathUtil import getPathName, getClusterSetPathName, getPlanPathName

DB_SOURCE = "./AZ"
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
            ensembles_collection.insert_one(ensemble)
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
            clusterDistances = []
            for i in range(len(df)):
                for j in range(i + 1, len(df)):
                    if i == j:
                        continue
                    xa = df.iloc[i, 1]
                    xb = df.iloc[j, 1]
                    ya = df.iloc[i, 2]
                    yb = df.iloc[j, 2]
                    dist = math.sqrt((xa - xb) ** 2 + (ya - yb) ** 2)
                    clusterDistances.append(dist)

            set = make_clusterSet(clusters, name, clusterDistances, path)
            sets_collection = mydb["ClusterSets"]
            sets_collection.insert_one(set)
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
            clusters_collection.insert_one(cluster)
            print("Cluster inserted: ", name)

        elif name in ["AZ", "LA", "NV"]:  # states
            ensembles = []
            ensembles_collection = mydb["Ensembles"]
            for file in file_list:
                ensembleId = ":".join(getPathName(path + "/" + file))

                ensemble = ensembles_collection.find_one({"_id": ensembleId})
                ensembles.append(ensemble)
            state = make_state(ensembles, name)
            states_collection = mydb["States"]
            states_collection.insert_one(state)
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
            plans_collection.insert_one(plan)
            print("Plan inserted: ", name)

make_summary(
    None,
    DB_SOURCE,
)
