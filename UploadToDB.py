import json
import os

import pymongo

from MakeMongoDocument import *

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
            sets = []
            sets_collection = mydb["ClusterSets"]
            numOfPlans = 0
            for file in file_list:
                if file in NOT_TO_ADD:
                    continue
                clusterSetId = ":".join(getPathName(path + "/" + file))

                set = sets_collection.find_one({"_id": clusterSetId})
                sets.append(set)
                numOfPlans += set["numOfPlans"]

            ensemble = make_ensemble(numOfPlans, sets, name, path)
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
            set = make_clusterSet(clusters, name, path)
            sets_collection = mydb["ClusterSets"]
            sets_collection.insert_one(set)
            print("ClusterSet inserted: ", name)

        elif "cluster" in name:
            plans = []
            plans_collection = mydb["DistrictPlans"]
            for file in file_list:
                planId = ":".join(getPathName(path + "/" + file))

                plan = plans_collection.find_one({"_id": planId})
                plans.append(plan)
            cluster = make_cluster(plans, name, path)
            clusters_collection = mydb["Clusters"]
            clusters_collection.insert_one(cluster)
            print("Cluster inserted: ", name)

        elif name in ["AZ", "LA", "NV"]:
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
        planId = ":".join(getPathName(path))
        existing_plan = plans_collection.find_one({"_id": planId})

        if not existing_plan:
            plan = make_plan(name, path)
            plans_collection.insert_one(plan)
            print("Plan inserted: ", name)


make_summary(
    None,
    DB_SOURCE,
)
