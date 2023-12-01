import json
import os

import pymongo

from MakeModels import *

myClient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myClient["Titans"]


def make_summary(root, path):
    name = os.path.splitext(os.path.basename(path))[0]
    list = []

    if os.path.isdir(path):
        file_list = os.listdir(path)
        for file in file_list:
            make_summary(name, os.path.join(path, file))
            list.append(file)
            
        if "Ensemble" in name:
            sets = []
            sets_collection = mydb["ClusterSets"]
            size = 0
            for file in file_list:
                set = sets_collection.find_one({"_id": file})
                sets.append(set)
            
            clusters = sets[0]["clusters"] #DBref{"$id": id, "$ref": Clusters}
            clusters_collection = mydb["Clusters"]
            cluster_ids = []
            for cluster_ref in clusters:
                cluster = clusters_collection.find_one({"_id":cluster_ref.id})
                size += len(cluster["plans"])
            
            ensemble = make_ensemble(size, sets, name, root)
            ensembles_collection = mydb["Ensembles"]
            ensembles_collection.insert_one(ensemble)

        elif "Set" in name:
            clusters = []
            clusters_collection = mydb["Clusters"]
            for file in file_list:
                cluster = clusters_collection.find_one({"_id": file})
                clusters.append(cluster)
            set = make_clusterSet(clusters, name, root)
            sets_collection = mydb["ClusterSets"]
            sets_collection.insert_one(set)

        elif "Cluster" in name:
            plans = []
            plans_collection = mydb["DistrictPlans"]
            for file in file_list:
                file = str(file).split(".")[0]
                plan = plans_collection.find_one({"_id": file})
                plans.append(plan)
            cluster = make_cluster(plans, name, root)
            clusters_collection = mydb["Clusters"]
            clusters_collection.insert_one(cluster)

        else:
            # TODO
            ensembles = []
            ensembles_collection = mydb["Ensembles"]
            for file in file_list:
                ensemble = ensembles_collection.find_one({"_id": file})
                ensembles.append(ensemble)
            state = make_state(ensembles, name)
            states_collection = mydb["States"]
            states_collection.insert_one(state)

    # If it is a json file, make it into an object and save it inside DB
    else:
        plans_collection = mydb["DistrictPlans"]
        existing_plan = plans_collection.find_one({"_id": name})
        if not existing_plan:
            plan = make_plan(path, name, root)
            plans_collection.insert_one(plan)
    

make_summary(None, "C:\\Users/ufg11\\Desktop\\ca-server\\src\\main\\java\\com\\cse416\\titans\\reseources\\AZ")