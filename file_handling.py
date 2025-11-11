import csv
import json

def reorganize_json(filename):
    data = None
    with open(filename) as file:
        data = json.load(file)
    # TODO: reorganize JSON files into list of dicts with id field
    reorgd = []
    id = 0
    for ent in data:
        reorgd.append({"id":id, "name":ent}|data[ent])
        id+=1
    with open(filename, "w") as file:
        json.dump(reorgd, file)

reorganize_json("teams.json")