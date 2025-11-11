import csv
import json

def reorganize_json(filename):
    data = None
    reorgd = []
    id = 0
    for ent in data:
        reorgd.append({"id":id, "name":ent}|data[ent])
        id+=1
    with open(filename, "w") as file:
        json.dump(reorgd, file)

def json_to_csv(filename):
    data = None
    with open(filename) as file:
        data = json.load(file)
    fieldnames = data[0].keys()
    with open(filename[:filename.find(".")]+".csv", "w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

def get_id_from_name(cache: dict, name: str, src_data:list, i: int, rel_field, rel_data:list, foreign_data):
    cached = cache.get(name)
    if cached:
        rel_data.append((src_data[i]["id"],cached))
    else:
        for j in range(len(foreign_data)):
            if foreign_data[j]["abbreviation"]==name:
                cache[name]=foreign_data[j]["id"]
                rel_data.append((src_data[i]["id"],foreign_data[j]["id"]))

def replace_with_ids(src_file, foreign_file, rel_field, rel_singular: bool = False):
    src_data = None
    with open(src_file) as file:
        src_data = json.load(file)
    foreign_data = None
    with open(foreign_file) as file:
        foreign_data = json.load(file)
    cache = {}
    rel_data = []
    for i in range(len(src_data)):
        if not rel_singular:
            for name in src_data[i][rel_field]:
                get_id_from_name(cache, name, src_data, i, rel_field, rel_data, foreign_data)
        else:
            get_id_from_name(cache, src_data[i][rel_field], src_data, i, rel_field, rel_data, foreign_data)
        del src_data[i][rel_field]
    with open(src_file, "w") as file:
        json.dump(src_data, file)
    with open(f"{rel_field}{"_team" if rel_field=="coaches" else ""}.json", "w") as file:
        json.dump(rel_data, file)

# standardize height
data = None
with open("players.json") as file:
    data = json.load(file)
for i in range(len(data)):
    height = data[i]["height"]
    feet, inches = 0, 0
    if "-" in height:
        feet = int(height[:height.find("-")])
        inches = int(height[height.find("-")+1:])
    else:
        foot_mark = height.find("'")
        feet = int(height[:foot_mark])
        inch_end_ind = len(height)-1
        while not height[inch_end_ind].isnumeric():
            inch_end_ind-=1
        inch_end_ind+=1
        inch_start = foot_mark+1 if height[foot_mark+1]!=" " else foot_mark+2
        inches = int(height[inch_start:inch_end_ind])
    data[i]["height"] = feet*12 + inches
with open("players.json", "w") as file:
    json.dump(data, file)

#extract_relationships("players.json", ["plays_for", "attended"])