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

FILES = [
    "coaches.json",
    "conferences.json",
    "players.json",
    "schools.json",
    "teams.json"
]
"""for file in FILES:
    json_to_csv(file)
json_to_csv("attended.json",fieldnames=["player_id", "school_id"])
json_to_csv("coaches_team.json", fieldnames=["coach_id","team_id"])
json_to_csv("member_of.json", fieldnames=["team_id", "conference_id"])
json_to_csv("plays_for.json", fieldnames=["player_id", "team_id"])"""
