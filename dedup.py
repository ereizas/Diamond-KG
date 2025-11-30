import json
import pandas as pd
import pickle
from deduplipy.deduplicator import Deduplicator

def get_df_from_dict(data):
    for i in range(len(data)):
        if data[i].get("weight"):
            data[i]["id"] = str(data[i]["id"])
            data[i]["weight"] = str(data[i]["weight"])
            data[i]["height"] = str(data[i]["height"])
    return pd.DataFrame(data)

def get_df_from_json(filename:str):
    with open(filename) as file:
        data = json.load(file)
    return get_df_from_dict(data)

def train_dedup(class_name: str, fields:list[str], df: pd.DataFrame):
    deduplicator = Deduplicator(fields)
    deduplicator.fit(df)
    with open(f'{class_name}.pkl', 'wb') as f:
        pickle.dump(deduplicator, f)

# TODO: add deletion of relevant relationships
def dedup(pkl_file:str, df: pd.DataFrame, json_file: str, rels: dict[list], pair_ind:int):
    with open(pkl_file, 'rb') as f:
        deduplicator = pickle.load(f)

    deduped = deduplicator.predict(df)
    for col in df.columns:
        if col not in deduped.columns:
            deduped[col] = df[col]

    merged_rows = []

    for cluster_id, group in deduped.groupby("deduplication_id"):
        rep = group.iloc[0].to_dict()
        rep_ind = 0
        if len(group)>1:
            print(group)
        for i in range(1, len(group)):
            if len(group.iloc[i].to_dict()["name"])>len(rep["name"]):
                rep = group.iloc[i].to_dict()
                rep_ind = i
        merged_rows.append(rep)
        if len(group)==1:
            continue
        others = set()
        for i in range(len(group)):
            if i!=rep_ind:
                others.add(group.iloc[i].to_dict()["id"])
        for rel in rels:
            for i in range(len(rels[rel])):
                if rels[rel][i][pair_ind] in others:
                    rels[rel][i][pair_ind]=rep["id"]
                    
    df_unique = pd.DataFrame(merged_rows).reset_index(drop=True)
    dedup_lst = [df_unique.iloc[i].to_dict() for i in range(len(df_unique))]
    for i in range(len(dedup_lst)):
        if dedup_lst[i].get("id"):
            dedup_lst[i]["id"]=int(dedup_lst[i]["id"])
        if dedup_lst[i].get("weight") and dedup_lst[i].get("weight").isnumeric():
            dedup_lst[i]["weight"]=int(dedup_lst[i]["weight"])
        if dedup_lst[i].get("height") and dedup_lst[i].get("height").isnumeric():
            dedup_lst[i]["height"]=int(dedup_lst[i]["height"])

    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(dedup_lst, f, indent=2, ensure_ascii=False)

def check_for_dups(data:dict):
    names = {}
    for ent in data:
        if type(ent) is not dict:
            continue
        names[ent["name"]]=names.get(ent["name"],0)+1
    for name in names:
        if names[name]>1:
            print(name)

def check_for_dups_in_json(filename:str):
    with open(filename) as file:
        data = json.load(file)
    check_for_dups(data)

"""player_df = get_df_from_json("players_temp.json")
cols_to_string = ["name", "position", "height", "weight"]
for col in cols_to_string:
    player_df[col] = player_df[col].astype(str)
#train_dedup("players", cols_to_string, player_df)
rels = {}
with open("plays_for_temp.json") as plays_for_file:
    rels["plays_for"]=json.load(plays_for_file)
with open("attended_temp.json") as attended_file:
    rels["attended"]=json.load(attended_file)
dedup("players.pkl", player_df, "players_temp.json", rels)
check_for_dups_in_json("players_temp.json")
with open("plays_for_temp.json","w") as plays_for_file:
    json.dump(rels["plays_for"], plays_for_file)
with open("attended_temp.json", "w") as attended_file:
    json.dump(rels["attended"], attended_file)"""
#coach_df = get_df_from_json("coaches.json")
#train_dedup("coaches", ["name", "role"], coach_df)
#dedup("coaches.pkl", coach_df, "coaches.json")
#check_for_dups_in_json("players.json")
"""check_for_dups_in_json("temp_coaches.json")
check_for_dups_in_json("coaches.json")"""

# dedup schools
# TODO: get recently scraped schools in temp file and dedup
schools_dict = None

"""with open("schools_temp.json") as file:
    schools_dict = json.load(file)
school_df = get_df_from_dict(schools_dict)
#train_dedup("schools",["name","type"], school_df)
rels = {}
with open("attended.json") as attended_file:
    rels["attended"]=json.load(attended_file)
dedup("schools.pkl", school_df, "schools_temp.json", rels, 1)
with open("attended.json", "w") as file:
    json.dump(rels["attended"], file)"""
