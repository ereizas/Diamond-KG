import json
import os
import pandas as pd
import pickle
from deduplipy.deduplicator import Deduplicator

def get_df(filename:str):
    with open(filename) as file:
        data = json.load(file)
        for key in data:
            data[key]["weight"] = str(data[key]["weight"])
    return pd.DataFrame(data.values())

def train_dedup(class_name: str, fields:list[str], df: pd.DataFrame):
    deduplicator = Deduplicator(fields)
    deduplicator.fit(df)
    with open(f'{class_name}.pkl', 'wb') as f:
        pickle.dump(deduplicator, f)

def dedup(pkl_file:str, df: pd.DataFrame, json_file: str):
    deduplicator = None
    with open(pkl_file, 'rb') as f:
        deduplicator = pickle.load(f)
    deduped = deduplicator.predict(df)
    df_unique = deduped.sort_values('deduplication_id').groupby('deduplication_id', as_index=False).first()
    df_unique = df_unique.drop(columns=['deduplication_id'])
    dedup_dict = {str(i): df_unique.iloc[i].to_dict() for i in range(len((df_unique)))}
    with open(json_file, "w") as f:
        json.dump(dedup_dict, f, indent=2)

players_df = get_df("players.json")
#train_dedup("players", ["name", "position", "height", "weight"], players_df)
#dedup("players.pkl", players_df, "players.json")

"""with open("players.json") as file:
    data = json.load(file)
names = {}
for player in data:
    names[data[player]["name"]]=names.get(data[player]["name"],0)+1
for name in names:
    if names[name]>1:
        print(name)"""
