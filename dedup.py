import json
import os
import pandas as pd
import pickle
from deduplipy.deduplicator import Deduplicator
#TODO: dedup coaches

def get_df_from_dict(data):
    for key in data:
        if "weight" in data[key]:
            data[key]["weight"] = str(data[key]["weight"])
    return pd.DataFrame(data.values())

def get_df_from_json(filename:str):
    with open(filename) as file:
        data = json.load(file)
    return get_df_from_dict(data)

def train_dedup(class_name: str, fields:list[str], df: pd.DataFrame):
    deduplicator = Deduplicator(fields)
    deduplicator.fit(df)
    with open(f'{class_name}.pkl', 'wb') as f:
        pickle.dump(deduplicator, f)

def dedup(pkl_file:str, df: pd.DataFrame, json_file: str):
    with open(pkl_file, 'rb') as f:
        deduplicator = pickle.load(f)

    # Run deduplication
    deduped = deduplicator.predict(df)

    # If deduplipy dropped non-comparison fields, re-attach them manually
    for col in df.columns:
        if col not in deduped.columns:
            deduped[col] = df[col]

    merged_rows = []

    for cluster_id, group in deduped.groupby("deduplication_id"):
        rep = group.iloc[0].to_dict()

        # --- Combine list-like fields (plays_for, attended) ---
        for list_field in ["plays_for", "attended"]:
            if list_field in group.columns:
                combined = []
                for lst in group[list_field].dropna():
                    if isinstance(lst, list):
                        combined.extend(lst)
                    elif isinstance(lst, str):  # Sometimes stored as JSON/text
                        try:
                            combined.extend(json.loads(lst))
                        except Exception:
                            combined.append(lst)
                # Remove duplicates while preserving order
                seen = set()
                rep[list_field] = [x for x in combined if not (x in seen or seen.add(x))]

        merged_rows.append(rep)

    # Convert merged clusters to DataFrame
    df_unique = pd.DataFrame(merged_rows).reset_index(drop=True)

    # Export as {id: {...}} dictionary
    dedup_dict = {str(i): df_unique.iloc[i].to_dict() for i in range(len(df_unique))}

    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(dedup_dict, f, indent=2, ensure_ascii=False)

def check_for_dups(data:dict):
    names = {}
    for ent in data:
        names[data[ent]["name"]]=names.get(data[ent]["name"],0)+1
    for name in names:
        if names[name]>1:
            print(name)

def check_for_dups_in_json(filename:str):
    with open(filename) as file:
        data = json.load(file)
    check_for_dups(data)

"""players_df = get_df_from_json("players.json")
players_df["attended_str"] = players_df["attended"].apply(
    lambda x: ", ".join(sorted(set(x))) if isinstance(x, list) else str(x)
)"""
"""train_dedup("players", ["name", "position", "height", "weight", "attended_str"], players_df)
dedup("players.pkl", players_df, "players.json")"""

#coach_df = get_df_from_json("coaches.json")
#train_dedup("coaches", ["name", "role"], coach_df)
#dedup("coaches.pkl", coach_df, "coaches.json")
check_for_dups_in_json("players.json")
check_for_dups_in_json("coaches.json")
