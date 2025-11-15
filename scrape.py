import requests
from bs4 import BeautifulSoup
import re
import json
import dedup
from neo4j import GraphDatabase
from config import neo4j_pass

SIDEARM_SCHOOL_TO_ROSTER_URLS = {
    "University of North Carolina": ["https://goheels.com/sports/baseball/roster/"],
    "University of Florida": ["https://floridagators.com/sports/baseball/roster/"],
    "Mississippi State University": [
        "https://hailstate.com/sports/baseball/roster/",
        "https://hailstate.com/sports/baseball/coaches/"
    ]
}

TABLE_SCHOOL_TO_ROSTER_URLS = {
    "Pennsylvania State University": ["https://gopsusports.com/sports/baseball/roster/season/"],
    "Vanderbilt University": ["https://vucommodores.com/sports/baseball/roster/season/"],
    #"University of Arkansas": ["https://arkansasrazorbacks.com/sport/m-basebl/roster/"],
    "University of Miami": ["https://miamihurricanes.com/sports/baseball/roster/season/"],
    "Oregon State University": ["https://osubeavers.com/sports/baseball/roster/"],
    "University of Tennessee": ["https://utsports.com/sports/baseball/roster/"]
}

SCHOOL_ATTR_TO_COL = {
    "Pennsylvania State University": {"name": 1, "position": 3, "height": 5, "weight": 6, "attended": [8,9]},
    "Vanderbilt University": {"name": 1, "position": 2, "height": 4, "weight": 5, "attended": [8]},
    #"University of Arkansas": {},
    "University of Miami": {"name": 1, "position": 2, "height": 3, "weight": 4, "attended": [8,7], },
    "Oregon State University": {"name": 2, "position": 3, "height": 5, "weight": 6, "attended": [8], "parse_hs": True, "pic_offset":True},
    "University of Tennessee": {"name": 1, "position": 3, "height": 5, "weight": 6, "attended": [7,8], "parse_hs": True, "pic_offset":True}
}

def convert_height_str(height):
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
    return feet*12 + inches

def get_school_id(school_to_id:dict, school:str, curr_school_id:int, data:dict, curr_player_id:int):
    """Updates school ID and retrieves the next ID to be assigned."""
    id = school_to_id.get(school)
    if id==None:
        school_to_id[school] = curr_school_id
        data["School"].append(
            {
                "id": curr_school_id,
                "name": school,
                "type": "high school" if school.endswith("HS") or "High School" in school 
                or school.endswith("Academy") else "university"
            }
        )
        curr_school_id+=1
    data["attended"].append((curr_player_id, school_to_id[school]))
    return curr_school_id

def scrape_sidearm_roster(
    url:str,
    data:dict, 
    school:str,
    year: int,
    curr_player_id:int,
    curr_coach_id:int,
    curr_school_id:int,
    school_to_id:dict[str, int],
    team_to_id: dict[str, int]
):
    response = requests.get(url)
    if response.status_code!=200:
        print(response.status_code)
        return curr_player_id, curr_coach_id, curr_school_id
    soup = BeautifulSoup(response.content, "html.parser")
    div_tags = soup.find_all("div",attrs={"class":re.compile("(person-card.*items-center)|heading-divider")})
    get_coaches = False
    for tag in div_tags:
        tag_class = tag.get("class")
        if tag_class and "heading-divider" in tag_class[0] and tag.text and "Coach" not in tag.text:
            break
        if tag_class and "heading-divider" in tag_class[0] and "Coach" in tag.text:
            get_coaches = True
            continue
        name = tag.find("h3")
        if not name:
            continue 
        if not get_coaches:
            stats = tag.find("div",attrs={"class":re.compile("bio-stats")})
            if stats:
                position = stats.find("span",attrs={"data-test-id":re.compile("person-position")})
                height = stats.find("span",attrs={"data-test-id":re.compile("person-season")})
                weight = stats.find("span",attrs={"data-test-id":re.compile("person-weight")})
                last_schools = None
                last_schools_tag = tag.find("span", attrs={"data-test-id":re.compile("person-high-school")})
                # add to attended relationship and add new school entity as needed
                if last_schools_tag:
                    last_schools = last_schools_tag.text[len("Last School")+1:].strip().split(" / ")
                    for schl in last_schools:
                        curr_school_id = get_school_id(school_to_id, schl, curr_school_id, data, curr_player_id)
                        
                weight = weight.text[len("Weight")+1:weight.text.rfind("lbs")].strip() if weight else None
                space_ind = weight.find(" ")
                position_start_ind = 0
                if "position" in position.text.lower():
                    position_start_ind+=len("position")
                    while not position.text[position_start_ind].isalpha():
                        position_start_ind+=1
                data["Player"].append(
                    {
                        "id": curr_player_id,
                        "name": name.text,
                        "position": position.text[position_start_ind:].strip() if position else None,
                        "height": convert_height_str(height.text[len("Height")+1:].strip()) if height else None,
                        "weight": int(weight) if space_ind==-1 else int(weight[:space_ind])
                    }
                )
                data["plays_for"].append((curr_player_id, team_to_id[f"{school} {year}"]))
                curr_player_id+=1
        if get_coaches:
            role = tag.find("div", attrs={"class": None, "data-test-id": None})
            data["Coach"].append(
                {
                    "id": curr_coach_id,
                    "name": name.text,
                    "role": role.text if role else None
                }
            )
            data["coaches_team"].append((curr_coach_id, team_to_id.get(f"{school} {year}")))
            curr_coach_id+=1
    return curr_player_id, curr_coach_id, curr_school_id

def scrape_sidearm_coach_page(url, data, school, year, curr_coach_id, team_to_id):
    response = requests.get(url)
    if response.status_code!=200:
        print(response.status_code)
        return curr_coach_id
    soup = BeautifulSoup(response.content, "html.parser")
    tags = soup.find_all("tr")
    for tag in tags:
        columns = tag.find_all("span")
        if columns:
            data["Coach"].append(
                {
                    "id": curr_coach_id,
                    "name" : columns[0].text,
                    "role" : columns[1].text,
                    "coaches": [f"{school} {str(year)}"]
                }
            )
            data["coaches_team"].append((curr_coach_id, team_to_id[f"{school} {year}"]))
            curr_coach_id+=1
    return curr_coach_id

def scrape_table(
    url:str,
    data:dict, 
    school:str,
    year: int,
    curr_player_id:int,
    curr_coach_id:int,
    curr_school_id:int,
    school_to_id:dict,
    team_to_id:dict
):
    response = requests.get(url)
    if response.status_code!=200:
        print(response.status_code)
        return curr_player_id, curr_coach_id
    soup = BeautifulSoup(response.content, "html.parser")
    tables = soup.find_all("table")
    player_rows = tables[0].find_all("tr")
    player = {}
    for i in range(1, len(player_rows)):
        cols = player_rows[i].find_all(re.compile("td|th"))
        if len(cols)>5:
            for attr in SCHOOL_ATTR_TO_COL[school]:
                if type(SCHOOL_ATTR_TO_COL[school][attr]) is int:
                    value = cols[SCHOOL_ATTR_TO_COL[school][attr]].text.replace("\\n","").strip()
                    if attr=="height":
                        value = convert_height_str(value)
                    elif attr=="weight":
                        space_ind = value.find(" ")
                        if space_ind!=-1:
                            value = value[:space_ind]
                    player[attr] = value if attr!="weight" else int(value)
                elif type(SCHOOL_ATTR_TO_COL[school][attr]) is list:
                    for ind in SCHOOL_ATTR_TO_COL[school][attr]:
                        prev_school = cols[ind].text.replace("\\n","").strip()
                        if SCHOOL_ATTR_TO_COL[school].get("parse_hs") and "/" in prev_school:
                            prev_school = prev_school.split(" / ")[1].strip()
                        curr_school_id = get_school_id(school_to_id, prev_school, curr_school_id, data, curr_player_id)
            data["Player"].append(player)
            data["plays_for"].append((curr_player_id, team_to_id[f"{school} {year}"]))
            curr_player_id+=1
    if len(tables)>1:
        coach_rows = tables[1].find_all("tr")
        for i in range(1, len(coach_rows)):
            cols = coach_rows[i].find_all(re.compile("td|th"))
            offset = int(SCHOOL_ATTR_TO_COL[school].get("pic_offset")) if "pic_offset" in SCHOOL_ATTR_TO_COL[school] else 0
            if "Coach" not in cols[1+offset].text:
                continue
            data["Coach"].append(
                {
                    "id": curr_coach_id,
                    "name": cols[0+offset].text.replace("\\n",""),
                    "role": cols[1+offset].text
                }
            )
            data["coaches_team"].append((curr_coach_id, team_to_id.get(f"{school} {year}")))
            curr_coach_id+=1
    return curr_player_id, curr_coach_id, curr_school_id

def get_id_mapping(filename):
    to_id = {}
    with open(filename) as file:
        temp = json.load(file)
        for row in temp:
            to_id[row["name"]]=row["id"]
    return to_id

def get_next_id(json_file):
    data = None
    with open(json_file) as file:
        data = json.load(file)
    return max([row["id"] for row in data])+1

def upload_to_neo4j(data, node_names, relationships, rel_to_ents):
    URI = "neo4j+s://10cc4287.databases.neo4j.io"
    AUTH = ("neo4j", neo4j_pass)
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        driver.verify_connectivity()
        for node in node_names:
            if not data.get(node):
                continue
            keys = list(data[node][0].keys())
            for row in data[node]:
                attrs_q_str = "{"
                for i in range(len(keys)-1):
                    attrs_q_str+=f"{keys[i]}:${keys[i]},"
                attrs_q_str += f"{keys[len(keys)-1]}:${keys[len(keys)-1]}"+"}"
                driver.execute_query(
                    f"CREATE (p1: {node} {attrs_q_str})",
                    row
                )
        for rel in relationships:
            if not data.get(rel):
                continue
            for row in data[rel]:
                res = driver.execute_query(
                    f"MATCH (n1: {rel_to_ents[rel][0]}{{id:$id1}})" \
                    f"MATCH (n2: {rel_to_ents[rel][1]}{{id:$id2}})" \
                    f"CREATE (n1)-[:{rel.upper()}]->(n2)",
                    {"id1":row[0], "id2":row[1]}
                )

if __name__=="__main__":
    # Pre-run TODO: Fill out team data before running for team_to_id mapping
    years = [2025, 2026]
    # data not loaded from files, so that deduping is easier
    data = {
        "Player": [],
        "Coach":[],
        "School": [],
        "Team": [],
        "Conference": [],
        "attended": [],
        "coaches_team": [],
        "plays_for": [],
        "member_of": []
    }
    """school_to_id = get_id_mapping("schools.json")
    team_to_id = get_id_mapping("teams.json")
    curr_player_id, curr_coach_id, curr_school_id = get_next_id("players.json"), get_next_id("coaches.json"), get_next_id("schools.json")
    for school in SIDEARM_SCHOOL_TO_ROSTER_URLS:
        for i in range(len(SIDEARM_SCHOOL_TO_ROSTER_URLS[school])):
            for year in years:
                url = SIDEARM_SCHOOL_TO_ROSTER_URLS[school][i]
                if not url.endswith("coaches/"):
                    curr_player_id, curr_coach_id, curr_school_id = scrape_sidearm_roster(url+str(year), data, school, year, curr_player_id, curr_coach_id, curr_school_id, school_to_id, team_to_id)
                else:
                    curr_coach_id = scrape_sidearm_coach_page(url+str(year), data, school, year, curr_coach_id, team_to_id)"""
    """for school in TABLE_SCHOOL_TO_ROSTER_URLS:
        for i in range(len(TABLE_SCHOOL_TO_ROSTER_URLS[school])):
            for year in years:
                url = TABLE_SCHOOL_TO_ROSTER_URLS[school][i]
                curr_player_id, curr_coach_id, curr_school_id = scrape_table(
                    url + str(year) if not url.endswith("season/") else f"{url}{year-1}-{str(year)[2:]}",
                    data,
                    school,
                    year,
                    curr_player_id,
                    curr_coach_id,
                    curr_school_id,
                    school_to_id,
                    team_to_id
                )"""
    """print("Player duplicates:")
    dedup.check_for_dups(data["Player"])
    print("Coach duplicates:\n")
    dedup.check_for_dups(data["Coach"])"""
    """with open("players.json") as player_file:
        data["Player"]=json.load(player_file)+data["Player"]
    with open("coaches.json") as coach_file:
        data["Coach"]=json.load(coach_file)+data["Coach"]
    with open("schools.json") as school_file:
        data["School"]=json.load(school_file)+data["School"]
    with open("attended.json") as attended_file:
        data["attended"]=json.load(attended_file)+data["attended"]
    with open("coaches_team.json") as coaches_team_file:
        data["coaches_team"]=json.load(coaches_team_file)+data["coaches_team"]
    with open("plays_for.json") as plays_for_file:
        data["plays_for"]=json.load(plays_for_file)+data["plays_for"]
    with open("players.json", "w") as player_file:
        json.dump(data["Player"], player_file)
    with open("coaches.json", "w") as coach_file:
        json.dump(data["Coach"], coach_file)
    with open("schools.json", "w") as schools_file:
        json.dump(data["School"], schools_file)"""
    """with open("players.json") as player_file:
        data["Player"]=json.load(player_file)
    with open("schools.json") as school_file:
        data["School"]=json.load(school_file)
    with open("conferences.json") as conf_file:
        data["Conference"]=json.load(conf_file)
    with open("attended.json") as attended_file:
        data["attended"]=json.load(attended_file)
    with open("coaches.json") as coach_file:
        data["Coach"]=json.load(coach_file)
    with open("teams.json") as team_file:
        data["Team"]=json.load(team_file)
    with open("coaches_team.json") as coaches_team_file:
        data["coaches_team"]=json.load(coaches_team_file)
    with open("plays_for.json") as plays_for_file:
        data["plays_for"]=json.load(plays_for_file)
    with open("member_of.json") as mem_of_file:
        data["member_of"]=json.load(mem_of_file)"""
    """upload_to_neo4j(
        data,
        ["Player", "Coach", "School", "Conference", "Team"],
        ["attended", "coaches_team", "plays_for", "member_of"],
        {
            "attended":  ("Player", "School"),
            "coaches_team": ("Coach", "Team"),
            "plays_for": ("Player", "Team"),
            "member_of": ("Team", "Conference")
        }
    )"""
