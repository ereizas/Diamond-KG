import requests
from bs4 import BeautifulSoup
import re
import json
import dedup
from neo4j import GraphDatabase
from config import neo4j_pass
import file_handling
# NOTE: Keep *_temp.json files for upload to Neo4J

#!!!TODO: normalize positions and get major
#!!!TODO: write query for top majors and show count
#!!!TODO: plan retrieval of different positions played for transfers

# TODO: rescrape attended and plays_for relationships
# TODO: fix scraped schools
# TODO: when updating try to exclude those with an ID that exists in the database
SIDEARM_SCHOOL_TO_ROSTER_URLS = {
    "University of North Carolina": ["https://goheels.com/sports/baseball/roster/"],
    "University of Florida": ["https://floridagators.com/sports/baseball/roster/"],
    "Mississippi State University": [
        "https://hailstate.com/sports/baseball/roster/",
        "https://hailstate.com/sports/baseball/coaches/"
    ]
}

TABLE_SCHOOL_TO_ROSTER_URLS = {
    #"Pennsylvania State University": ["https://gopsusports.com/sports/baseball/roster/season/"],
    #"Vanderbilt University": ["https://vucommodores.com/sports/baseball/roster/season/"],
    #"University of Miami": ["https://miamihurricanes.com/sports/baseball/roster/season/"],
    #"Oregon State University": ["https://osubeavers.com/sports/baseball/roster/"],
    #"University of Tennessee": ["https://utsports.com/sports/baseball/roster/"],
    #"University of Arkansas": ["https://arkansasrazorbacks.com/sport/m-basebl/roster/?season=","https://arkansasrazorbacks.com/sport/m-basebl/roster/?season="],
    "Bloomsburg University": ["https://bloomsburgathletics.com/sports/baseball/roster/"]
}

NON_DEFAULT_TABLE_SCHOOL_TO_ROSTER_URLS = {
    "Lafeyette University": ["https://goleopards.com/sports/baseball/roster/"],
    "Towson University": ["https://towsontigers.com/sports/baseball/roster/"],
    "Maryville College": ["https://mcscots.com/sports/baseball/roster/"],
    #"Flagler College": ["https://flaglerathletics.com/sports/baseball/roster/"],
    #"Belmont University": ["https://belmontbruins.com/sports/baseball/roster/"],
    #"Lynchburg University": ["https://lynchburgsports.com/sports/baseball/roster/"],
    #"University of Pittsburgh at Johnstown": ["https://pittjohnstownathletics.com/sports/baseball/roster/"],
    #"York College of Pennsylvania": ["https://ycpspartans.com/sports/baseball/roster/"],
    #"Oberlin College": ["https://goyeo.com/sports/baseball/roster/"],
    #"Monmouth University": ["https://monmouthhawks.com/sports/baseball/roster/"]
}

SCHOOL_ATTR_TO_COL = {
    #"Pennsylvania State University": {"name": 1, "position": 3, "height": 5, "weight": 6, "attended": [8,9]},
    #"Vanderbilt University": {"name": 1, "position": 2, "height": 4, "weight": 5, "attended": [8]},
    #"University of Miami": {"name": 1, "position": 2, "height": 3, "weight": 4, "attended": [8,7]},
    #"Oregon State University": {"name": 2, "position": 3, "height": 5, "weight": 6, "attended": [8], "parse_hs": True, "pic_offset":True},
    #"University of Tennessee": {"name": 1, "position": 3, "height": 5, "weight": 6, "attended": [7,8], "parse_hs": True, "pic_offset":True},
    #"University of Arkansas": {"name": 1, "position": 2, "height": 4, "weight": 5, "attended": [8]},
    "Bloomsburg University": {"name": 1, "position": 2, "height": 3, "weight": 4, "attended": [8,7], "major": 9, "parse_hs": True, "pic_offset": True},
    "Lafeyette University": {"name": 1, "position": 2, "height": 4, "weight": 5, "attended": [7], "major": 8, "parse_hs": True, "pic_offset": True},
    "Towson University": {"name": 1, "position": 2, "height": 3, "weight": 4, "attended": [8,7], "major": 9, "parse_hs": True, "pic_offset": True},
    "Maryville College": {"name": 1, "position": 3, "height": 5, "weight": 6, "attended": [8,7], "major": 9, "parse_hs": True, "pic_offset":True},
    #"Flagler College": {"name": 2, "position": 3, "height": 5, "weight": 6, "attended": [8], "parse_hs": True, "pic_offset":True},
    #"Belmont University": {"name": 1, "position": 2, "height": 5, "weight": 6, "attended": [8,9], "pic_offset": True},
    #"Lynchburg University": {"name": 1, "position": 2, "height": 3, "attended": [5], "parse_hs": True, "pic_offset": True},
    #"University of Pittsburgh at Johnstown": {"name": 1, "position": 2, "height": 3, "weight": 4, "attended": [7], "parse_hs": True, "pic_offset": True},
    #"York College of Pennsylvania": {"name": 1, "position": 2, "height": 4, "weight": 5, "attended": [7], "parse_hs": True, "pic_offset": True},
    #"Oberlin College": {"name": 2, "position": 4, "attended": [5], "parse_hs": True, "pic_offset": True},
    #"Monmouth University 2025": {"name": 1, "position": 2, "height": 5, "weight": 6, "attended": [7,8], "parse_hs": True, "pic_offset": True},
}

def convert_height_str(height):
    if len(height.strip())<3:
        return None
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

def normalize_delimiters(s):
    if s is None:
        return None

    # Lowercase for easier matching
    s = s.lower().strip()
    # Replace slashes, pipes, semicolons with commas
    s = re.sub(r"[\/|;]", ",", s)
    # Replace multiple commas or comma + whitespace with a single comma
    s = re.sub(r"\s*,\s*", ",", s)
    # Replace multiple spaces with a single space
    s = re.sub(r"\s+", " ", s)
    # Remove stray spaces around commas again
    s = re.sub(r"\s*,\s*", ",", s)
    return s

def get_school_id(
    school_to_id:dict,
    school:str,
    curr_school_id:int, 
    data:dict, 
    curr_player_id:int,
    col_name: str
):
    """Updates school ID and retrieves the next ID to be assigned."""
    if not school:
        return curr_school_id
    id = school_to_id.get(school)
    if id==None:
        school_to_id[school] = curr_school_id
        typ = None
        if ("High School" in col_name and "Previous School" not in col_name) or school.endswith("HS") or "High School" in school or school.endswith("Academy") :
            typ = "high school"
        elif col_name=="Previous School" or "College" in school or "University" in school:
            typ = "university"
        data["School"].append(
            {
                "id": curr_school_id,
                "name": school,
                "type": typ,
                "division": None
            }
        )
        curr_school_id+=1
    data["attended"].append([curr_player_id, school_to_id[school]])
    return curr_school_id

def get_id_mapping(filename):
    to_id = {}
    with open(filename) as file:
        temp = json.load(file)
        for row in temp:
            to_id[row["name"] if not row.get("year") else f"{row["name"]} {row["year"]}"]=row["id"]
    return to_id

player_to_id = get_id_mapping("players.json")

POSITION_TOKEN_MAP = {
    # Catcher
    "c": "C",
    "catcher": "C",

    # Infield
    "if": "INF",
    "inf": "INF",
    "infield": "INF",
    "cif": "INF",
    "m-inf": "INF",

    # Middle infield
    "mif": "MIF",
    "mif": "MIF",

    # Bases
    "1b": "1B",
    "2b": "2B",
    "3b": "3B",
    
    # Shortstop
    "ss": "SS",

    # Outfield
    "of": "OF",
    "outfield": "OF",

    # Pitcher (generic)
    "p": "P",
    "rp": "P",     # relief pitcher → still "P"

    # Right-handed pitcher
    "rhp": "RHP",
    "right-handed pitcher": "RHP",
    "right-handed": "RHP",   # appears in combined tokens

    # Left-handed pitcher
    "lhp": "LHP",
    "left-handed pitcher": "LHP",
    "left-handed": "LHP",

    "ut": "UTL",
    "utl": "UTL",
    "utility": "UTL",
    "dh": "DH",

    # Extra variants seen in data
    "c/": "C",     # in case tokenization leaves trailing slash
    "b": "",     # ambiguous unless part of 1B/2B/3B
}

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
                    last_schools = last_schools_tag.text[len("Last School")+1:].strip().split("/")
                    name = name.text.strip()
                    for schl in last_schools:
                        curr_school_id = get_school_id(school_to_id, schl.strip(), curr_school_id, data, curr_player_id)
                        
                weight = weight.text[len("Weight")+1:weight.text.rfind("lbs")].strip() if weight else None
                space_ind = weight.find(" ")
                position_start_ind = 0
                if "position" in position.text.lower():
                    position_start_ind+=len("position")
                    while not position.text[position_start_ind].isalpha():
                        position_start_ind+=1
                if position:
                    position = normalize_delimiters(position.text[position_start_ind:].strip())
                position = ",".join([POSITION_TOKEN_MAP[pos] for pos in position.split(",")])
                data["Player"].append(
                    {
                        "id": curr_player_id,
                        "name": name.text.replace("  "," "),
                        "position": position if position else None, # None if blank str
                        "height": convert_height_str(height.text[len("Height")+1:].strip()) if height else None,
                        "weight": int(weight) if space_ind==-1 else int(weight[:space_ind]),
                        "major": None
                    }
                )
                data["plays_for"].append((curr_player_id, team_to_id[f"{school} {year}"]))
                curr_player_id+=1
        if get_coaches:
            role = tag.find("div", attrs={"class": None, "data-test-id": None})
            data["Coach"].append(
                {
                    "id": curr_coach_id,
                    "name": name.text.replace("  ", ""),
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
                    "name" : columns[0].text.replace("  ", " "),
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
        print(url)
        print(f"{school} {year}")
        print(response.status_code)
        return curr_player_id, curr_coach_id, curr_school_id
    soup = BeautifulSoup(response.content, "html.parser")
    tables = soup.find_all("table")
    j = 0 
    player_rows = tables[0].find_all("tr")
    while j<len(tables) and len(player_rows)<=2:
        player_rows=tables[j].find_all("tr")
        j+=1
    col_names = player_rows[0].find_all(re.compile("td|th"))
    attr_to_col = SCHOOL_ATTR_TO_COL.get(school)
    if not attr_to_col:
        attr_to_col = SCHOOL_ATTR_TO_COL.get(f"{school} {year}")
    for i in range(1, len(player_rows)):
        player = {"id": curr_player_id}
        cols = player_rows[i].find_all(re.compile("td|th"))
        if len(cols)>5:
            for attr in attr_to_col:
                if type(attr_to_col[attr]) is int:
                    value = cols[attr_to_col[attr]].text.replace("\\n","").strip()
                    if attr=="height":
                        value = convert_height_str(value)
                    elif attr=="weight":
                        space_ind = value.find(" ")
                        if space_ind!=-1:
                            value = value[:space_ind]
                    player[attr] = value
                elif attr=="position":
                    position = normalize_delimiters(cols[attr_to_col[attr]])
                    position = ",".join([POSITION_TOKEN_MAP[pos] for pos in position.split(",")])
                    player[attr] = position if position else None
                elif type(attr_to_col[attr]) is list:
                    for ind in attr_to_col[attr]:
                        sub_col_names = col_names[ind].text.split("/")
                        prev_school = cols[ind].text.replace("\\n","").strip()
                        prev_schools = prev_school.split("/")
                        for i in range(len(prev_schools)):
                            if (i<len(sub_col_names) and "School" in sub_col_names[i]) or i>=len(sub_col_names):
                                curr_school_id = get_school_id(school_to_id, prev_schools[i].strip(), curr_school_id, data, curr_player_id, col_names[ind])
            data["Player"].append(player)
            if team_to_id.get(f"{school} {year}"):
                data["plays_for"].append((curr_player_id, team_to_id[f"{school} {year}"]))
            curr_player_id+=1
    if j<len(tables):
        coach_rows = tables[j].find_all("tr")
        for i in range(1, len(coach_rows)):
            cols = coach_rows[i].find_all(re.compile("td|th"))
            offset = int(attr_to_col.get("pic_offset")) if "pic_offset" in attr_to_col else 0
            if len(cols)<2 or "Coach" not in cols[1+offset].text:
                continue
            data["Coach"].append(
                {
                    "id": curr_coach_id,
                    "name": cols[0+offset].text.replace("\\n","").strip().replace("  ", " "),
                    "role": cols[1+offset].text.replace("\\n","").strip()
                }
            )
            data["coaches_team"].append((curr_coach_id, team_to_id.get(f"{school} {year}")))
            curr_coach_id+=1
    return curr_player_id, curr_coach_id, curr_school_id

def scrape_coach_table(
    url:str,
    data:dict, 
    school:str,
    year: int,
    curr_coach_id:int,
    team_to_id:dict
):
    response = requests.get(url)
    if response.status_code!=200:
        print(response.status_code)
        return curr_coach_id
    soup = BeautifulSoup(response.content, "html.parser")
    tables = soup.find_all("table")
    attr_to_col = SCHOOL_ATTR_TO_COL.get(school)
    if not attr_to_col:
        attr_to_col = SCHOOL_ATTR_TO_COL.get(f"{school} {year}")
    offset = int(attr_to_col.get("pic_offset")) if "pic_offset" in attr_to_col else 0
    coach_rows = tables[0].find_all("td",string=re.compile("Coach"))
    for i in range(1, len(coach_rows)):
        cols = coach_rows[i].find_all(re.compile("td|th"))
        role = cols[1+offset].text.replace("\\n","").strip()
        if "Coach" in role:
            data["Coach"].append(
                {
                    "id": curr_coach_id,
                    "name": cols[0+offset].text.replace("\\n","").strip().replace("  ", " "),
                    "role": role
                }
            )
            data["coaches_team"].append((curr_coach_id, team_to_id.get(f"{school} {year}")))
            curr_coach_id+=1
    return curr_coach_id
        
def scrape_tables(urls, data, curr_player_id, curr_coach_id, curr_school_id, school_to_id, team_to_id, url_addition=""):
    for school in urls:
        for i in range(len(urls[school])):
            for year in years:
                url = urls[school][i]
                if i==0:
                    curr_player_id, curr_coach_id, curr_school_id = scrape_table(
                        url + str(year) + url_addition if "season" not in url else f"{url}{year-1}-{str(year)[2:]}",
                        data,
                        school,
                        year,
                        curr_player_id,
                        curr_coach_id,
                        curr_school_id,
                        school_to_id,
                        team_to_id
                    )
                """else:
                    print(url + str(year) + url_addition if "season" not in url else f"{url}{year-1}-{str(year)[2:]}"+"#coaches")
                    curr_coach_id = scrape_coach_table(
                        url + str(year) + url_addition if "season" not in url else f"{url}{year-1}-{str(year)[2:]}"+"#coaches",
                        data,
                        school,
                        year,
                        curr_coach_id,
                        team_to_id
                    )"""
    return curr_player_id, curr_coach_id, curr_school_id

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
                driver.execute_query(
                    f"MATCH (n1: {rel_to_ents[rel][0]}{{id:$id1}})" \
                    f"MATCH (n2: {rel_to_ents[rel][1]}{{id:$id2}})" \
                    f"CREATE (n1)-[:{rel.upper()}]->(n2)",
                    {"id1":row[0], "id2":row[1]}
                )

if __name__=="__main__":
    # and then previous schools
    # Pre-run TODO: Fill out team data before running for team_to_id mapping
    years = [2025]
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
    #school_to_id = get_id_mapping("schools.json")
    #team_to_id = get_id_mapping("teams.json")
    #curr_player_id, curr_coach_id, curr_school_id = get_next_id("players.json"), get_next_id("coaches.json"), get_next_id("schools.json")
    """for school in SIDEARM_SCHOOL_TO_ROSTER_URLS:
        for i in range(len(SIDEARM_SCHOOL_TO_ROSTER_URLS[school])):
            for year in years:
                url = SIDEARM_SCHOOL_TO_ROSTER_URLS[school][i]
                if not url.endswith("coaches/"):
                    curr_player_id, curr_coach_id, curr_school_id = scrape_sidearm_roster(url+str(year), data, school, year, curr_player_id, curr_coach_id, curr_school_id, school_to_id, team_to_id)
                else:
                    curr_coach_id = scrape_sidearm_coach_page(url+str(year), data, school, year, curr_coach_id, team_to_id)"""
    #curr_player_id, curr_coach_id, curr_school_id = scrape_tables(TABLE_SCHOOL_TO_ROSTER_URLS, data, curr_player_id, curr_coach_id, curr_school_id, school_to_id, team_to_id)
    #curr_player_id, curr_coach_id, curr_school_id = scrape_tables(NON_DEFAULT_TABLE_SCHOOL_TO_ROSTER_URLS, data, curr_player_id, curr_coach_id, curr_school_id, school_to_id, team_to_id, url_addition="?view=2")
    #concatenate data and check for dups
    #file_handling.concat_to_main_file("players.json", "players_temp.json", data, "Player")
    #file_handling.concat_to_main_file("coaches.json", "coaches_temp.json", data, "Coach")
    #file_handling.concat_to_main_file("coaches_team.json", "coaches_team_temp.json", data, "coaches_team")
    #file_handling.concat_to_main_file("plays_for.json", "plays_for_temp.json", data, "plays_for")
    #file_handling.concat_to_main_file("schools.json", "schools_temp.json", data, "School")
    #file_handling.concat_to_main_file("attended.json", "attended_temp.json", data, "attended")
    """with open("players_temp.json", "w") as file:
        json.dump(data["Player"], file)
    with open("coaches_temp.json", "w") as file:
        json.dump(data["Coach"], file)
    with open("schools_temp.json", "w") as school_file:
        json.dump(data["School"], school_file)
    with open("attended_temp.json", "w") as attended_file:
        json.dump(data["attended"], attended_file)
    with open("coaches_team_temp.json", "w") as coaches_team_file:
        json.dump(data["coaches_team"], coaches_team_file)
    with open("plays_for_temp.json", "w") as plays_for_file:
        json.dump(data["plays_for"], plays_for_file)"""
    
    # script to check for teams that have players who went to the same highschool
    """team_to_players = {}
    for pid, tid in data["plays_for"]:
        ttp_get = team_to_players.get(tid, [])
        ttp_get.append(pid)
        team_to_players[tid]=ttp_get
    for tid in team_to_players:
        schools = set()
        for player_id in team_to_players[tid]:
            for pid,sid in data["attended"]:
                is_hs = False
                for school in data["School"]:
                    if school["id"]==sid and school["type"]=="high school":
                        is_hs = True
                if is_hs and pid==player_id:
                    if sid in schools:
                        print(sid)
                    schools.add(sid)"""
    # TODO: merge in normalized player positions and document strategy
    # load files for upload to neo4j
    with open("players.json") as file:
        data["Player"]=json.load(file)
    with open("attended.json") as attended_file:
        data["attended"]=json.load(attended_file)
    with open("plays_for.json") as plays_for_file:
        data["plays_for"]=json.load(plays_for_file)
    """with open("schools_temp.json") as school_file:
        data["School"]=json.load(school_file)
    with open("conferences_temp.json") as conf_file:
        data["Conference"]=json.load(conf_file)
    
    with open("coaches_temp.json") as file:
        data["Coach"]=json.load(file)
    with open("teams.json") as team_file:
        data["Team"]=json.load(team_file)
    with open("coaches_team.json") as coaches_team_file:
        data["coaches_team"]=json.load(coaches_team_file)
    with open("member_of.json") as mem_of_file:
        data["member_of"]=json.load(mem_of_file)"""
    
    upload_to_neo4j(
        data,
        ["Player", "Coach", "School", "Conference", "Team"],
        ["attended", "coaches_team", "plays_for", "member_of"],
        {
            "attended":  ("Player", "School"),
            "coaches_team": ("Coach", "Team"),
            "plays_for": ("Player", "Team"),
            "member_of": ("Team", "Conference")
        }
    )
