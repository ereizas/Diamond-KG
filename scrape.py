import requests
from bs4 import BeautifulSoup
import re
import json
import dedup
# TODO: append dicts to json files
# TODO: use foreign ids for relationships if possible
# TODO: ensure only number is captured for weight
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

def scrape_sidearm_roster(
    url:str,
    data:dict, 
    school:str,
    year: int,
    curr_player_id:int,
    curr_coach_id:int
):
    response = requests.get(url)
    if response.status_code!=200:
        print(response.status_code)
        return curr_player_id, curr_coach_id
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
        """if not get_coaches:
            stats = tag.find("div",attrs={"class":re.compile("bio-stats")})
            if stats:
                position = stats.find("span",attrs={"data-test-id":re.compile("person-position")})
                height = stats.find("span",attrs={"data-test-id":re.compile("person-season")})
                weight = stats.find("span",attrs={"data-test-id":re.compile("person-weight")})
                last_schools = None
                last_schools_tag = tag.find("span", attrs={"data-test-id":re.compile("person-high-school")})
                if last_schools_tag:
                    last_schools = last_schools_tag.text[len("Last School")+1:].strip().split(" / ")

                
                #clean up for adding relationships
                for pid in data["Player"]:
                    if data["Player"][pid]["name"]==name.text:
                        data["Player"][pid]["plays_for"].append(f"{school} {year}")
                        if last_schools:
                            for schl in last_schools:
                                if schl not in data["Player"][pid]["attended"]:
                                    data["Player"][pid]["attended"].append(schl)
                
                
                data["Player"][curr_player_id]={
                    "name": name.text,
                    # TODO: clean up scraping so it can capture 1 char "C"
                    "position": position.text[len("Position ")+1:].strip() if position else None,
                    "height": convert_height_str(height.text[len("Height")+1:].strip()) if height else None,
                    "weight": int(weight.text[len("Weight")+1:weight.text.rfind("lbs")].strip()) if weight else None
                }
                if last_schools:
                    for schl in last_schools:
                        data["School"][schl] = {"type": "high school" if schl.endswith("HS") or "High School" in schl or schl.endswith("Academy") else "university"}
                        data["Player"][curr_player_id]["attended"].append(schl)
                data["Player"][curr_player_id]["plays_for"] = f"{school} {year}"
                
                curr_player_id+=1"""
        if get_coaches:
            role = tag.find("div", attrs={"class": None, "data-test-id": None})
            data["Coach"][curr_coach_id]={
                "name": name.text,
                "role": role.text if role else None
            }
            data["Coach"][curr_coach_id]["coaches"]= [f"{school} {str(year)}"]
            curr_coach_id+=1
    return curr_player_id, curr_coach_id

def scrape_sidearm_coach_page(url, data, school, year, curr_coach_id):
    response = requests.get(url)
    if response.status_code!=200:
        print(response.status_code)
        return curr_coach_id
    soup = BeautifulSoup(response.content, "html.parser")
    tags = soup.find_all("tr")
    for tag in tags:
        columns = tag.find_all("span")
        if columns:
            data["Coach"][curr_coach_id] = {
                "name" : columns[0].text,
                "role" : columns[1].text,
                "coaches": [f"{school} {str(year)}"]
            }
            curr_coach_id+=1
    return curr_coach_id

def scrape_table(
    url:str,
    data:dict, 
    school:str,
    year: int,
    curr_player_id:int,
    curr_coach_id:int
):
    response = requests.get(url)
    if response.status_code!=200:
        print(response.status_code)
        return curr_player_id, curr_coach_id
    soup = BeautifulSoup(response.content, "html.parser")
    tables = soup.find_all("table")
    player_rows = tables[0].find_all("tr")
    for i in range(1, len(player_rows)):
        data["Player"][str(curr_player_id)] = {}
        cols = player_rows[i].find_all(re.compile("td|th"))
        if len(cols)>5:
            for attr in SCHOOL_ATTR_TO_COL[school]:
                if type(SCHOOL_ATTR_TO_COL[school][attr]) is int:
                    value = cols[SCHOOL_ATTR_TO_COL[school][attr]].text.replace("\\n","").strip()
                    data["Player"][str(curr_player_id)][attr] = value if attr!="height" else convert_height_str(value)
                elif type(SCHOOL_ATTR_TO_COL[school][attr]) is list:
                    data["Player"][str(curr_player_id)][attr] = []
                    first_school_listed = True
                    for ind in SCHOOL_ATTR_TO_COL[school][attr]:
                        prev_school = cols[ind].text.replace("\\n","").strip()
                        if SCHOOL_ATTR_TO_COL[school].get("parse_hs") and "/" in prev_school:
                            prev_school = prev_school.split(" / ")[1].strip()
                        data["Player"][str(curr_player_id)][attr].append(prev_school)
                        if first_school_listed:
                            data["School"][prev_school] = {"type": "high school"}
                            first_school_listed = False
                        else:
                            data["School"][prev_school] = {"type": "university"}
            data["Player"][str(curr_player_id)]["plays_for"] = [f"{school} {year}"]
            curr_player_id+=1
    if len(tables)>1:
        coach_rows = tables[1].find_all("tr")
        for i in range(1, len(coach_rows)):
            cols = coach_rows[i].find_all(re.compile("td|th"))
            offset = int(SCHOOL_ATTR_TO_COL[school].get("pic_offset")) if "pic_offset" in SCHOOL_ATTR_TO_COL[school] else 0
            if "Coach" not in cols[1+offset].text:
                continue
            data["Coach"][str(curr_coach_id)]={
                "name": cols[0+offset].text.replace("\\n",""),
                "role": cols[1+offset].text,
                "coaches": [f"{school} {str(year)}"]
            }
            curr_coach_id+=1
    return curr_player_id, curr_coach_id

def get_next_id(json_file):
    data = None
    with open(json_file) as file:
        data = json.load(file)
    return max([int(key) for key in data.keys()])+1

if __name__=="__main__":
    years = [2025, 2026]
    # data not loaded from files, so that deduping is easier
    data = {
        "Player": {},
        "Coach": {},
        "School": {}
    }
    curr_player_id, curr_coach_id = get_next_id("players.json"), get_next_id("coaches.json")
    for school in SIDEARM_SCHOOL_TO_ROSTER_URLS:
        for i in range(len(SIDEARM_SCHOOL_TO_ROSTER_URLS[school])):
            for year in years:
                url = SIDEARM_SCHOOL_TO_ROSTER_URLS[school][i]
                if not url.endswith("coaches/"):
                    curr_player_id, curr_coach_id = scrape_sidearm_roster(url+str(year), data, school, year, curr_player_id, curr_coach_id)
                else:
                    curr_coach_id = scrape_sidearm_coach_page(url+str(year), data, school, year, curr_coach_id)
    """for school in TABLE_SCHOOL_TO_ROSTER_URLS:
        for i in range(len(TABLE_SCHOOL_TO_ROSTER_URLS[school])):
            for year in years:
                url = TABLE_SCHOOL_TO_ROSTER_URLS[school][i]
                curr_player_id, curr_coach_id = scrape_table(
                    url + str(year) if not url.endswith("season/") else f"{url}{year-1}-{str(year)[2:]}",
                    data,
                    school,
                    year,
                    curr_player_id,
                    curr_coach_id
                )"""
    """with open("temp_players.json","w") as file:
        json.dump(data["Player"], file)"""
    with open("temp_coaches.json","w") as file:
        json.dump(data["Coach"], file)
    #coaches_df = dedup.get_df_from_dict(data["Coach"])
    """print("Player duplicates:")
    dedup.check_for_dups(data["Player"])"""
    print("Coach duplicates:\n")
    dedup.check_for_dups(data["Coach"])
    """with open("players.json") as player_file:
        data["Player"]|=json.load(player_file)
    with open("coaches.json") as coach_file:
        data["Coach"]|=json.load(coach_file)
    with open("schools.json") as school_file:
        data["School"]|=json.load(school_file)
    with open("players.json", "w") as player_file:
        json.dump(players_data, player_file)
    with open("coaches.json", "w") as coach_file:
        json.dump(data["Coach"], coach_file)
    with open("schools.json", "w") as schools_file:
        json.dump(data["School"], schools_file)"""
