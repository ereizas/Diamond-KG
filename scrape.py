import requests
from bs4 import BeautifulSoup
import re
import json

SCHOOL_TO_ROSTER_URLS = {
    "University of North Carolina": ["https://goheels.com/sports/baseball/roster/"],
    "University of Florida": ["https://floridagators.com/sports/baseball/roster/"],
    "Mississippi State University": [
        "https://hailstate.com/sports/baseball/roster/",
        "https://hailstate.com/sports/baseball/coaches/"
    ]
}

def scrape_sidearm_roster(url, data, curr_player_id:int, curr_coach_id:int):
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
        if not get_coaches:
            stats = tag.find("div",attrs={"class":re.compile("bio-stats")})
            if stats:
                # TODO scrape relationships
                # TODO scrape highschools
                position = stats.find("span",attrs={"data-test-id":re.compile("person-position")})
                year = stats.find("span",attrs={"data-test-id":re.compile("person-title")})
                height = stats.find("span",attrs={"data-test-id":re.compile("person-season")})
                weight = stats.find("span",attrs={"data-test-id":re.compile("person-weight")})
                data["Player"][curr_player_id]={
                    "name": name.text,
                    "position": position.text[len("Position ")+1:].strip() if position else None,
                    "year": year.text[len("Academic Year")+1:].strip() if year else None,
                    "height": height.text[len("Height")+1:].strip() if height else None,
                    "weight": int(weight.text[len("Weight")+1:weight.text.rfind("lbs")].strip()) if weight else None
                }
                curr_player_id+=1
        else:
            role = tag.find("div", attrs={"class": None, "data-test-id": None})
            data["Coach"][curr_coach_id]={
                "name": name.text,
                "role": role.text if role else None
            }
            curr_coach_id+=1
    return curr_player_id, curr_coach_id

def scrape_sidearm_coach_page(url, data, curr_coach_id):
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
                "position" : columns[1].text
            }
            curr_coach_id+=1
    return curr_coach_id

if __name__=="__main__":
    years = [2025, 2026]
    data = {
        "Player": {},
        "Coach": {}
    }
    relationships = {
        "plays_for" : {},
        "coaches": {},
        "attended": {}
    }
    curr_player_id, curr_coach_id = 0, 0
    for school in SCHOOL_TO_ROSTER_URLS:
        for i in range(len(SCHOOL_TO_ROSTER_URLS[school])):
            for year in years:
                url = SCHOOL_TO_ROSTER_URLS[school][i]
                if not url.endswith("coaches/"):
                    curr_player_id, curr_coach_id = scrape_sidearm_roster(url+str(year), data, curr_player_id, curr_coach_id)
                else:
                    curr_coach_id = scrape_sidearm_coach_page(url+str(year),data, curr_coach_id)

    with open("players.json", "w") as player_file:
        json.dump(data["Player"], player_file)
    with open("coaches.json", "w") as coach_file:
        json.dump(data["Coach"], coach_file)
