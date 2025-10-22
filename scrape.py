import requests
from bs4 import BeautifulSoup
import re
import json

def scrape_sidearm_roster(url, data, curr_player_id:int, curr_coach_id:int):
    response = requests.get(url)
    if response.status_code!=200:
        return response.status_code
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
                year = stats.find("span",attrs={"data-test-id":re.compile("person-title")})
                height = stats.find("span",attrs={"data-test-id":re.compile("person-season")})
                weight = stats.find("span",attrs={"data-test-id":re.compile("person-weight")})
                data["Player"][curr_player_id]={
                    "name": name.text,
                    "position": position.text[len("Position ")+1:] if position else None,
                    "year": year.text[len("Academic Year")+1:] if year else None,
                    "height": height.text[len("Height")+1:] if height else None,
                    "weight": weight.text[len("Weight")+1:] if weight else None
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
        return response.status_code
    soup = BeautifulSoup(response.content, "html.parser")
    tags = soup.find_all("tr")
    for tag in tags:
        columns = tag.find_all("span")
        data["Coach"][curr_coach_id] = {
            "name" : columns[0].text,
            "position" : columns[1].text
        }
        curr_coach_id+=1
    return curr_coach_id


sidearm_rosters = [
    "https://goheels.com/sports/baseball/roster",
    "https://floridagators.com/sports/baseball/roster",
    "https://hailstate.com/sports/baseball/roster"
]
sidearm_coach_pages = [
    "https://hailstate.com/sports/baseball/coaches"
]
data = {
    "Player": {},
    "Coach": {}
}
curr_player_id, curr_coach_id = 0, 0
for i in range(len(sidearm_rosters)):
    curr_player_id, curr_coach_id = scrape_sidearm_roster(sidearm_rosters[i], data, curr_player_id, curr_coach_id)

for i in range(len(sidearm_coach_pages)):
    curr_coach_id = scrape_sidearm_coach_page(sidearm_coach_pages[i],data, curr_coach_id)

with open("players.json", "w") as player_file:
    json.dump(data["Player"], player_file)

with open("coaches.json", "w") as coach_file:
    json.dump(data["Coach"], coach_file)

