import requests
import json
from datetime import datetime, date

class APIHandler(object):
    def __init__(self):
        self.key = "JqxDNo9CdaIpMnsXcZlYFbPZmWnxhW4eulavQAbAIHFMXlYpZihY0vVW9WBC"
        self.url_prefix = "https://soccer.sportmonks.com/api/v2.0/"

    def get_seasons(self):
        response_api = requests.get(self.url_prefix + "seasons?api_token=" + self.key)
        raw_json = json.loads(response_api.text)
        seasons = []
        for curr_data in raw_json["data"]:
            # this is the league we've subscribed
            if curr_data["league_id"] == 8:
                print(curr_data)
                seasons.append(curr_data["id"])
        return seasons

    def get_teams_data(self):
        seasons = self.get_seasons()
        teams_set = set()
        teams_data = []
        for season in seasons:
            response_api = requests.get(self.url_prefix + f"teams/season/{season}?api_token=" + self.key)
            raw_json = json.loads(response_api.text)

            for team in raw_json["data"]:
                team_data = dict()
                if team["id"] not in teams_set:
                    team_data["id"] = team["id"]
                    team_data["name"] = team["name"]
                    teams_data.append(team_data)
                    teams_set.add(team["id"])
        return teams_data

    def get_venues_data_by_id(self, venues_ids):
        print("venues ids: ", venues_ids)
        venues_data = []
        for venue_id in venues_ids:
            response_api = requests.get(self.url_prefix + f"venues/{venue_id}?api_token=" + self.key)
            raw_json = json.loads(response_api.text)
            print(raw_json)
            venue = raw_json["data"]
            venues_data.append({"id": venue["id"], "name": venue["name"]})

        return venues_data

    def get_extra_venues_data(self):
        response_api = requests.get(self.url_prefix + "seasons?api_token=" + self.key)
        raw_json = json.loads(response_api.text)
        num_of_pages = raw_json["meta"]["pagination"]["total_pages"]
        seasons = []
        extra_venues_data = []
        venues_ids = set()
        for i in range(1, num_of_pages + 1):
            response_api = requests.get(self.url_prefix + f"seasons?page={i}&api_token=" + self.key)
            raw_json = json.loads(response_api.text)
            for curr_data in raw_json["data"]:
                # this is the league we've subscribed
                if curr_data["league_id"] != 8:
                    seasons.append(curr_data["id"])
        for season_id in seasons:
            response_api = requests.get(self.url_prefix + f"venues/season/{season_id}?api_token=" + self.key)
            raw_json = json.loads(response_api.text)
            for curr_data in raw_json["data"]:
                if curr_data["id"] not in venues_ids:
                    extra_venues_data.append({"id": curr_data["id"], "name": curr_data["name"]})
                    venues_ids.add(curr_data["id"])
                if len(extra_venues_data) > 1500:
                    return extra_venues_data
            print("curr extra venues: ", extra_venues_data)
        return extra_venues_data

    def get_countries_data(self):
        countries_data = []
        for i in range(1, 4):
            response_api = requests.get(self.url_prefix + f"countries?page={i}&api_token=" + self.key)
            raw_json = json.loads(response_api.text)
            for curr_data in raw_json["data"]:
                country_data = dict()
                country_data["id"] = curr_data["id"]
                country_data["name"] = curr_data["name"]
                if curr_data.get("extra") and curr_data["extra"].get("continent"):
                    country_data["continent"] = curr_data["extra"]["continent"]
                else:
                    country_data["continent"] = None
                countries_data.append(country_data)
        return countries_data

    def get_games_data(self):
        games_data = []
        all_teams = set()
        for i in range(1, 70):
            response_api = requests.get(
                self.url_prefix + f"fixtures/between/2004-01-01/2024-01-01?page={i}&leagues=8&api_token=" + self.key)
            raw_json = json.loads(response_api.text)
            # print(raw_json)
            for game in raw_json["data"]:
                # print("here")
                game_data = dict()
                game_data["id"] = game["id"]
                game_data["winner_id"] = game["winner_team_id"]
                game_data["team_id_1"] = game["localteam_id"]
                game_data["team_id_2"] = game["visitorteam_id"]
                game_data["venue_id"] = game["venue_id"]
                all_teams.add(game_data["team_id_1"])
                all_teams.add(game_data["team_id_2"])
                games_data.append(game_data)
        print(all_teams)
        return games_data

    def get_players_data(self):
        players_data = []
        # get all countries
        all_countries_ids = [country["id"] for country in self.get_countries_data()]
        all_teams_ids = [team["id"] for team in self.get_teams_data()]

        # get players by countries
        for i in range(0, len(all_countries_ids)):
            country_id = all_countries_ids[i]
            count_country_players = 0
            response_api = requests.get(self.url_prefix + f"countries/{country_id}/players?api_token=" + self.key)
            raw_json = json.loads(response_api.text)
            num_of_pages = raw_json["meta"]["pagination"]["total_pages"]
            for i in range(1, num_of_pages + 1):
                response_api = requests.get(
                    self.url_prefix + f"countries/{country_id}/players?page={i}&api_token=" + self.key)
                raw_json = json.loads(response_api.text)
                if not raw_json.get("data"):
                    continue
                for player in raw_json["data"]:
                    if player["team_id"] in all_teams_ids:
                        player_data = dict()
                        player_data["id"] = player["player_id"]
                        player_data["name"] = player["fullname"]
                        player_data["team_id"] = player["team_id"]
                        player_data["country_id"] = country_id
                        player_data["age"] = self.get_age_by_birth_date(player["birthdate"])
                        players_data.append(player_data)
                        count_country_players += 1
        return players_data

    def get_top_players_data(self):
        seasons = [12962, 16036, 17420, 18378, 19734]
        top_players_data = []
        all_teams_ids = [team["id"] for team in self.get_teams_data()]
        print(f"teams ids: {all_teams_ids}")
        players_teams = {}
        for season in seasons:
            response_api = requests.get(self.url_prefix + f"topscorers/season/{season}?api_token=" + self.key)
            raw_json = json.loads(response_api.text)
            for player in raw_json["data"]["goalscorers"]["data"]:
                if player['player_id'] in players_teams.keys():
                    player_team_id = players_teams['player_id']
                else:
                    response_api = requests.get(self.url_prefix + f"players/{player['player_id']}?api_token=" + self.key)
                    raw_json = json.loads(response_api.text)
                    player_team_id = raw_json["data"]["team_id"]

                if player_team_id in all_teams_ids:
                    player_data = dict()
                    player_data["position"] = player["position"]
                    player_data["season_id"] = player["season_id"]
                    player_data["player_id"] = player["player_id"]
                    player_data["goals"] = player["goals"]
                    top_players_data.append(player_data)
            print(f"updated top players after season: {season}\n {top_players_data}")
        return top_players_data

    def get_players_to_insert_for_top_players(self, top_players_data, all_players_data, all_teams_ids):
        players_to_insert = []
        for player in top_players_data:
            if player["player_id"] not in all_players_data.keys():
                response_api = requests.get(self.url_prefix + f"players/{player['player_id']}?api_token=" + self.key)
                raw_json = json.loads(response_api.text)
                if raw_json["data"]["team_id"] in all_teams_ids:
                    player_data = dict()
                    player_data["id"] = raw_json["data"]["player_id"]
                    player_data["name"] = raw_json["data"]["fullname"]
                    player_data["team_id"] = raw_json["data"]["team_id"]
                    player_data["country_id"] = raw_json["data"]["country_id"]
                    players_to_insert.append(player_data)
            print(f"num of players to insert = {len(players_to_insert)}")

        print(players_to_insert)
        return players_to_insert

    def get_age_by_birth_date(self, birth_date_str):
        if birth_date_str is None:
            return
        birth_date = datetime.strptime(birth_date_str, '%d/%m/%Y').date()
        today = date.today()
        age = today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
        return age


handler = APIHandler()
# print(handler.get_top_players_data())
# print(handler.get_seasons())
