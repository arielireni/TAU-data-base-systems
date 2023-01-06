import requests
import json


class APIHandler(object):
    def __init__(self):
        self.key = "7iPmwT0hc2XS4GWDqJoOeZHU2l1ziT8K"
        self.url_prefix = "http://api.cfl.ca/v1/"

    def get_games_data(self):
        games_data = []
        response_api = requests.get(self.url_prefix + "games/2015?key=" + self.key)
        raw_json = json.loads(response_api.text)
        for game in raw_json["data"]:
            game_data = dict()
            game_data["id"] = game["game_id"]
            game_data["date_start"] = game["date_start"]
            game_data["season"] = game["season"]
            game_data["attendance"] = game["attendance"]
            game_data["team_id_1"] = game["team_1"]["team_id"]
            game_data["team_id_2"] = game["team_2"]["team_id"]
            games_data.append(game_data)
        return games_data

    def get_players_data(self):
        players_data = []
        response_api = requests.get(self.url_prefix + "players?key=" + self.key)
        raw_json = json.loads(response_api.text)
        for player in raw_json["data"]:
            player_data = dict()
            player_data["id"] = player["cfl_central_id"]
            player_data["first_name"] = player["first_name"]
            player_data["last_name"] = player["last_name"]
            player_data["position_id"] = player["position"]["position_id"]
            players_data.append(player_data)
        return players_data

    def get_positions_data(self):
        positions_data = {}
        response_api = requests.get(self.url_prefix + "players?key=" + self.key)
        raw_json = json.loads(response_api.text)
        for player in raw_json["data"]:
            position_id = player["position"]["position_id"]
            description = player["position"]["description"]
            if position_id not in positions_data.keys():
                positions_data[position_id] = description

        result = []
        for position, description in positions_data.items():
            result.append({"position_id": position, "description": description})
        return result

    def get_teams_data(self):
        teams_data = []
        response_api = requests.get(self.url_prefix + "teams?key=" + self.key)
        raw_json = json.loads(response_api.text)
        for team in raw_json["data"]:
            team_data = dict()
            team_data["id"] = team["team_id"]
            team_data["name"] = team["full_name"]
            team_data["location"] = team["location"]
            team_data["division_id"] = team["division_id"]
            teams_data.append(team_data)
        return teams_data

    def get_players_teams_data(self):
        players_teams_data = []
        response_api = requests.get(self.url_prefix + "players?include=current_team&key=" + self.key)
        raw_json = json.loads(response_api.text)
        for player in raw_json["data"]:
            player_team_data = dict()
            player_team_data["player_id"] = player["cfl_central_id"]
            # TODO: add team here !!!
            player_team_data["team_id"] = ""
            players_teams_data.append(player_team_data)
        return players_teams_data

# handler = APIHandler()
# handler.get_games_data()
