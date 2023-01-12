import json
import logging

import mysql.connector
import requests

from api_handler import APIHandler

logger = logging.getLogger()
api_handler = APIHandler()
tables = [
    {
        "name": "Teams",
        "columns": "id INT PRIMARY KEY,"
                   "name VARCHAR(255)"
    },
    {
        "name": "Venues",
        "columns": "id INT PRIMARY KEY,"
                   "name VARCHAR(255)"
    },
    {
        "name": "Games",
        "columns": "id INT PRIMARY KEY,"
                   "winner_id INT," 
                   "team_id_1 INT,"
                   "team_id_2 INT,"
                   "venue_id INT," 
                   "FOREIGN KEY (team_id_1) REFERENCES Teams (id) ON UPDATE CASCADE ON DELETE CASCADE,"
                   "FOREIGN KEY (team_id_2) REFERENCES Teams (id) ON UPDATE CASCADE ON DELETE CASCADE,"
                   "FOREIGN KEY (winner_id) REFERENCES Teams (id) ON UPDATE CASCADE ON DELETE CASCADE,"
                   "FOREIGN KEY (venue_id) REFERENCES Venues (id) ON UPDATE CASCADE ON DELETE CASCADE"
    },
    {
        "name": "Countries",
        "columns": "id INT PRIMARY KEY,"
                   "name VARCHAR(255),"
                   "continent VARCHAR(255)"
    },
    {
        "name": "Players",
        "columns": "id INT PRIMARY KEY,"
                   "name VARCHAR(255),"
                   "team_id INT,"
                   "country_id INT,"
                   "age INT,"
                   "FOREIGN KEY (team_id) REFERENCES Teams (id) ON UPDATE CASCADE ON DELETE CASCADE,"
                   "FOREIGN KEY (country_id) REFERENCES Countries (id) ON UPDATE CASCADE ON DELETE CASCADE,"
                   "INDEX player_name USING HASH (name)"
    },
    {
        "name": "TopPlayers",
        "columns": "position INT,"
                   "season_id INT,"
                   "player_id INT,"
                   "goals INT,"
                   "PRIMARY KEY (position, season_id),"
                   "FOREIGN KEY (player_id) REFERENCES Players (id) ON UPDATE CASCADE ON DELETE CASCADE"
    },

]


class DBHandler(object):
    def __init__(self, host="mysqlsrv1.cs.tau.ac.il", user="arielireni", password="ar17063", port=3306):
        self.host = host,
        self.user = user,
        self.password = password,
        self.port = port
        self.db_connection = self.connect()
        logger.info(f"connection: {self.db_connection}")

    def connect(self):
        db_connection = mysql.connector.connect(
            host="localhost",
            user="arielireni",
            password="ar17063",
            database="arielireni",
            port=3305,
        )

        return db_connection

    def create_table(self, table_dict):
        logger.info("before query")
        query = "CREATE TABLE IF NOT EXISTS arielireni.{table_name} ({table_colums})".format(
            table_name=table_dict['name'],
            table_colums=table_dict['columns'])
        logger.info("after query")
        print(self.db_connection.server_port)
        cursor = self.db_connection.cursor()
        try:
            cursor.execute(query)
        except Exception as e:
            logger.info("failed to execute query with error: ", str(e))
        finally:
            cursor.close()

    def create_all_tables(self):
        for table_dict in tables:
            self.create_table(table_dict)

    def insert_to_teams_table(self):
        teams_data = api_handler.get_teams_data()
        cursor = self.db_connection.cursor()
        query = "INSERT INTO Teams (id, name) VALUES (%s, %s)"
        for team in teams_data:
            values = (team["id"], team["name"])
            cursor.execute(query, values)
        self.db_connection.commit()
        cursor.close()

    def insert_to_venues_table(self, venues_ids):
        venues_data = api_handler.get_venues_data_by_id(venues_ids)
        cursor = self.db_connection.cursor()
        query = "INSERT INTO Venues (id, name) VALUES (%s, %s)"
        for venue in venues_data:
            values = (venue["id"], venue["name"])
            cursor.execute(query, values)
        self.db_connection.commit()
        cursor.close()

    def insert_to_venues_games_table(self):
        count_unmatched = 0
        unmatched_venues = set()
        games_data = api_handler.get_games_data()
        all_venues = set()
        for game in games_data:
            if game["venue_id"]:
                all_venues.add(game["venue_id"])
        # insert all venues by venue id
        self.insert_to_venues_table(all_venues)

        cursor = self.db_connection.cursor()
        query = "INSERT INTO Games (id, winner_id, team_id_1, team_id_2, venue_id) VALUES (%s, %s, %s, %s, %s)"
        for game in games_data:
            values = (game["id"], game["winner_id"], game["team_id_1"], game["team_id_2"], game["venue_id"])
            print(values)
            try:
                cursor.execute(query, values)
            except:
                count_unmatched += 1
                unmatched_venues.add(game["venue_id"])
                continue
        self.db_connection.commit()
        cursor.close()

    def insert_more_venues_to_venues_table(self):
        venues_data = api_handler.get_extra_venues_data()
        cursor = self.db_connection.cursor()
        query = "INSERT INTO Venues (id, name) VALUES (%s, %s)"
        for venue in venues_data:
            values = (venue["id"], venue["name"])
            print(values)
            try:
                cursor.execute(query, values)
                print("succeeded")
            except:
                print("failed")
                continue
        self.db_connection.commit()
        cursor.close()

    def insert_to_countries_table(self):
        countries_data = api_handler.get_countries_data()
        cursor = self.db_connection.cursor()
        query = "INSERT INTO Countries (id, name, continent) VALUES (%s, %s, %s)"
        for country in countries_data:
            values = (country["id"], country["name"], country["continent"])
            cursor.execute(query, values)
        self.db_connection.commit()
        cursor.close()

    def insert_to_players_table(self):
        players_data = api_handler.get_players_data()
        cursor = self.db_connection.cursor()
        query = "INSERT INTO Players (id, name, team_id, country_id, age) VALUES (%s, %s, %s, %s, %s)"
        for player in players_data:
            values = (player["id"], player["name"], player["team_id"], player["country_id"], player["age"])
            cursor.execute(query, values)
        self.db_connection.commit()
        cursor.close()

    def insert_top_players_to_players_table(self):
        top_players_data = api_handler.get_top_players_data()
        all_teams_ids = [team["id"] for team in api_handler.get_teams_data()]

        all_players_data = {}
        cursor = self.db_connection.cursor()
        query = "SELECT * FROM Players"
        cursor.execute(query)
        for row in cursor:
            all_players_data.update({row[0]: row})

        players_to_insert = api_handler.get_players_to_insert_for_top_players(top_players_data, all_players_data, all_teams_ids)

        # insert non-existing players
        query = "INSERT INTO Players (id, name, team_id, country_id) VALUES (%s, %s, %s, %s)"
        for player in players_to_insert:
            values = (player["id"], player["name"], player["team_id"], player["country_id"])
            cursor.execute(query, values)
        self.db_connection.commit()
        cursor.close()

    def insert_to_top_players_table(self):
        top_players_data = api_handler.get_top_players_data()
        print("final top players data: ", top_players_data)
        cursor = self.db_connection.cursor()
        query = "INSERT INTO TopPlayers (position, season_id, player_id, goals) VALUES (%s, %s, %s, %s)"
        for top_player in top_players_data:
            values = (top_player["position"], top_player["season_id"], top_player["player_id"], top_player["goals"])
            print(values)
            cursor.execute(query, values)
        self.db_connection.commit()
        cursor.close()

    def insert_to_all_tables(self):
        # TODO
        return

    def execute_query(self, query):
        cursor = self.db_connection.cursor()
        try:
            cursor.execute(query)
            return cursor.fetchall()
        finally:
            cursor.close()
            self.db_connection.close()

    def fix_players_with_age(self):
        cursor = self.db_connection.cursor()
        query = "SELECT * FROM Players"
        cursor.execute(query)
        players_ids = set()
        for row in cursor:
            players_ids.add(row[0])
        for player_id in players_ids:
            response = requests.get(f'{api_handler.url_prefix}players/{player_id}?api_token={api_handler.key}')
            data = json.loads(response.text)['data']
            birthdate_str = data['birthdate']
            age = api_handler.get_age_by_birth_date(birthdate_str)
            if age:
                query = f"UPDATE Players SET age = {age} WHERE id = {player_id}"
            else:
                query = f"UPDATE Players SET age = null WHERE id = {player_id}"
            print(query)
            print("updated player: ", player_id)
            cursor.execute(query)
        self.db_connection.commit()
        cursor.close()


db_handler = DBHandler(host="localhost", port=3305)
# db_handler.insert_to_table()
# cursor = db_handler.db_connection.cursor()
# query = "INSERT INTO Teams (id, name) VALUES (%s, %s)"
# cursor.execute(query, (1, "test"))
# db_handler.db_connection.commit()
# cursor.close()

# query = "CREATE TABLE IF NOT EXISTS arielireni.{table_name} ({table_colums})".format(
#     table_name="test",
#     table_colums="position INT")
# cursor = db_handler.db_connection.cursor()
# cursor.execute(query)
# cursor.close()

db_handler.insert_more_venues_to_venues_table()
