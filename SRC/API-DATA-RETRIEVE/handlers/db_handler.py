import logging

import mysql.connector
from api_handler import APIHandler

logger = logging.getLogger()
api_handler = APIHandler()
tables = [
    {
        "name": "Teams",
        "columns": "id INT PRIMARY KEY,"
                   "name VARCHAR(255),"
                   "location VARCHAR(255),"
                   "division_id INT,"
    },
    {
        "name": "Games",
        "columns": "id INT PRIMARY KEY,"
                   "date_start DATE,"
                   "season INT,"
                   "attendance INT,"
                   "team_id_1 INT,"
                   "team_id_2 INT,"
                   "FOREIGN KEY (team_id_1) REFERENCES Teams (id) ON UPDATE CASCADE ON DELETE CASCADE,"
                   "FOREIGN KEY (team_id_2) REFERENCES Teams (id) ON UPDATE CASCADE ON DELETE CASCADE,"
    },
    {
        "name": "Positions",
        "columns": "id INT PRIMARY KEY,"
                   "description VARCHAR(255),"
    },
    {
        "name": "Players",
        "columns": "id INT PRIMARY KEY,"
                   "first_name VARCHAR(255),"
                   "last_name VARCHAR(255),"
                   "position_id INT,"
                   "FOREIGN KEY (position_id) REFERENCES Positions (id) ON UPDATE CASCADE ON DELETE CASCADE,"
    },
    {
        "name": "PlayersTeams",
        "columns": "player_id INT,"
                   "team_id INT,"
                   "PRIMARY KEY (player_id, team_id),"
                   "FOREIGN KEY (player_id) REFERENCES Players (id) ON UPDATE CASCADE ON DELETE CASCADE,"
                   "FOREIGN KEY (team_id) REFERENCES Teams (id) ON UPDATE CASCADE ON DELETE CASCADE,"
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
            host="mysqlsrv1.cs.tau.ac.il",
            user="arielireni",
            password="ar17063",
            database="arielireni",
            port=3306,
        )

        return db_connection

    def create_table(self, table_dict):
        logger.info("before query")
        query = "CREATE TABLE IF NOT EXISTS arielireni.{table_name} ({table_colums})".format(
            table_name=table_dict['name'],
            table_colums={table_dict['columns']})
        logger.info("after query")
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

    def insert_to_games_table(self):
        games_data = api_handler.get_games_data()
        cursor = self.db_connection.cursor()
        query = "INSERT INTO Games (id, date_start, season, attendance, team_id_1, team_id_2) VALUES (%s, %s, %s, %s, %s, %s)"
        for game in games_data:
            values = (game["id"], game["date_start"], game["season"], game["attendance"], game["team_id_1"], game["team_id_2"])
            cursor.execute(query, values)
        self.db_connection.commit()
        cursor.close()

    def insert_to_players_table(self):
        players_data = api_handler.get_players_data()
        cursor = self.db_connection.cursor()
        query = "INSERT INTO Players (id, first_name, last_name, position_id) VALUES (%s, %s, %s, %s)"
        for player in players_data:
            values = (player["id"], player["first_name"], player["last_name"], player["position_id"])
            cursor.execute(query, values)
        self.db_connection.commit()
        cursor.close()

    def insert_to_positions_table(self):
        positions_data = api_handler.get_positions_data()
        cursor = self.db_connection.cursor()
        query = "INSERT INTO Positions (id, description) VALUES (%s, %s)"
        for position in positions_data:
            values = (position["id"], position["description"])
            cursor.execute(query, values)
        self.db_connection.commit()
        cursor.close()

    def insert_to_teams_table(self):
        teams_data = api_handler.get_teams_data()
        cursor = self.db_connection.cursor()
        # TODO: maybe we'll need to add " to all string values
        query = "INSERT INTO Teams (id, name, location, position_id) VALUES (%s, %s, %s, %s)"
        for team in teams_data:
            values = (team["id"], team["name"], team["location"], team["division_id"])
            cursor.execute(query, values)
        self.db_connection.commit()
        cursor.close()

    def insert_to_players_teams_table(self):
        players_teams_data = api_handler.get_players_teams_data()
        cursor = self.db_connection.cursor()
        # TODO: maybe we'll need to add " to all string values
        query = "INSERT INTO PlayersTeams (player_id, team_id) VALUES (%s, %s)"
        for player_team in players_teams_data:
            values = (player_team["player_id"], player_team["team_id"])
            cursor.execute(query, values)
        self.db_connection.commit()
        cursor.close()

    def insert_to_all_tables(self):
        self.insert_to_games_table()
        self.insert_to_players_teams_table()
        self.insert_to_positions_table()
        self.insert_to_teams_table()
        self.insert_to_players_teams_table()

# db_handler = DBHandler()
# db_handler.insert_to_table()
