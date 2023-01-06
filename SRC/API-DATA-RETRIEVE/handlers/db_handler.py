import logging

import mysql.connector

logger = logging.getLogger()
tables = [
    {
        "name": "Game",
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
        "name": "Players",
        "columns": "id INT PRIMARY KEY,"
                   "first_name VARCHAR(255),"
                   "last_name VARCHAR(255),"
                   "position_id INT,"
                   "FOREIGN KEY (position_id) REFERENCES Positions (id) ON UPDATE CASCADE ON DELETE CASCADE,"
    },
    {
        "name": "Positions",
        "columns": "id INT PRIMARY KEY,"
                   "description VARCHAR(255),"
    },
    {
        "name": "Teams",
        "columns": "id INT PRIMARY KEY,"
                   "name VARCHAR(255),"
                   "location VARCHAR(255),"
                   "division_id INT,"
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
        query = "CREATE TABLE IF NOT EXISTS arielireni.{table_name} ({table_colums})".format(table_name=table_dict['name'],
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

    def insert_to_table(self):
        query = "INSERT INTO PlayersTeams (player_id, team_id) VALUES (%s, %s)"
        values = (1, 2)
        cursor = self.db_connection.cursor()
        cursor.execute(query, values)
        self.db_connection.commit()
        logger.info(cursor.rowcount, "record inserted")
        cursor.close()