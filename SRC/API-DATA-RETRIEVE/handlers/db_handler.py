import mysql

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

    def connect(self, database=None):
        db_connection = mysql.connector.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            port=self.port,
            database=database
        )

        return db_connection

    def create_table(self, table_dict):
        query = f"CREATE TABLE IF NOT EXISTS {table_dict['name']} {table_dict['columns']}"
        cursor = self.db_connection.cursor()
        try:
            cursor.execute(query)
        except Exception as e:
            print("failed to execute query with error: ", e)
        finally:
            cursor.close()

    def create_all_tables(self):
        for table_dict in tables:
            self.create_table(table_dict)
