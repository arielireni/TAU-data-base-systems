from db_handler import DBHandler

db_handler = DBHandler(host="localhost", port=3305)
db_handler.insert_to_all_tables()
db_handler.db_connection.close()
