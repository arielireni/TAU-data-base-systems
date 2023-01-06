from handlers.db_handler import DBHandler

if __name__ == "main":
  db_handler = DBHandler()
  db_handler.create_all_tables()
  db_handler.db_connection.close()
