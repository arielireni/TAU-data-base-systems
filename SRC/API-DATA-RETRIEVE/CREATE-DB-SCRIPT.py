from handlers.db_handler import DBHandler

if __name__ == "main":
  db_handler = DBHandler(host="localhost", port=3305)
  db_handler.create_all_tables()
