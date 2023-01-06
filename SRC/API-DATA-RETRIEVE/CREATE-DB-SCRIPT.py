from handlers.db_handler import DBHandler

if __name__ == "main":
    print("!!! 1")
    db_handler = DBHandler()
    print("!!! 2")
    db_handler.create_all_tables()
    print("!!! 3")
    db_handler.db_connection.close()
    print("!!! 4")
