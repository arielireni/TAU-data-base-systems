from SRC.CREATE_DB_SCRIPT.db_handler import DBHandler
import pandas as pd
from tabulate import tabulate

db_handler = DBHandler()


def query_1():
    """
    returns all the European players under age of x
    """
    age_limit = str(input('Insert the desired age limit: '))
    if not age_limit.isnumeric():
        return "Invalid parameter passed"

    try:
        query = f"SELECT p.name as player_name, " \
                f"(SELECT c.name FROM Countries c WHERE c.id = p.country_id) as country_name," \
                f"p.age " \
                f"FROM Players p " \
                f"WHERE " \
                f"(SELECT c.continent FROM Countries c WHERE c.id = p.country_id) = 'Europe' " \
                f"AND p.age < {age_limit}"

        query_output = db_handler.execute_query(query)
        if len(query_output) == 0:
            print("Sorry, we didn't find the information you requested. \n Please with try again with different "
                  "parameters. ")
        else:
            print(f"All the European players under age of {age_limit} are: ")
            print(tabulate(query_output, headers=['Name', 'Country', 'Age']))
    except:
        print("We had trouble finding the data you requested. Please try again.")


def query_2():
    """
    returns the venue which team x lost the most in, and the number of looses
    """
    desired_team = str(input('Insert the desired team: '))

    try:
        query = f"SELECT t.name as team_name, v.name as venue_name, MAX(losses.num_losses) as number_of_losses " \
                f"FROM Teams t, Venues v, (SELECT team_id_1 as team_id, venue_id, COUNT(*) as num_losses " \
                f"FROM Games WHERE (team_id_1 = team_id_1 OR team_id_2 = team_id_1) " \
                f"AND winner_id != team_id_1 GROUP BY team_id_1, venue_id) as losses " \
                f"WHERE t.id = losses.team_id AND v.id = " \
                f"(SELECT venue_id " \
                f"FROM (SELECT team_id_1 as team_id, venue_id, COUNT(*) as num_losses " \
                f"FROM Games WHERE (team_id_1 = team_id_1 OR team_id_2 = team_id_1) AND winner_id != team_id_1 " \
                f"GROUP BY team_id_1, venue_id) as all_losses " \
                f"WHERE all_losses.team_id = losses.team_id ORDER BY num_losses DESC LIMIT 1) " \
                f"AND t.name = '{desired_team}' " \
                f"GROUP BY t.name, v.name"

        query_output = db_handler.execute_query(query)
        if len(query_output) == 0:
            print("Sorry, we didn't find the information you requested. \n Please with try again with different "
                  "parameters. ")
        else:
            print(f"The venue which team {desired_team} lost the most in, and the number of looses:")
            print(tabulate(query_output, headers=['Team', 'Name', 'Number of losses']))
    except:
        print("We had trouble finding the data you requested. Please try again.")


def query_3():
    """
    returns the top x teams who have the most players which are top scorers
    """
    desired_limit = str(input('Insert the desired number of teams: '))
    if not desired_limit.isnumeric():
        return "Invalid parameter passed"

    try:
        query = f"SELECT t.name as team_name, Count(*) as num_of_players " \
                f"FROM Teams as t, TopPlayers as top, Players as p " \
                f"WHERE t.id = p.team_id AND p.id = top.player_id " \
                f"GROUP BY t.id " \
                f"ORDER BY num_of_players " \
                f"DESC LIMIT {desired_limit}"

        query_output = db_handler.execute_query(query)
        if len(query_output) == 0:
            print("Sorry, we didn't find the information you requested. \n Please with try again with different "
                  "parameters. ")
        else:
            print(f"the top {desired_limit} teams who have the most players which are top scorers:")
            print(tabulate(query_output, headers=['Name', 'Number of players']))
    except:
        print("We had trouble finding the data you requested. Please try again.")


def query_4():
    """
    returns the home winning percentage of team x
    """
    desired_team_name = str(input('Insert the desired team name: '))

    try:
        query = f"SELECT t.name as team_name, " \
                f"(SELECT COUNT(*) FROM Games g " \
                f"WHERE g.team_id_1 = t.id AND g.winner_id = t.id) / (SELECT COUNT(*) FROM Games g " \
                f"WHERE g.team_id_1 = t.id OR g.team_id_2 = t.id) as home_win_percentage " \
                f"FROM Teams t " \
                f"WHERE t.name = '{desired_team_name}' " \
                f"ORDER BY home_win_percentage DESC"

        query_output = db_handler.execute_query(query)
        if len(query_output) == 0:
            print("Sorry, we didn't find the information you requested. \n Please with try again with different "
                  "parameters. ")
        else:
            print(f"The home winning percentage of team {desired_team_name}:")
            print(tabulate(query_output, headers=['Name', 'Home win percentage']))
    except:
        print("We had trouble finding the data you requested. Please try again.")


def query_5():
    """
    returns all the players from country x which are top scorers and under age of y
    """
    desired_country_name = str(input('Insert the desired country name: '))
    desired_age_limit = str(input('Insert the desired age limit: '))
    if not desired_age_limit.isnumeric():
        return "Invalid age limit parameter passed"

    try:
        query = f"SELECT p.name, top.goals " \
                f"FROM Players as p, TopPlayers as top, Countries as c " \
                f"WHERE p.id = top.player_id " \
                f"AND p.country_id = c.id " \
                f"AND c.name = '{desired_country_name}' " \
                f"AND p.age < {desired_age_limit} " \
                f"ORDER BY top.goals DESC"

        query_output = db_handler.execute_query(query)
        if len(query_output) == 0:
            print("Sorry, we didn't find the information you requested. \n Please with try again with different "
                  "parameters. ")
        else:
            print(f"The players from country {desired_country_name} which are top scorers and under age of {desired_age_limit}:")
            print(tabulate(query_output, headers=['Name', 'Goals']))
    except:
        print("We had trouble finding the data you requested. Please try again.")


def query_6():
    """
    returns the player data by the user's keyword
    """
    print("Enter the keyword for searching a player...")
    user_keyword = input()
    try:
        query = f"SELECT p.name as player_name , c.name as country_name, t.name as team_name " \
                f"FROM Players as p, Countries as c, Teams as t " \
                f"WHERE p.name LIKE '%{user_keyword}%' " \
                f"AND p.country_id = c.id " \
                f"AND p.team_id = t.id"

        query_output = db_handler.execute_query(query)
        if len(query_output) == 0:
            print("Sorry, we didn't find the information you requested. \n Please with try again with different "
                  "parameters. ")
        else:
            print(f"Here are the players we've found by your keyword:")
            print(tabulate(query_output, headers=['Player', 'Country', 'Team']))
    except:
        print("We had trouble finding the data you requested. Please try again.")


def main():
    if hasattr(__builtins__, 'raw_input'):
        __builtins__.input = getattr(__builtins__, 'raw_input')
    queries = [query_1, query_2, query_3, query_4, query_5, query_6]
    print('Press the query number you would like to execute:')
    print('1 - all the European players under age of x')
    print('2 - the venue which team x lost the most in, and the number of looses')
    print('3 - the top x teams who have the most players which are top scorers')
    print('4 - the home winning percentage of team x')
    print('5 - all the players from country x which are top scorers and under age of y')
    print('6 - the player data by the user keyword')
    query_num = input()
    while query_num not in ['1', '2', '3', '4', '5', '6']:
        print('Invalid query number. Please try again')
        query_num = input()
    queries[int(query_num) - 1]()


if __name__ == '__main__':
    main()
