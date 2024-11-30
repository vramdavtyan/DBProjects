import pandas as pd
from pymongo import MongoClient
import ast
from DFStruct import *

# Connect MongoDB create/recreate DB,tables
def Mongo_connect(*args):
    
    df_shots=args[0]
    df_matches=args[1]
    df_players=args[2]
    df_teams=args[3]

    # Connect to MongoDB locally
    client = MongoClient("mongodb://localhost:27017")

    # Connect to MongoDB atlas
    #client = MongoClient("mongodb+srv://<username>:<password>@<cluster-url>/") # YOU SHOULD CHANGE TO YOUR CREDENTIALS AND SERVER INFO, just copy the ones from mongo atlas connect

    client.drop_database("DB_football")
    print("Database dropped")

    db = client['DB_football'] 
    # Drop previous tables
    db['shots'].drop()
    db['matches'].drop()
    db['players'].drop()
    db['teams'].drop()

    print("shots Collection dropped")
    print("matches Collection dropped")
    print("players Collection dropped")
    print("teams Collection dropped")

    # Create tables
    shots_collection = db["shots"]
    matches_collection = db["matches"]
    players_collection = db["players"]
    teams_collection = db["teams"]

    shots_initial_count = shots_collection.count_documents({})
    matches_initial_count = matches_collection.count_documents({})
    players_initial_count = players_collection.count_documents({})
    teams_initial_count = teams_collection.count_documents({})

    # print(f"# documents: {initial_count_shots}") 
    try:
        shots_dict = df_shots.to_dict(orient="records")
        matches_dict = df_matches.to_dict(orient="records")
        players_dict = df_players.to_dict(orient="records")
        teams_dict = df_teams.to_dict(orient="records")

        shots_result = shots_collection.insert_many(shots_dict, ordered=False)
        matches_result = matches_collection.insert_many(matches_dict, ordered=False)
        players_result = players_collection.insert_many(players_dict, ordered=False)
        teams_result = teams_collection.insert_many(teams_dict, ordered=False)

        shots_valid_count = shots_collection.count_documents({}) - shots_initial_count
        matches_valid_count = matches_collection.count_documents({}) - matches_initial_count
        players_valid_count = players_collection.count_documents({}) - players_initial_count
        teams_valid_count = teams_collection.count_documents({}) - teams_initial_count

        # print(f"{valid_count} documents inserted.")

    except Exception as e:
        shots_attempted_count = len(shots_dict)
        shots_valid_count = shots_collection.count_documents({}) - shots_initial_count

        matches_attempted_count = len(matches_dict)
        matches_valid_count = matches_collection.count_documents({}) - matches_initial_count

        players_attempted_count = len(players_dict)
        players_valid_count = players_collection.count_documents({}) - players_initial_count

        teams_attempted_count = len(teams_dict)
        teams_valid_count = teams_collection.count_documents({}) - teams_initial_count

        print(f"{shots_valid_count} shots documents inserted")
        print(f"{shots_attempted_count - shots_valid_count} shots documents failed to insert")

        print(f"{matches_valid_count} matches documents inserted")
        print(f"{matches_attempted_count - matches_valid_count} matches documents failed to insert")

        print(f"{players_valid_count} players documents inserted")
        print(f"{players_attempted_count - players_valid_count} players documents failed to insert")

        print(f"{teams_valid_count} teams documents inserted")
        print(f"{teams_attempted_count - teams_valid_count} teams documents failed to insert")

    print(f"# shots documents: {shots_collection.count_documents({})}")
    print(f"# matches documents: {matches_collection.count_documents({})}")
    print(f"# players documents: {players_collection.count_documents({})}")
    print(f"# teams documents: {teams_collection.count_documents({})}")

    # client.close()

    return client,shots_collection,matches_collection,players_collection,teams_collection

# Drop columns from tables
def handle_all_tables(*args):
    shots_collection=args[0]
    matches_collection=args[1]
    players_collection=args[2]
    teams_collection=args[3]

    shots_drop_columns=args[4]
    matches_drop_columns=args[5]
    players_drop_columns=args[6]
    teams_drop_columns=args[7]

    # Delete columns from each table
    for field_to_remove in shots_drop_columns:
        shots_collection.update_many({}, {"$unset": {field_to_remove: ""}})
        
    for field_to_remove in matches_drop_columns:
        matches_collection.update_many({}, {"$unset": {field_to_remove: ""}})

    for field_to_remove in players_drop_columns:
        players_collection.update_many({}, {"$unset": {field_to_remove: ""}})

    for field_to_remove in teams_drop_columns:
        teams_collection.update_many({}, {"$unset": {field_to_remove: ""}})


    return shots_collection,matches_collection,players_collection,teams_collection

# get client,collections and csvs
def get_all_data():    
    # get cloumns to drop
    shots_drop_columns,matches_drop_columns,players_drop_columns,teams_drop_columns=get_drop_columns()
    # create df from csvs
    df_shots,df_matches,df_players,df_teams=read_all_csvs(shots_drop_columns,matches_drop_columns,players_drop_columns,teams_drop_columns)
    # create collection from dfs
    client,shots_collection,matches_collection,players_collection,teams_collection=Mongo_connect(df_shots,df_matches,df_players,df_teams)
    # drop columns in collections
    shots_collection,matches_collection,players_collection,teams_collection=handle_all_tables(shots_collection,matches_collection,players_collection,teams_collection,shots_drop_columns,matches_drop_columns,players_drop_columns,teams_drop_columns)

    return client,shots_collection,matches_collection,players_collection,teams_collection,df_shots,df_matches,df_players,df_teams


