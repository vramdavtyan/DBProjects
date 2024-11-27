import pandas as pd
from pymongo import MongoClient

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


# read all csvs
def read_all_csvs(*args):
    shots_drop_columns=args[0]
    matches_drop_columns=args[1]
    players_drop_columns=args[2]
    teams_drop_columns=args[3]

    df_shots=pd.read_csv('data/shots.csv',index_col=0)
    df_shots=df_shots.drop(columns=shots_drop_columns)

    df_matches=pd.read_csv('data/matches.csv',index_col=0)
    df_matches=df_matches.drop(columns=matches_drop_columns)

    # don't drop id for players
    df_players=pd.read_csv('data/all_players.csv')
    df_players=df_players.drop(columns=players_drop_columns)

    # get name and id from id -> en/players/id/name
    df_players['name']=df_players['id'].str.split('/').str[4]
    df_players['id']=df_players['id'].str.split('/').str[3]
    # reorder columns
    df_players = df_players.loc[:, ['id', 'name','Pos', 'Matches', 'club_id', 'MP', 'year']]


    df_teams=pd.read_csv('data/teams.csv',index_col=0)
    df_teams=df_teams.drop(columns=teams_drop_columns)


    return df_shots,df_matches,df_players,df_teams
        
# get client,collections and csvs
def get_all_data():
    shots_drop_columns=["Minute","Squad","xG","PSxG","Notes","SCA 1_Player","SCA 1_Event","SCA 2_Player","SCA 2_Event"]
    matches_drop_columns=['Unnamed: 1', 'Referee', 'Notes', 'played']
    players_drop_columns=["Per 90 Minutes_G-PK", "Per 90 Minutes_G+A-PK", "Per 90 Minutes_xG",
                          "Per 90 Minutes_xAG", "Per 90 Minutes_xG+xAG", "Per 90 Minutes_npxG",
                          "Per 90 Minutes_npxG+xAG",'Playing Time_MP', 'Playing Time_Starts', 
                          'Playing Time_Min','Playing Time_90s', 'Performance_Gls', 
                          'Performance_Ast','Performance_G+A', 'Performance_G-PK', 
                          'Performance_PK','Performance_PKatt', 'Performance_CrdY', 
                          'Performance_CrdR','Expected_xG', 'Expected_npxG', 'Expected_xAG', 
                          'Expected_npxG+xAG','Progression_PrgC', 'Progression_PrgP', 
                          'Progression_PrgR','Per 90 Minutes_Gls', 'Per 90 Minutes_Ast', 
                          'Per 90 Minutes_G+A']
    teams_drop_columns=[]


    df_shots,df_matches,df_players,df_teams=read_all_csvs(shots_drop_columns,matches_drop_columns,players_drop_columns,teams_drop_columns)
    client,shots_collection,matches_collection,players_collection,teams_collection=Mongo_connect(df_shots,df_matches,df_players,df_teams)
    shots_collection,matches_collection,players_collection,teams_collection=handle_all_tables(shots_collection,matches_collection,players_collection,teams_collection,shots_drop_columns,matches_drop_columns,players_drop_columns,teams_drop_columns)

    return client,shots_collection,matches_collection,players_collection,teams_collection,df_shots,df_matches,df_players,df_teams


