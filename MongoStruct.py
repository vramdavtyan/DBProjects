import pandas as pd
from pymongo import MongoClient
# import mysql.connector


# Read csv and drop columns
def handle_csv(file_name,drop_columns):
    df=pd.read_csv(file_name,index_col=0)
    df=df.drop(columns=drop_columns)

    return df

# Connect MongoDB create/recreate DB,tables
def Mongo_connect(df_shots,df_matches):
    # Connect to MongoDB locally
    client = MongoClient("mongodb://localhost:27017")

    # Connect to MongoDB atlas
    #client = MongoClient("mongodb+srv://<username>:<password>@<cluster-url>/") # YOU SHOULD CHANGE TO YOUR CREDENTIALS AND SERVER INFO, just copy the ones from mongo atlas connect

    client.drop_database("DB_football")
    print("Database dropped")

    db = client['DB_football'] # you can use the name you want for the database
    db['shots'].drop()
    print("Collection dropped")

    shots_collection = db["shots"]
    matches_collection = db["matches"]

    shots_initial_count = shots_collection.count_documents({})
    matches_initial_count = matches_collection.count_documents({})

    # print(f"# documents: {initial_count_shots}") 
    try:
        shots_dict = df_shots.to_dict(orient="records")
        matches_dict = df_matches.to_dict(orient="records")

        shots_result = shots_collection.insert_many(shots_dict, ordered=False)
        matches_result = matches_collection.insert_many(matches_dict, ordered=False)

        shots_valid_count = shots_collection.count_documents({}) - shots_initial_count
        matches_valid_count = matches_collection.count_documents({}) - matches_initial_count

        # print(f"{valid_count} documents inserted.")

    except Exception as e:
        shots_attempted_count = len(shots_dict)
        shots_valid_count = shots_collection.count_documents({}) - shots_initial_count

        matches_attempted_count = len(matches_dict)
        matches_valid_count = matches_collection.count_documents({}) -matches_initial_count

        print(f"{shots_valid_count} shots documents inserted")
        print(f"{shots_attempted_count - shots_valid_count} shots documents failed to insert")

        print(f"{matches_valid_count} matches documents inserted")
        print(f"{matches_attempted_count - matches_valid_count} matches documents failed to insert")


    print(f"# shots documents: {shots_collection.count_documents({})}")
    print(f"# matches documents: {matches_collection.count_documents({})}")

    # client.close()

    return client,shots_collection,matches_collection

# read all csvs
def read_all_csv():
    shots_drop_columns=['xG','PSxG','Notes','SCA 1_Player', 'SCA 1_Event', 'SCA 2_Player','SCA 2_Event']
    matches_drop_columns=['home_xg']

    df_shots=handle_csv('data/shots.csv',shots_drop_columns)
    df_matches=handle_csv('data/matches.csv',matches_drop_columns)

    return df_shots,df_matches
        
# get client,collections and csvs
def get_collections():
    df_shots,df_matches=read_all_csv()
    client,shots_collection,matches_collection=Mongo_connect(df_shots,df_matches)

    return client,df_shots,df_matches,shots_collection,matches_collection


