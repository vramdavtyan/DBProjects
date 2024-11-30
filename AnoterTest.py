import mysql.connector
import pandas as pd
import numpy as np

# Connect to MySQL
database_name = "DB_Football"

connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="_!0qo__trL-mY##3oSoiI#93",
    database=database_name
)

cursor = connection.cursor()

# Create database if not exists
cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
cursor.execute(f"USE {database_name}")

# List of CSV files to process
csv_files = [
    "data/matches.csv",
    "data/shots.csv",
    "data/all_players.csv",
    "data/teams.csv"
    # Add other CSV filenames here
] # Replace with actual file paths

# Loop through each CSV file
for csv_file in csv_files:
    print(f"Processing file: {csv_file}")

    # Load the CSV file into a pandas DataFrame with the correct dtypes
    try:
        df = pd.read_csv(csv_file)
    except Exception as e:
        print(f"Error reading {csv_file}: {e}")
        continue

    # Handle missing data (NaNs), replace with None (NULL for MySQL)
    df = df.where(pd.notnull(df), None)

    # Print the first few rows to check data structure (optional)
    # print(f"Dataframe Head of {csv_file}:")
    # print(df.head())

    # Convert DataFrame to a list of dictionaries (records)
    try:
        data_dict = df.to_dict(orient="records")
    except Exception as e:
        print(f"Error converting DataFrame to dict for {csv_file}: {e}")
        continue

    # Define table name based on the CSV file name
    table_name = csv_file.split("/")[-1].split(".")[0]  # Extract table name from the file name

    # Drop the table if it exists
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

    # Dynamically create table based on the DataFrame columns
    columns = df.columns
    column_defs = []
    for col in columns:
        dtype = df[col].dtype
        if np.issubdtype(dtype, np.number):
            if np.issubdtype(dtype, np.integer):
                column_defs.append(f"{col} INT")
            else:
                column_defs.append(f"{col} FLOAT")
        elif np.issubdtype(dtype, np.datetime64):
            column_defs.append(f"{col} DATETIME")
        else:
            column_defs.append(f"{col} VARCHAR(255)")

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        {', '.join(column_defs)}
    );
    """
    cursor.execute(create_table_query)

    # Insert data into the table
    insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))});"

    # Prepare data to insert, ensuring that None values are handled properly
    # Specifically handle the 'NaN' or missing values
    data_to_insert = [
        tuple(item[col] if pd.notna(item[col]) else None for col in columns)
        for item in data_dict
    ]

    # Print the prepared data (for debugging)
    # print(f"Data to be inserted into {table_name}: {data_to_insert}")

    try:
        cursor.executemany(insert_query, data_to_insert)
        connection.commit()
        print(f"Inserted {cursor.rowcount} rows into {table_name}.")
    except mysql.connector.Error as err:
        print(f"Error inserting data into {table_name}: {err}")
        connection.rollback()

    # Query and show indexes for the created table
    sql_show_indexes = f"SHOW INDEX FROM {table_name};"
    cursor.execute(sql_show_indexes)
    index_list = cursor.fetchall()
    print(f"List of indexes for {table_name}: {index_list}")

# Close cursor and connection
cursor.close()
connection.close()

print("\nData inserted successfully for all files!")
