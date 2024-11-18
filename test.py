import pandas as pd




filepath='DB/Project/data/'
filename='teams.csv'
filename=filepath+filename
df=pd.read_csv(filename)  
# # df=pd.DataFrame('matches.csv')
columns=df.columns

print(columns)
for i in columns:
    print(i)