#Build an automated data pipeline for the project
#Write a script that pulls the data sets choosen from the internet, transforms it and fixes errors, and finally stores  data in the data directory
#Place the script in the /project directory (any file name is fine)
#Add a /project/pipeline.sh that starts the pipeline as I would do from the command line as entry point: if I run my script on my command line using `python3 /project/pipeline.py`, create a /project/pipeline.sh with the content: 
#!/bin/bash
#python3 /project/pipeline.py
#The output of the script should be: datasets in my /data directory (e.g., as SQLite databases) 
#Do NOT check in data sets, just the script
#I can use .gitignore to avoid checking in files on git
#This data set will be the base for my data report in future project work
#Update the issues and project plan if necessary


import pandas as pd
import sqlite3
import os


def extract_data():
    url_csv1 = 'https://data.cityofnewyork.us/api/views/e5c5-ieuv/rows.csv?accessType=DOWNLOAD'
    url_csv2 = 'https://data.cityofnewyork.us/api/views/74kb-55u9/rows.csv?accessType=DOWNLOAD'
    
    data_dir = './data/raw_csv/'
    os.makedirs(data_dir, exist_ok=True)
    
    math_r_2006_2012 = pd.read_csv(url_csv1)
    math_r_2013_2023 = pd.read_csv(url_csv2, low_memory=False)    
    return math_r_2006_2012, math_r_2013_2023

def transform1(math_r_2006_2012):

    col_keep = [
        'Report Category', 
        'Geographic Subdivision', 
        'Grade', 
        'Year', 
        'Student Category', 
        'Number Tested', 
        'Pct Level 1',
        'Pct Level 2',
        'Pct Level 3',
        'Pct Level 4',
        'Pct Level 3 and 4'       
    ]

    math_r_2006_2012 = math_r_2006_2012[math_r_2006_2012['Number Tested'] > 5].copy()
    
    math_r_2006_2012 = math_r_2006_2012[math_r_2006_2012['Grade'] != "All Grades"]
    
    exclude = ["ELL", "EP", "All Students", "ELLs"]
    math_r_2006_2012 = math_r_2006_2012[~math_r_2006_2012['Student Category'].isin(exclude)]
    
    math_r_2006_2012['Grade'] = pd.to_numeric(math_r_2006_2012['Grade']).astype(int)
    
    prob_col = math_r_2006_2012.columns[1]
    math_r_2006_2012.loc[:, prob_col] = math_r_2006_2012[prob_col].astype(str)
    math_r_2006_2012 = math_r_2006_2012[~math_r_2006_2012[prob_col].str.isdigit()]
    
    math_r_2006_2012 = math_r_2006_2012[col_keep]

    math_r_2006_2012.rename(columns={'Pct Level 1': '% of Students Level 1', 
                         'Pct Level 2': '% of Students Level 2',
                         'Pct Level 3': '% of Students Level 3',
                         'Pct Level 4': '% of Students Level 4',
                         'Pct Level 3 and 4': '% of Students Level 3 & 4'}, inplace=True)
    
   # math_r_2006_2012['No of Students Level 1'] = math_r_2006_2012['No of Students Level 1'].replace('s', 0).astype(int)
    math_r_2006_2012['% of Students Level 1'] = math_r_2006_2012['% of Students Level 1'].replace('s', 0).astype(float)
    #math_r_2006_2012['No of Students Level 2'] = math_r_2006_2012['No of Students Level 2'].replace('s', 0).astype(int)
    math_r_2006_2012['% of Students Level 2'] = math_r_2006_2012['% of Students Level 2'].replace('s', 0).astype(float)
    math_r_2006_2012['% of Students Level 3'] = math_r_2006_2012['% of Students Level 3'].replace('s', 0).astype(float)
    math_r_2006_2012['% of Students Level 4'] = math_r_2006_2012['% of Students Level 4'].replace('s', 0).astype(float)
    #math_r_2006_2012['No of Students Level 3 & 4'] = math_r_2006_2012['No of Students Level 3 & 4'].replace('s', 0).astype(int)
    math_r_2006_2012['% of Students Level 3 & 4'] = math_r_2006_2012['% of Students Level 3 & 4'].replace('s', 0).astype(float)   
    
    math_r_2006_2012 = math_r_2006_2012.drop_duplicates()
    
    return math_r_2006_2012

def transform2(math_r_2013_2023):

    col_keep = [
        'Report Category', 
        'Geographic Subdivision', 
        'Grade', 
        'Year', 
        'Student Category', 
        'Number Tested', 
        'Pct Level 1',
        'Pct Level 2',
        'Pct Level 3',
        'Pct Level 4',
        'Pct Level 3 and 4'       
    ]
    math_r_2013_2023 = math_r_2013_2023[math_r_2013_2023['Number Tested'] > 5].copy()   
    
    math_r_2013_2023 = math_r_2013_2023[math_r_2013_2023['Grade'] != "All Grades"]
    
    allowed = ["SWD", "Female", "Male", "Asian", "Black", "Hispanic", "White", "Not SWD"]
    math_r_2013_2023 = math_r_2013_2023[math_r_2013_2023['Student Category'].isin(allowed)]
    
    math_r_2013_2023['Grade'] = pd.to_numeric(math_r_2013_2023['Grade']).astype(int)
    
    prob_col = math_r_2013_2023.columns[1]
    math_r_2013_2023.loc[:, prob_col] = math_r_2013_2023[prob_col].astype(str)
    math_r_2013_2023 = math_r_2013_2023[~math_r_2013_2023[prob_col].str.isdigit()]
    
    math_r_2013_2023 = math_r_2013_2023[col_keep]

    math_r_2013_2023.rename(columns={'Pct Level 1': '% of Students Level 1', 
                         'Pct Level 2': '% of Students Level 2',
                         'Pct Level 3': '% of Students Level 3',
                         'Pct Level 4': '% of Students Level 4',
                         'Pct Level 3 and 4': '% of Students Level 3 & 4'}, inplace=True)
    
  #  math_r_2013_2023['No of Students Level 1'] = math_r_2013_2023['No of Students Level 1'].replace('s', 0).astype(int)
    math_r_2013_2023['% of Students Level 1'] = math_r_2013_2023['% of Students Level 1'].replace('s', 0).astype(float)
  #  math_r_2013_2023['No of Students Level 2'] = math_r_2013_2023['No of Students Level 2'].replace('s', 0).astype(int)
    math_r_2013_2023['% of Students Level 2'] = math_r_2013_2023['% of Students Level 2'].replace('s', 0).astype(float)
    math_r_2013_2023['% of Students Level 3'] = math_r_2013_2023['% of Students Level 3'].replace('s', 0).astype(float)
    math_r_2013_2023['% of Students Level 4'] = math_r_2013_2023['% of Students Level 4'].replace('s', 0).astype(float)
  #  math_r_2013_2023['No of Students Level 3 & 4'] = math_r_2013_2023['No of Students Level 3 & 4'].replace('s', 0).astype(int)
    math_r_2013_2023['% of Students Level 3 & 4'] = math_r_2013_2023['% of Students Level 3 & 4'].replace('s', 0).astype(float)  
       
    math_r_2013_2023 = math_r_2013_2023.drop_duplicates()

    return math_r_2013_2023


def data_load():
    
    math_r_2006_2012, math_r_2013_2023 = extract_data()
    
    math_r_2006_2012 = transform1(math_r_2006_2012)
    math_r_2013_2023 = transform2(math_r_2013_2023)
    
    database_name = 'math_2006_2023.sqlite'
    conn = sqlite3.connect(database_name)
    table_name1 = 'Math_Results_2006_2012'
    table_name2 = 'Math_Results_2013_2023'
    math_r_2006_2012.to_sql(table_name1, conn, if_exists='replace', index=False)
    math_r_2013_2023.to_sql(table_name2, conn, if_exists='replace', index=False)
    print(f"Data successfully saved to SQLite database '{database_name}'.")
    conn.close()
    
if __name__ == "__main__":
    data_load()
