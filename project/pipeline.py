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
    
    math_test_result_2006_2012 = pd.read_csv(url_csv1)
    math_test_result_2013_2023 = pd.read_csv(url_csv2, low_memory=False)
    
    return math_test_result_2006_2012, math_test_result_2013_2023

def transform1(math_test_result_2006_2012):

    columns_to_keep = [
        'Report Category', 
        'Geographic Subdivision', 
        'Grade', 
        'Year', 
        'Student Category', 
        'Number Tested', 
        'Pct Level 1', 
        'Pct Level 2',
        'Pct Level 3',
        'Pct Level 4'       
    ]
    
    problematic_column = math_test_result_2006_2012.columns[1]
    math_test_result_2006_2012[problematic_column] = math_test_result_2006_2012[problematic_column].astype(str)
    math_test_result_2006_2012 = math_test_result_2006_2012[~math_test_result_2006_2012[problematic_column].str.isdigit()]
    
    math_test_result_2006_2012 = math_test_result_2006_2012[columns_to_keep]

    math_test_result_2006_2012.rename(columns={'Pct Level 1': 'Students Percentage Scored Level 1', 
                         'Pct Level 2': 'Students Percentage Scored Level 2', 
                         'Pct Level 3': 'Students Percentage Scored Level 3', 
                         'Pct Level 4': 'Students Percentage Scored Level 4'}, inplace=True)
    
    math_test_result_2006_2012['Students Percentage Scored Level 1'] = math_test_result_2006_2012['Students Percentage Scored Level 1'].replace('s', 0)
    math_test_result_2006_2012['Students Percentage Scored Level 2'] = math_test_result_2006_2012['Students Percentage Scored Level 2'].replace('s', 0)
    math_test_result_2006_2012['Students Percentage Scored Level 3'] = math_test_result_2006_2012['Students Percentage Scored Level 3'].replace('s', 0)
    math_test_result_2006_2012['Students Percentage Scored Level 4'] = math_test_result_2006_2012['Students Percentage Scored Level 4'].replace('s', 0)   
    
    math_test_result_2006_2012 = math_test_result_2006_2012.drop_duplicates()
    
    return math_test_result_2006_2012

def transform2(math_test_result_2013_2023):

    columns_to_keep = [
        'Report Category', 
        'Geographic Subdivision', 
        'Grade', 
        'Year', 
        'Student Category', 
        'Number Tested', 
        'Pct Level 1', 
        'Pct Level 2',
        'Pct Level 3',
        'Pct Level 4'       
    ]
    
    problematic_column = math_test_result_2013_2023.columns[1]
    math_test_result_2013_2023[problematic_column] = math_test_result_2013_2023[problematic_column].astype(str)
    math_test_result_2013_2023 = math_test_result_2013_2023[~math_test_result_2013_2023[problematic_column].str.isdigit()]
    
    math_test_result_2013_2023 = math_test_result_2013_2023[columns_to_keep]

    math_test_result_2013_2023.rename(columns={'Pct Level 1': 'Students Percentage Scored Level 1', 
                         'Pct Level 2': 'Students Percentage Scored Level 2', 
                         'Pct Level 3': 'Students Percentage Scored Level 3', 
                         'Pct Level 4': 'Students Percentage Scored Level 4'}, inplace=True)
    
    math_test_result_2013_2023['Students Percentage Scored Level 1'] = math_test_result_2013_2023['Students Percentage Scored Level 1'].replace('s', 0)
    math_test_result_2013_2023['Students Percentage Scored Level 2'] = math_test_result_2013_2023['Students Percentage Scored Level 2'].replace('s', 0)
    math_test_result_2013_2023['Students Percentage Scored Level 3'] = math_test_result_2013_2023['Students Percentage Scored Level 3'].replace('s', 0)
    math_test_result_2013_2023['Students Percentage Scored Level 4'] = math_test_result_2013_2023['Students Percentage Scored Level 4'].replace('s', 0)
       
    math_test_result_2013_2023 = math_test_result_2013_2023.drop_duplicates()

    return math_test_result_2013_2023


def data_load():
    
    math_test_result_2006_2012, math_test_result_2013_2023 = extract_data()
    
    math_test_result_2006_2012 = transform1(math_test_result_2006_2012)
    math_test_result_2013_2023 = transform2(math_test_result_2013_2023)
    
    database_name = 'math_2006_2023.sqlite'
    conn = sqlite3.connect(database_name)
    table_name1 = 'Math_Results_2006_2012'
    table_name2 = 'Math_Results_2013_2023'
    math_test_result_2006_2012.to_sql(table_name1, conn, if_exists='replace', index=False)
    math_test_result_2013_2023.to_sql(table_name2, conn, if_exists='replace', index=False)
    print(f"Data successfully saved to SQLite database '{database_name}'.")
    conn.close()
    
if __name__ == "__main__":
    data_load()