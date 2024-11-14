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
import requests
import pandas as pd
import csv
import os
import sqlite3
from sqlalchemy import create_engine



def extract_data():
    url_csv1 = 'https://data.cityofnewyork.us/api/views/mjux-q9d4/rows.csv'
    url_csv2 = 'https://data.cityofnewyork.us/api/views/kkng-ugna/rows.csv'
    url_csv3 = 'https://data.cityofnewyork.us/api/views/ci36-d7ea/rows.csv'
    
    school_progress_report = pd.read_csv(url_csv1)
    high_school_quality = pd.read_csv(url_csv2)
    early_school_quality = pd.read_csv(url_csv3)
    
    return school_progress_report, high_school_quality, early_school_quality

#data = pd.read_csv(url_csv1)

def transform1(school_progress_report):

    columns_to_keep = [
        'SCHOOL', 
        'PRINCIPAL', 
        'SCHOOL LEVEL*', 
        '2011-2012 OVERALL GRADE', 
        '2011-2012 OVERALL SCORE', 
        '2011-2012 PROGRESS CATEGORY SCORE', 
        '2011-2012 PROGRESS GRADE', 
        '2011-2012 PERFORMANCE CATEGORY SCORE', 
        '2011-2012 PERFORMANCE GRADE', 
        '2010-11 PROGRESS REPORT GRADE', 
        '2009-10 PROGRESS REPORT GRADE'
]

    data = school_progress_report[columns_to_keep]
    data.dropna(subset=columns_to_keep, how='any', inplace=True)
    return school_progress_report

def transform2(high_school_quality):

    columns_to_keep = [
        'school_name', 
        'enrollment', 
        'school_type', 
        'QR_1_1', 
        'QR_2_2', 
        'QR_3_4', 
        'QR_4_1', 
        'QR_5_1', 
        'Dates_of_Review', 
        'gender_female_pct', 
        'gender_male_pct', 
        'cap_sc_pct'
]

    data = high_school_quality[columns_to_keep]
    data.dropna(subset=columns_to_keep, how='any', inplace=True)
    return high_school_quality

# Step 3: Save the Transformed Data to an SQLite Database
# Connect to SQLite database (or create it if it doesn't exist)
database_name = 'school_data.sqlite'
conn = sqlite3.connect(database_name)

# Save the transformed dataframe to a table called 'school_performance'
table_name = 'School_Performance_Report'
school_progress_report.to_sql(table_name, conn, if_exists='replace', index=False)

# Confirm the data has been saved
print(f"Data successfully saved to SQLite database '{database_name}' in table '{table_name}'.")

# Close the database connection
conn.close()