# Project Plan

## Unpacking the Link Between Schools Progress Report and Schools Quality in New York City
<!-- Give your project a short title. -->

## Main Question

<!-- Think about one main question you want to answer based on the data. -->
 How does the quality of a school affect on their yearly progress?


## Description

<!-- Describe your data science project in max. 200 words. Consider writing about why and how you attempt it. -->
In order to assess the quality of a school we can take a look at three spreadsheets. One describes, 2011-2012 overall school progress report, another describes the 2019-2020 high school quality report, and the final one describes 2019-2020 early childhood school. All of the reports are of the schools based in New York City, USA. To make all this data into a usable format for the data pipeline, we need to study the overall grades for every year as well as the other metrics. After doing that we will be able to determine if the quality of a school truly affects their yearly progress or not.

## Datasources

<!-- Describe each datasources you plan to use in a section. Use the prefic "DatasourceX" where X is the id of the datasource. -->

### Datasource1: 2011-2012 All Schools Progress Report

* Metadata URL: <https://catalog.data.gov/dataset/2011-2012-school-progress-report-all-schools>

* Data URL: <https://data.cityofnewyork.us/api/views/mjux-q9d4/rows.csv>

* Data Type: CSV
  
The dataset provides yearly overall performance of the schools (Elementary, Middle and High School) from 2011 to 2012.

### Datasource2: 2019-20 School Quality Guide Early Childhood

* Metadata URL: <https://catalog.data.gov/dataset/2019-20-school-quality-guide-early-childhood>

* Data URL: <https://data.cityofnewyork.us/api/views/kkng-ugna/rows.csv>

* Data Type: CSV
  
The dataset provides yearly overall qualification of high schools from 2019 to 2020.

### Datasource3: 2019-20 School Quality Guide High Schools

* Metadata URL: <https://catalog.data.gov/dataset/2019-20-school-quality-guide-high-schools>

* Data URL: <https://data.cityofnewyork.us/api/views/ci36-d7ea/rows.csv>

* Data Type: CSV
  
The dataset provides yearly overall qualification of early childhood schools from 2019 to 2020.


## Work Packages

<!-- List of work packages ordered sequentially, each pointing to an issue with more details. -->
1. Dataset Collection.
2. Building an Automated Data Pipeline.
3. Data Cleaning and Preprocessing.
4. Data Analysis and Correlation
5. Data Visualization.
6. Reporting and Documentation.
