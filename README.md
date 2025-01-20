## Math Matters: Examining the New York State Students Performance Over the Time (2006-2023)


## Main Question

How have the students performance in New York City Math exams expanded in the past 18 years, and what factors are involved in the changes in different demographics and regions?


## Description

Mathematics is a fundamental part of life, influencing every aspect of daily activities and decision-making. From early childhood, math skills lay the foundation for critical thinking and problem-solving. This project analyzes 18 years of student performance data from New York State Math exams to uncover long-term trends and insights. By combining two datasets, 2006 to 2012 and 2013 to 2023, it will investigate long-term patterns in test outcomes between grade levels, demographic groupings, and geographic areas in New York.

## Datasources

### Datasource1: Math Test Results 2006-2012

* Metadata URL: <https://catalog.data.gov/dataset/math-test-results-2006-2012>

* Data URL: <https://data.cityofnewyork.us/api/views/e5c5-ieuv/rows.csv>

* Data Type: CSV
  
This dataset contains the results of the students of New York State Math exams for the years 2006 to 2012.

### Datasource2: Math Test Results 2013-2023

* Metadata URL: <https://catalog.data.gov/dataset/math-test-results-2013-2023>

* Data URL: <https://data.cityofnewyork.us/api/views/74kb-55u9/rows.csv>

* Data Type: CSV
  
This dataset contains the results of the students of New York State Math exams for the years 2013 to 2023.

## Work Packages

1. Dataset Collection.
2. Building an Automated Data Pipeline.
3. Data Cleaning and Preprocessing.
4. Data Analysis and Correlation
5. Data Visualization.
6. Reporting and Documentation.


## Project Work
Your data engineering project will run alongside lectures during the semester. We will ask you to regularly submit project work as milestones, so you can reasonably pace your work. All project work submissions **must** be placed in the `project` folder.

### Exporting a Jupyter Notebook
Jupyter Notebooks can be exported using `nbconvert` (`pip install nbconvert`). For example, to export the example notebook to HTML: `jupyter nbconvert --to html examples/final-report-example.ipynb --embed-images --output final-report.html`


## Exercises
During the semester you will need to complete exercises using [Jayvee](https://github.com/jvalue/jayvee). You **must** place your submission in the `exercises` folder in your repository and name them according to their number from one to five: `exercise<number from 1-5>.jv`.

In regular intervals, exercises will be given as homework to complete during the semester. Details and deadlines will be discussed in the lecture, also see the [course schedule](https://made.uni1.de/).

### Exercise Feedback
We provide automated exercise feedback using a GitHub action (that is defined in `.github/workflows/exercise-feedback.yml`). 

To view your exercise feedback, navigate to Actions → Exercise Feedback in your repository.

The exercise feedback is executed whenever you make a change in files in the `exercise` folder and push your local changes to the repository on GitHub. To see the feedback, open the latest GitHub Action run, open the `exercise-feedback` job and `Exercise Feedback` step. You should see command line output that contains output like this:

```sh
Found exercises/exercise1.jv, executing model...
Found output file airports.sqlite, grading...
Grading Exercise 1
	Overall points 17 of 17
	---
	By category:
		Shape: 4 of 4
		Types: 13 of 13
```
