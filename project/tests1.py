import unittest
import os
import sqlite3
import pandas as pd
from pipeline import extract_data, transform1, transform2, data_load

class Pipeline_Test(unittest.TestCase):
    
# Setting up a test directory
    def setUp(self):

        self.test_db = 'test_math_2006_2023.sqlite'
        if os.path.exists(self.test_db):
            os.remove(self.test_db)
  
# Testing the data extraction phase
    def test_extract_data(self):
        
        data_2006_2012, data_2013_2023 = extract_data()
        self.assertIsInstance(data_2006_2012, pd.DataFrame)
        self.assertIsInstance(data_2013_2023, pd.DataFrame)
        self.assertGreater(len(data_2006_2012), 0)
        self.assertGreater(len(data_2013_2023), 0)

# Testing the transformation of the dataset of 2006 to 2012       
    def test_transform1(self):

        data_2006_2012, _ = extract_data()
        transformed_data = transform1(data_2006_2012)
        self.assertIn('Students Percentage Scored Level 4', transformed_data.columns)
        self.assertNotIn('s', transformed_data['Students Percentage Scored Level 4'].unique())

        
if __name__ == "__main__":
    print("Tests are running now ")
    unittest.main()