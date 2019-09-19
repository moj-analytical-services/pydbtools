"""
Testing DatabaseMeta, TableMeta
"""

import unittest
import numpy as np
import pandas as pd

import pydbtools as pydb

class ReadSqlTest(unittest.TestCase):
    """
    Test packages read_sql function works
    """
    def test_read_sql_output(self):
        df = pydb.read_sql("SELECT * from dbtools.test_data")

        df_test = isinstance(df, pd.DataFrame)
        self.assertTrue(df_test)

        size_test = len(df) == 10
        self.assertTrue(size_test)

        col_test = list(df.columns) == ['character_col', 'int_col', 'long_col', 'date_col', 'datetime_col','boolean_col', 'float_col', 'double_col']
        self.assertTrue(col_test)

        # character test
        self.assertTrue(df.character_col.dtype == np.dtype('object'))
        self.assertIsInstance(df.iloc[0]['character_col'], str)
        
        # int test        
        self.assertTrue(df.int_col.dtype == np.dtype('float'))
        self.assertIsInstance(df.iloc[0]['int_col'], np.float64)

        # long test
        self.assertTrue(df.long_col.dtype == np.dtype('float'))
        self.assertIsInstance(df.iloc[0]['long_col'], np.float64)

        # date test
        self.assertTrue(df.date_col.dtype == np.dtype('<M8[ns]'))
        self.assertIsInstance(df.iloc[0]['date_col'], pd._libs.tslibs.timestamps.Timestamp)

        # datetime test
        self.assertTrue(df.datetime_col.dtype == np.dtype('<M8[ns]'))
        self.assertIsInstance(df.iloc[0]['datetime_col'], pd._libs.tslibs.timestamps.Timestamp)

        # boolean test
        self.assertTrue(df.character_col.dtype == np.dtype('object'))
        self.assertIsInstance(df.iloc[0]['boolean_col'], bool)

        # float test
        self.assertTrue(df.float_col.dtype == np.dtype('float'))
        self.assertIsInstance(df.iloc[0]['float_col'], np.float64)

        # double test
        self.assertTrue(df.double_col.dtype == np.dtype('float'))
        self.assertIsInstance(df.iloc[0]['double_col'], np.float64)

        