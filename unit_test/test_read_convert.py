from unittest import TestCase

from app.data_reader import DataReader
from app.constants import *


class TestReadWrite(TestCase):

    def setUp(self):
        self.single_file = PATH_TO_FARE_DATA / 'trip_fare_8.csv'
        self.another_file = PATH_TO_FARE_DATA / 'trip_fare_7.csv'
        self.string_path = 'F:\\spark_data\\fare\\trip_fare_8.csv'

    def test_easy_single_read(self):
        df = DataReader.read_the_data(self.single_file)
        df_2 = DataReader.read_the_data(str(self.single_file), schema=FARE_SCHEMA)
        self.assertIsNotNone(df)
        self.assertIsNotNone(df_2)
        self.assertLess(0, df.count())
        self.assertLess(0, df_2.count())

    def test_read_and_join(self):
        dfs = DataReader.read_the_data([self.single_file, self.another_file],
                                       schema=FARE_SCHEMA)
        self.assertEqual(2, len(dfs))

        joined_df = DataReader.read_the_data([self.single_file, self.another_file],
                                             schema=FARE_SCHEMA,
                                             join_df=True)

        self.assertEqual(0, joined_df.count() - dfs[0].count() - dfs[1].count())

    def test_read_from_dir(self):
        dfs, _ = DataReader.read_from_dir(PATH_TO_FARE_DATA,
                                          FARE_SCHEMA,
                                          limits_of_files=2)
        self.assertEqual(2, len(dfs))

        joined_df, _ = DataReader.read_from_dir(PATH_TO_FARE_DATA,
                                                FARE_SCHEMA,
                                                join_df=True,
                                                limits_of_files=2)
        self.assertEqual(0, joined_df.count() - dfs[0].count() - dfs[1].count())

    def test_convert_to_parquet_single_file(self):
        result = DataReader.convert_to_parquet(self.single_file,
                                               FARE_SCHEMA,
                                               'F:/parquet_data/fare')
        self.assertTrue(result)

    def test_convert_to_parquet_multi_files_with_join(self):
        result = DataReader.convert_to_parquet([self.single_file, self.another_file],
                                               FARE_SCHEMA,
                                               'F:/parquet_data/fare',
                                               join_df=True)
        self.assertTrue(result)

    def test_convert_to_parquet_multi_files(self):
        result = DataReader.convert_to_parquet([self.single_file, self.another_file],
                                               FARE_SCHEMA,
                                               'F:/parquet_data/fare')
        self.assertTrue(result)

    def test_read_with_path_as_a_string(self):
        df = DataReader.read_the_data(self.single_file,
                                      FARE_SCHEMA)
        self.assertLess(0, df.count())

    def test_convert_dir_to_parquet_no_join(self):
        DataReader.convert_dir_to_parquet(PATH_TO_FARE_DATA,
                                          FARE_SCHEMA,
                                          'F:/parquet_data/fare',
                                          limits_of_files=2,
                                          join_df=False)

    def test_convert_dir_to_parquet_join(self):
        DataReader.convert_dir_to_parquet(PATH_TO_FARE_DATA,
                                          FARE_SCHEMA,
                                          'F:/parquet_data/fare',
                                          limits_of_files=2,
                                          join_df=True)
