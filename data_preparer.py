from app.constants import *
from app.data_reader import DataReader
from pathlib import Path


def join_and_convert_to_parquet_fare() -> None:
    """
    Converts data from all the csv files into the parquet format
    :return: None
    """
    DataReader.convert_dir_to_parquet(PATH_TO_FARE_DATA,
                                      schema=FARE_SCHEMA,
                                      parent_dir_for_save=PARQUET_FARE_DATA,
                                      join_df=True)


def join_and_convert_to_parquet_trip() -> None:
    """
    Converts data from all the csv files into the parquet format
    :return: None
    """
    DataReader.convert_dir_to_parquet(PATH_TO_TRIP_DATA,
                                      schema=TRIP_SCHEMA,
                                      parent_dir_for_save=PARQUET_TRIP_DATA,
                                      join_df=True)


def make_the_sample(parent_dir_path: str | Path,
                    path_to_save: str | Path,
                    share_of_sample: float = 0.005) -> None:
    """
    Make the sample from the date in parquet format and save them also in the parquet format
    :param parent_dir_path: dir for data reading
    :param path_to_save: dir for data saving
    :param share_of_sample: size of the sample
    :return: None
    """
    df = DataReader.read_parquet(parent_dir_path)
    df = df.sample(fraction=share_of_sample)
    df.write.parquet(path_to_save, mode='overwrite')


if __name__ == '__main__':
    join_and_convert_to_parquet_trip()
    join_and_convert_to_parquet_fare()
    make_the_sample(PARQUET_TRIP_DATA,
                    PARQUET_TRIP_SAMPLE)
    make_the_sample(PARQUET_FARE_DATA,
                    PARQUET_FARE_SAMPLE)
