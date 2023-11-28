import pathlib

from typing import List, Tuple
from pathlib import Path
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType


class DataReader:
    """
    Implements read/convert operations for csv files
    """

    @classmethod
    def read_the_data(cls,
                      path_to_file: str | Path | List[str] | List[Path],
                      schema: StructType | None = None,
                      join_df: bool = False) -> DataFrame | List[DataFrame]:
        """
        Reads the single or many files and returns them as Spark dataframe or the list of Spark dataframes respectively.

        :param path_to_file: path to the initial file or to a list of files
        :param schema: Spark schema for the file(s)
        :param join_df: if True all the files will be joined in a single DataFrame, otherwise, the list of DataFrames will be returned
        :return: the Spark dataframe or the list of them
        """
        if isinstance(path_to_file, str) | isinstance(path_to_file, Path):
            return cls._read_the_csv(path_to_file, schema)
        elif isinstance(path_to_file, list):
            list_of_df = [cls._read_the_csv(path, schema) for path in path_to_file]
            if not join_df:
                return list_of_df
            else:
                return cls._join_frames(list_of_df)
        raise TypeError('Unprocessable type of path data')

    @classmethod
    def read_from_dir(cls,
                      path_to_dir: str | Path,
                      schema: StructType | None,
                      limits_of_files: int = -1,
                      join_df: bool = False) -> Tuple[DataFrame | List[DataFrame], List[Path]]:
        """
        Reads the files from the directory. Simplify the operation of reading all the files from a particular dir. For some purposes (testing, exploration) only the limited quantity of files can be read.

        :param path_to_dir: path to the dir where the files are
        :param schema: Spark schema for the file(s)
        :param limits_of_files: the parameter defines how many files to read. -1 should be set to read all the files
        :param join_df: if True all the files will be joined in a single DataFrame, otherwise, the list of DataFrames will be returned
        :return: the Spark dataframe or the list of them
        """
        pathlib_path = Path(path_to_dir) if isinstance(path_to_dir, str) else path_to_dir
        if not pathlib_path.is_dir():
            raise ValueError('The given path is not a dir')

        list_of_files, count = [], 0
        for file in pathlib_path.glob('*.csv'):
            list_of_files.append(file)
            count += 1
            if (limits_of_files > -1) & (count == limits_of_files):
                break

        return cls.read_the_data(list_of_files, schema, join_df), list_of_files

    @classmethod
    def convert_to_parquet(cls,
                           path_to_file: str | Path | List[str] | List[Path],
                           schema: StructType | None,
                           parent_dir_for_save: str | Path,
                           join_df: bool = False) -> bool:
        """
        Converts one or several csv files into the parquet format. For using with Spark the parquet format is the faster solution comparing with csv.

        :param path_to_file: path to the initial file or to a list of files
        :param schema: Spark schema for the file(s)
        :param parent_dir_for_save: the directory to save parquet file(s)
        :param join_df: if True all the files will be joined in a single DataFrame, otherwise, the list of DataFrames will be returned
        :return: True, if the operation was successful, False otherwise
        """
        if isinstance(path_to_file, list) & (not join_df):
            file_names = [pathlib.Path(file_name).name.split('.')[0] for file_name in path_to_file]
            dir_names = [pathlib.Path(parent_dir_for_save) / file_name for file_name in file_names]
            try:
                for file_name, dir_name in zip(path_to_file, dir_names):
                    data_to_save = cls._read_the_csv(file_name, schema)
                    data_to_save.write.parquet(dir_name, mode='overwrite')
                return True
            except Exception as ex:
                print(ex)
                return False
        else:
            try:
                if pathlib.Path(path_to_file).exists():
                    data_to_save = cls._read_the_csv(path_to_file, schema)
                    data_to_save.write.parquet(parent_dir_for_save, mode='overwrite')
                else:
                    raise FileNotFoundError(f'File {str(path_to_file)} is not found')
                return True
            except Exception as ex:
                print(ex)
                return False

    @classmethod
    def convert_dir_to_parquet(cls,
                               path_to_dir: str | Path,
                               schema: StructType | None,
                               parent_dir_for_save: str | Path,
                               limits_of_files: int = -1,
                               join_df: bool = False) -> bool:
        """
        Converts one or several csv files in the directory into the parquet format. Simplify the operation of converting all the files from a particular dir. For some purposes (testing, exploration) only the limited quantity of files can be transformed.

        :param path_to_dir: path to the dir where the files are
        :param schema: Spark schema for the file(s)
        :param parent_dir_for_save: the directory to save parquet file(s)
        :param limits_of_files: the parameter defines how many files to read.
                                -1 should be set to read all the files
        :param join_df: if True all the files will be joined in a single DataFrame,
               otherwise, the list of DataFrames will be returned
        :return: True, if the operation was successful, False otherwise
        """
        data, files = cls.read_from_dir(path_to_dir, schema, limits_of_files, join_df)
        if isinstance(data, DataFrame):
            data.write.parquet(parent_dir_for_save, mode='overwrite')
        elif isinstance(data, list):
            if not join_df:
                for df, file_name in zip(data, files):
                    dir_suffix = str(pathlib.Path(file_name).name).split('.')[0]
                    path_to_dir = pathlib.Path(parent_dir_for_save) / dir_suffix
                    df.write.parquet(str(path_to_dir), mode='overwrite')
            else:
                df = cls.read_from_dir(path_to_dir, schema, limits_of_files, join_df=True)
                df.write.parquet(parent_dir_for_save, mode='overwrite')

    @classmethod
    def read_parquet(cls,
                     parent_dir: str | Path) -> DataFrame:
        """
        Reads the parquet format, returns the dataframe.

        :param parent_dir: the directory to read from
        :return: the Spark Dataframe
        """
        return spark.read.parquet(parent_dir)

    @classmethod
    def _read_the_csv(cls,
                      path: str | Path,
                      schema: StructType | None) -> DataFrame:
        """
        Reads the single csv file into the Spark Dataframe.

        :param path: path to the initial csv file
        :param schema: Spark schema for the file(s)
        :return: the Spark dataframe
        """
        pathlib_path = Path(path) if isinstance(path, str) else path
        if not pathlib_path.exists():
            raise FileNotFoundError('The file path does not exists')
        return spark.read.csv(str(path), schema=schema, header=True)

    @classmethod
    def _join_frames(cls,
                     list_of_df: List[DataFrame]) -> DataFrame:
        """
        Converts the list of the dataframes with the same schema into the single one.

        :param list_of_df: the list of Spark dataframes
        :return: the Spark DataFrame
        """
        return reduce(DataFrame.unionAll, list_of_df)
