"""
Collection of handy functions.
"""
import os
from datetime import timedelta, datetime, timezone
from pathlib import Path
from typing import List, Tuple, Any, Optional
import pandas as pd
from pandas import DataFrame


__DATETIME_FORMAT_SHORT__: str = '%Y-%m-%d'
__TIMESTAMP_FORMAT__: str = '%Y%m%d-%H%M%S'


def format_timedelta(time_delta: timedelta) -> str:
    hours, remainder = divmod(time_delta.total_seconds(), 3600)
    minutes, seconds = divmod(remainder, 60)
    return str(f"{int(hours):02d}h:{int(minutes):02d}m:{seconds:06.3f}s")


def require(condition: bool, error_message: str) -> None:
    """
    Like assert, but it's called 'require'
    """
    if not condition:
        raise ValueError(error_message)


def days_between(max_date: str, min_date: str) -> int:
    return (str_to_datetime(max_date) - str_to_datetime(min_date)).days


def today_str() -> str:
    return datetime.strftime(datetime.now(), __DATETIME_FORMAT_SHORT__)


def now_str() -> str:
    return datetime.strftime(datetime.now(), __TIMESTAMP_FORMAT__)


def timestamp_to_str(timestamp: float) -> str:
    return datetime.strftime(timestamp_to_datetime(timestamp), __DATETIME_FORMAT_SHORT__)


def timestamp_to_dir_name(timestamp: float) -> str:
    return datetime.strftime(timestamp_to_datetime(timestamp), __TIMESTAMP_FORMAT__)


def datetime_to_timestamp(d: datetime) -> float:
    return d.replace(tzinfo=timezone.utc).timestamp()


def timestamp_to_datetime(timestamp: float) -> datetime:
    return datetime.utcfromtimestamp(timestamp)


def str_to_datetime(date: str) -> datetime:
    return datetime.strptime(date, '%Y-%m-%d')


def datetime_to_str(date: datetime) -> str:
    return date.strftime(__DATETIME_FORMAT_SHORT__)


def unzip(ts: List[Tuple]) -> (List[Any], List[Any]):
    return zip(*ts) if len(ts) != 0 else [], []


def create_dir_if_not_exists(directory: str) -> None:
    if os.path.isdir(directory):
        return
    os.makedirs(directory)


def delete_file_if_exists(path: str) -> None:
    if os.path.exists(path):
        if os.path.isdir(path):
            raise ValueError
        os.remove(path)


def resolve_file_path(path: str) -> str:
    if path is None:
        return
    if os.path.exists(path):
        return path
    path = os.path.join(os.getcwd(), path)
    return path


def create_directory(name: str) -> Path:
    path = Path(name)

    path.mkdir(parents=True, exist_ok=True)
    return path


def resolve_non_conflicting_file_name(name: str, path: str) -> str:
    """
        Return the first non-conflicting file name for the given starting
        name, in the given directory.
        If the given name is not used, it will be returned
        otherwise, (n) will be appended where n increases until there is no conflict
    :param name: starting file name
    :param path: directory in which to search
    :return: non-conflicting file name
    """
    if not os.path.exists(os.path.join(path, name)):
        return name

    for i in range(1000):
        n_name: str = f"{name} ({i})"
        if not os.path.exists(os.path.join(path, n_name)):
            return n_name
    return None


def read_csv(path: Path, dtype: object = None, usecols: List = None, index_col: List = None) -> DataFrame:
    """
    This function takes in a path to a csv file, a datatype specification object, and a list of columns to use
    and returns the loaded DataFrame.
    :param path: Path to a .csv file
    :param dtype: A DataType specification for the new DataFrame
    :param usecols: A list of columns to keep
    :param index_col: The index column to use
    :return: A DataFrame from the .csv file
    """
    df = pd.DataFrame()
    for chunk in pd.read_csv(path, dtype=dtype, usecols=usecols, chunksize=2097152, index_col=index_col):
        df = pd.concat([df, chunk])  # ignore_index=True
    return df


def save_csv(df: DataFrame, path: Path, index: Optional[bool] = False) -> None:
    """
    This function takes in a DataFrame, a path to save the file to, and a boolean value 'index' of whether
    or not to save the index of the DataFrame to the csv file.
    :param df: The DataFrame to save
    :param path: The path for the saved DataFrame
    :param index: A boolean indication of whether or not to save the index of the DataFrame to the csv file.
    :return: None
    """
    frame_width = df.shape[1]
    chunk_size = round(256000 / frame_width)
    df.to_csv(path, index=index, chunksize=chunk_size)


def persist_dataframe(dataframe: DataFrame, directory: str, file: str) -> None:
    """"
    Persists the provided dataframe to disk as a CSV file, in the directory provided, with the given file name
    :param dataframe: DataFrame to persist
    :param directory: directory on disk
    :param file: filename of the output CSV file
    """
    create_dir_if_not_exists(directory)
    output_file = os.path.join(directory, file)
    delete_file_if_exists(output_file)
    dataframe.to_csv(output_file, index=False)


def decorrupt(cusip: object) -> str:
    """
    Excel sometimes mangles the CUSIP. (The occasional presence of an E in the value
    makes excel think it's an exponential and it "formats" it). For example:

     Excel converts 100375E09 to 1.00375E+09

     This function checks for such corruption and corrects it
    """
    s = str(cusip)
    return s if 'E+' not in s else s.replace('.', '').replace('+', '')


def find_min_max_date_df(dataframes: List[DataFrame]):
    """
    This function takes a list of DataFrames and returns the min and max dates found in any DataFrame.
    :param dataframes: A list of DataFrames
    :return: min date, max date
    """
    mins = []
    maxes = []

    for dataframe in dataframes:
        mins.append(dataframe.date.min())
        maxes.append(dataframe.date.max())
    minimum = min(mins)
    maximum = max(maxes)

    return minimum, maximum