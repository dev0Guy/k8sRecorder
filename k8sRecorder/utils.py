from prometheus_api_client import PrometheusConnect, MetricSnapshotDataFrame
from typing import Dict, List, Tuple
import scipy.stats as stats
import pandas as pd
import numpy as np
import logging
import re
import os


def set_logging(logger: logging) -> None:
    logging.basicConfig(
        level=logging.WARNING,  # Set the desired logging level
        format="%(asctime)s :%(levelname)s: %(filename)s \t [%(funcName)s] %(message)s",
        handlers=[
            # logging.FileHandler('app.log'),  # Add a file handler to log to a file
            logging.StreamHandler()  # Add a stream handler to log to the console
        ],
    )


def pod_up_time(
    inputpath: os.PathLike,
    outputpath: os.PathLike,
) -> pd.DataFrame:
    df: pd.DataFrame = pd.read_csv(inputpath)
    min_timestamps = df.groupby(["metric_name", "pod"])["timestamp"].min()
    min_timestamps = min_timestamps.reset_index(drop=False)
    min_timestamps.to_csv(outputpath, index=False)
    return min_timestamps


async def record(
    outpath: os.PathLike,
    prom: PrometheusConnect,
) -> None:
    metrics_names: List[str] = prom.all_metrics()
    metrics_tuples: List[Tuple[str, Dict]] = [
        (name, prom.custom_query(query=f"sum(rate({name}[5m])) by (pod)"))
        for name in metrics_names
    ]
    metrics_tuples: List[Tuple[str, pd.DataFrame]] = list(
        map(
            lambda x: (x[0], MetricSnapshotDataFrame(x[1])),
            filter(lambda x: x[1], metrics_tuples),
        )
    )
    for name, df in metrics_tuples:
        df["metric_name"] = name
    metrics_df = map(lambda x: x[1], metrics_tuples)
    df: pd.DataFrame = pd.concat(metrics_df)
    df.dropna(subset=["pod"], how="all", inplace=True)
    _record_into_csv(outpath, df)


async def create_metrics_distribution(
    inputpath: os.PathLike,
    settings_df: pd.DataFrame,
):
    df: pd.DataFrame = pd.read_csv(inputpath)
    #
    pass


async def save_data(
    metrics_path: os.PathLike,
    settings_path: os.PathLike,
    save_path: os.PathLike,
    prom: PrometheusConnect,
):
    merged_df: pd.DataFrame = _data_preprocess(metrics_path, settings_path)
    print(merged_df.head())
    merged_df.to_csv(save_path)

    # merged_df.rename(columns={"timestamp_x":"timestamp", "value_x": "value"},inplace=True)
    # print(f"Merged: {merged_df.columns}")
    # print("#"*50)
    # print(metric_df.head())

    # remove run time by it creation time
    # create avg run time
    # create distribution of run time and metrics
    # sample from distribution evrey x seconds


def _data_preprocess(
    metrics_path: os.PathLike,
    settings_path: os.PathLike,
):
    metric_df: pd.DataFrame = pd.read_csv(metrics_path)
    settings_df: pd.DataFrame = pd.read_csv(settings_path)
    merged_df = metric_df.merge(settings_df, on=["metric_name", "pod"], how="left")
    merged_df["timestamp_x"] = pd.to_datetime(
        merged_df["timestamp_x"], format="%Y-%m-%d %H:%M:%S.%f", errors="coerce"
    )
    merged_df["timestamp_y"] = pd.to_datetime(
        merged_df["timestamp_y"], format="%Y-%m-%d %H:%M:%S.%f", errors="coerce"
    )
    merged_df["timestamp"] = (
        merged_df["timestamp_x"] - merged_df["timestamp_y"]
    ).astype(np.int64) // 10**9
    merged_df.dropna(inplace=True, how="any")
    merged_df.drop(["timestamp_x", "timestamp_y"], axis=1, inplace=True)
    return merged_df


def resource_limit(resource_val: str | float | int) -> str | float | int:
    match resource_val:
        case int(resource_val):
            val = float(resource_val)
        case float(resource_val):
            val = resource_val
        case str(resource_val):
            val = "".join(re.findall(r"\d+", resource_val))
            val = float(val)
        case _:
            val = None
    return val


def create_gassian_for_each_column(df: pd.DataFrame) -> Dict[str, stats.gaussian_kde]:
    return dict((column, stats.gaussian_kde(df[column])) for column in df.columns)


def _record_into_csv(path: os.PathLike, row: pd.DataFrame):
    file_exist = os.path.exists(path)
    if file_exist:
        df: pd.DataFrame = pd.read_csv(path)
        df = pd.concat([df, row], ignore_index=True)
    else:
        df = row
    df.to_csv(path, index=False)
