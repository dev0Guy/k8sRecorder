from prometheus_api_client import PrometheusConnect, MetricSnapshotDataFrame
from typing import List, Tuple, Dict
import numpy as np
import pandas as pd
import asyncio
import logging
import os


async def on_start(
    path: os.PathLike,
    prom: PrometheusConnect,
    interval: int,
):
    """
    the function will run untill stoped. will reocrd all of pod metrics and push into
    path.
    """
    print("#"*50)
    print(path)
    print("#"*50)
    while True:
        logging.warn("Start Record Interval")
        interval_information_df: pd.DataFrame = await _record_loop(path=path, prom=prom)
        _append_to_csv(df=interval_information_df, path=path)
        logging.warn("Finish Record Interval {interval}")
        await asyncio.sleep(interval)


async def on_end(
    logger: logging,
    inppath: os.PathLike,
    tmppath: os.PathLike,
    outpath: os.PathLike,
    **kwargs,
) -> None:
    pod_start_time: pd.DataFrame = _pod_uptime(
        inputpath=inppath,
    )
    # save in tmo file on failure
    pod_start_time.to_csv(tmppath, index=False)
    delta_pod_time: pd.DataFrame = _preprocess_change_timerange_to_relative(
        metrics_path=inppath,
        settings_path=tmppath,
    )
    # save in case of failure
    delta_pod_time.to_csv(outpath)


def _append_to_csv(
    path: os.PathLike,
    df: pd.DataFrame,
):
    """
    Append to existing csv the information. If columns doesn't exist will create new records
    """
    file_exist = os.path.exists(path)
    if file_exist:
        csv_df: pd.DataFrame = pd.read_csv(path)
        csv_df = pd.concat([csv_df, df], ignore_index=True)
    else:
        csv_df = df
    csv_df.to_csv(path, index=False)


async def _record_loop(
    path: os.PathLike,
    prom: PrometheusConnect,
) -> pd.DataFrame:
    """
    The loop that will provide and record for a certain time the pod metrics
    """
    # get all of the merics
    metrics_names: List[str] = prom.all_metrics()
    # get the information for each metric
    metrics_tuples: List[Tuple[str, Dict]] = [
        (name, prom.custom_query(query=f"sum(rate({name}[5m])) by (pod)"))
        for name in metrics_names
    ]
    # make a list of metric name and pd of values
    metrics_tuples: List[Tuple[str, pd.DataFrame]] = list(
        map(
            lambda x: (x[0], MetricSnapshotDataFrame(x[1])),
            filter(lambda x: x[1], metrics_tuples),
        )
    )
    # add to df the name of the metrics and remove the tple into df
    for name, df in metrics_tuples:
        df["metric_name"] = name
    metrics_df = map(lambda x: x[1], metrics_tuples)
    # concate the list of metrics into one df
    df: pd.DataFrame = pd.concat(metrics_df)
    # remove all empties values
    df.dropna(subset=["pod"], how="all", inplace=True)
    return df


def _preprocess_change_timerange_to_relative(
    metrics_path: os.PathLike,
    settings_path: os.PathLike,
) -> pd.DataFrame:
    """
    Parse data infromation and change it to relative from pod start time
    """
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


def _pod_uptime(
    inputpath: os.PathLike,
) -> pd.DataFrame:
    df: pd.DataFrame = pd.read_csv(inputpath)
    min_timestamps = df.groupby(["metric_name", "pod"])["timestamp"].min()
    min_timestamps = min_timestamps.reset_index(drop=False)
    return min_timestamps
