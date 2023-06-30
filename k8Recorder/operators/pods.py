from prometheus_api_client import PrometheusConnect, MetricSnapshotDataFrame
from typing import Dict, Mapping, Tuple, List
from k8Recorder._types import ContainerTypes
from k8Recorder._types import PodTypes
from k8Recorder.utils import resource_limit
import pandas as pd
import logging
import os


async def check_limits(
    uid: PodTypes.PodId,
    name: PodTypes.PodName,
    spec: PodTypes.PodSpec,
    logger: logging,
    operation: PodTypes.Operation,
    **kwargs,
):
    all_containers: Mapping[ContainerTypes.ContainerName, Dict] = list(
        _get_pods_resources_limit(name, spec)
    )
    containers_with_limits: Dict[ContainerTypes.ContainerName, Dict] = dict(
        filter(lambda x: x[1], all_containers)
    )
    if len(all_containers) > len(containers_with_limits):
        logger.warning(f"Pod: {name} has container with no limits")


async def record(
    outpath: os.PathLike,
    prom: PrometheusConnect,
):
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


async def replay_init(
    folder: os.PathLike,
    uid: PodTypes.PodId,
    name: PodTypes.PodName,
    spec: PodTypes.PodSpec,
    **kwargs,
):
    filepath: os.PathLike = os.path.join(folder, f"{name}.csv")
    file_exist = os.path.exists(filepath)
    # if not file_exist:
    #     logging.error(f"Pod: {name} has no replay information, can't find {filepath}.")
    #     return None
    # df: pd.DataFrame = pd.read_csv(filepath)
    # distribution = stats.gaussian_kde(df["Value"])
    # print(type(distribution))


def _get_container_resource_limit(container: Dict) -> Tuple[str, Dict]:
    limits: dict = container.get("resources", {}).get("limits", None)
    container_name = container.get("name", "UNKNOWN")
    resource_mapper = {}
    if limits is not None:
        resource_mapper = dict(
            map(lambda item: (item[0], resource_limit(item[1])), limits.items())
        )
    return container_name, resource_mapper


def _get_pods_resources_limit(name: str, spec: Dict) -> Mapping:
    limits_dicts = map(
        lambda container: _get_container_resource_limit(container),
        spec.get("containers", {}),
    )
    return limits_dicts


def _record_into_csv(path: os.PathLike, row: pd.DataFrame):
    file_exist = os.path.exists(path)
    if file_exist:
        df: pd.DataFrame = pd.read_csv(path)
        df = pd.concat([df, row], ignore_index=True)
    else:
        df = row
    df.to_csv(path, index=False)
