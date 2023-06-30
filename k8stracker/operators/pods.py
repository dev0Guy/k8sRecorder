from prometheus_api_client import PrometheusConnect
from k8stracker._types import ContainerTypes
from k8stracker._types import PodTypes
from k8stracker.utils import resource_limit
from typing import Dict, Mapping, Tuple
import k8stracker.query as k8s_queries
from time import time
import pandas as pd
import logging
import inspect
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
    uid: PodTypes.PodId,
    name: PodTypes.PodName,
    spec: PodTypes.PodSpec,
    **kwargs,
):
    # combine outputPath + name
    filepath: os.PathLike = os.path.join(
        outpath,
        f"{name}.csv",
    )
    # get all custom queries
    queries: Mapping["ConstName", "Queries"] = filter(
        lambda x: x[0].isupper(), inspect.getmembers(k8s_queries.PodQueries)
    )
    # execute queries and get values
    def query_exec(inp):
        metric_name, query_str = inp
        query = query_str.format(pod_name=name)
        val = prom.custom_query(query)[0].get("value")[1]  # get only the value no time
        return metric_name, val

    row: Dict["metricName", "MetricValue"] = dict(map(query_exec, queries))
    # append to csv
    _record_into_csv(filepath, row)


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


def _record_into_csv(path: os.PathLike, row: dict):
    row["Time"] = time()
    file_exist = os.path.exists(path)
    if file_exist:
        df: pd.DataFrame = pd.read_csv(path)
        df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    else:
        df = pd.DataFrame([row])
    df.to_csv(path, index=False)
