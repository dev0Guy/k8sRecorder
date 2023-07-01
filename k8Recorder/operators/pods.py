from typing import Dict, Mapping, Tuple
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
