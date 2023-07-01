import k8sRecorder._types.containers as ContainerTypes
import k8sRecorder._types.pods as PodTypes
from k8sRecorder.utils import resource_limit
from typing import Dict, Mapping, Tuple
import pandas as pd
import logging


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
