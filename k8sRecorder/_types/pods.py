from typing import NewType, Dict
from enum import Enum, auto

PodId = NewType("PodId", str)
PodName = NewType("PodName", str)
PodSpec = NewType("PodSpec", Dict)


class Operation(Enum):
    Create = auto()
    Update = auto()
