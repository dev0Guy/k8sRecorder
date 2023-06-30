import scipy.stats as stats
from typing import Dict
import pandas as pd
import logging
import re


def set_logging() -> None:
    logging.basicConfig(
        level=logging.WARNING,  # Set the desired logging level
        format="%(asctime)s :%(levelname)s: %(filename)s \t [%(funcName)s] %(message)s",
        handlers=[
            # logging.FileHandler('app.log'),  # Add a file handler to log to a file
            logging.StreamHandler()  # Add a stream handler to log to the console
        ],
    )


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
