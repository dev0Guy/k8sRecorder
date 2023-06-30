from prometheus_api_client import PrometheusConnect
from k8Recorder.operators import podsOperators
from k8Recorder.utils import set_logging
import kubernetes as k8s
import logging
import click
import kopf
import os

OUTPUT_FODLER: os.PathLike
INPUT_FILDER: os.PathLike
PROM: PrometheusConnect

@kopf.on.startup()
async def k8s_tracker_startup(logger: logging, **kwargs):
    global PROM
    k8s.config.load_kube_config()
    set_logging()
    PROM = PrometheusConnect(url="http://127.0.0.1:52030/", disable_ssl=True)
    logger.warn("Starting ...")


@kopf.on.cleanup()
async def k8s_tracker_cleanup(logger: logging, **kwargs):
    logger.info("Finished ...")


# @kopf.on.create("v1", "pods")
# async def on_pod_creation(**kwargs):
#     operation = PodOperation.Create
#     await podsOperators.check_limits(operation=operation, **kwargs)


# @kopf.on.update("v1", "pods")
# async def on_pod_update(**kwargs):
#     operation = PodOperation.Update
#     await podsOperators.check_limits(operation=operation, **kwargs)


@kopf.timer("pods", interval=5)
async def record(**kwargs):
    await podsOperators.record(OUTPUT_FODLER, PROM, **kwargs)


@click.command()
@click.option("--dumpTo", help="Folder reocrd will be push into")
@click.option("--loadFrom", help="Folder with all pod csv record files")
def run_operators(dumpto: os.PathLike, loadfrom: os.PathLike):
    global OUTPUT_FODLER, INPUT_FILDER
    if dumpto and loadfrom:
        logging.error(f"Can only load or dump, given both.")
        return None
    dumpto = dumpto if dumpto else "./output"
    # check if folder exists
    out_folder_exist: bool = os.path.isdir(dumpto) 
    in_folder_exist: bool = os.path.isdir(loadfrom) if loadfrom else False
    # check it
    if not out_folder_exist: 
        logging.warn(f" {dumpto} folder don't exist, creating it ...")
        os.makedirs(dumpto)
    if in_folder_exist and not in_folder_exist: 
        logging.warn(f" {loadfrom} folder don't exist, creating it ...")
        os.makedirs(loadfrom)
    OUTPUT_FODLER = dumpto
    INPUT_FILDER = loadfrom
    # run all operations
    kopf.run()
