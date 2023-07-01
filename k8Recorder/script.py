from prometheus_api_client import PrometheusConnect
from k8Recorder.operators import podsOperators
import k8Recorder.utils as utils
import kubernetes as k8s
import logging
import asyncio
import uvloop
import click
import kopf
import os

OUTPUT_FOLDER: os.PathLike
INPUT_FOLDER: os.PathLike
PROM: PrometheusConnect
######################
SETTINGS_FILE: os.PathLike = 'settings.csv'
METRIC_FILE: os.PathLike = 'metrics.csv'
DATA_FILE: os.PathLike = 'data.csv'

@kopf.on.startup()
async def k8s_tracker_startup(logger: logging, **kwargs):
    global PROM
    k8s.config.load_kube_config()
    utils.set_logging()
    PROM = PrometheusConnect(url="http://127.0.0.1:56185", disable_ssl=True)
    logger.warn("Starting ...")
    asyncio.create_task(record())


@kopf.on.cleanup()
async def k8s_tracker_cleanup(logger: logging, **kwargs):
    logger.info("Finished ...")
    metrics_file_path: os.PathLike = os.path.join(
        OUTPUT_FOLDER,
        METRIC_FILE,
    )
    setting_file_path : os.PathLike = os.path.join(
        OUTPUT_FOLDER,
        SETTINGS_FILE,
    )
    data_file_path : os.PathLike = os.path.join(
        OUTPUT_FOLDER,
        DATA_FILE,
    )
    logger.warn(f"Create settings file in {setting_file_path}")
    _ = utils.pod_up_time(
        inputpath=metrics_file_path,
        outputpath=setting_file_path,
    )
    logger.warn("Created Settigns csv")
    await utils.save_data(
        metrics_path=metrics_file_path,
        settings_path=setting_file_path,
        save_path=data_file_path,
        prom=PROM,
    )


# @kopf.on.create("v1", "pods")
# async def on_pod_creation(**kwargs):
#     operation = PodOperation.Create
#     await podsOperators.check_limits(operation=operation, **kwargs)


# @kopf.on.update("v1", "pods")
# async def on_pod_update(**kwargs):
#     operation = PodOperation.Update
#     await podsOperators.check_limits(operation=operation, **kwargs)


async def record(interval: int = 5):
    output_file_path: os.PathLike = os.path.join(
        OUTPUT_FOLDER,
        METRIC_FILE,
    )
    while True:
        logging.info("record epoch")
        await utils.record(output_file_path, PROM)
        logging.info("waiting for {interval}")
        await asyncio.sleep(interval)


@click.command()
@click.option("--dumpTo", help="Folder reocrd will be push into")
@click.option("--loadFrom", help="Folder with all pod csv record files")
def run_operators(dumpto: os.PathLike, loadfrom: os.PathLike):
    global OUTPUT_FOLDER, INPUT_FOLDER
    if dumpto and loadfrom:
        logging.error(f"Can only load or dump, given both.")
        return None
    dumpto = dumpto if dumpto else "./output"
    # check if folder exists
    out_folder_exist: bool = os.path.isdir(dumpto)
    in_folder_exist: bool = os.path.isdir(loadfrom) if loadfrom else False
    # alert if file doesn't exist
    if not out_folder_exist:
        logging.warn(f" {dumpto} folder don't exist, creating it ...")
        os.makedirs(dumpto)
    if in_folder_exist and not in_folder_exist:
        logging.eror(f" {loadfrom} folder don't exist, creating it ...")
    OUTPUT_FOLDER = dumpto
    INPUT_FOLDER = loadfrom
    # change event loop to 2x
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    # run all operations
    kopf.run()
