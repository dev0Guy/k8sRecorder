from prometheus_api_client import PrometheusConnect
from k8sRecorder._types.actions import ScriptAction
from k8sRecorder.operators import record, replay
from k8sRecorder import utils
import asyncio
import logging
import uvloop
import click
import kopf
import os

OUTPUT_FOLDER: os.PathLike
INPUT_FILE: os.PathLike
SCRIPT_ACTION: ScriptAction
PROM: PrometheusConnect
######################
SETTINGS_FILE: os.PathLike = "settings.csv"
METRIC_FILE: os.PathLike = "metrics.csv"
DATA_FILE: os.PathLike = "data.csv"
PROMETHEUS_URL: str = "http://127.0.0.1:60046"
INTERVAL: int = 5


@kopf.on.startup()
async def k8s_tracker_startup(logger: logging, **kwargs):
    # setup loggs
    utils.set_logging(logger=logger) 
    logger.warn("Kopf Starting...")
    global PROM
    PROM = PrometheusConnect(url=PROMETHEUS_URL, disable_ssl=True)
    func, func_kwargs = lambda x: x, {}
    # run Record/Replay
    match SCRIPT_ACTION:
        case ScriptAction.Record:
            func = record.on_start
            func_kwargs = {
                "interval": INTERVAL,
                "path": METRIC_FILE,
                "prom": PROM,
            }
        case ScriptAction.Replay:
            func = replay.on_start
            func_kwargs = {}
        case _:
            logger.error("NEED TOCHECK")
            return
    # None blocking run
    print(f"Func: {func} args {func_kwargs}")
    asyncio.create_task(func(**func_kwargs))


@kopf.on.cleanup()
async def k8s_tracker_cleanup(logger: logging, **kwargs):
    # call replay or record end function
    logger.warn("Kopf Cleanup...")
    match SCRIPT_ACTION:
        case ScriptAction.Record:
            logger.warn("Cleanup - record")
            await record.on_end(
                logger=logger,
                inppath=METRIC_FILE,
                tmppath=SETTINGS_FILE,
                outpath=DATA_FILE,
            )
        case ScriptAction.Replay:
            logger.warn("Cleanup - replay")
            await replay.on_end(
                logger=logger,
            )
        case _:
            logger.error("SOME PROBLEM")


@click.command()
@click.option("--dumpTo", help="Folder reocrd will be push into")
@click.option("--loadFrom", help="Folder with all pod csv record files")
def run_operators(dumpto: os.PathLike, loadfrom: os.PathLike):
    global OUTPUT_FOLDER, INPUT_FILE, SCRIPT_ACTION
    if dumpto and loadfrom:
        logging.error(f"Can only load or dump, given both.")
        return None
    dumpto = dumpto if dumpto else "./output"
    # check if folder exists
    out_folder_exist: bool = os.path.isdir(dumpto)
    in_folder_exist: bool = os.path.isfile(loadfrom) if loadfrom else False
    SCRIPT_ACTION = ScriptAction.Replay if in_folder_exist else ScriptAction.Record
    # alert if file doesn't exist
    if not out_folder_exist:
        logging.warn(f" {dumpto} folder don't exist, creating it ...")
        os.makedirs(dumpto)
    if in_folder_exist and in_folder_exist:
        logging.eror(f" {loadfrom} Cant Reocrd And Replay at the same Time")
        return
    OUTPUT_FOLDER = dumpto
    INPUT_FILE = loadfrom
    # change event loop to 2x
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    # run all operations
    kopf.run()
