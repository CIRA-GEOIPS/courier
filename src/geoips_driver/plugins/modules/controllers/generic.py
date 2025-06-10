"""Generic controller plugin used to pilot GeoIPS processing for NRT products."""

import logging
import time
from multiprocessing import Process

from geoips.commandline.log_setup import setup_logging

from geoips_driver import interfaces

setup_logging()
LOG = logging.getLogger(__name__)

interface = "controllers"
name = "generic"
family = "standard"


def call(controller_config_name, port=6580):
    """Initiate daemon-like NRT processing using GeoIPS.

    Parameters
    ----------
    controller_config_name: str
        - The name of the controller_config plugin used to inform this controller how to
          operate.
    port: int
        - The port number for microservices to send data over (no external access
          provided)
    """
    controller_config = interfaces.driver_configs.get_plugin(controller_config_name)
    driver = interfaces.drivers.get_plugin("default")

    data_monitors = controller_config.spec.data_monitors
    drivers = controller_config.spec.drivers

    dm_processes = {}
    driver_processes = {}

    for dm in data_monitors:
        dm_plg = interfaces.data_monitors.get_plugin(dm.name)
        p = Process(target=dm_plg, args=(dm.arguments, port))
        dm_processes[dm.name] = p

    for driver in drivers:
        querier = driver.arguments.querier
        dispatcher = driver.arguments.dispatcher
        cadence = driver.arguments.cadence
        offset = driver.arguments.offset
        # NOTE: Need to create a 'driver' plugin which accepts a querier and a
        # dispatcher as its arguments.
        p = Process(
            target=driver,
            args=(querier, dispatcher, cadence, offset, port),
        )
        driver_processes[driver.name] = p

    while True:
        dead_dmp = 0
        dead_dp = 0
        # Check the state of the processes this controller has spawned
        for pname, p in dm_processes.items():
            if not p.is_alive():
                LOG.warning(
                    f"Process with name '{pname}', ID '{p.pid}' is no longer alive.",
                )
                p.join()
                p.close()
                dead_dmp += 1
        for pname, p in driver_processes.items():
            if not p.is_alive():
                LOG.warning(
                    f"Process with name '{pname}', ID '{p.pid}' is no longer alive.",
                )
                p.join()
                p.close()
                dead_dp += 1
        # If all data_monitor processes are dead or all driver_processes are dead
        # terminate this process
        if dead_dmp == len(dm_processes) or dead_dp == len(driver_processes):
            break
        # Wait thirty seconds before validating processes again
        time.sleep(30)

    LOG.interactive("Generic controller has no more work to do and is now terminating.")
