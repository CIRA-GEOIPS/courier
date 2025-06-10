"""Default driver plugin module.

Takes in a querier plugin and dispatcher plugin used to drive NRT processing making use
of GeoIPS.
"""

import logging
import socket

from geoips_driver.interfaces import dispatchers, queriers


interface = "drivers"
name = "default"
family = "standard"


def call(querier, dispatcher, cadence, offset, port):
    """Drive GeoIPS NRT processing usign the specified querier and dispatcher.

    Where the driver communicates with one or more data monitors on the port provided.

    Parameters
    ----------
    querier: pydantic-based Querier Object
        - An object containing information on what querier plugin to use, as well
          as the search parameters that querier will need to know to search an
          information storage system appropriately.
    dispatcher: pydantic-based Dispatcher Object
        - An object containing information on what dispatcher plugin to use, as
          well as additional arguments needed to appropriately spawn a job/process
          for GeoIPS to run
    cadence: str
        - How often a job should be dispatched. Formatted using dateparser's natural
          language format. See https://dateparser.readthedocs.io/en/latest/index.html
          for more information.
    offset: str
        - Time offset from the top of the hour to dispatch a process at. Formatted using
          dateparser's natural language format. See
          https://dateparser.readthedocs.io/en/latest/index.html for more information.
    port: int
        - The port number for microservices to reveive data over (no external access
          provided)
    """
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(("localhost", port))
    # I think the int provided to listen is how many messages to go back for?
    server.listen(5)
    # TODO: implement logic to query for appropriate files when a file is found via the
    # data_monitors that are running.

    qplg = queriers.get_plugin(querier.name)
    dplg = dispatchers.get_plugin(dispatcher.name)
    # when a file is found: implement the pseudo-code written below
    # required_files = qplg(finfo, querier.arguments.source_names)
    # if required_files:
    #   dplg(<some_info>)
