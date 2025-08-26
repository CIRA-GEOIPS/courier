"""Dispatcher plugin which spawns processes via a multiprocessing pool."""

import multiprocessing as mp
from os.path import basename
import time

# import jinja2

from geoips_driver.clean.driver_components import process_utils, template_utils

interface = "dispatchers"
name = "pool"
family = "multiprocessing"


def call(template, steps, template_dir=None, core_count=None, display_name=None):
    """Dispatch processes in a pool-driven fashion.

    Parameters
    ----------
    template: str
        - The name of the template we are going to use to produce GeoIPS bash scripts
    steps: SimpleNamespace
        - A SimpleNamespace object representing an ordered dictionary which depicts the
          order of operations needed to produce the correct output
    template_dir: str, default=None
        - The path to the directory which contains 'template'. If None, this defaults to
          $GEOIPS_PACKAGES_DIR/geoips_driver/geoips_driver/templates
    core_count: int, default=None
        - How many cores the dispatcher should allocate for your workflow(s). If None,
          this is dynamically determined via the number of outputs produced via your
          workflow(s)
    display_name: str, default=None
        - The process display name for the jobs dispatched. If None, this is dynamically
          determined via the workflow(s) provided.
    """
    template = template_utils.get_template(template, template_dir=template_dir)
