"""Daemon which produces GeoIPS Stitched-Infrared unprojected imagery.

Uses data from satellite sensor pairs GOES16/18-ABI, M09/10-SEVIRI, GK2A-AMI, and
H09-AHI on bands at or near 10.4 micron (Channel 13).
"""

import logging
import os
from pathlib import Path
import shutil
import time

from geoips_driver.algorithm_info import (
    algorithms,
    calendar_to_julian,
    curr_calendar_date,
    NewJulianDateException,
)

from geoips_driver.driver_components import FileLocator, ProcessSpawner


LOG = logging.getLogger(__name__)


class StitchedInfraredRunner(ProcessSpawner):
    """Daemon which produces GeoIPS NRT Stitched Infrared outputs.

    Searches through multiple directories (denoted by satellite and sensor) for data
    closest to the nearest 30 minute mark. Once data from all satellites has arrived,
    copy the data over to a consolidated location to perform GeoIPS processing on.

    Submits jobfiles via slurm or executes multiple processes in parallel using python
    multiprocess for GeoIPS-based processing. This will likely need to be adjusted based
    on the hardware in which you are running on.
    """

    last_processed_hhnn = None

    def __init__(self, algorithm, sat, sensor, sector, use_slurm=True):
        """Initialize the daemon to listen for arriving data at 'watch_directory.

        Parameters
        ----------
        algorithm: str
            - The algorithm which we want observe for produced data
        sat: str
            - The name of the satellite which uses 'algorithm'
        sensor: str
            - The sensor on 'sat' which makes use of 'algorithm'
        sector: str
            - The geospatial sector containing data produced from 'algorithm.sat.sensor'
        use_slurm: bool (default = True)
            - Whether or not we want to use slurm-based automated processing. If False,
              we'll use multiprocessing to automate.
        """
        if algorithm not in algorithms:
            raise NotImplementedError(
                f"Algorithm '{algorithm}' hasn't been implemented in "
                "geoips_driver.algorithm_info:algorithms. Please create an info "
                f"container for '{algorithm}' before instantiating a watcher for it."
            )
        self.alg_info = algorithms[algorithm]
        self.use_slurm = use_slurm
        # NOTE: most likely need to add a check here to ensure that
        # sat, sensor, and sector are valid keys to this information dictionary
        self.watch_directory = f"{self.alg_info.paths[sat][sensor][sector]}/data/"
        if not os.path.exists(self.watch_directory):
            os.makedirs(self.watch_directory)

        self.sector = self.alg_info.sector_mapping[sector]

        if self.use_slurm:
            self.outdir = self.alg_info.slurm_dir
        else:
            self.outdir = self.alg_info.mp_dir

        # prev_hhnn = nearest_half_hour_utc()
        calendar_date = curr_calendar_date()
        jdate = calendar_to_julian()[-3:]
        year = calendar_date[0:4]
        month = calendar_date[4:6]
        day = calendar_date[6:8]
        prev_hhnn = "0000"
        date_dict = {
            "jdate": jdate,
            "year": year,
            "month": month,
            "day": day,
            "hhnn": prev_hhnn,
        }
        # hh = prev_hhnn[:2]
        # nn = prev_hhnn[4:]
        next_hhnn = None
        while True:
            if next_hhnn is None or next_hhnn != prev_hhnn:
                if next_hhnn is None:
                    # If None, set next_hhnn to the original value of prev_hhnn. This
                    # ensures that comparisons in the future will be valid time stamps
                    # instead of None vs a time stamp.
                    next_hhnn = prev_hhnn
                # This assumes the files will eventually arrive. If a
                # 'FileNotFoundError' is raised, we've been searching for too long and
                # the files most likely won't arrive. Reset our search to the next
                # timestep.
                try:
                    self.start_processing(date_dict)
                except FileNotFoundError:
                    print(
                        f"All required files for timestep {next_hhnn} weren't found. "
                        "Skipping to next timestep."
                    )

                prev_hhnn = next_hhnn
                hh = prev_hhnn[:2]
                nn = prev_hhnn[2:]

                if nn == "30":
                    hh = str((int(hh) + 1) % 24).zfill(2)
                    nn = "00"
                    if hh == "00":
                        raise NewJulianDateException(
                            "New date has started. Reinitialize this watcher."
                        )
                else:
                    nn = "30"
                next_hhnn = f"{hh}{nn}"
                date_dict["hhnn"] = next_hhnn
            # next_hhnn = nearest_half_hour_utc()

    def start_processing(self, date_dict):
        """Kick off processing of Stitched-Infrared imagery for arriving data.

        Parameters
        ----------
        date_dict: dict
            - Dictionary of information pertaining to the datetime that we want to
              operate on. Formatted {jdate, year, month, day, hhnn}, where all
              of those keys are string values representing their corresponding time.
        """
        times_searched = 0
        finfo = self.get_file_info(date_dict)
        self.fl = FileLocator(finfo)
        ffound = self.fl.all_files_found()
        while not ffound:
            print(f"Waiting for data to be transferred to a temporary directory.")
            print(f"{(times_searched * 30) / 60} minutes elapsed.")
            time.sleep(30)
            ffound = self.fl.all_files_found()
            times_searched += 1
            if times_searched >= 121:
                # We've been searching for longer than an hour. This means the files
                # we're expecting probably will not arrive. Raise an error and search
                # for the next time step.
                raise FileNotFoundError(
                    "Process has been searching for required hours for over an hour and"
                    " they haven't been found. Resetting search to next timestep."
                )

        self.required_filepaths = self.fl.required_filepaths
        # List of file paths that end in '.bz2'. These need to be decompressed before
        # they can be used in GeoIPS (Himwarai9 AHI Files)
        # bzipped_fpaths = list(
        #     filter(lambda f: Path(f).suffix == ".bz2", self.required_filepaths)
        # )
        # All the other files used in this process are ready to go. No decompression
        # needed.
        # ready_fpaths = list(
        #     filter(lambda f: Path(f).suffix != ".bz2", self.required_filepaths)
        # )
        # self.bzip_files_temporarily(bzipped_fpaths)
        # Make sure the files have decompressed fully
        time.sleep(6)
        # Copy all files over to a temporary directory
        for fpath in self.required_filepaths:
            shutil.copy(fpath, self.watch_directory)
        # Make sure the files have copied over fully
        time.sleep(6)
        # Spawn your processes!
        self.spawn_processes()
        # Clean up output directory once processes have finished
        for fname in os.listdir(self.watch_directory):
            os.remove(f"{self.watch_directory}{fname}")

    def get_file_info(self, date_dict) -> dict:
        """Return a dictionary of file information used to locate files needed.

        Where the dictionary takes on the form (can be repeated):
        {keyX: {'searchdir': fpath, 'fpatterns': list(str), 'num_expected_files': int}}

        Parameters
        ----------
        date_dict: dict
            - Dictionary of information pertaining to the datetime that we want to
              operate on. Formatted {jdate, year, month, day, hhnn}, where all
              of those keys are string values representing their corresponding time.
        """
        jdate = date_dict["jdate"]
        year = date_dict["year"]
        month = date_dict["month"]
        day = date_dict["day"]
        hhnn = date_dict["hhnn"]
        # If this watcher was just initialized and the watch directory doesn't exist,
        # wait for the directory to be created before initializing the actual watcher.
        finfo = {
            "GOES16": {
                "searchdir": f"/mnt/sat/grb/goes16/{year}/{year}_{month}_{day}_{jdate}/abi/L1b/RadF",  # NOQA
                "fpatterns": [f"*M6C13*s{year}{jdate}{hhnn}*"],
                "num_expected_files": 1,
            },
            "GOES18": {
                "searchdir": f"/mnt/sat/grb/goes18/{year}/{year}_{month}_{day}_{jdate}/abi/L1b/RadF",  # NOQA
                "fpatterns": [f"*M6C13*s{year}{jdate}{hhnn}*"],
                "num_expected_files": 1,
            },
            "M09": {
                "searchdir": f"/mnt/sat/meteosat/meteosat-09/{year}{month}{day}/MSG2",
                "fpatterns": [
                    f"H-000-MSG2__-MSG2_IODC___-_________-EPI______-{year}{month}{day}{hhnn}-__",  # NOQA
                    f"H-000-MSG2__-MSG2_IODC___-_________-PRO______-{year}{month}{day}{hhnn}-__",  # NOQA
                    f"H-000-MSG2__-MSG2_IODC___-IR_108___-00000[1-8]___-{year}{month}{day}{hhnn}-C_",  # NOQA
                ],
                "num_expected_files": 10,
            },
            "M10": {
                "searchdir": f"/mnt/sat/meteosat/meteosat-10/{year}{month}{day}/MSG3",
                "fpatterns": [
                    f"H-000-MSG3__-MSG3________-_________-EPI______-{year}{month}{day}{hhnn}-__",  # NOQA
                    f"H-000-MSG3__-MSG3________-_________-PRO______-{year}{month}{day}{hhnn}-__",  # NOQA
                    f"H-000-MSG3__-MSG3________-IR_108___-00000[1-8]___-{year}{month}{day}{hhnn}-C_",  # NOQA
                ],
                "num_expected_files": 10,
            },
            "GK2A": {
                "searchdir": f"/mnt/GK2A/AMI/L1B/FD/{year}{month}/{day}/{hhnn[0:2]}",
                "fpatterns": [f"*ir105_fd020ge_*{year}{month}{day}{hhnn}*"],
                "num_expected_files": 1,
            },
            "H09": {
                "searchdir": f"/mnt/sat/ahi-unzip/himawari9/{year}{month}{day}",
                "fpatterns": [f"*{year}{month}{day}_{hhnn}_B13_FLDK_*_S[01][0-9]10*"],
                "num_expected_files": 10,
            },
        }

        return finfo

    def spawn_processes(self):
        """Execute a series of processes using data found at file_path.

        If self.use_slurm is True, Submit a series jobfiles to slurm via 'sbatch' for
        the incoming file(s).
        Otherwise, use multiprocessing to execute your processes in parallel.

        Where the set of processes / jobfiles is from the number of output types and
        product types.

            - In sequence:
                - output_type_1 (in parallel if using multiprocessing):
                    - product_1 (jobfile if using slurm)
                    - product_2 (otherwise it is one of the parallel processes)
                    - ...
                - output_type_2:
                    - product_1
                    - product_2
                - ...

        Parameters
        ----------
        fpath: str
            - The path to the data file which just arrived.
        """
        for output_type in self.alg_info.output_types:
            # Produce all specified output types
            script_paths = []
            for sector in list(self.alg_info.sector_mapping.values()):
                # For the sectors specified in your algorithm
                for product_name in self.alg_info.product_names:
                    # Do this for a set of GeoIPS products that we want to produce
                    script_path = self.create_bash_script(
                        product_name,
                        output_type,
                        sector=sector,
                        template_name="georing_infrared_template",
                    )
                    if self.use_slurm:
                        # Create, submit, and the execute job using sbatch (SLURM)
                        fname = f"{product_name}_{output_type}"
                        self.run_jobfile(fname, script_path)
                    else:
                        # Otherwise just add the recently created bash script to the
                        # list of scripts we'll execute
                        script_paths.append(script_path)
            if not self.use_slurm:
                self.parallel_process(script_paths)


def start_watching():
    """Start up the daemon which produces GeoIPS Stitched-Infrared outputs.

    Uses data from satellite sensor pairs GOES16/18-ABI, M09/10-SEVIRI, GK2A-AMI, and
    H09-AHI on bands at or near 10.4 micron (Channel 13).
    """
    StitchedInfraredRunner("StitchedInfrared", "ALL", "ALL", "ALL", use_slurm=False)


def main():
    """Start up the daemon and begin watching for data.

    When data is found, create and submit slurm jobfiles or execute bash scripts which
    will produce Stitched-Infrared Imagery on the overcast_georing grid.

    If the daemon crashes for some reason, catch that failure, wait 5 seconds, and
    attempt to restart it.
    """
    print("Initializing NASWatcher")
    while True:
        try:
            start_watching()
        except Exception and NewJulianDateException as e:
            if type(e).__name__ != "NewJulianDateException":
                LOG.error(f"Watcher crashed with error: {e}")
            else:
                print(e)
            time.sleep(5)  # Wait a bit before restarting


if __name__ == "__main__":
    main()
