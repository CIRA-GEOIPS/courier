"""Information necessary to add metadata to GOES-16 ABI L1B data files.

Information necessary to monitor for GOES16 ABI L1B data files at CIRA.
"""

from courier.schema import DataMonitorConfig

CONFIG = DataMonitorConfig(
    name="goes16_abi",
    spec={
        "file_metadata": {
            "goes16_abi_l1b": {
                "source": "goes16",
                "instrument": "abi",
                "processing_stage": "L1B",
                "date": r".*s(?P<YYYY>\d{4})(?P<JJJ>\d{3})(?P<HH>\d{2})(?P<NN>\d{2}).*",
                "match": [
                    r".*M6C(0[1-9]|1[0-6]).*s\d{4}\d{3}\d{2}\d{2}.*",
                ],
            },
            "full-disk": {
                "domain": "Full-Disk",
                "num_expected": 16,
                "match": [
                    r".*RadF.*M6C(0[1-9]|1[0-6]).*s\d{4}\d{3}\d{2}\d{2}.*",
                ],
            },
            "conus": {
                "domain": "CONUS",
                "num_expected": 16,
                "match": [
                    r".*RadC.*M6C(0[1-9]|1[0-6]).*s\d{4}\d{3}\d{2}\d{2}.*",
                ],
            },
            "meso1": {
                "domain": "Meso1",
                "num_expected": 16,
                "match": [
                    r".*RadM1.*M6C(0[1-9]|1[0-6]).*s\d{4}\d{3}\d{2}\d{2}.*",
                ],
            },
            "meso2": {
                "domain": "Meso2",
                "num_expected": 16,
                "match": [
                    r".*RadM2.*M6C(0[1-9]|1[0-6]).*s\d{4}\d{3}\d{2}\d{2}.*",
                ],
            },
        },
    },
)
