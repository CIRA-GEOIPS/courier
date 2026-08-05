"""Information necessary to add metadata to Himawari-9 AHI L1B data files."""

from courier.schema import DataMonitorConfig

CONFIG = DataMonitorConfig(
    name="himawari9_ahi",
    spec={
        "file_metadata": {
            "himawari9_ahi_l1b": {
                "source": "himawari9",
                "instrument": "ahi",
                "processing_stage": "L1B",
                "date": r"HS_H09_(?P<YYYY>\d{4})(?P<MM>\d{2})(?P<DD>\d{2})_(?P<HH>\d{2})(?P<NN>\d{2})",
                "match": [
                    r".*\d{4}\d{2}\d{2}_\d{2}\d{2}.*_FLDK_.*_S[01][0-9]10.*",
                ],
            },
            "full-disk": {
                "domain": "Full-Disk",
                "match": [
                    r".*\d{4}\d{2}\d{2}_\d{2}\d{2}.*_FLDK_.*_S[01][0-9]10.*",
                ],
            },
            "japan": {
                "domain": "Japan",
                "num_expected": 40,
                "match": [
                    r".*\d{4}\d{2}\d{2}_\d{2}\d{2}.*_JP0[1-4]_.*_S0101.*",
                ],
            },
            "meso1": {
                "domain": "Meso1",
                "num_expected": 40,
                "match": [
                    r".*\d{4}\d{2}\d{2}_\d{2}\d{2}.*_R30[1-4]_.*_S0101.*",
                ],
            },
        },
    },
)
