"""Information necessary to add metadata to GK-2A AMI L1B data files."""

from courier.schema import DataMonitorConfig

CONFIG = DataMonitorConfig(
    name="gk2a_ami",
    spec={
        "file_metadata": {
            "gk2a_ami_le1b": {
                "source": "gk2a",
                "instrument": "ami",
                "processing_stage": "L1B",
                "num_expected": 16,
                "date": r"gk2a_ami_le1b_(?P<YYYY>\d{4})(?P<MM>\d{2})(?P<DD>\d{2})(?P<HH>\d{2})(?P<NN>\d{2})",
                "match": [
                    r"gk2a_ami_le1b_*gk2a_ami_le1b_\d{4}\d{2}\d{2}\d{2}\d{2}.*",
                ],
            },
            "full-disk": {
                "domain": "Full-Disk",
                "match": [
                    r"gk2a_ami_le1b_fd*ge_*gk2a_ami_le1b_\d{4}\d{2}\d{2}\d{2}\d{2}.*",
                ],
            },
            "extended-local-area": {
                "domain": "extended-local-area",
                "match": [
                    r"gk2a_ami_le1b_ela*lc_*gk2a_ami_le1b_\d{4}\d{2}\d{2}\d{2}\d{2}.*",
                ],
            },
            "local-area": {
                "domain": "local-area",
                "match": [
                    r"gk2a_ami_le1b_la*ge_*gk2a_ami_le1b_\d{4}\d{2}\d{2}\d{2}\d{2}.*",
                ],
            },
        },
    },
)
