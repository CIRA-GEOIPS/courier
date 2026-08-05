"""Information necessary to add metadata to Meteosat-9 SEVIRI L1B data files."""

from courier.schema import DataMonitorConfig

CONFIG = DataMonitorConfig(
    name="meteosat9_seviri",
    spec={
        "file_metadata": {
            "meteosat9_seviri": {
                "source": "meteosat9",
                "instrument": "seviri",
                "processing_stage": "L1B",
                "domain": "Full-Disk",
                "num_expected": 90,
                "date": r"H-000-MSG2__-MSG2________-.*-(?P<YYYY>\d{4})(?P<MM>\d{2})(?P<DD>\d{2})(?P<HH>\d{2})(?P<NN>\d{2})-",
                "match": [
                    r"H-000-MSG2__-MSG2________-_________-EPI______-\d{4}\d{2}\d{2}\d{2}\d{2}-__",
                    r"H-000-MSG2__-MSG2________-_________-PRO______-\d{4}\d{2}\d{2}\d{2}\d{2}-__",
                    r"H-000-MSG2__-MSG2________-IR_.*___-00000[1-8]___-\d{4}\d{2}\d{2}\d{2}\d{2}-C_",
                    r"H-000-MSG2__-MSG2________-VIS.*___-00000[1-8]___-\d{4}\d{2}\d{2}\d{2}\d{2}-C_",
                    r"H-000-MSG2__-MSG2________-WV_.*___-00000[1-8]___-\d{4}\d{2}\d{2}\d{2}\d{2}-C_",
                ],
            },
        },
    },
)
