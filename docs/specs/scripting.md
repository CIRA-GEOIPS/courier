# 1. Introduction

## 1.1 Introduction, Aim

As Courier develops as a service, it gradually picks up more use-cases. Some of these use-cases cannot be fulfilled with a static YAML file. Adding scripting functionality to Courier seems like a natural solution to this, allowing users to express dynamic behavior and more complex workflows without over-reliance on plugin development.

Right now, scripting functionality is meant to add some flexibility where static YAML lacks. Courier is **YAML-first**, and there will be no implemented functionality that is exclusive to scripting.

## 1.2 Goals
- Configuration in-between steps
- Dynamic configuration generation
- Run-time configuration management

# 2. Example Usage
Running Courier from a pre-existing service file:
```
import courier

client = courier.from_yaml("service.yaml")

client.start()
```

Configuring and Running a Courier service from scratch:
```
import courier
import os

client = courier.from_env(name="demo")

file_watcher_config = {
  "path": "~/incoming",
}

# unspecified attributes resolve to their default values.
file_watcher = courier.new_service(service_kind=courier.data_monitor, name="file_system_poller_watchdog", identifier="data-monitor-file-system-poller-watchdog", config=file_watcher_config)

client.add_service(file_watcher)

job_builder_config = {
  "files_per_job": 13
}

job_builder = courier.new_service(service_kind=courier.job_builder, name="file_count_builder", identifier="job-builder-file-count-builder", config=job_builder_config)

client.add_service(job_builder)

dispatcher_config = {
  "workflow_name": "abi_airmass",
  "step_override_strings": ["abi_Airmass.spec.steps.algorithm.output_units=kelvin", "filename_formatter.basedir=/Users/demo/output"]
}

dispatcher = courier.new_service(service_kind=courier.dispatcher, name="serial_geoips", identifier="dispatcher-serial-geoips", config=dispatcher_config)

client.add_service(dispatcher)

client.start()
```

Changing a value based on variability
```
import courier
from datetime import datetime

client = courier.from_yaml("service.yaml")

today = datetime.now().strftime("%Y%m%d")

client.override(
  "dispatcher.step_override_strings",
  [f"output.basedir=/data/results/{today}"]
)
```

Automatically discover inputs
```
from pathlib import Path
import courier

client = courier.from_env(name="satellite_processing")

for directory in Path("/data/satellites").glob("*/incoming"):
    name = directory.parent.name

    service = courier.new_service(
        service_kind=courier.data_monitor,
        name=f"{name}_watcher",
        identifier=f"{name}-watcher",
        config={"path": str(directory)}
    )

    client.add_service(service)

# add other services...

client.start()
```
