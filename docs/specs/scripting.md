# Introduction

# Example Usage

Running Courier from a pre-existing service file:
```
import courier

client = courier.from_yaml("service.yaml")

client.start()
```

Configuring a Courier service yourself:
```
import courier

client = courier.from_env(name="demo")

file_watcher_config = {
  "path": "~/incoming",
}

# unspecified attributes resolve to their default values.
file_watcher = courier.new_service(service_kind=courier.data_monitor, name="file_system_poller_watchdog", identifier="data-monitor-file-system-poller-watchdog", config=file_watcher_config)

client.add_service(file_watcher)
```

