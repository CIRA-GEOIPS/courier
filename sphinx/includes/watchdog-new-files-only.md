(watchdog-timing)=
> **Important:** The file watcher only detects files created or moved into the directory **after** the service starts. Pre-existing files are ignored. Use `touch` to create new test files, or `cp` to create copies with new names, while the service is running.
