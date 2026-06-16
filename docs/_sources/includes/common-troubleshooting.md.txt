**Files not detected**

- The file watcher only detects files created or moved **after** the service starts (see the note above about watchdog file detection timing).
- Check file permissions: `ls -la <watched-directory>`.
- Verify the watched path matches the `config.path` setting.
