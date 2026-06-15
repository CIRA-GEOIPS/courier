**Files not detected**

- The file watcher only detects files created or moved **after** the service starts (see the note above about watchdog file detection timing).
- Check file permissions: `ls -la <watched-directory>`.
- Verify the watched path matches the `config.path` setting.

**RabbitMQ connection failed**

- Verify RabbitMQ is running: `docker ps | grep rabbitmq` (Docker) or `sudo systemctl status rabbitmq-server` (system service).
- Check credentials match your RabbitMQ setup (default: `admin`/`admin_test`).
- Try `http://localhost:15672` to access the RabbitMQ management UI.
