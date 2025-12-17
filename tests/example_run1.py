from geoips_driver import dummy_cli
from geoips_driver.interfaces.module_based.service import (
    ServiceConfig, create_service_with_plugins)


def run_service(config: dict)-> None:
    """Run a dummy service using the dummy-cli module."""
    print("Running example1 service with config:")
    service_config = ServiceConfig(
    rabbitmq_url=f"amqp://{config.spec.rabbitmq.username}:{config.spec.rabbitmq.password}@{config.spec.rabbitmq.host}:{config.spec.rabbitmq.port}/", # type: ignore
    service_namespace=config.spec.service_namespace, # type: ignore
    service_id=config.name, # type: ignore
    heartbeat_interval = config.spec.heartbeat_interval, # type: ignore
    )
    print(service_config)
    print()


    import geoips_driver.plugins.modules.data_monitors.file_system_poller_watchdog as file_system_polling
    import geoips_driver.plugins.modules.dispatchers.serial_bash as serial_bash_dispatcher
    import geoips_driver.plugins.modules.job_builders.dummy_job_builder as dummy_job_builder
    available_plugins = {
        file_system_polling.FileSystemPoller.name.lower(): file_system_polling.FileSystemPoller,
        dummy_job_builder.DummyJobBuilder.name.lower(): dummy_job_builder.DummyJobBuilder,
        serial_bash_dispatcher.SerialBashDispatcher.name.lower(): serial_bash_dispatcher.SerialBashDispatcher,
    }
    print("Available plugins:")
    print(available_plugins)
    print()

    plugins = []
    for plugin in config.spec.run: #type: ignore
        print(plugin)
        if plugin.spec.name.lower() not in available_plugins:
            raise ValueError(f"Plugin {plugin.spec.name} not found.")
        else:
            plugins.append((available_plugins[plugin.spec.name.lower()], plugin.spec.config))
    print(plugins)
    service = create_service_with_plugins(service_config, plugins)
    service.start()
    return



dummy_cli.run_with_config = run_service # type: ignore
dummy_cli.app()
