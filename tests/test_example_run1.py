from geoips_driver import dummy_cli
from geoips_driver.interfaces.module_based.service import create_service_with_plugins, ServiceConfig

def run_service(config: dict)-> None:
    """Run a dummy service using the dummy-cli module."""
    print("Running example1 service with config:")
    service_config = ServiceConfig(
    rabbitmq_url=f"amqp://{config.spec.rabbitmq.username}:{config.spec.rabbitmq.password}@{config.spec.rabbitmq.host}:{config.spec.rabbitmq.port}/",
    service_namespace=config.spec.service_namespace,
    service_id=config.name
    )
    print(service_config)
    print()


    import geoips_driver.plugins.modules.data_monitors.file_system_polling as file_system_polling
    import geoips_driver.plugins.modules.job_builders.dummy_job_builder as dummy_job_builder
    import geoips_driver.plugins.modules.dispatchers.serial_bash as serial_dispatcher
    available_plugins = {
        file_system_polling.FileSystemPoller.name.lower(): file_system_polling.FileSystemPoller,
        dummy_job_builder.DummyJobBuilder.name.lower(): dummy_job_builder.DummyJobBuilder,
        serial_dispatcher.SerialDispatcher.name.lower(): serial_dispatcher.SerialDispatcher,
    }
    print("Available plugins:")
    print(available_plugins)
    print()

    plugins = []
    for plugin in config.spec.run:
        print(plugin)
        if plugin.spec.name.lower() not in available_plugins:
            raise ValueError(f"Plugin {plugin.spec.name} not found.")
        else:
            plugins.append((available_plugins[plugin.spec.name.lower()], plugin.spec.config))
    print(plugins)
    service = create_service_with_plugins(service_config, plugins)
    service.start()
    return



dummy_cli.run_with_config = run_service
dummy_cli.app()

"""

from geoips_driver.plugins.modules.job_queuers.dummy_job_queuer import (
OVERCASTJobQueuer,
)

from geoips_driver.plugins.modules.data_monitors.file_system_polling import (
FileSystemPoller,
)
from geoips_driver.plugins.modules.dispatchers.serial import (
SerialDispatcher,
)

plugins = [
(FileSystemPoller, plugin_config),
(OVERCASTJobQueuer, plugin_config),
(SerialDispatcher, plugin_config),
]


"""