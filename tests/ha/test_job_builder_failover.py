import redis
from courier.config import ServiceConfig
from courier.interfaces.plugin_protocol import ServicePlugin
from courier.plugins.job_builders.dummy_job_builder import DummyJobBuilder
from courier.service import Service

REDIS_PORT = 6379
REDIS_DB = 1


def build_service(job_builder_class: type[ServicePlugin], job_builder_name: str):
    service_config = ServiceConfig(
        namespace="ha-test-courier-service", broker_url="redis://localhost:6379/0"
    )
    service = Service(config=service_config)

    job_builder_config = {
        "state_sync": {"host": "redis.internal", "port": REDIS_PORT, "db": REDIS_DB},
        "targets": ["test-ha"],
        "payload": {
            "payload-bash-payload": {
                "kind": "payload",
                "name": f"{job_builder_name}",
                "config": {"script": "echo 'ha test complete'"},
            },
        },
    }

    service.register_plugin(
        job_builder_class,
        job_builder_config,
        identifier=f"dummy-jobbuilder",
    )

    service.configure_routing(
        dispatcher_identifiers=["test-ha"],
        builder_identifiers=["dummy-jobbuilder"],
        builder_targets={"dummy-jobbuilder": ("test-sink",)},
        allow_implicit_target=False,
    )

    service.start()


def test_jobs_survive_failing_service():
    state_redis = redis.Redis(host="localhost", port=6379, decode_responses=True)

    def get_subscribers():
        state_redis.pubsub_numsub()
