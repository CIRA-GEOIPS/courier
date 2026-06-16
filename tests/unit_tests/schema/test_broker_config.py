"""Unit tests for broker configuration schema models.

Tests cover all four transport types (AMQP, Redis, Memory, URL), the
discriminated ``BrokerConfig`` union, backward-compatible YAML parsing,
immutability guarantees, and edge cases around validation boundaries.
"""

from __future__ import annotations

import copy
from typing import Any

import pytest
import yaml
from pydantic import BaseModel, ValidationError

from courier.schema import (
    AmqpBrokerConfig,
    BrokerConfig,
    MemoryBrokerConfig,
    RedisBrokerConfig,
    ServiceConfigModel,
    UrlBrokerConfig,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _BrokerWrapper(BaseModel):
    """Thin wrapper so ``BrokerConfig`` is validated as a model field.

    The ``BeforeValidator`` on ``BrokerConfig`` only fires when Pydantic
    processes the type as a field annotation — ``TypeAdapter`` skips it.
    """

    broker: BrokerConfig


def _validate(data: Any) -> AmqpBrokerConfig | RedisBrokerConfig | MemoryBrokerConfig | UrlBrokerConfig:
    """Validate *data* through the ``BrokerConfig`` discriminated union."""
    return _BrokerWrapper(broker=data).broker


_MINIMAL_AMQP = {"host": "rabbit", "username": "u", "password": "p"}

_MINIMAL_SERVICE_YAML = """\
apiVersion: runcourier.dev/v1alpha1
kind: Service
metadata:
  name: svc
  namespace: ns
  description: d
spec:
  broker: {broker}
  run:
    - step:
        kind: k
        name: n
"""


def _service(broker_yaml: str) -> ServiceConfigModel:
    """Build a ``ServiceConfigModel`` by inlining *broker_yaml*."""
    raw = _MINIMAL_SERVICE_YAML.format(broker=broker_yaml)
    return ServiceConfigModel(**yaml.safe_load(raw))


# ═══════════════════════════════════════════════════════════════════════════
# AmqpBrokerConfig
# ═══════════════════════════════════════════════════════════════════════════


class TestAmqpBrokerConfig:
    """Tests for the AMQP transport configuration."""

    def test_minimal_construction(self) -> None:
        cfg = AmqpBrokerConfig(**_MINIMAL_AMQP)
        assert cfg.transport == "amqp"
        assert cfg.port == 5672
        assert cfg.vhost == "/"
        assert cfg.ssl is False
        assert cfg.max_retries == 5

    def test_to_url_default_vhost(self) -> None:
        cfg = AmqpBrokerConfig(**_MINIMAL_AMQP)
        assert cfg.to_url() == "amqp://u:p@rabbit:5672/"

    def test_to_url_custom_vhost(self) -> None:
        cfg = AmqpBrokerConfig(**_MINIMAL_AMQP, vhost="/prod")
        assert cfg.to_url() == "amqp://u:p@rabbit:5672/prod"

    def test_to_url_vhost_without_leading_slash(self) -> None:
        cfg = AmqpBrokerConfig(**_MINIMAL_AMQP, vhost="staging")
        assert cfg.to_url() == "amqp://u:p@rabbit:5672/staging"

    def test_to_url_ssl(self) -> None:
        cfg = AmqpBrokerConfig(**_MINIMAL_AMQP, ssl=True)
        assert cfg.to_url().startswith("amqps://")

    def test_to_url_ssl_custom_port(self) -> None:
        cfg = AmqpBrokerConfig(**_MINIMAL_AMQP, ssl=True, port=5671)
        assert cfg.to_url() == "amqps://u:p@rabbit:5671/"

    def test_custom_max_retries(self) -> None:
        cfg = AmqpBrokerConfig(**_MINIMAL_AMQP, max_retries=0)
        assert cfg.max_retries == 0

    def test_port_boundaries(self) -> None:
        assert AmqpBrokerConfig(**_MINIMAL_AMQP, port=1).port == 1
        assert AmqpBrokerConfig(**_MINIMAL_AMQP, port=65535).port == 65535

    def test_port_zero_rejected(self) -> None:
        with pytest.raises(ValidationError, match="port"):
            AmqpBrokerConfig(**_MINIMAL_AMQP, port=0)

    def test_port_above_max_rejected(self) -> None:
        with pytest.raises(ValidationError, match="port"):
            AmqpBrokerConfig(**_MINIMAL_AMQP, port=65536)

    def test_negative_max_retries_rejected(self) -> None:
        """Values below -1 are rejected; -1 means retry forever."""
        with pytest.raises(ValidationError, match="max_retries"):
            AmqpBrokerConfig(**_MINIMAL_AMQP, max_retries=-2)

    def test_max_retries_accepts_minus_one(self) -> None:
        """-1 is valid: retry forever on broker connection."""
        cfg = AmqpBrokerConfig(**_MINIMAL_AMQP, max_retries=-1)
        assert cfg.max_retries == -1

    def test_max_retries_accepts_zero(self) -> None:
        """0 is valid: no retries."""
        cfg = AmqpBrokerConfig(**_MINIMAL_AMQP, max_retries=0)
        assert cfg.max_retries == 0

    def test_empty_host_rejected(self) -> None:
        with pytest.raises(ValidationError, match="host"):
            AmqpBrokerConfig(host="", username="u", password="p")

    def test_whitespace_only_host_rejected(self) -> None:
        with pytest.raises(ValidationError, match="host"):
            AmqpBrokerConfig(host="   ", username="u", password="p")

    def test_empty_username_rejected(self) -> None:
        with pytest.raises(ValidationError, match="username"):
            AmqpBrokerConfig(host="h", username="", password="p")

    def test_empty_password_rejected(self) -> None:
        with pytest.raises(ValidationError, match="password"):
            AmqpBrokerConfig(host="h", username="u", password="")

    def test_empty_vhost_rejected(self) -> None:
        with pytest.raises(ValidationError, match="vhost"):
            AmqpBrokerConfig(**_MINIMAL_AMQP, vhost="")

    def test_missing_host_rejected(self) -> None:
        with pytest.raises(ValidationError, match="host"):
            AmqpBrokerConfig(username="u", password="p")  # type: ignore[call-arg]

    def test_missing_username_rejected(self) -> None:
        with pytest.raises(ValidationError, match="username"):
            AmqpBrokerConfig(host="h", password="p")  # type: ignore[call-arg]

    def test_missing_password_rejected(self) -> None:
        with pytest.raises(ValidationError, match="password"):
            AmqpBrokerConfig(host="h", username="u")  # type: ignore[call-arg]

    def test_extra_fields_rejected(self) -> None:
        with pytest.raises(ValidationError, match="extra"):
            AmqpBrokerConfig(**_MINIMAL_AMQP, bogus="x")  # type: ignore[call-arg]

    def test_frozen(self) -> None:
        cfg = AmqpBrokerConfig(**_MINIMAL_AMQP)
        with pytest.raises(ValidationError):
            cfg.host = "other"  # type: ignore[misc]

    def test_whitespace_stripped(self) -> None:
        cfg = AmqpBrokerConfig(host="  rabbit  ", username=" u ", password=" p ")
        assert cfg.host == "rabbit"
        assert cfg.username == "u"
        assert cfg.password == "p"

    def test_password_with_special_characters(self) -> None:
        cfg = AmqpBrokerConfig(host="h", username="u", password="p@ss:w/rd")
        assert "p@ss:w/rd" in cfg.to_url()

    def test_ipv4_host(self) -> None:
        cfg = AmqpBrokerConfig(host="192.168.1.1", username="u", password="p")
        assert cfg.to_url() == "amqp://u:p@192.168.1.1:5672/"

    def test_ipv6_host(self) -> None:
        cfg = AmqpBrokerConfig(host="::1", username="u", password="p")
        assert "::1" in cfg.to_url()


# ═══════════════════════════════════════════════════════════════════════════
# RedisBrokerConfig
# ═══════════════════════════════════════════════════════════════════════════


class TestRedisBrokerConfig:
    """Tests for the Redis transport configuration."""

    def test_minimal_defaults(self) -> None:
        cfg = RedisBrokerConfig(transport="redis")
        assert cfg.host == "localhost"
        assert cfg.port == 6379
        assert cfg.password == ""
        assert cfg.db == 0
        assert cfg.ssl is False
        assert cfg.max_retries == 5

    def test_to_url_no_password(self) -> None:
        cfg = RedisBrokerConfig(transport="redis")
        assert cfg.to_url() == "redis://localhost:6379/0"

    def test_to_url_with_password(self) -> None:
        cfg = RedisBrokerConfig(transport="redis", password="secret")
        assert cfg.to_url() == "redis://:secret@localhost:6379/0"

    def test_to_url_ssl(self) -> None:
        cfg = RedisBrokerConfig(transport="redis", ssl=True)
        assert cfg.to_url().startswith("rediss://")

    def test_to_url_custom_db(self) -> None:
        cfg = RedisBrokerConfig(transport="redis", db=15)
        assert cfg.to_url().endswith("/15")

    def test_to_url_full(self) -> None:
        cfg = RedisBrokerConfig(
            transport="redis",
            host="redis.prod",
            port=6380,
            password="pw",
            db=3,
            ssl=True,
        )
        assert cfg.to_url() == "rediss://:pw@redis.prod:6380/3"

    def test_db_zero_boundary(self) -> None:
        assert RedisBrokerConfig(transport="redis", db=0).db == 0

    def test_negative_db_rejected(self) -> None:
        with pytest.raises(ValidationError, match="db"):
            RedisBrokerConfig(transport="redis", db=-1)

    def test_port_boundaries(self) -> None:
        assert RedisBrokerConfig(transport="redis", port=1).port == 1
        assert RedisBrokerConfig(transport="redis", port=65535).port == 65535

    def test_port_zero_rejected(self) -> None:
        with pytest.raises(ValidationError, match="port"):
            RedisBrokerConfig(transport="redis", port=0)

    def test_empty_host_rejected(self) -> None:
        with pytest.raises(ValidationError, match="host"):
            RedisBrokerConfig(transport="redis", host="")

    def test_whitespace_only_host_rejected(self) -> None:
        with pytest.raises(ValidationError, match="host"):
            RedisBrokerConfig(transport="redis", host="   ")

    def test_extra_fields_rejected(self) -> None:
        with pytest.raises(ValidationError, match="extra"):
            RedisBrokerConfig(transport="redis", cluster=True)  # type: ignore[call-arg]

    def test_negative_max_retries_rejected(self) -> None:
        with pytest.raises(ValidationError, match="max_retries"):
            RedisBrokerConfig(transport="redis", max_retries=-2)

    def test_max_retries_accepts_minus_one(self) -> None:
        cfg = RedisBrokerConfig(transport="redis", max_retries=-1)
        assert cfg.max_retries == -1

    def test_frozen(self) -> None:
        cfg = RedisBrokerConfig(transport="redis")
        with pytest.raises(ValidationError):
            cfg.db = 5  # type: ignore[misc]


# ═══════════════════════════════════════════════════════════════════════════
# MemoryBrokerConfig
# ═══════════════════════════════════════════════════════════════════════════


class TestMemoryBrokerConfig:
    """Tests for the in-memory transport configuration."""

    def test_defaults(self) -> None:
        cfg = MemoryBrokerConfig(transport="memory")
        assert cfg.transport == "memory"
        assert cfg.max_retries == 5

    def test_to_url(self) -> None:
        assert MemoryBrokerConfig(transport="memory").to_url() == "memory://"

    def test_custom_max_retries(self) -> None:
        cfg = MemoryBrokerConfig(transport="memory", max_retries=0)
        assert cfg.max_retries == 0

    def test_negative_max_retries_rejected(self) -> None:
        """Values below -1 are rejected; -1 means retry forever."""
        with pytest.raises(ValidationError, match="max_retries"):
            MemoryBrokerConfig(transport="memory", max_retries=-2)

    def test_max_retries_accepts_minus_one(self) -> None:
        """-1 is valid: retry forever on broker connection."""
        cfg = MemoryBrokerConfig(transport="memory", max_retries=-1)
        assert cfg.max_retries == -1

    def test_max_retries_accepts_zero(self) -> None:
        """0 is valid: no retries."""
        cfg = MemoryBrokerConfig(transport="memory", max_retries=0)
        assert cfg.max_retries == 0

    def test_extra_fields_rejected(self) -> None:
        with pytest.raises(ValidationError, match="extra"):
            MemoryBrokerConfig(transport="memory", host="h")  # type: ignore[call-arg]

    def test_frozen(self) -> None:
        cfg = MemoryBrokerConfig(transport="memory")
        with pytest.raises(ValidationError):
            cfg.max_retries = 10  # type: ignore[misc]


# ═══════════════════════════════════════════════════════════════════════════
# UrlBrokerConfig
# ═══════════════════════════════════════════════════════════════════════════


class TestUrlBrokerConfig:
    """Tests for the generic URL passthrough configuration."""

    def test_basic(self) -> None:
        cfg = UrlBrokerConfig(transport="url", url="sqs://key:secret@")
        assert cfg.to_url() == "sqs://key:secret@"

    def test_to_url_passthrough_preserves_exact_string(self) -> None:
        raw = "mongodb://user:p%40ss@mongo.host:27017/kombu_default"
        cfg = UrlBrokerConfig(transport="url", url=raw)
        assert cfg.to_url() == raw

    @pytest.mark.parametrize(
        "url",
        [
            "sqs://",
            "redis+sentinel://sentinel:26379/master",
            "confluentkafka://localhost:9092",
            "sqla+postgresql://user:pw@db:5432/mydb",
            "filesystem://",
            "azureservicebus://policy:key@namespace",
            "gcpubsub://projects/my-project",
            "consul://consul.local:8500",
            "etcd://localhost:2379",
            "pyro://localhost/kombu.broker",
            "zookeeper://zk:2181/vhost",
        ],
        ids=[
            "sqs",
            "redis-sentinel",
            "kafka",
            "sqlalchemy",
            "filesystem",
            "azure-service-bus",
            "gcp-pubsub",
            "consul",
            "etcd",
            "pyro",
            "zookeeper",
        ],
    )
    def test_kombu_transport_urls(self, url: str) -> None:
        cfg = UrlBrokerConfig(transport="url", url=url)
        assert cfg.to_url() == url

    def test_empty_url_rejected(self) -> None:
        with pytest.raises(ValidationError, match="url"):
            UrlBrokerConfig(transport="url", url="")

    def test_whitespace_only_url_rejected(self) -> None:
        with pytest.raises(ValidationError, match="url"):
            UrlBrokerConfig(transport="url", url="   ")

    def test_missing_url_rejected(self) -> None:
        with pytest.raises(ValidationError, match="url"):
            UrlBrokerConfig(transport="url")  # type: ignore[call-arg]

    def test_extra_fields_rejected(self) -> None:
        with pytest.raises(ValidationError, match="extra"):
            UrlBrokerConfig(transport="url", url="sqs://", host="h")  # type: ignore[call-arg]

    def test_negative_max_retries_rejected(self) -> None:
        with pytest.raises(ValidationError, match="max_retries"):
            UrlBrokerConfig(transport="url", url="sqs://", max_retries=-2)

    def test_max_retries_accepts_minus_one(self) -> None:
        cfg = UrlBrokerConfig(transport="url", url="sqs://", max_retries=-1)
        assert cfg.max_retries == -1

    def test_frozen(self) -> None:
        cfg = UrlBrokerConfig(transport="url", url="memory://")
        with pytest.raises(ValidationError):
            cfg.url = "other://"  # type: ignore[misc]


# ═══════════════════════════════════════════════════════════════════════════
# BrokerConfig discriminated union
# ═══════════════════════════════════════════════════════════════════════════


class TestBrokerConfigUnion:
    """Tests for the ``BrokerConfig`` discriminated union type alias."""

    def test_amqp_via_explicit_transport(self) -> None:
        cfg = _validate({"transport": "amqp", **_MINIMAL_AMQP})
        assert isinstance(cfg, AmqpBrokerConfig)

    def test_amqp_inferred_when_host_present(self) -> None:
        cfg = _validate(_MINIMAL_AMQP)
        assert isinstance(cfg, AmqpBrokerConfig)

    def test_memory_default_when_transport_and_host_omitted(self) -> None:
        cfg = _validate({})
        assert isinstance(cfg, MemoryBrokerConfig)

    def test_memory_default_with_max_retries_only(self) -> None:
        cfg = _validate({"max_retries": 3})
        assert isinstance(cfg, MemoryBrokerConfig)
        assert cfg.max_retries == 3

    def test_redis_selected(self) -> None:
        cfg = _validate({"transport": "redis"})
        assert isinstance(cfg, RedisBrokerConfig)

    def test_memory_selected(self) -> None:
        cfg = _validate({"transport": "memory"})
        assert isinstance(cfg, MemoryBrokerConfig)

    def test_url_selected(self) -> None:
        cfg = _validate(
            {"transport": "url", "url": "kafka://localhost:9092"},
        )
        assert isinstance(cfg, UrlBrokerConfig)

    def test_unknown_transport_rejected(self) -> None:
        with pytest.raises(ValidationError):
            _validate({"transport": "carrier_pigeon"})

    def test_pre_validator_does_not_mutate_input(self) -> None:
        data = dict(_MINIMAL_AMQP)
        original = copy.deepcopy(data)
        _validate(data)
        assert data == original

    def test_pre_validator_passes_through_model_instances(self) -> None:
        cfg = MemoryBrokerConfig(transport="memory")
        result = _validate(cfg)
        assert isinstance(result, MemoryBrokerConfig)

    def test_each_variant_has_max_retries(self) -> None:
        configs = [
            _validate(_MINIMAL_AMQP),
            _validate({"transport": "redis"}),
            _validate({"transport": "memory"}),
            _validate({"transport": "url", "url": "sqs://"}),
        ]
        for cfg in configs:
            assert hasattr(cfg, "max_retries")
            assert cfg.max_retries == 5

    def test_each_variant_has_to_url(self) -> None:
        configs = [
            _validate(_MINIMAL_AMQP),
            _validate({"transport": "redis"}),
            _validate({"transport": "memory"}),
            _validate({"transport": "url", "url": "sqs://"}),
        ]
        for cfg in configs:
            assert isinstance(cfg.to_url(), str)
            assert len(cfg.to_url()) > 0


# ═══════════════════════════════════════════════════════════════════════════
# Full ServiceConfigModel integration (YAML round-trips)
# ═══════════════════════════════════════════════════════════════════════════


class TestServiceConfigBrokerIntegration:
    """Tests that BrokerConfig works end-to-end inside ServiceConfigModel."""

    def test_existing_yaml_without_transport(self) -> None:
        broker = """
            host: rabbitmq
            port: 5672
            username: admin
            password: admin_test"""
        cfg = _service(broker)
        assert isinstance(cfg.spec.broker, AmqpBrokerConfig)
        assert cfg.spec.broker.to_url() == "amqp://admin:admin_test@rabbitmq:5672/"

    def test_explicit_amqp_transport(self) -> None:
        broker = """
            transport: amqp
            host: h
            username: u
            password: p"""
        cfg = _service(broker)
        assert isinstance(cfg.spec.broker, AmqpBrokerConfig)

    def test_redis_transport_in_yaml(self) -> None:
        broker = """
            transport: redis
            host: redis.local
            password: pw
            db: 3"""
        cfg = _service(broker)
        assert isinstance(cfg.spec.broker, RedisBrokerConfig)
        assert cfg.spec.broker.to_url() == "redis://:pw@redis.local:6379/3"

    def test_memory_transport_in_yaml(self) -> None:
        broker = """
            transport: memory"""
        cfg = _service(broker)
        assert isinstance(cfg.spec.broker, MemoryBrokerConfig)
        assert cfg.spec.broker.to_url() == "memory://"

    def test_url_transport_in_yaml(self) -> None:
        broker = """
            transport: url
            url: "sqs://key:secret@"
            max_retries: 10"""
        cfg = _service(broker)
        assert isinstance(cfg.spec.broker, UrlBrokerConfig)
        assert cfg.spec.broker.to_url() == "sqs://key:secret@"
        assert cfg.spec.broker.max_retries == 10

    def test_real_example1_yaml(self) -> None:
        path = "tests/example1.yaml"
        with open(path) as f:
            raw = yaml.safe_load(f)
        cfg = ServiceConfigModel(**raw)
        assert isinstance(cfg.spec.broker, AmqpBrokerConfig)
        assert "rabbitmq" in cfg.spec.broker.to_url()

    def test_real_demo_yaml(self) -> None:
        path = "tests/demo.yaml"
        with open(path) as f:
            raw = yaml.safe_load(f)
        cfg = ServiceConfigModel(**raw)
        assert isinstance(cfg.spec.broker, AmqpBrokerConfig)
        assert cfg.spec.broker.host == "rabbitmqhost"

    def test_broker_omitted_defaults_to_memory(self) -> None:
        raw = yaml.safe_load("""\
apiVersion: runcourier.dev/v1alpha1
kind: Service
metadata:
  name: svc
  namespace: ns
  description: d
spec:
  run:
    - step:
        kind: k
        name: n
""")
        cfg = ServiceConfigModel(**raw)
        assert isinstance(cfg.spec.broker, MemoryBrokerConfig)
        assert cfg.spec.broker.to_url() == "memory://"

    def test_plugin_config_omitted_defaults_to_none(self) -> None:
        raw = yaml.safe_load("""\
apiVersion: runcourier.dev/v1alpha1
kind: Service
metadata:
  name: svc
  namespace: ns
  description: d
spec:
  run:
    - step:
        kind: k
        name: n
""")
        cfg = ServiceConfigModel(**raw)
        assert cfg.spec.run[0].spec.config is None


# ═══════════════════════════════════════════════════════════════════════════
# Edge cases and boundary conditions
# ═══════════════════════════════════════════════════════════════════════════


class TestEdgeCases:
    """Boundary conditions and tricky inputs."""

    def test_amqp_vhost_multiple_leading_slashes(self) -> None:
        cfg = AmqpBrokerConfig(**_MINIMAL_AMQP, vhost="///deep")
        assert cfg.to_url().endswith("/deep")

    def test_amqp_vhost_slash_only(self) -> None:
        cfg = AmqpBrokerConfig(**_MINIMAL_AMQP, vhost="/")
        assert cfg.to_url() == "amqp://u:p@rabbit:5672/"

    def test_amqp_max_retries_zero_is_valid(self) -> None:
        cfg = AmqpBrokerConfig(**_MINIMAL_AMQP, max_retries=0)
        assert cfg.max_retries == 0

    def test_amqp_very_large_max_retries(self) -> None:
        cfg = AmqpBrokerConfig(**_MINIMAL_AMQP, max_retries=999_999)
        assert cfg.max_retries == 999_999

    def test_redis_high_db_number(self) -> None:
        cfg = RedisBrokerConfig(transport="redis", db=100)
        assert cfg.to_url().endswith("/100")

    def test_url_with_very_long_string(self) -> None:
        long_url = "sqs://" + "a" * 2000
        cfg = UrlBrokerConfig(transport="url", url=long_url)
        assert cfg.to_url() == long_url

    def test_url_with_unicode(self) -> None:
        cfg = UrlBrokerConfig(transport="url", url="amqp://用户:密码@host/")
        assert cfg.to_url() == "amqp://用户:密码@host/"

    def test_amqp_host_with_subdomain(self) -> None:
        cfg = AmqpBrokerConfig(
            host="rabbit.prod.internal.example.com",
            username="u",
            password="p",
        )
        assert "rabbit.prod.internal.example.com" in cfg.to_url()

    def test_discriminator_case_sensitive(self) -> None:
        with pytest.raises(ValidationError):
            _validate({"transport": "AMQP", **_MINIMAL_AMQP})

    def test_discriminator_none_rejected(self) -> None:
        with pytest.raises(ValidationError):
            _validate({"transport": None, **_MINIMAL_AMQP})

    def test_empty_dict_defaults_to_memory(self) -> None:
        cfg = _validate({})
        assert isinstance(cfg, MemoryBrokerConfig)

    def test_non_dict_input_rejected(self) -> None:
        with pytest.raises(ValidationError):
            _validate("amqp://localhost")

    def test_list_input_rejected(self) -> None:
        with pytest.raises(ValidationError):
            _validate([{"transport": "memory"}])

    def test_int_input_rejected(self) -> None:
        with pytest.raises(ValidationError):
            _validate(42)

    def test_amqp_port_as_string_coerced(self) -> None:
        with pytest.raises(ValidationError):
            AmqpBrokerConfig(**_MINIMAL_AMQP, port="not_a_number")

    def test_amqp_port_float_rejected(self) -> None:
        with pytest.raises(ValidationError):
            AmqpBrokerConfig(**_MINIMAL_AMQP, port=56.72)

    def test_redis_password_empty_string_no_auth_in_url(self) -> None:
        cfg = RedisBrokerConfig(transport="redis", password="")
        assert "@" not in cfg.to_url()

    def test_redis_password_whitespace_stripped_becomes_empty(self) -> None:
        cfg = RedisBrokerConfig(transport="redis", password="   ")
        assert cfg.password == ""
        assert "@" not in cfg.to_url()
