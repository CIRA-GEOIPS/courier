"""The parts of the poison-message guard that need no broker.

The behaviour itself lives in ``tests/rabbitmq/test_poison_message_progress.py``
-- redelivery is a real-broker property. What is checked here is the arithmetic
that decides *when* a message has run out of attempts, because getting it wrong
in either direction is silent: too eager parks healthy messages, too lax
restores the loop the guard exists to break.
"""

from __future__ import annotations

import pytest

from courier.broker.kombu import DELIVERY_ATTEMPT_HEADER, delivery_attempt
from courier.config import ServiceConfig
from courier.constants import MAX_QUEUE_NAME_LENGTH, dead_letter_queue_for
from courier.errors import ConfigurationError, InvalidIdentifierError


class TestDeliveryAttempt:
    """How many times a message has been tried, read off its headers."""

    def test_a_message_with_no_attempt_header_is_on_its_first_attempt(self) -> None:
        """Every message already queued when this ships has no header.

        Treating those as anything other than a first attempt would park a
        backlog of perfectly good messages on upgrade.
        """
        assert delivery_attempt({}) == 1

    def test_the_attempt_header_is_read_back_after_a_round_trip(self) -> None:
        """Headers arrive as strings, so the count has to survive being one."""
        assert delivery_attempt({DELIVERY_ATTEMPT_HEADER: "4"}) == 4

    @pytest.mark.parametrize(
        "value",
        ["", "   ", "not-a-number", "3.5", "-2", "0", "99999999999999999999x"],
    )
    def test_an_unusable_count_falls_back_to_a_first_attempt(self, value: str) -> None:
        """A header that cannot be trusted must not be able to exhaust the budget.

        Anything can set a header. Reading garbage as a huge number would park
        the message immediately; reading it as a first attempt costs at most
        one extra retry.
        """
        assert delivery_attempt({DELIVERY_ATTEMPT_HEADER: value}) == 1

    def test_other_headers_do_not_affect_the_count(self) -> None:
        """Trace context travels in the same dict and must not be mistaken for it."""
        headers = {"traceparent": "00-abc-def-01", DELIVERY_ATTEMPT_HEADER: "2"}
        assert delivery_attempt(headers) == 2


class TestDeadLetterQueueNaming:
    """The parked queue's name is derived, not configured."""

    def test_the_dead_letter_queue_sits_beside_the_queue_it_serves(self) -> None:
        """It shares the source queue's namespace, so a prune can pair them up."""
        assert (
            dead_letter_queue_for("ns-FilesFound-jb") == "ns-FilesFound-jb-DeadLetter"
        )

    def test_a_name_the_broker_would_refuse_is_rejected_here_instead(self) -> None:
        """Failing at declare time would mean failing with poison already in hand.

        The suffix pushes a name that was legal on its own over the broker's
        limit. That has to surface when the consumer subscribes, not when a
        message finally needs somewhere to go.
        """
        longest = "q" * MAX_QUEUE_NAME_LENGTH
        with pytest.raises(InvalidIdentifierError) as caught:
            dead_letter_queue_for(longest)
        assert str(MAX_QUEUE_NAME_LENGTH) in str(caught.value)

    def test_a_name_that_still_fits_is_allowed(self) -> None:
        """The guard bounds the result, not the input."""
        base = "q" * (MAX_QUEUE_NAME_LENGTH - len("-DeadLetter"))
        assert len(dead_letter_queue_for(base)) == MAX_QUEUE_NAME_LENGTH


class TestRedeliveryConfiguration:
    """``broker_max_redeliveries`` bounds retries; it cannot disable the bound."""

    def test_zero_is_allowed_and_means_park_on_the_first_failure(self) -> None:
        """A deployment that would rather triage than retry can say so."""
        assert ServiceConfig(prometheus_port=0, broker_max_redeliveries=0)

    def test_a_negative_bound_is_refused_with_a_reason(self) -> None:
        """Negative compares as "already exhausted" and would park everything."""
        with pytest.raises(ConfigurationError) as caught:
            ServiceConfig(prometheus_port=0, broker_max_redeliveries=-1)
        assert "broker_max_redeliveries" in str(caught.value)
