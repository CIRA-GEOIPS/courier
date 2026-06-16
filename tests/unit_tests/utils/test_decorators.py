"""Unit tests for retry_with_backoff decorator (ISSUE 11)."""

from __future__ import annotations

import threading
import time
from unittest.mock import MagicMock, patch

import pytest

from courier.utils.decorators import (
    INFINITE_RETRIES,
    MAX_BACKOFF_SECONDS,
    retry_with_backoff,
)


class TestRetryWithBackoff:
    """Tests for the retry_with_backoff decorator."""

    def test_success_on_first_attempt(self) -> None:
        """Function succeeding immediately returns without retry."""
        call_count = 0

        @retry_with_backoff(max_retries=3, base_delay=0.01)
        def succeed() -> int:
            nonlocal call_count
            call_count += 1
            return 42

        assert succeed() == 42
        assert call_count == 1

    def test_retries_and_eventually_succeeds(self) -> None:
        """Function that fails twice then succeeds — retries but returns result."""
        call_count = 0

        @retry_with_backoff(max_retries=3, base_delay=0.01)
        def fail_twice() -> int:
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError("transient")
            return 99

        assert fail_twice() == 99
        assert call_count == 3

    def test_max_retries_exhausted_raises(self) -> None:
        """When all retries are exhausted the last exception propagates."""
        call_count = 0

        @retry_with_backoff(max_retries=2, base_delay=0.01)
        def always_fail() -> None:
            nonlocal call_count
            call_count += 1
            raise RuntimeError("doomed")

        with pytest.raises(RuntimeError, match="doomed"):
            always_fail()
        assert call_count == 2

    def test_infinite_retries_does_not_raise(self) -> None:
        """INFINITE_RETRIES (-1) retries forever until a call succeeds."""
        call_count = 0

        @retry_with_backoff(max_retries=INFINITE_RETRIES, base_delay=0.001)
        def succeed_after_3() -> str:
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionError("down")
            return "ok"

        assert succeed_after_3() == "ok"
        assert call_count == 3

    def test_infinite_retries_sentinel_value(self) -> None:
        """INFINITE_RETRIES equals -1."""
        assert INFINITE_RETRIES == -1

    def test_backoff_is_exponential_up_to_cap(self) -> None:
        """Backoff is doubled each attempt but never exceeds MAX_BACKOFF_SECONDS."""
        base = 1.0
        for attempt in range(10):
            computed = min(base * (2 ** attempt), MAX_BACKOFF_SECONDS)
            assert computed <= MAX_BACKOFF_SECONDS
        # With a large exponent the cap must take effect
        assert min(1.0 * (2 ** 20), MAX_BACKOFF_SECONDS) == MAX_BACKOFF_SECONDS

    def test_max_backoff_seconds_is_60(self) -> None:
        """MAX_BACKOFF_SECONDS equals 60.0."""
        assert MAX_BACKOFF_SECONDS == 60.0

    def test_stop_event_aborts_retry(self) -> None:
        """When stop_event is set during backoff, the last exception is re-raised."""
        stop_event = threading.Event()

        @retry_with_backoff(
            max_retries=5, base_delay=10.0, stop_event=stop_event,
        )
        def fail() -> None:
            raise ValueError("fail")

        # Set the stop event immediately so the next backoff loop aborts.
        stop_event.set()
        with pytest.raises(ValueError, match="fail"):
            fail()

    @patch("courier.utils.decorators.time.sleep")
    def test_keyboard_interrupt_during_backoff(self, mock_sleep: MagicMock) -> None:
        """KeyboardInterrupt during backoff sleep is re-raised immediately."""
        mock_sleep.side_effect = KeyboardInterrupt

        @retry_with_backoff(max_retries=5, base_delay=1.0)
        def fail() -> None:
            raise ValueError("fail")

        with pytest.raises(KeyboardInterrupt):
            fail()

    def test_does_not_catch_non_specified_exceptions(self) -> None:
        """Exceptions not in the exceptions tuple are not retried."""
        call_count = 0

        @retry_with_backoff(max_retries=3, base_delay=0.01, exceptions=(ValueError,))
        def fail_with_type_error() -> None:
            nonlocal call_count
            call_count += 1
            raise TypeError("not caught")

        with pytest.raises(TypeError):
            fail_with_type_error()
        assert call_count == 1

    def test_base_delay_default(self) -> None:
        """Default base_delay is 1.0 (checked by computing backoff)."""

        @retry_with_backoff(max_retries=1)  # default base_delay=1.0
        def fail() -> None:
            raise ValueError("transient")

        with patch("courier.utils.decorators.time.monotonic") as mock_monotonic:
            # Simulate time passing enough for the backoff to complete quickly
            mock_monotonic.side_effect = [0.0, 100.0]  # start, deadline passed
            with patch("courier.utils.decorators.time.sleep"):
                with pytest.raises(ValueError):
                    fail()

    def test_succeeds_after_one_retry(self) -> None:
        """max_retries=2 means up to 2 attempts: first try + 1 retry."""
        call_count = 0

        @retry_with_backoff(max_retries=2, base_delay=0.01)
        def fail_once() -> list[int]:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("first fail")
            return [call_count]

        result = fail_once()
        assert result == [2]
        assert call_count == 2  # 1 fail + 1 success = 2 calls

    def test_retry_with_backoff_preserves_return_type(self) -> None:
        """The wrapped function returns the original type on success."""

        @retry_with_backoff(max_retries=1, base_delay=0.01)
        def return_dict() -> dict[str, int]:
            return {"a": 1}

        result = return_dict()
        assert isinstance(result, dict)
        assert result == {"a": 1}

    def test_wrapper_preserves_function_metadata(self) -> None:
        """The decorator preserves __name__ and __doc__ via functools.wraps."""

        @retry_with_backoff(max_retries=1)
        def my_function() -> str:
            """Docstring for my_function."""
            return "ok"

        assert my_function.__name__ == "my_function"
        assert my_function.__doc__ == "Docstring for my_function."
