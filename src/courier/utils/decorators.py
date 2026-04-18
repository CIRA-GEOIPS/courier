"""Decorators for retry logic and execution logging."""

import threading
import time
from collections.abc import Callable
from functools import wraps
from typing import TypeVar

from courier.errors import CourierError
from courier.utils.logging import get_logger

T = TypeVar("T")

_logger = get_logger("module", "decorators", None)


def retry_with_backoff(
    max_retries: int = 5,
    base_delay: float = 1.0,
    exceptions: tuple[type[Exception], ...] = (Exception,),
    stop_event: threading.Event | None = None,
) -> Callable[[Callable[..., T]], Callable[..., T | None]]:
    """Create retry decorator with exponential backoff for transient failures.

    Returns a decorator that retries function execution on specified exceptions
    with exponentially increasing delay between attempts.

    The backoff sleep is implemented as a short-polling loop (0.1 s ticks) so
    that it can be interrupted promptly by either:

    * A ``KeyboardInterrupt`` (Ctrl-C delivered to the main thread while the
      signal handler has *not* been overridden to suppress it), or
    * A ``threading.Event`` passed as *stop_event* being set (useful when a
      custom ``SignalHandler`` absorbs SIGINT/SIGTERM before Python can raise
      ``KeyboardInterrupt``).

    Parameters
    ----------
    max_retries : int, default=5
        Maximum number of retry attempts before giving up.
    base_delay : float, default=1.0
        Initial delay in seconds, doubled after each failure.
    exceptions : tuple of type[Exception], default=(Exception,)
        Exception types that trigger retry attempts.
    stop_event : threading.Event or None, default=None
        Optional event that, when set, causes the backoff sleep to be aborted
        and the last caught exception to be re-raised immediately.  Pass the
        same ``Event`` that your signal handler sets on SIGINT/SIGTERM so the
        retry loop exits as soon as shutdown is requested.

    Returns
    -------
    Callable[[Callable[..., T]], Callable[..., T | None]]
        Decorator function that wraps target functions with retry logic.
        The wrapped function returns the original return type T on success,
        or None if all retries are exhausted without raising.

    Raises
    ------
    Exception
        Re-raises the caught exception if max_retries is reached, or if the
        stop_event is set during a backoff sleep.
    KeyboardInterrupt
        Re-raised immediately if Ctrl-C interrupts a backoff sleep.

    Examples
    --------
    >>> @retry_with_backoff(max_retries=2, base_delay=0.1)
    ... def unstable_function():
    ...     return "success"
    >>> result = unstable_function()
    >>> result
    'success'
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T | None]:
        @wraps(func)
        def wrapper(*args: object, **kwargs: object) -> T | None:
            last_exc: Exception | None = None
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exc = e
                    if attempt == max_retries - 1:
                        _logger.exception(
                            f"Max retries ({max_retries}) reached for {func.__name__}",
                        )
                        raise

                    wait_time = base_delay * (2**attempt)
                    _logger.warning(
                        f"Attempt {attempt + 1} failed for {func.__name__}: {e}. "
                        f"Retrying in {wait_time} seconds...",
                    )

                    deadline = time.monotonic() + wait_time
                    try:
                        while time.monotonic() < deadline:
                            if stop_event is not None and stop_event.is_set():
                                _logger.info(
                                    f"Retry aborted for {func.__name__}: "
                                    "stop event set during backoff.",
                                )
                                raise last_exc
                            time.sleep(min(0.1, deadline - time.monotonic()))
                    except KeyboardInterrupt:
                        _logger.info(
                            f"Retry backoff interrupted by Ctrl-C for "
                            f"{func.__name__}, aborting retries.",
                        )
                        raise

            return None

        return wrapper

    return decorator


def log_execution(func: Callable[..., T]) -> Callable[..., T]:  # noqa: UP047
    """Create decorator for logging function execution and exceptions.

    Wraps functions to log debug messages on entry/success and exception
    details on failure, then re-raises exceptions for proper error handling.

    Parameters
    ----------
    func : Callable[..., T]
        Function to be wrapped with execution logging.

    Returns
    -------
    Callable[..., T]
        Wrapped function with execution logging behavior. All exceptions
        are re-raised after logging so they propagate to the caller.

    Examples
    --------
    >>> @log_execution
    ... def sample_function(x):
    ...     return x * 2
    >>> result = sample_function(5)
    >>> result
    10
    """

    @wraps(func)
    def wrapper(*args: object, **kwargs: object) -> T:
        instance_logger = getattr(args[0], "_logger", None) if args else None
        active_logger = instance_logger or _logger

        active_logger.debug(f"Executing {func.__name__}")
        try:
            result = func(*args, **kwargs)
            active_logger.debug(f"Successfully executed {func.__name__}")
            return result  # noqa: TRY300
        except CourierError:
            active_logger.exception(f"Error in {func.__name__}")
            raise

    return wrapper
