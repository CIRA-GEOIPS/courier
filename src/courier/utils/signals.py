"""OS signal handling for graceful service shutdown."""

import signal
import threading
from typing import Any

from courier.utils.logging import get_logger

_logger = get_logger("module", "signals", None)


class SignalHandler:
    """Handles OS signals for graceful service shutdown.

    Manages SIGTERM and SIGINT signals to enable graceful shutdown of the
    service when requested by the operating system or user interruption.

    Attributes
    ----------
    _shutdown_requested : bool
        Whether a shutdown signal has been received.
    stop_event : threading.Event
        Event that is set when a shutdown signal is received.  Can be passed
        to ``retry_with_backoff`` so that long backoff sleeps are interrupted
        immediately when the user presses Ctrl-C or SIGTERM is received.

    Properties
    ----------
    shutdown_requested : bool
        Read-only property indicating if shutdown was requested.

    Examples
    --------
    >>> handler = SignalHandler()
    >>> handler.shutdown_requested
    False
    """

    def __init__(self) -> None:
        """Initialize signal handler and register signal callbacks."""
        self._shutdown_requested = False
        self.stop_event = threading.Event()
        self._setup_signal_handlers()

    def _setup_signal_handlers(self) -> None:
        """Configure signal handlers for SIGTERM and SIGINT."""
        signal.signal(signal.SIGTERM, self._handle_shutdown_signal)
        signal.signal(signal.SIGINT, self._handle_shutdown_signal)

    def _handle_shutdown_signal(self, signal_num: int, _frame: Any) -> None:
        """Handle received shutdown signals by setting shutdown flag.

        Parameters
        ----------
        signal_num : int
            Signal number that was received.
        _frame : Any
            Current stack frame (unused but required by signal handler interface).
        """
        _logger.info(f"Received signal {signal_num}, requesting graceful shutdown...")
        self._shutdown_requested = True
        self.stop_event.set()

    @property
    def shutdown_requested(self) -> bool:
        """Check if graceful shutdown has been requested via signal.

        Returns
        -------
        bool
            True if shutdown signal was received, False otherwise.

        Examples
        --------
        >>> handler = SignalHandler()
        >>> handler.shutdown_requested
        False
        """
        return self._shutdown_requested
