"""Utilities for retrying operations that may fail temporarily."""

import functools
import time
from collections.abc import Callable
from typing import TypeVar


F = TypeVar("F", bound=Callable)


def retry(
    max_attempts: int = 3,
    exceptions: tuple[type[Exception], ...] = (Exception,),
    delay: float = 0,
    backoff: float = 1,
    retry_if: Callable[[Exception], bool] | None = None,
) -> Callable[[F], F]:
    """Retry a function after selected exceptions.

    ``delay`` is the wait before the second attempt; each subsequent wait is
    multiplied by ``backoff``. ``max_attempts`` includes the initial call.
    """
    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1")
    if delay < 0:
        raise ValueError("delay must not be negative")
    if backoff < 1:
        raise ValueError("backoff must be at least 1")

    def decorate(function: F) -> F:
        @functools.wraps(function)
        def wrapped(*args, **kwargs):
            wait = delay
            for attempt in range(max_attempts):
                try:
                    return function(*args, **kwargs)
                except exceptions as error:
                    if retry_if is not None and not retry_if(error):
                        raise
                    if attempt == max_attempts - 1:
                        raise
                    if wait:
                        time.sleep(wait)
                        wait *= backoff

        return wrapped  # type: ignore[return-value]

    return decorate
