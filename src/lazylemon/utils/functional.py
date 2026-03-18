"""Functional programming utilities."""

from collections.abc import Callable, Iterable
from functools import reduce
from typing import TypeVar

T = TypeVar("T")
R = TypeVar("R")


def compose(*functions: Callable[..., object]) -> Callable[[object], object]:
    """Compose functions from right to left.

    Creates a new function that applies the given functions in reverse order,
    passing the result of each function as input to the next.

    Parameters
    ----------
    *functions : Callable
        Variable number of functions to compose. Functions are applied
        right-to-left (last function is applied first).

    Returns
    -------
    Callable[[Any], Any]
        Composed function that applies all input functions in sequence.

    Examples
    --------
    >>> add_one = lambda x: x + 1
    >>> multiply_two = lambda x: x * 2
    >>> composed = compose(add_one, multiply_two)
    >>> composed(3)  # (3 * 2) + 1
    7
    """
    return reduce(lambda f, g: lambda x: f(g(x)), functions, lambda x: x)


def pipe(*functions: Callable[..., object]) -> Callable[[object], object]:
    """Pipe functions from left to right.

    Creates a new function that applies the given functions in order,
    passing the result of each function as input to the next.

    Parameters
    ----------
    *functions : Callable
        Variable number of functions to pipe. Functions are applied
        left-to-right (first function is applied first).

    Returns
    -------
    Callable[[Any], Any]
        Piped function that applies all input functions in sequence.

    Examples
    --------
    >>> add_one = lambda x: x + 1
    >>> multiply_two = lambda x: x * 2
    >>> piped = pipe(add_one, multiply_two)
    >>> piped(3)  # (3 + 1) * 2
    8
    """
    return reduce(lambda f, g: lambda x: g(f(x)), functions, lambda x: x)


# ignore on this line because the return type is a Callable that takes T | None
# which is not best practice in 3.12 but required to support 3.11+
def maybe(default: T) -> Callable[[T | None], T]:  # noqa: UP047
    """Return value or default if None.

    Creates a function that returns the input value if not None,
    otherwise returns the specified default value.

    Parameters
    ----------
    default : T
        Default value to return when input is None.

    Returns
    -------
    Callable[[T | None], T]
        Function that returns input value or default.

    Examples
    --------
    >>> maybe_zero = maybe(0)
    >>> maybe_zero(None)
    0
    >>> maybe_zero(5)
    5
    """
    return lambda x: x if x is not None else default


def filter_map[T, R](
    predicate: Callable[[T], bool],
    transform: Callable[[T], R],
    items: Iterable[T],
) -> list[R]:
    """Filter and map in a single operation.

    Filters items using a predicate function and transforms matching
    items using a transform function in a single pass.

    Parameters
    ----------
    predicate : Callable[[T], bool]
        Function to test each item. Only items returning True are transformed.
    transform : Callable[[T], R]
        Function to transform filtered items.
    items : Iterable[T]
        Items to filter and transform.

    Returns
    -------
    list[R]
        List of transformed items that passed the predicate test.

    Examples
    --------
    >>> filter_map(lambda x: x % 2 == 0, lambda x: x * 2, [1, 2, 3, 4])
    [4, 8]
    """
    return [transform(item) for item in items if predicate(item)]
