from collections.abc import Callable
from typing import Any

TaskFunc = Callable[..., Any]


class UnknownTaskError(KeyError):
    """A job asked for a task name that no worker has registered."""


_REGISTRY: dict[str, TaskFunc] = {}


def task(name: str | None = None) -> Callable[[TaskFunc], TaskFunc]:
    """Register a function as a runnable task.

    Usage:
        @task("send_email")
        def send_email(to: str, subject: str) -> dict: ...

    A job with task_name="send_email" and payload={"to": "a@b.com", "subject": "Hi"}
    makes the worker call send_email(to="a@b.com", subject="Hi").
    The function's return value must be JSON-serializable; it is stored as the job result.
    """

    def decorator(func: TaskFunc) -> TaskFunc:
        task_name = name or func.__name__
        existing = _REGISTRY.get(task_name)
        if existing is not None and existing is not func:
            raise ValueError(f"task {task_name!r} is already registered")
        _REGISTRY[task_name] = func
        return func

    return decorator


def get_task(name: str) -> TaskFunc:
    try:
        return _REGISTRY[name]
    except KeyError:
        raise UnknownTaskError(name) from None


def registered_tasks() -> list[str]:
    return sorted(_REGISTRY)