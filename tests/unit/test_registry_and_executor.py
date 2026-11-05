import multiprocessing as mp
import time

import pytest

from worker import registry
from worker.executor import _child_main, execute
from worker.registry import UnknownTaskError, get_task, registered_tasks, task


@pytest.fixture
def clean_registry():
    """Tests may register throwaway tasks; put the registry back afterwards."""
    saved = dict(registry._REGISTRY)
    yield
    registry._REGISTRY.clear()
    registry._REGISTRY.update(saved)


def run_child(task_name, payload):
    """Run the child-side logic in this process and return what it reports back."""
    receiver, sender = mp.Pipe(duplex=False)
    _child_main(task_name, payload, sender)
    return receiver.recv()


# ---- registry ----------------------------------------------------------------------------


def test_task_registers_under_given_name_and_stays_callable(clean_registry):
    @task("double")
    def double(x):
        return {"x2": x * 2}

    assert get_task("double") is double
    assert double(4) == {"x2": 8}  # decorator returns the function unchanged


def test_task_defaults_to_function_name(clean_registry):
    @task()
    def my_task():
        return {}

    assert "my_task" in registered_tasks()


def test_registering_a_different_function_under_a_taken_name_fails(clean_registry):
    @task("dup")
    def first():
        return {}

    with pytest.raises(ValueError, match="already registered"):

        @task("dup")
        def second():
            return {}


def test_registering_the_same_function_twice_is_allowed(clean_registry):
    def f():
        return {}

    task("same")(f)
    task("same")(f)  # e.g. a module being reloaded
    assert get_task("same") is f


def test_get_task_unknown_name_raises(clean_registry):
    with pytest.raises(UnknownTaskError):
        get_task("does_not_exist")


def test_sample_tasks_are_registered_and_list_is_sorted():
    import worker.tasks  # noqa: F401

    names = registered_tasks()
    assert {"add", "send_email", "sleep", "always_fail"} <= set(names)
    assert names == sorted(names)


# ---- child-side logic (runs in-process, so throwaway test tasks are visible) ---------------


def test_child_reports_dict_result_unchanged(clean_registry):
    task("ok")(lambda: {"a": 1})
    assert run_child("ok", {}) == ("ok", {"a": 1})


def test_child_wraps_non_dict_result(clean_registry):
    task("seven")(lambda: 7)
    assert run_child("seven", {}) == ("ok", {"value": 7})


def test_child_rejects_result_that_is_not_json_serializable(clean_registry):
    task("bad_result")(lambda: {"s": {1, 2, 3}})  # sets can't be stored as JSON
    kind, message = run_child("bad_result", {})
    assert kind == "error"
    assert "TypeError" in message


def test_child_reports_exception_traceback(clean_registry):
    def boom():
        raise RuntimeError("kaboom")

    task("boom")(boom)
    kind, message = run_child("boom", {})
    assert kind == "error"
    assert "RuntimeError: kaboom" in message


def test_child_reports_unknown_task():
    assert run_child("nope", {}) == ("error", "unknown task: 'nope'")


# ---- execute(): real child process --------------------------------------------------------


def test_execute_success():
    result = execute("add", {"a": 2, "b": 3}, timeout_seconds=10)
    assert result.ok
    assert result.result == {"sum": 5}
    assert result.error is None and result.timed_out is False


def test_execute_captures_task_exception():
    result = execute("send_email", {"to": "not-an-email"}, timeout_seconds=10)
    assert not result.ok
    assert result.timed_out is False
    assert "ValueError: invalid email address" in result.error


def test_execute_unknown_task():
    result = execute("nope", {}, timeout_seconds=10)
    assert not result.ok
    assert result.error == "unknown task: 'nope'"


def test_execute_wrong_payload_keys_is_a_failure_not_a_crash():
    result = execute("add", {"a": 1, "unexpected": 2}, timeout_seconds=10)
    assert not result.ok
    assert "TypeError" in result.error


def test_execute_kills_task_that_exceeds_timeout():
    started = time.monotonic()
    result = execute("sleep", {"seconds": 30}, timeout_seconds=1)
    elapsed = time.monotonic() - started
    assert not result.ok
    assert result.timed_out is True
    assert "timed out" in result.error
    assert elapsed < 10  # returned shortly after the timeout, not after the 30s sleep