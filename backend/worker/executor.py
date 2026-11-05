import json
import multiprocessing as mp
import traceback
from dataclasses import dataclass
from multiprocessing.connection import Connection
from typing import Any

from worker.registry import UnknownTaskError, get_task

# "spawn" starts a clean interpreter: no inherited DB/Redis connections or threads.
_CTX = mp.get_context("spawn")
_MAX_ERROR_CHARS = 4000


@dataclass(frozen=True)
class ExecutionResult:
    ok: bool
    result: dict[str, Any] | None = None
    error: str | None = None
    timed_out: bool = False


def _child_main(task_name: str, payload: dict[str, Any], conn: Connection) -> None:
    """Runs inside the child process. Always reports back through the pipe."""
    try:
        import worker.tasks  # noqa: F401  (importing registers every task)

        value = get_task(task_name)(**payload)
        json.dumps(value)  # fail here, not later, if the result can't be stored as JSON
        result = value if isinstance(value, dict) else {"value": value}
        conn.send(("ok", result))
    except UnknownTaskError:
        conn.send(("error", f"unknown task: {task_name!r}"))
    except BaseException:
        conn.send(("error", traceback.format_exc()))
    finally:
        conn.close()


def execute(task_name: str, payload: dict[str, Any], timeout_seconds: float) -> ExecutionResult:
    """Run one task in a child process; kill it if it exceeds timeout_seconds."""
    parent_conn, child_conn = _CTX.Pipe(duplex=False)
    proc = _CTX.Process(target=_child_main, args=(task_name, payload, child_conn), daemon=True)
    proc.start()
    child_conn.close()  # parent keeps only the receiving end
    try:
        if not parent_conn.poll(timeout_seconds):
            proc.kill()
            proc.join()
            return ExecutionResult(
                ok=False, timed_out=True, error=f"timed out after {timeout_seconds}s"
            )
        try:
            kind, value = parent_conn.recv()
        except EOFError:  # child died (crash, OOM kill) without reporting
            proc.join()
            return ExecutionResult(
                ok=False, error=f"task process exited unexpectedly (exit code {proc.exitcode})"
            )
        proc.join()
        if kind == "ok":
            return ExecutionResult(ok=True, result=value)
        return ExecutionResult(ok=False, error=value[-_MAX_ERROR_CHARS:])
    finally:
        parent_conn.close()