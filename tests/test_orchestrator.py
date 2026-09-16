import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from medalloan.orchestrator import DAG, Task


def test_controlled_failure_recovers_on_retry():
    attempts = 0

    def flaky_task():
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("controlled failure")
        return "recovered"

    dag = DAG(name="failure-recovery", resource_pools={"test": 1})
    dag.add_task(Task(
        name="flaky",
        action=flaky_task,
        max_attempts=2,
        backoff_seconds=0,
        resource_pool="test",
    ))

    assert dag.run() == {"flaky": "recovered"}
    assert attempts == 2


def test_controlled_failure_is_raised_after_retry_budget():
    attempts = 0

    def always_fails():
        nonlocal attempts
        attempts += 1
        raise RuntimeError("unrecoverable failure")

    dag = DAG(name="failure-budget")
    dag.add_task(Task(name="broken", action=always_fails, max_attempts=2))

    with pytest.raises(RuntimeError, match="unrecoverable failure"):
        dag.run()
    assert attempts == 2
