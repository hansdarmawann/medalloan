"""Small, dependency-free orchestration primitives for the training pipeline."""

from dataclasses import dataclass, field
import time
from concurrent.futures import ThreadPoolExecutor
from threading import BoundedSemaphore
from typing import Callable


class OrchestrationError(RuntimeError):
    """Raised when a DAG is invalid or cannot be executed."""


@dataclass(frozen=True)
class Task:
    name: str
    action: Callable[[], object]
    upstream: tuple[str, ...] = ()
    max_attempts: int = 1
    backoff_seconds: float = 0.0
    timeout_seconds: float | None = None
    resource_pool: str | None = None


@dataclass
class DAG:
    name: str
    schedule: str | None = None
    event_triggers: set[str] = field(default_factory=set)
    tasks: dict[str, Task] = field(default_factory=dict)
    max_concurrency: int = 1
    resource_pools: dict[str, int] = field(default_factory=dict)

    def add_task(self, task: Task) -> None:
        if task.name in self.tasks:
            raise OrchestrationError(f"Duplicate task: {task.name}")
        self.tasks[task.name] = task

    def topological_order(self) -> list[Task]:
        pending = dict(self.tasks)
        order: list[Task] = []
        while pending:
            ready = [task for task in pending.values()
                     if all(dep not in pending for dep in task.upstream)]
            if not ready:
                raise OrchestrationError("DAG contains an unknown dependency or cycle")
            for task in sorted(ready, key=lambda item: item.name):
                order.append(task)
                pending.pop(task.name)
        return order

    def run(self, trigger: str = "manual") -> dict[str, object]:
        if trigger != "manual" and trigger not in self.event_triggers and trigger != self.schedule:
            raise OrchestrationError(f"Trigger not configured for DAG: {trigger}")
        pools = {name: BoundedSemaphore(size) for name, size in self.resource_pools.items()}
        results: dict[str, object] = {}
        with ThreadPoolExecutor(max_workers=self.max_concurrency) as executor:
            for task in self.topological_order():
                if task.resource_pool and task.resource_pool not in pools:
                    raise OrchestrationError(f"Unknown resource pool: {task.resource_pool}")
                future = executor.submit(self._execute_task, task, pools)
                try:
                    results[task.name] = future.result(timeout=task.timeout_seconds)
                except TimeoutError as error:
                    future.cancel()
                    raise OrchestrationError(
                        f"Task timed out after {task.timeout_seconds}s: {task.name}"
                    ) from error
        return results

    @staticmethod
    def _execute_task(task: Task, pools: dict[str, BoundedSemaphore]):
        attempts = max(1, task.max_attempts)
        for attempt in range(attempts):
            semaphore = pools.get(task.resource_pool) if task.resource_pool else None
            try:
                if semaphore:
                    semaphore.acquire()
                return task.action()
            except Exception:
                if attempt + 1 >= attempts:
                    raise
                if task.backoff_seconds > 0:
                    time.sleep(task.backoff_seconds * (2 ** attempt))
            finally:
                if semaphore:
                    semaphore.release()
