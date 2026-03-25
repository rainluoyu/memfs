"""
Async worker for MemFS.
Handles background operations using thread pool.
"""

import threading
import time
from concurrent.futures import ThreadPoolExecutor, Future, as_completed
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Union
from enum import Enum


class TaskType(Enum):
    """Types of background tasks."""

    SWAP_OUT = "swap_out"
    SWAP_IN = "swap_in"
    PRELOAD = "preload"
    GC = "gc"
    WRITE = "write"
    DELETE = "delete"
    CUSTOM = "custom"


@dataclass
class Task:
    """Represents a background task."""

    task_id: str
    task_type: TaskType
    func: Callable
    args: tuple
    kwargs: dict
    priority: int = 5
    created_at: float = None

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = time.time()


@dataclass
class TaskResult:
    """Result of a background task."""

    task_id: str
    success: bool
    result: Any = None
    error: Optional[str] = None
    duration_ms: float = 0.0


class AsyncWorker:
    """
    Thread pool worker for asynchronous operations.

    Provides non-blocking execution of file operations.
    """

    def __init__(
        self, max_workers: int = 4, queue_size: int = 100, daemon: bool = True
    ):
        """
        Initialize async worker.

        Args:
            max_workers: Maximum number of worker threads.
            queue_size: Maximum queued tasks.
            daemon: Whether threads should be daemon threads.
        """
        self.max_workers = max_workers
        self.queue_size = queue_size
        self._daemon = daemon

        self._executor = ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="memfs_worker"
        )

        self._lock = threading.Lock()
        self._pending_tasks: Dict[str, Future] = {}
        self._task_counter = 0
        self._shutdown = False

        self._stats = {
            "submitted": 0,
            "completed": 0,
            "failed": 0,
            "cancelled": 0,
        }

    def submit(
        self,
        func: Callable,
        *args,
        task_type: TaskType = TaskType.CUSTOM,
        priority: int = 5,
        timeout: Optional[float] = None,
        **kwargs,
    ) -> str:
        """
        Submit a task for background execution.

        Args:
            func: Function to execute.
            *args: Positional arguments for func.
            task_type: Type of task.
            priority: Task priority (not used for execution order).
            timeout: Task timeout in seconds.
            **kwargs: Keyword arguments for func.

        Returns:
            Task ID for tracking.

        Raises:
            RuntimeError: If worker is shut down.
            QueueFull: If task queue is full.
        """
        if self._shutdown:
            raise RuntimeError("Worker is shut down")

        with self._lock:
            if len(self._pending_tasks) >= self.queue_size:
                from queue import Full

                raise Full("Task queue is full")

            self._task_counter += 1
            task_id = f"task_{self._task_counter}_{int(time.time() * 1000)}"

        task = Task(
            task_id=task_id,
            task_type=task_type,
            func=func,
            args=args,
            kwargs=kwargs,
            priority=priority,
        )

        future = self._executor.submit(self._execute_task, task, timeout)

        with self._lock:
            self._pending_tasks[task_id] = future
            self._stats["submitted"] += 1

        return task_id

    def _execute_task(self, task: Task, timeout: Optional[float] = None) -> TaskResult:
        """Execute a task and return result."""
        start_time = time.time()

        try:
            result = task.func(*task.args, **task.kwargs)
            duration_ms = (time.time() - start_time) * 1000

            return TaskResult(
                task_id=task.task_id,
                success=True,
                result=result,
                duration_ms=duration_ms,
            )
        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000
            return TaskResult(
                task_id=task.task_id,
                success=False,
                error=str(e),
                duration_ms=duration_ms,
            )

    def get_result(
        self, task_id: str, timeout: Optional[float] = None
    ) -> Optional[TaskResult]:
        """
        Get result for a task.

        Args:
            task_id: Task ID.
            timeout: Wait timeout in seconds.

        Returns:
            TaskResult or None if not found.
        """
        with self._lock:
            future = self._pending_tasks.get(task_id)

        if not future:
            return None

        try:
            result = future.result(timeout=timeout)

            with self._lock:
                if result.success:
                    self._stats["completed"] += 1
                else:
                    self._stats["failed"] += 1
                del self._pending_tasks[task_id]

            return result
        except TimeoutError:
            return None
        except Exception as e:
            return TaskResult(
                task_id=task_id,
                success=False,
                error=str(e),
            )

    def cancel(self, task_id: str) -> bool:
        """
        Cancel a pending task.

        Args:
            task_id: Task ID.

        Returns:
            True if cancelled, False if already completed or not found.
        """
        with self._lock:
            future = self._pending_tasks.get(task_id)

        if not future:
            return False

        cancelled = future.cancel()

        if cancelled:
            with self._lock:
                del self._pending_tasks[task_id]
                self._stats["cancelled"] += 1

        return cancelled

    def cancel_all(self) -> int:
        """
        Cancel all pending tasks.

        Returns:
            Number of cancelled tasks.
        """
        cancelled = 0

        with self._lock:
            task_ids = list(self._pending_tasks.keys())

        for task_id in task_ids:
            if self.cancel(task_id):
                cancelled += 1

        return cancelled

    def wait_all(self, timeout: Optional[float] = None) -> List[TaskResult]:
        """
        Wait for all pending tasks to complete.

        Args:
            timeout: Maximum wait time in seconds.

        Returns:
            List of TaskResult for all tasks.
        """
        results = []
        start_time = time.time()

        with self._lock:
            futures = [(tid, f) for tid, f in self._pending_tasks.items()]

        for task_id, future in futures:
            remaining = None
            if timeout:
                elapsed = time.time() - start_time
                remaining = max(0, timeout - elapsed)

            try:
                result = future.result(timeout=remaining)
                results.append(result)
            except TimeoutError:
                break
            except Exception as e:
                results.append(
                    TaskResult(
                        task_id=task_id,
                        success=False,
                        error=str(e),
                    )
                )

        return results

    def shutdown(self, wait: bool = True):
        """
        Shut down the worker.

        Args:
            wait: Whether to wait for pending tasks.
        """
        self._shutdown = True
        self._executor.shutdown(wait=wait)

    def get_stats(self) -> dict:
        """Get worker statistics."""
        with self._lock:
            return {
                **self._stats,
                "pending": len(self._pending_tasks),
                "max_workers": self.max_workers,
                "queue_size": self.queue_size,
            }

    @property
    def is_shutdown(self) -> bool:
        """Check if worker is shut down."""
        return self._shutdown
