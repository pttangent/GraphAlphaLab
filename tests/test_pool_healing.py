from __future__ import annotations

from collections import deque

import pytest

from concurrent.futures.process import BrokenProcessPool

from graphalphalab.dual_theme_global_dag import _healable_pool_run


class _FakeFuture:
    def __init__(self, fn, task, broken):
        self._fn = fn
        self._task = task
        self._broken = broken

    def result(self):
        if self._broken[0]:
            raise BrokenProcessPool("boom")
        return self._fn(self._task)


class _FakeExecutor:
    generation = 0
    fail_tasks: set = set()

    def __init__(self, max_workers):
        self.max_workers = max_workers

    def __enter__(self):
        type(self).generation += 1
        self._broken = [type(self).generation == 1]
        return self

    def __exit__(self, *args):
        return False

    def submit(self, fn, task):
        return _FakeFuture(self._wrap(fn), task, self._broken)

    @staticmethod
    def _wrap(fn):
        def run(task):
            if task in _FakeExecutor.fail_tasks:
                raise ValueError(f"boom on {task}")
            return {"unit": task}
        return run


@pytest.fixture
def fake_wait(monkeypatch):
    monkeypatch.setattr(
        "graphalphalab.dual_theme_global_dag.wait",
        lambda active, return_when=None: (set(active), set()),
    )
    _FakeExecutor.generation = 0
    _FakeExecutor.fail_tasks = set()
    return _FakeExecutor


def test_pool_break_requeues_and_steps_down(fake_wait) -> None:
    tasks = deque(["a", "b", "c", "d"])
    succeeded: list[str] = []
    failures: list[str] = []
    rebuilds: list[tuple[int, int, int, int]] = []
    _healable_pool_run(
        tasks,
        initial_workers=8,
        min_workers=2,
        run_task=lambda task: {"unit": task},
        on_success=lambda task, result: succeeded.append(task),
        on_task_failure=lambda task, exc: failures.append(str(exc)),
        on_pool_rebuild=lambda old, new, n, rq: rebuilds.append((old, new, n, rq)),
        executor_factory=fake_wait,
    )
    assert sorted(succeeded) == ["a", "b", "c", "d"]
    assert failures == []
    assert rebuilds == [(8, 6, 1, 4)]
    assert tasks == deque()


def test_task_failure_does_not_trigger_rebuild(fake_wait) -> None:
    _FakeExecutor.generation = 1  # skip the broken first generation
    _FakeExecutor.fail_tasks = {"bad"}
    tasks = deque(["ok", "bad"])
    succeeded: list[str] = []
    failures: list[str] = []
    rebuilds: list = []
    _healable_pool_run(
        tasks,
        initial_workers=4,
        min_workers=2,
        run_task=lambda task: {"unit": task},
        on_success=lambda task, result: succeeded.append(task),
        on_task_failure=lambda task, exc: failures.append(task),
        on_pool_rebuild=lambda *args: rebuilds.append(args),
        executor_factory=fake_wait,
    )
    assert succeeded == ["ok"]
    assert failures == ["bad"]
    assert rebuilds == []


def test_pool_break_at_minimum_workers_raises(fake_wait) -> None:
    tasks = deque(["x"])
    with pytest.raises(RuntimeError, match="cannot heal further"):
        _healable_pool_run(
            tasks,
            initial_workers=4,
            min_workers=4,
            run_task=lambda task: {"unit": task},
            on_success=lambda task, result: None,
            on_task_failure=lambda task, exc: None,
            on_pool_rebuild=lambda *args: None,
            executor_factory=fake_wait,
        )
