"""Regression coverage for manual cron subprocess result/cleanup authority."""

from __future__ import annotations

import multiprocessing
import queue

import pytest


class _FakeQueue:
    def __init__(self, result=None, *, raises_empty=False):
        self._result = result
        self._raises_empty = raises_empty
        self.closed = False
        self.joined = False

    def get(self, timeout):
        assert timeout > 0
        if self._raises_empty:
            raise queue.Empty
        return self._result

    def close(self):
        self.closed = True

    def join_thread(self):
        self.joined = True


class _LingeringProcess:
    def __init__(self, *, alive_after_start=True):
        self.alive = alive_after_start
        self.started = False
        self.terminated = False
        self.killed = False
        self.join_calls = []
        self.exitcode = None

    def start(self):
        self.started = True

    def join(self, timeout=None):
        self.join_calls.append(timeout)

    def is_alive(self):
        return self.alive

    def terminate(self):
        self.terminated = True
        self.alive = False
        self.exitcode = -15

    def kill(self):
        self.killed = True
        self.alive = False
        self.exitcode = -9


class _StubbornProcess(_LingeringProcess):
    def terminate(self):
        self.terminated = True

    def kill(self):
        self.killed = True
        self.alive = False
        self.exitcode = -9


class _FakeContext:
    def __init__(self, process, result_queue):
        self._process = process
        self._queue = result_queue

    def Queue(self, maxsize):
        assert maxsize == 1
        return self._queue

    def Process(self, *, target, args):
        assert callable(target)
        assert len(args) == 3
        return self._process


def _install_fake_context(monkeypatch, process, result_queue):
    monkeypatch.setattr(
        multiprocessing,
        "get_context",
        lambda method: _FakeContext(process, result_queue),
    )


def test_returned_success_remains_authoritative_when_child_cleanup_lingers(monkeypatch):
    import api.routes as routes

    expected = (True, "output", "final", None)
    process = _LingeringProcess()
    result_queue = _FakeQueue(("ok", expected))
    _install_fake_context(monkeypatch, process, result_queue)

    result = routes._run_cron_job_in_profile_subprocess(
        {"id": "job-ok"},
        "/tmp/profile-home",
    )

    assert result == expected
    assert process.started is True
    assert process.terminated is True
    assert process.is_alive() is False
    assert result_queue.closed is True
    assert result_queue.joined is True


def test_returned_success_force_kills_child_when_terminate_does_not_reap(monkeypatch):
    import api.routes as routes

    expected = (True, "output", "final", None)
    process = _StubbornProcess()
    result_queue = _FakeQueue(("ok", expected))
    _install_fake_context(monkeypatch, process, result_queue)

    result = routes._run_cron_job_in_profile_subprocess(
        {"id": "job-stubborn"},
        "/tmp/profile-home",
    )

    assert result == expected
    assert process.terminated is True
    assert process.killed is True
    assert process.is_alive() is False


def test_returned_child_error_is_preserved_when_cleanup_lingers(monkeypatch):
    import api.routes as routes

    process = _LingeringProcess()
    result_queue = _FakeQueue(("error", "child boom", "traceback text"))
    _install_fake_context(monkeypatch, process, result_queue)

    with pytest.raises(RuntimeError, match="child boom"):
        routes._run_cron_job_in_profile_subprocess(
            {"id": "job-error"},
            "/tmp/profile-home",
        )

    assert process.terminated is True
    assert process.is_alive() is False
    assert result_queue.closed is True
    assert result_queue.joined is True


def test_no_result_timeout_still_fails_closed_and_reaps_child(monkeypatch):
    import api.routes as routes

    process = _LingeringProcess()
    result_queue = _FakeQueue(raises_empty=True)
    _install_fake_context(monkeypatch, process, result_queue)
    monkeypatch.setattr(routes, "_cron_subprocess_result_timeout_seconds", lambda job: 0.01)

    with pytest.raises(RuntimeError, match="produced no result"):
        routes._run_cron_job_in_profile_subprocess(
            {"id": "job-timeout"},
            "/tmp/profile-home",
        )

    assert process.terminated is True
    assert process.is_alive() is False
    assert result_queue.closed is True
    assert result_queue.joined is True
