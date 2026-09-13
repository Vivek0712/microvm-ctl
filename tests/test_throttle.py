"""Token bucket + retry behaviour (no AWS, no sleeping longer than a few ms)."""

import time

import pytest
from botocore.exceptions import ClientError

from microvm.throttle import Throttled, TokenBucket


def _client_error(code):
    return ClientError({"Error": {"Code": code, "Message": code}}, "RunMicrovm")


def test_bucket_paces_to_rate():
    bucket = TokenBucket(rate_per_second=50)  # capacity 50, so drain it first
    for _ in range(50):
        bucket.acquire()
    t0 = time.monotonic()
    for _ in range(5):
        bucket.acquire()
    elapsed = time.monotonic() - t0
    assert 0.08 <= elapsed < 0.5  # 5 tokens at 50/s is 100 ms, give or take


def test_retries_throttling_then_succeeds(monkeypatch):
    monkeypatch.setattr("microvm.throttle.time.sleep", lambda s: None)
    calls = []

    def flaky(**kw):
        calls.append(kw)
        if len(calls) < 3:
            raise _client_error("ThrottlingException")
        return {"ok": True}

    assert Throttled(flaky, rate_per_second=1000)(x=1) == {"ok": True}
    assert len(calls) == 3


def test_non_retryable_error_is_raised_immediately(monkeypatch):
    monkeypatch.setattr("microvm.throttle.time.sleep", lambda s: None)
    calls = []

    def bad(**kw):
        calls.append(kw)
        raise _client_error("ValidationException")

    with pytest.raises(ClientError):
        Throttled(bad, rate_per_second=1000)()
    assert len(calls) == 1


def test_gives_up_after_max_attempts(monkeypatch):
    monkeypatch.setattr("microvm.throttle.time.sleep", lambda s: None)
    calls = []

    def always(**kw):
        calls.append(kw)
        raise _client_error("TooManyRequestsException")

    with pytest.raises(ClientError):
        Throttled(always, rate_per_second=1000, max_attempts=4)()
    assert len(calls) == 4
