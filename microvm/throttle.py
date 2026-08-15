"""Client-side throttling + retry for the Lambda MicroVMs control-plane API.

The service enforces low TPS quotas on mutating calls (RunMicrovm 5/s,
SuspendMicrovm 2/s, TerminateMicrovm 10/s). A fleet-wide scale operation
that naively fans out will be throttled; every mutating call in this
package goes through a token bucket + exponential backoff with jitter.
"""

from __future__ import annotations

import random
import threading
import time

from botocore.exceptions import ClientError

RETRYABLE = {"ThrottlingException", "TooManyRequestsException", "ServiceQuotaExceededException"}


class TokenBucket:
    def __init__(self, rate_per_second: float, burst: int | None = None):
        self.rate = rate_per_second
        self.capacity = burst or max(1, int(rate_per_second))
        self.tokens = float(self.capacity)
        self.updated = time.monotonic()
        self.lock = threading.Lock()

    def acquire(self) -> None:
        while True:
            with self.lock:
                now = time.monotonic()
                self.tokens = min(self.capacity, self.tokens + (now - self.updated) * self.rate)
                self.updated = now
                if self.tokens >= 1:
                    self.tokens -= 1
                    return
                wait = (1 - self.tokens) / self.rate
            time.sleep(wait)


class Throttled:
    """Wraps an API callable with a token bucket and retry-with-jitter."""

    def __init__(self, fn, rate_per_second: float, max_attempts: int = 6):
        self.fn = fn
        self.bucket = TokenBucket(rate_per_second)
        self.max_attempts = max_attempts

    def __call__(self, **kwargs):
        delay = 0.5
        for attempt in range(1, self.max_attempts + 1):
            self.bucket.acquire()
            try:
                return self.fn(**kwargs)
            except ClientError as e:
                code = e.response.get("Error", {}).get("Code", "")
                if code not in RETRYABLE or attempt == self.max_attempts:
                    raise
                if code == "ServiceQuotaExceededException":
                    # capacity quotas (e.g. fleet memory) free up on the order of
                    # minutes as terminations settle — wait much longer than for TPS
                    time.sleep(min(10 * attempt, 45) + random.uniform(0, 5))
                else:
                    time.sleep(delay + random.uniform(0, delay))
                    delay = min(delay * 2, 8.0)
