"""EndpointClient token caching and retry logic against a fake HTTP session."""

import time

import pytest

from microvm.config import AUTH_HEADER, PORT_HEADER, PlaneConfig
from microvm.endpoint import EndpointClient


class FakeResponse:
    def __init__(self, status):
        self.status_code = status


class FakeApi:
    def __init__(self):
        self.minted = 0

    def get_microvm(self, microvmIdentifier):
        return {"endpoint": "abc.lambda-microvm.us-east-1.on.aws"}

    def create_microvm_auth_token(self, **kw):
        self.minted += 1
        return {"authToken": {AUTH_HEADER: f"jwe-{self.minted}"}}


class FakeHttp:
    def __init__(self, statuses):
        self.statuses = list(statuses)
        self.calls = []

    def request(self, method, url, headers=None, timeout=None, **kw):
        self.calls.append({"method": method, "url": url, "headers": dict(headers), **kw})
        return FakeResponse(self.statuses.pop(0))


@pytest.fixture()
def client(monkeypatch):
    monkeypatch.setattr("microvm.endpoint.microvm_client", lambda region, profile: FakeApi())
    monkeypatch.setattr("microvm.endpoint.time.sleep", lambda s: None)
    return EndpointClient(PlaneConfig(region="us-east-1"), "microvm-1")


def test_token_is_minted_once_and_cached(client):
    client.http = FakeHttp([200, 200])
    client.get("/a")
    client.get("/b")
    assert client.api.minted == 1
    assert client.http.calls[0]["headers"][AUTH_HEADER] == "jwe-1"


def test_403_remints_token_and_keeps_caller_headers(client):
    client.http = FakeHttp([403, 200])
    resp = client.post("/x", port=9000, headers={"Content-Type": "application/json"}, data=b"{}")
    assert resp.status_code == 200
    first, second = client.http.calls
    assert first["headers"][AUTH_HEADER] == "jwe-1"
    assert second["headers"][AUTH_HEADER] == "jwe-2"
    for call in (first, second):  # nothing lost on the retry
        assert call["headers"]["Content-Type"] == "application/json"
        assert call["headers"][PORT_HEADER] == "9000"
        assert call["data"] == b"{}"


def test_502_is_retried_within_resume_patience(client):
    client.http = FakeHttp([502, 502, 200])
    assert client.get("/state", resume_patience=30).status_code == 200
    assert len(client.http.calls) == 3


def test_502_is_returned_once_patience_is_exhausted(client, monkeypatch):
    clock = [1000.0]
    monkeypatch.setattr("microvm.endpoint.time.time", lambda: clock[0])
    client.http = FakeHttp([502, 502])

    def sleep(s):
        clock[0] += 40  # one retry burns the whole 30 s patience window

    monkeypatch.setattr("microvm.endpoint.time.sleep", sleep)
    assert client.get("/state", resume_patience=30).status_code == 502
    assert len(client.http.calls) == 2


def test_429_backs_off_then_succeeds(client):
    client.http = FakeHttp([429, 429, 200])
    assert client.get("/x").status_code == 200
    assert len(client.http.calls) == 3


def test_token_expiry_triggers_remint(client, monkeypatch):
    client.http = FakeHttp([200, 200])
    client.get("/a")
    client._token_expiry = time.time() - 1
    client.get("/b")
    assert client.api.minted == 2
