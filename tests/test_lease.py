"""Lease contract on the control-plane side (pure Python, no AWS)."""

import json

import pytest

from microvm.config import PlaneConfig
from microvm.fleet import FleetManager, IdlePolicy
from microvm.lease import KINDS, Lease, LeasePolicy, client_token, decode_payload, encode_payload


def test_kinds_and_valid_leases():
    assert KINDS == ("sfn", "durable", "http", "sqs", "eventbridge", "none")
    Lease("sfn", token="t").validate()
    Lease("none").validate()
    Lease("http", token="t", target="https://x.example/cb").validate()
    Lease("sqs", token="t", target="https://sqs.us-east-1.amazonaws.com/1/q").validate()
    Lease("eventbridge", token="t", target="bus").validate()


@pytest.mark.parametrize("lease,needle", [
    (Lease("nope"), "lease.kind must be one of"),
    (Lease("sfn"), "lease.token is required for kind 'sfn'"),
    (Lease("http", token="t"), "lease.target is required for kind 'http'"),
    (Lease("http", token="t", target="queue-name"), r"must be an http\(s\) URL"),
    (Lease("sqs", token="t"), "lease.target is required for kind 'sqs'"),
    (Lease("sfn", token="t" * 1025), "lease.token is 1025 chars, limit 1024"),
    (Lease("sfn", token="t", heartbeat_s=0), "heartbeat_s must be positive"),
    (Lease("sfn", token="t", heartbeat_s="30"), "heartbeat_s must be a number"),
    (Lease("sfn", token="t", region=""), "lease.region must be a non-empty string"),
])
def test_validate_messages(lease, needle):
    with pytest.raises(ValueError, match=needle):
        lease.validate()


def test_to_dict_drops_none_and_roundtrips():
    lease = Lease("sfn", token="tok", id="exec-1")
    d = lease.to_dict()
    assert d == {"kind": "sfn", "token": "tok", "region": "us-east-1", "heartbeat_s": 30, "id": "exec-1"}
    assert "target" not in d
    assert Lease.from_dict(d) == lease


@pytest.mark.parametrize("d,needle", [
    ("x", "must be a JSON object"),
    ({"token": "t"}, "lease.kind is missing"),
    ({"kind": "sfn", "token": "t", "bogus": 1}, "unknown fields: \\['bogus'\\]"),
    ({"kind": "http", "token": "t"}, "lease.target is required"),
])
def test_from_dict_rejects(d, needle):
    with pytest.raises(ValueError, match=needle):
        Lease.from_dict(d)


def test_encode_decode_payload():
    lease = Lease("http", token="tok", target="https://x.example/cb", heartbeat_s=15, id="job-7")
    raw = encode_payload(lease, {"steps": ["echo hi"]})
    assert ": " not in raw and ", " not in raw  # compact separators
    assert json.loads(raw) == {"lease": lease.to_dict(), "task": {"steps": ["echo hi"]}}
    back, task = decode_payload(raw)
    assert back == lease and task == {"steps": ["echo hi"]}


def test_encode_payload_refuses_bodies():
    with pytest.raises(ValueError, match=r"runHookPayload is \d+ chars, limit 4096: pass pointers"):
        encode_payload(Lease("none"), {"body": "x" * 5000})


def test_decode_payload_kind_none_and_invalid():
    lease, task = decode_payload('{"lease": {"kind": "none"}}')
    assert lease.kind == "none" and task == {}
    for raw in (None, "", "not json", '{"tenant": "t1"}', '{"lease": {"kind": "sfn"}}',
                '{"lease": {"kind": "none"}, "task": []}'):
        with pytest.raises(ValueError):
            decode_payload(raw)


def test_client_token_is_stable_and_salted():
    a = client_token(Lease("sfn", token="tok"))
    assert a == client_token(Lease("sfn", token="tok")) and len(a) == 64
    assert int(a, 16)  # hex
    assert a != client_token(Lease("sfn", token="tok"), salt="retry-1")
    assert a != client_token(Lease("durable", token="tok"))
    assert a != client_token(Lease("sfn", token="tok", id="run-2"))
    none_a, none_b = client_token(Lease("none")), client_token(Lease("none"))
    assert none_a != none_b  # nothing single-use to key on: never collide across launches


def test_policy_idle_policy_and_duration_cap():
    policy = LeasePolicy()
    assert policy.idle_policy() == IdlePolicy(max_idle=900, suspended_for=60, auto_resume=False)
    assert policy.max_duration() == 1020
    assert LeasePolicy(budget_s=40_000, slack_s=100).max_duration() == 28_800
    assert LeasePolicy(budget_s=60, heartbeat_timeout_s=10, slack_s=30).max_duration() == 90


def test_fleet_manager_lease_builds_the_run_call(monkeypatch):
    monkeypatch.setattr("microvm.fleet.image_arn", lambda name, region, profile: f"arn:img:{name}")
    fm = FleetManager.__new__(FleetManager)
    fm.cfg = PlaneConfig(region="us-east-1")
    sent = []

    def fake_run(**params):
        sent.append(params)
        return {"microvmId": "mvm-1", "state": "PENDING", "imageArn": params["imageIdentifier"],
                "imageVersion": "3"}

    fm._run = fake_run
    lease = Lease("sfn", token="tok", id="exec-1")
    vm = fm.lease("img", lease, {"n": 1}, LeasePolicy(budget_s=300, slack_s=60),
                  version="3", execution_role="arn:role", egress=["arn:egress"])
    assert vm.microvm_id == "mvm-1"
    (params,) = sent
    assert params["runHookPayload"] == encode_payload(lease, {"n": 1})
    assert params["clientToken"] == client_token(lease)
    assert params["maximumDurationInSeconds"] == 360
    assert params["idlePolicy"] == {"maxIdleDurationSeconds": 300, "suspendedDurationSeconds": 60,
                                    "autoResumeEnabled": False}
    assert params["imageVersion"] == "3" and params["executionRoleArn"] == "arn:role"
    assert params["egressNetworkConnectors"] == ["arn:egress"]
    # the same lease launched twice carries the same token: the service dedupes it
    fm.lease("img", lease, {"n": 1})
    assert sent[1]["clientToken"] == params["clientToken"]
    assert sent[1]["maximumDurationInSeconds"] == LeasePolicy().max_duration()


def test_package_exports():
    import microvm

    assert microvm.Lease is Lease and microvm.LeasePolicy is LeasePolicy
