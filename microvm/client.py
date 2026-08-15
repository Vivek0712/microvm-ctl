"""boto3 client factory with a vendored `lambda-microvms` service model.

The service model ships inside this package (microvm/data), so the control
plane works even on boto3/botocore versions that predate the service —
botocore's loader picks up our data directory first.
"""

from __future__ import annotations

import threading
from pathlib import Path

import boto3
import botocore.session

_DATA_DIR = str(Path(__file__).parent / "data")
_lock = threading.Lock()
_sessions: dict[tuple[str | None, str], boto3.Session] = {}


def _session(profile: str | None, region: str) -> boto3.Session:
    """One boto3 Session per (profile, region), with our model dir on the loader path."""
    key = (profile, region)
    with _lock:
        if key not in _sessions:
            bc = botocore.session.Session()
            loader = bc.get_component("data_loader")
            if _DATA_DIR not in loader.search_paths:
                loader.search_paths.insert(0, _DATA_DIR)
            _sessions[key] = boto3.Session(
                profile_name=profile, region_name=region, botocore_session=bc
            )
        return _sessions[key]


def microvm_client(region: str = "us-east-1", profile: str | None = None):
    """A `lambda-microvms` client (RunMicrovm, SuspendMicrovm, images, tokens…)."""
    return _session(profile, region).client("lambda-microvms")


def lambda_client(service: str, region: str = "us-east-1", profile: str | None = None):
    """Any other client (s3, iam, logs, cloudwatch…) on the same session/credentials."""
    return _session(profile, region).client(service)


_accounts: dict[tuple[str | None, str], str] = {}


def account_id(region: str = "us-east-1", profile: str | None = None) -> str:
    key = (profile, region)
    if key not in _accounts:
        _accounts[key] = _session(profile, region).client("sts").get_caller_identity()["Account"]
    return _accounts[key]


def image_arn(name_or_arn: str, region: str, profile: str | None = None) -> str:
    """The API's imageIdentifier params want full ARNs; accept a bare name anywhere."""
    if name_or_arn.startswith("arn:"):
        return name_or_arn
    return f"arn:aws:lambda:{region}:{account_id(region, profile)}:microvm-image:{name_or_arn}"
