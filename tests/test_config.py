import pytest

from microvm.config import PlaneConfig


def test_region_arns(monkeypatch):
    monkeypatch.delenv("MVM_REGION", raising=False)
    cfg = PlaneConfig(region="eu-west-1")
    assert cfg.base_image_arn == "arn:aws:lambda:eu-west-1:aws:microvm-image:al2023-1"
    assert cfg.egress_internet.endswith(":INTERNET_EGRESS")
    assert cfg.ingress_none.endswith(":NO_INGRESS")


def test_unsupported_region_is_rejected():
    with pytest.raises(ValueError):
        PlaneConfig(region="ap-south-1")


def test_env_overrides(monkeypatch):
    monkeypatch.setenv("MVM_REGION", "us-west-2")
    monkeypatch.setenv("MVM_ARTIFACT_BUCKET", "b")
    monkeypatch.setenv("MVM_BUILD_ROLE_ARN", "arn:build")
    cfg = PlaneConfig()
    assert (cfg.region, cfg.artifact_bucket, cfg.build_role_arn) == ("us-west-2", "b", "arn:build")
