"""Shared configuration for the control plane.

Lambda MicroVMs is available in: us-east-1, us-east-2, us-west-2,
eu-west-1, ap-northeast-1. All microVMs are ARM64 (Graviton).
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

SUPPORTED_REGIONS = ("us-east-1", "us-east-2", "us-west-2", "eu-west-1", "ap-northeast-1")

# AWS-managed network connector ARNs (account-agnostic, per region).
INGRESS_ALL = "arn:aws:lambda:{region}:aws:network-connector:aws-network-connector:ALL_INGRESS"
INGRESS_NONE = "arn:aws:lambda:{region}:aws:network-connector:aws-network-connector:NO_INGRESS"
INGRESS_SHELL = "arn:aws:lambda:{region}:aws:network-connector:aws-network-connector:SHELL_INGRESS"
EGRESS_INTERNET = "arn:aws:lambda:{region}:aws:network-connector:aws-network-connector:INTERNET_EGRESS"

# Lambda-managed base image every microVM image builds on.
MANAGED_BASE_IMAGE = "arn:aws:lambda:{region}:aws:microvm-image:al2023-1"

# Reserved proxy headers on the microVM endpoint.
AUTH_HEADER = "X-aws-proxy-auth"
PORT_HEADER = "X-aws-proxy-port"
FORCE_H2_HEADER = "X-aws-proxy-force-h2"

# In-VM runtime hook base path (served BY your app, called by Lambda).
HOOK_BASE_PATH = "/aws/lambda-microvms/runtime/v1"

# Published TPS quotas the control plane throttles around (soft, raisable).
TPS = {
    "RunMicrovm": 5,
    "SuspendMicrovm": 2,
    "ResumeMicrovm": 5,
    "TerminateMicrovm": 10,
    "GetMicrovm": 100,
    "CreateMicrovmAuthToken": 50,
}

MAX_DURATION_SECONDS = 28_800  # 8 h hard ceiling across RUNNING + SUSPENDED


@dataclass
class PlaneConfig:
    """Everything the control plane needs to know about one deployment."""

    region: str = field(default_factory=lambda: os.environ.get("MVM_REGION", "us-east-1"))
    profile: str | None = field(default_factory=lambda: os.environ.get("MVM_PROFILE") or os.environ.get("AWS_PROFILE"))
    artifact_bucket: str | None = field(default_factory=lambda: os.environ.get("MVM_ARTIFACT_BUCKET"))
    build_role_arn: str | None = field(default_factory=lambda: os.environ.get("MVM_BUILD_ROLE_ARN"))
    execution_role_arn: str | None = field(default_factory=lambda: os.environ.get("MVM_EXECUTION_ROLE_ARN"))

    def __post_init__(self) -> None:
        if self.region not in SUPPORTED_REGIONS:
            raise ValueError(
                f"Lambda MicroVMs is not available in {self.region}; pick one of {SUPPORTED_REGIONS}"
            )

    # Convenience per-region ARNs -------------------------------------------------
    @property
    def base_image_arn(self) -> str:
        return MANAGED_BASE_IMAGE.format(region=self.region)

    @property
    def ingress_all(self) -> str:
        return INGRESS_ALL.format(region=self.region)

    @property
    def ingress_none(self) -> str:
        return INGRESS_NONE.format(region=self.region)

    @property
    def ingress_shell(self) -> str:
        return INGRESS_SHELL.format(region=self.region)

    @property
    def egress_internet(self) -> str:
        return EGRESS_INTERNET.format(region=self.region)
