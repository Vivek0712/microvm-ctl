"""One-time account bootstrap: artifact bucket + build/execution IAM roles.

Build role  — assumed by the image build infrastructure: reads the code
              artifact zip from S3 and writes build logs.
Execution   — assumed by the running microVM (credentials are vended inside
role          the VM): runtime logs plus whatever your workload needs.
              Build vs execution role separation is deliberate — never let
              the sandbox role read your artifact bucket.
"""

from __future__ import annotations

import json
import time

from microvm.client import lambda_client
from microvm.config import PlaneConfig

TRUST = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {"Service": "lambda.amazonaws.com"},
            "Action": ["sts:AssumeRole", "sts:TagSession"],
        }
    ],
}


def _ensure_role(iam, name: str, policy_doc: dict) -> str:
    try:
        arn = iam.get_role(RoleName=name)["Role"]["Arn"]
    except iam.exceptions.NoSuchEntityException:
        arn = iam.create_role(
            RoleName=name,
            AssumeRolePolicyDocument=json.dumps(TRUST),
            Description="awesome-microvm control plane",
        )["Role"]["Arn"]
        time.sleep(8)  # IAM eventual consistency before first use
    iam.put_role_policy(
        RoleName=name, PolicyName=f"{name}-inline", PolicyDocument=json.dumps(policy_doc)
    )
    return arn


def bootstrap(cfg: PlaneConfig, prefix: str = "awesome-microvm") -> dict:
    sts = lambda_client("sts", cfg.region, cfg.profile)
    account = sts.get_caller_identity()["Account"]
    bucket = cfg.artifact_bucket or f"{prefix}-artifacts-{account}-{cfg.region}"

    s3 = lambda_client("s3", cfg.region, cfg.profile)
    try:
        s3.head_bucket(Bucket=bucket)
    except Exception:
        params = {"Bucket": bucket}
        if cfg.region != "us-east-1":
            params["CreateBucketConfiguration"] = {"LocationConstraint": cfg.region}
        s3.create_bucket(**params)
        s3.put_public_access_block(
            Bucket=bucket,
            PublicAccessBlockConfiguration={
                "BlockPublicAcls": True, "IgnorePublicAcls": True,
                "BlockPublicPolicy": True, "RestrictPublicBuckets": True,
            },
        )

    iam = lambda_client("iam", cfg.region, cfg.profile)
    logs_stmt = {
        "Effect": "Allow",
        "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
        "Resource": f"arn:aws:logs:{cfg.region}:{account}:log-group:/aws/lambda/microvms/*",
    }
    build_role = _ensure_role(
        iam,
        f"{prefix}-build-role",
        {
            "Version": "2012-10-17",
            "Statement": [
                {"Effect": "Allow", "Action": "s3:GetObject", "Resource": f"arn:aws:s3:::{bucket}/*"},
                logs_stmt,
            ],
        },
    )
    execution_role = _ensure_role(
        iam,
        f"{prefix}-execution-role",
        {"Version": "2012-10-17", "Statement": [logs_stmt]},
    )
    return {
        "artifact_bucket": bucket,
        "build_role_arn": build_role,
        "execution_role_arn": execution_role,
    }
