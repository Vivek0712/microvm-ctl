"""Image factory: app directory -> zip -> S3 -> MicroVM image build -> ACTIVE version.

A MicroVM image is built by Lambda from a zip containing a Dockerfile at its
root. Lambda boots a fresh microVM from the managed AL2023 base image, runs
your Dockerfile, starts your ENTRYPOINT/CMD, waits for the /ready hook, then
snapshots memory + disk. Every RunMicrovm clones that snapshot.

Snapshot rules baked into this builder's defaults:
  - hooks enabled (run/resume/suspend/terminate) so per-VM uniqueness and
    credentials are re-established after clone/resume;
  - /validate enabled — it doubles as a warm-path exercise that lets Lambda
    prefetch hot snapshot regions and cut launch latency.
"""

from __future__ import annotations

import io
import time
import zipfile
from dataclasses import dataclass
from pathlib import Path

from microvm.client import microvm_client, lambda_client, image_arn
from microvm.config import PlaneConfig

TERMINAL_BUILD = {"SUCCESSFUL", "FAILED"}


class ImageBuildError(RuntimeError):
    pass


@dataclass
class BuiltImage:
    image_arn: str
    name: str
    version: str
    build_id: str | None = None
    memory_snapshot_bytes: int | None = None
    disk_snapshot_bytes: int | None = None
    build_seconds: float | None = None


def default_hooks(port: int = 8080) -> dict:
    """Enable the full hook contract on the app port."""
    return {
        "port": port,
        "microvmHooks": {
            "run": "ENABLED",
            "runTimeoutInSeconds": 60,
            "resume": "ENABLED",
            "resumeTimeoutInSeconds": 60,
            "suspend": "ENABLED",
            "suspendTimeoutInSeconds": 30,
            "terminate": "ENABLED",
            "terminateTimeoutInSeconds": 30,
        },
        "microvmImageHooks": {
            "ready": "ENABLED",
            "readyTimeoutInSeconds": 300,
            "validate": "ENABLED",
            "validateTimeoutInSeconds": 120,
        },
    }


class ImageBuilder:
    def __init__(self, config: PlaneConfig):
        self.cfg = config
        self.api = microvm_client(config.region, config.profile)
        self.s3 = lambda_client("s3", config.region, config.profile)

    # -- packaging ---------------------------------------------------------------
    def package(self, app_dir: str | Path) -> bytes:
        """Zip an app directory (Dockerfile at the zip root), and inject the
        zero-dependency hook runtime as `microvm_hooks.py` so any app can
        `from microvm_hooks import HookApp` with nothing to pip-install."""
        app_dir = Path(app_dir)
        if not (app_dir / "Dockerfile").exists():
            raise ImageBuildError(f"{app_dir} has no Dockerfile at its root")
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
            for p in sorted(app_dir.rglob("*")):
                if p.is_file() and not any(part in {".git", "__pycache__", ".venv"} for part in p.parts):
                    z.write(p, p.relative_to(app_dir))
            if "microvm_hooks.py" not in z.namelist():
                z.write(Path(__file__).parent / "hooks" / "server.py", "microvm_hooks.py")
        return buf.getvalue()

    def upload(self, name: str, payload: bytes) -> str:
        if not self.cfg.artifact_bucket:
            raise ImageBuildError("PlaneConfig.artifact_bucket is not set (MVM_ARTIFACT_BUCKET)")
        key = f"microvm-images/{name}/{int(time.time())}.zip"
        self.s3.put_object(Bucket=self.cfg.artifact_bucket, Key=key, Body=payload)
        return f"s3://{self.cfg.artifact_bucket}/{key}"

    # -- build -------------------------------------------------------------------
    def build(
        self,
        name: str,
        app_dir: str | Path,
        *,
        memory_mib: int = 2048,
        hooks: dict | None = None,
        environment: dict[str, str] | None = None,
        egress_connectors: list[str] | None = None,
        os_capabilities_all: bool = False,
        description: str | None = None,
        wait: bool = True,
    ) -> BuiltImage:
        """Create (or version-bump) an image from a local app directory and wait for the build."""
        uri = self.upload(name, self.package(app_dir))
        params: dict = {
            "baseImageArn": self.cfg.base_image_arn,
            "buildRoleArn": self._require("build_role_arn"),
            "codeArtifact": {"uri": uri},
            "resources": [{"minimumMemoryInMiB": memory_mib}],
            "cpuConfigurations": [{"architecture": "ARM_64"}],
            "hooks": hooks or default_hooks(),
            "egressNetworkConnectors": egress_connectors or [self.cfg.egress_internet],
        }
        if environment:
            params["environmentVariables"] = environment
        if os_capabilities_all:
            params["additionalOsCapabilities"] = ["ALL"]
        if description:
            params["description"] = description

        started = time.time()
        if self._image_exists(name):
            # update accepts neither `name` nor `tags` — they are create-only
            resp = self.api.update_microvm_image(imageIdentifier=self.arn(name), **params)
        else:
            resp = self.api.create_microvm_image(name=name, **params)
        built = BuiltImage(image_arn=resp["imageArn"], name=name, version=resp["imageVersion"])
        if wait:
            self.wait_for_build(built, started)
            self.ensure_active(built)
        return built

    def arn(self, name: str) -> str:
        return image_arn(name, self.cfg.region, self.cfg.profile)

    def _image_exists(self, name: str) -> bool:
        try:
            self.api.get_microvm_image(imageIdentifier=self.arn(name))
            return True
        except self.api.exceptions.ResourceNotFoundException:
            return False

    def _require(self, attr: str) -> str:
        v = getattr(self.cfg, attr)
        if not v:
            raise ImageBuildError(f"PlaneConfig.{attr} is not set")
        return v

    def wait_for_build(self, built: BuiltImage, started: float, timeout: int = 1800) -> None:
        """Poll the build until SUCCESSFUL; surface CloudWatch log pointer on failure."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            builds = self.api.list_microvm_image_builds(
                imageIdentifier=self.arn(built.name), imageVersion=built.version
            )["items"]
            if builds:
                b = builds[0]
                if b["buildState"] in TERMINAL_BUILD:
                    detail = self.api.get_microvm_image_build(
                        imageIdentifier=self.arn(built.name),
                        imageVersion=built.version,
                        buildId=b["buildId"],
                    )
                    if b["buildState"] == "FAILED":
                        raise ImageBuildError(
                            f"build failed: {detail.get('stateReason', 'unknown')} — "
                            f"see CloudWatch /aws/lambda/microvms/{built.name}"
                        )
                    snap = detail.get("snapshotBuild", {})
                    built.build_id = b["buildId"]
                    built.memory_snapshot_bytes = snap.get("memorySnapshotSizeInBytes")
                    built.disk_snapshot_bytes = snap.get("diskSnapshotSizeInBytes")
                    built.build_seconds = round(time.time() - started, 1)
                    return
            time.sleep(10)
        raise ImageBuildError(f"build of {built.name}:{built.version} timed out")

    def ensure_active(self, built: BuiltImage) -> None:
        v = self.api.get_microvm_image_version(
            imageIdentifier=self.arn(built.name), imageVersion=built.version
        )
        if v["status"] != "ACTIVE":
            self.api.update_microvm_image_version(
                imageIdentifier=self.arn(built.name), imageVersion=built.version, status="ACTIVE"
            )

    # -- introspection -----------------------------------------------------------
    def list_images(self) -> list[dict]:
        return self.api.get_paginator("list_microvm_images").paginate().build_full_result()["items"]

    def list_versions(self, name: str) -> list[dict]:
        return (
            self.api.get_paginator("list_microvm_image_versions")
            .paginate(imageIdentifier=self.arn(name))
            .build_full_result()["items"]
        )
