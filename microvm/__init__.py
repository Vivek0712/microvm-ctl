"""awesome-microvm — control & execution plane for AWS Lambda MicroVMs.

Control plane  : build images, run/suspend/resume/terminate, scale fleets.
Execution plane: authenticated calls into the microVM endpoint, in-VM hook server.
"""

from microvm.client import microvm_client, lambda_client
from microvm.config import PlaneConfig
from microvm.images import ImageBuilder, ImageBuildError
from microvm.fleet import Fleet, FleetManager
from microvm.endpoint import EndpointClient, EndpointError
from microvm.monitor import FleetMonitor, CostModel

__version__ = "0.1.0"

__all__ = [
    "microvm_client",
    "lambda_client",
    "PlaneConfig",
    "ImageBuilder",
    "ImageBuildError",
    "Fleet",
    "FleetManager",
    "EndpointClient",
    "EndpointError",
    "FleetMonitor",
    "CostModel",
]
