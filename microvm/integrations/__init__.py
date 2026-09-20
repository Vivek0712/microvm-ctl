"""Orchestrator integrations built on the lease contract in `microvm.lease`.

`stepfunctions` generates the JSONata state machine (one lease, or a Map fan-out
with `FanoutSpec`) and IAM statements (pure).
`durable` wraps a lease in a Lambda durable function; it needs
`pip install microvm-ctl[durable]` and is imported lazily so this package loads
on Python 3.9 without the durable SDK.
"""

from microvm.integrations.stepfunctions import (
    FanoutSpec,
    iam_statements,
    lease_state_machine,
    orchestrator_statements,
    policy_document,
)

__all__ = ["FanoutSpec", "lease_state_machine", "iam_statements", "orchestrator_statements",
           "policy_document"]
