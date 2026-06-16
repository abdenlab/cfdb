"""Format-specific preprocessing pipelines.

Each processor encapsulates the tool invocations needed to turn an upstream
file into Gosling-ready artifacts (sorted BAM + BAI, bgzipped text interval
+ TBI). Processors are stateless — all work happens inside ``run()`` —
which keeps them trivially pickle-safe for dispatch across the Wool worker
boundary.
"""

from cfdb.workflows.processors.base import Processor
from cfdb.workflows.processors.passthrough import PassthroughProcessor
from cfdb.workflows.processors.registry import ProcessorRegistry, default_registry

__all__ = [
    "PassthroughProcessor",
    "Processor",
    "ProcessorRegistry",
    "default_registry",
]
