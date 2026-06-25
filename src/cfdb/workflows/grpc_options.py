"""gRPC channel/server options for the wool worker dispatch channel.

Centralizes the keepalive and ping-enforcement settings the worker
applies to its gRPC server (and advertises to dispatching clients via
discovery metadata), so both worker entrypoints — ``worker_main`` (ECS)
and ``worker_lan`` (local) — stay in lockstep.

Why this exists: wool's defaults set the client keepalive cadence
(:attr:`ChannelOptions.keepalive_time_ms`, 30 s) *exactly equal* to the
server's no-data ping floor
(:attr:`WorkerOptions.http2_min_recv_ping_interval_without_data_ms`,
30 s), with a strike budget of 2. A workflow dispatch is a long-lived
stream-stream RPC that goes quiet for minutes during a subprocess stage
(``samtools sort`` / GNU ``sort`` on a multi-GB file); with
``keepalive_permit_without_calls`` on, the client keeps pinging into
that silence. Over a real network (Fargate's awsvpc ENI), inter-ping
arrival jitter routinely lands a ping a hair under the 30 s floor, which
the server counts as a strike — three strikes and it sends
``GOAWAY too_many_pings``, surfacing on the API as
``UNAVAILABLE: Too many pings`` and failing the job.

The fix is margin: the client pings once a minute while the server's
floor sits at 20 s, so a ping can arrive up to 3x early and still clear
the floor — strikes effectively never accumulate. The lowered server
floor plus the larger strike budget stop the GOAWAY even if the
advertised cadence somehow fails to propagate to the client, because the
GOAWAY is a purely server-side decision. All other channel settings
(100 MB message limits, stream concurrency, compression) keep wool's
defaults.
"""

from __future__ import annotations

from wool.runtime.worker.base import ChannelOptions, WorkerOptions

#: Client keepalive ping cadence (ms). Comfortably above the server
#: floor below so jitter never pushes a ping under the limit.
KEEPALIVE_TIME_MS = 60_000

#: Time (ms) to await a keepalive ack before declaring the peer dead.
KEEPALIVE_TIMEOUT_MS = 20_000

#: Server-side floor (ms) on the interval between no-data client pings.
#: Well under :data:`KEEPALIVE_TIME_MS` so a ping cannot be a strike.
MIN_RECV_PING_INTERVAL_MS = 20_000

#: Strike budget before the server sends ``GOAWAY too_many_pings``.
#: Cushion on top of the cadence margin.
MAX_PING_STRIKES = 5


def worker_grpc_options() -> WorkerOptions:
    """Build the :class:`WorkerOptions` for a cfdb wool worker.

    :returns:
        Worker server options carrying the keepalive cadence (advertised
        to clients) and the relaxed no-data ping enforcement that
        prevents spurious ``too_many_pings`` GOAWAYs on long-lived,
        low-data dispatch streams.
    """
    return WorkerOptions(
        channel=ChannelOptions(
            keepalive_time_ms=KEEPALIVE_TIME_MS,
            keepalive_timeout_ms=KEEPALIVE_TIMEOUT_MS,
            # Keep pinging during quiet stages so a genuinely dead worker
            # is still detected; the cadence margin makes this safe.
            keepalive_permit_without_calls=True,
        ),
        http2_min_recv_ping_interval_without_data_ms=MIN_RECV_PING_INTERVAL_MS,
        max_ping_strikes=MAX_PING_STRIKES,
    )
