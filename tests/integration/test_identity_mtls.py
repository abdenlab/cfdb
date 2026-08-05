"""Integration test for identity-based mutual TLS on the dispatch channel.

Exercises a real ``wool.WorkerPool`` over a real gRPC channel with real
certificates, because the thing under test *is* the TLS handshake — a
mocked ``as_provider`` proves cfdb calls wool correctly but says nothing
about whether the resulting channel actually comes up.

The worker leaf (from the shared ``worker_certs`` fixture in
``conftest``) carries exactly one SAN, the logical identity — no
``localhost``, no ``127.0.0.1``. That is what makes the pair of tests
meaningful: the certificate is unverifiable by address, so the dispatch
that succeeds can only have succeeded through the identity override,
and the one without an identity can only fail.

The two ends hold separate leaves signed by a shared CA, as they do in
deployment, which takes a little arranging: a spawning pool passes its
single ``credentials=`` to both the proxy and every worker it starts, so
the worker factory has to supply the worker's own (see
``_worker_factory``). Worth the arranging, because collapsing them into
one leaf would leave the API's client certificate untested — and a
client certificate that happens to also be the server's is exactly the
case where a mistake stays invisible.
"""

from __future__ import annotations

import asyncio
import logging
import re

import pytest
import wool

from cfdb.workflows.credentials import DEFAULT_TLS_IDENTITY, build_worker_credentials

from tests.integration.routines import echo


pytestmark = pytest.mark.integration

#: Bounds every dispatch so a handshake that stalls rather than failing
#: cannot hang the suite.
_DISPATCH_TIMEOUT_S = 30.0

#: How long to wait for the worker to be admitted to the pool. Long
#: enough for a subprocess spawn plus a TLS handshake on a loaded CI box,
#: short enough that the failing case does not dominate the suite.
_QUORUM_TIMEOUT_S = 20.0

#: Teardown grace for the passing case. A clean drain takes about two
#: seconds, so this has real headroom while still failing the test rather
#: than hanging it if graceful stop ever breaks again. Keeping it tight
#: is deliberate: an over-generous budget hides a broken stop path behind
#: a force-reap that still reports success.
_SHUTDOWN_TIMEOUT_S = 15.0

#: Teardown grace for the failing case only. The pool cannot reach a
#: worker it could never verify, so the stop RPC is doomed and the wait
#: is pure latency; the full budget above would be spent on it.
_FAILED_SHUTDOWN_TIMEOUT_S = 5.0


def _worker_factory(worker_certs):
    """Build a worker factory that keeps the worker's own credentials.

    A spawning pool hands its single ``credentials=`` to both the proxy
    it dials with and every worker it starts, which would collapse the
    two leaves into one. Declaring ``credentials`` and discarding it —
    while declaring keyword-only ``host`` so wool still prescribes the
    bind address — restores the split each process has in deployment,
    where it builds credentials from its own environment.

    The worker's own credentials carry the identity, matching what the
    entrypoints do via ``identity_from_env``. That is not redundant with
    the pool's: ``LocalWorker.stop`` dials this worker's subprocess to
    drain it, so the worker is a client on that one connection and
    verifies a certificate whose only SAN is the identity. Without it the
    stop RPC fails its name check and wool force-reaps the subprocess,
    losing in-flight work — visible here as the pool warning that it
    "stopped waiting for 1 worker(s) that did not stop gracefully".
    """
    ca, worker_cert, worker_key, _, _ = worker_certs

    def factory(*tags, credentials=None, host=None):
        return wool.LocalWorker(
            *tags,
            credentials=build_worker_credentials(
                ca, worker_cert, worker_key, identity=DEFAULT_TLS_IDENTITY
            ),
            host=host,
        )

    return factory


def _pool(worker_certs, *, identity, shutdown_timeout=_SHUTDOWN_TIMEOUT_S):
    """Build a one-worker pool over the minted material."""
    ca, _, _, api_cert, api_key = worker_certs
    return wool.WorkerPool(
        spawn=1,
        worker=_worker_factory(worker_certs),
        credentials=build_worker_credentials(
            ca, api_cert, api_key, identity=identity
        ),
        # Wait for the worker to register before returning from
        # __aenter__, so a dispatch is not racing its startup — without
        # this both tests fail with NoWorkersAvailable for reasons that
        # have nothing to do with TLS. Admission does not itself require
        # a completed handshake, so the gate removes the race and leaves
        # the dispatch to be decided by the certificate. (The API runs
        # quorum=0 instead, because a Fargate cold start outlasts any
        # sensible gate — a difference in startup policy, not in how the
        # channel is secured.)
        quorum=1,
        quorum_timeout=_QUORUM_TIMEOUT_S,
        lazy=False,
        shutdown_timeout=shutdown_timeout,
    )


class TestIdentityMutualTls:
    @pytest.mark.asyncio
    async def test_dispatch_should_succeed_when_identity_matches_the_certificate(
        self, worker_certs
    ):
        """Test that an identity makes an address-unverifiable cert usable.

        Given:
            A real worker whose certificate's only SAN is the logical
            identity, and credentials carrying that same identity.
        When:
            A routine is dispatched to it.
        Then:
            It should return its result over the mTLS channel, since the
            peer is verified against the identity rather than against
            the address the pool dialed.
        """
        # Arrange
        pool = _pool(worker_certs, identity=DEFAULT_TLS_IDENTITY)

        # Act
        async with pool:
            result = await asyncio.wait_for(
                echo("hello"), timeout=_DISPATCH_TIMEOUT_S
            )

        # Assert
        assert result == "hello"

    @pytest.mark.asyncio
    async def test_dispatch_should_fail_when_identity_is_not_configured(
        self, worker_certs, caplog
    ):
        """Test that the same certificate is unusable without an identity.

        Given:
            The same certificate, but credentials built with no
            identity, so verification falls back to the dialed address.
        When:
            A routine is dispatched to it.
        Then:
            It should fail on the certificate's name check, because the
            address the pool dialed appears in no SAN. This is what
            establishes that the passing case above is carried by the
            identity and not by some incidental match.
        """
        # Arrange
        pool = _pool(
            worker_certs, identity=None, shutdown_timeout=_FAILED_SHUTDOWN_TIMEOUT_S
        )

        # Act & assert
        # The worker registers, but its handshake fails; wool classifies
        # that as transient and skips the worker rather than evicting it,
        # so the balancer runs out of candidates and the proxy reports no
        # healthy worker. NoWorkersAvailable alone would also be raised by
        # a worker that never started or one rejected on version, so the
        # log assertion below is what ties the failure to verification.
        with caplog.at_level(logging.WARNING, logger="wool.runtime.worker.proxy"):
            async with pool:
                with pytest.raises(wool.NoWorkersAvailable):
                    await asyncio.wait_for(
                        echo("hello"), timeout=_DISPATCH_TIMEOUT_S
                    )

        assert "handshake failure" in caplog.text
        # The verification detail is authored by BoringSSL and surfaced
        # through grpcio ("Hostname Verification Check failed" today),
        # neither of which cfdb pins with an upper bound — so match the
        # concept, not the exact wording, or a grpcio bump turns this
        # into a red test that names nothing under test. The wool-owned
        # "handshake failure" assertion above is the stable contract.
        assert re.search(
            r"(?i)hostname verification|certificate verify", caplog.text
        )
