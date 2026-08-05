"""Fixtures shared across the integration test suite.

Session scope:
  - ``sample_data_root`` — a temp directory seeded with every fixture
    produced by ``fixtures.make_samples.generate_all``. Deterministic
    so cache keys (which depend on file md5s) stay stable.

Function scope:
  - ``wool_pool`` — a fresh ``wool.WorkerPool(spawn=1)`` per test.
    Function scope keeps test isolation strict — no worker-process
    memory bleeds between tests — at the cost of paying the ~3 s pool
    startup on every test that consumes the fixture.
  - ``integration_executor`` — a freshly-wired ``WoolExecutor`` with a
    real ``LocalFsCache`` and the real BAM / tabix processors, keyed on
    a tmp cache root per test so state does not leak between tests.

  - ``worker_certs`` — a CA plus the worker and API leaves the wool
    mutual-TLS tests mint. Shared here rather than per-module because
    the *shape* of that material is load-bearing (the worker leaf's
    sole SAN is the logical identity; the API leaf has none), and two
    hand-copied minters are how two tests start disagreeing about what
    a worker certificate is.
  - ``offline_aws_env`` — environment that lets a wool worker
    subprocess construct a boto3 client without reaching AWS.

Scenario model:
  - The ``Scenario`` dataclass + ``Format``/``Endpoint``/``Method``/...
    enums encode the cross-dimension axes that integration tests sweep
    over. ``filter_func`` encodes permanent cross-axis exclusions
    (passthrough × WARM, HEAD × COLD, etc.); ``KNOWN_BUGS`` is a
    separate table for transient, retry-then-xfail bugs the
    ``xfail_known_bugs`` async fixture handles per the project's
    Async Integration Testing guide.
"""

from __future__ import annotations

import asyncio
import dataclasses
import datetime
import http.server
import os
import shutil
import socketserver
import threading
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio
import wool
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

# Permit ``http://127.0.0.1`` access from workers — the integration
# suite serves sample fixtures over an in-process HTTP server because
# aiohttp does not accept ``file://`` URIs and mocking the fetcher
# doesn't cross the wool cloudpickle boundary. MUST be set before the
# wool worker subprocess spawns so it inherits the env var.
os.environ.setdefault("CFDB_URLSAFE_ALLOW_HTTP_LOOPBACK", "1")

from cfdb.workflows.cache import LocalFsCache
from cfdb.workflows.executor import WoolExecutor
from cfdb.workflows.lock import get_job
from cfdb.workflows.models import ACTIVE_STATUSES
from cfdb.workflows.processors.bam import BamIndexProcessor
from cfdb.workflows.processors.registry import ProcessorRegistry
from cfdb.workflows.processors.tabix import TabixIntervalProcessor

from tests.integration.fixtures.make_samples import SampleFile, generate_all


# Tools required for the session to proceed at all. Format-specific
# tools (``gffread`` for GTF, ``bedToBigBed``/``bigBedToBed`` for bigBed)
# are checked per-test and skipped individually when missing.
#
# ``zcat`` is on the core list because the tabix pipeline shells out to
# it for plain-text decompression — without it the BED/GFF/VCF e2e tests
# would fail at the subprocess layer rather than skipping at session
# entry.
REQUIRED_TOOLS = ("samtools", "bgzip", "tabix", "zcat")


def tool_available(name: str) -> bool:
    """Return True when ``name`` is resolvable via ``shutil.which``.

    A thin wrapper kept as a module-level helper so ``filter_func`` can
    reference it without pulling ``shutil`` into every parametrize
    expression. The result is computed at call time, not cached —
    pytest collection time is the only call site, so the cost is
    negligible.
    """
    return shutil.which(name) is not None


def _require_tools() -> None:
    """Skip the integration session when core tools are absent."""
    missing = [t for t in REQUIRED_TOOLS if shutil.which(t) is None]
    if missing:
        pytest.skip(
            f"Integration tests require {', '.join(missing)} on PATH",
            allow_module_level=True,
        )


# ---------------------------------------------------------------------------
# Scenario model — enums + dataclass + filter_func.
# ---------------------------------------------------------------------------


class Format(Enum):
    """File formats integration tests sweep over."""

    BAM = "BAM"
    SAM = "SAM"
    BED = "BED"
    NARROWPEAK = "NarrowPeak"
    BROADPEAK = "BroadPeak"
    VCF = "VCF"
    GFF3 = "GFF3"
    GTF = "GTF"
    BIGBED = "bigBed"
    CSV = "CSV"
    BIGWIG = "bigWig"


class Endpoint(Enum):
    """HTTP endpoints the routers expose."""

    DATA = "data"
    INDEX = "index"
    JOBS = "jobs"


class Method(Enum):
    """HTTP methods the routers handle."""

    GET = "GET"
    HEAD = "HEAD"


class CacheState(Enum):
    """Cache state at the start of a scenario."""

    COLD = "cold"
    WARM = "warm"


class Concurrency(Enum):
    """Concurrency degree for racing scenarios."""

    N2 = 2
    N10 = 10


class MutexBackend(Enum):
    """Mutex backend driving the partial-unique index test."""

    FAKE = "fake"
    MONGOMOCK = "mongomock"


class PickleBoundary(Enum):
    """Boundary across which a scenario serializes data."""

    NONE = "none"
    WOOL_WORKER = "wool_worker"


class RangeShape(Enum):
    """Shape of a Range request header for /index range sweeps."""

    EXPLICIT = "explicit"
    OPEN_ENDED = "open_ended"
    SUFFIX = "suffix"
    CLAMPED = "clamped"


@dataclass(frozen=True)
class Scenario:
    """Cross-dimension test scenario built from optional enum axes.

    A ``Scenario`` carries one slot per dimension; unset slots are
    ``None``. ``__or__`` merges two partial scenarios into a richer
    one, refusing conflicting non-None overlaps so test-generation code
    cannot accidentally smush two incompatible axis values together.

    ``__str__`` produces dash-joined enum names suitable as pytest IDs,
    skipping any None slot so the readable axis label stays compact.
    """

    format: Format | None = None
    endpoint: Endpoint | None = None
    method: Method | None = None
    cache_state: CacheState | None = None
    concurrency: Concurrency | None = None
    mutex_backend: MutexBackend | None = None
    pickle_boundary: PickleBoundary | None = None

    def __or__(self, other: "Scenario") -> "Scenario":
        merged: dict[str, Any] = {}
        for field in dataclasses.fields(self):
            left = getattr(self, field.name)
            right = getattr(other, field.name)
            if left is not None and right is not None and left != right:
                raise ValueError(
                    f"Scenario.__or__ conflict on field {field.name!r}: "
                    f"{left!r} vs {right!r}"
                )
            merged[field.name] = left if left is not None else right
        return Scenario(**merged)

    @property
    def is_complete(self) -> bool:
        """Return True when every dimension is set."""
        return all(
            getattr(self, field.name) is not None
            for field in dataclasses.fields(self)
        )

    def __str__(self) -> str:
        parts: list[str] = []
        for field in dataclasses.fields(self):
            value = getattr(self, field.name)
            if value is None:
                continue
            parts.append(value.name if isinstance(value, Enum) else str(value))
        return "-".join(parts) if parts else "EMPTY"


def filter_func(row: list[Any]) -> bool:
    """Permanent exclusions across a pairwise sweep row.

    These are constraints, not bugs — the production code is deliberately
    structured so the excluded combinations cannot legitimately occur,
    so we drop them at generation time rather than xfailing them.

    Bugs (transient, retry-then-xfail) go in ``KNOWN_BUGS`` and are
    handled by the ``xfail_known_bugs`` fixture.
    """
    # Pull dimensions out of the row by isinstance check so AllPairs can
    # be called with rows of any axis ordering.
    fmt = next((v for v in row if isinstance(v, Format)), None)
    endpoint = next((v for v in row if isinstance(v, Endpoint)), None)
    method = next((v for v in row if isinstance(v, Method)), None)
    cache_state = next((v for v in row if isinstance(v, CacheState)), None)
    concurrency = next((v for v in row if isinstance(v, Concurrency)), None)
    mutex_backend = next((v for v in row if isinstance(v, MutexBackend)), None)

    # 1. CSV / bigWig are passthrough formats — they produce no cache, so
    # /jobs has nothing to surface and a "WARM cache" notion is undefined.
    if fmt in (Format.CSV, Format.BIGWIG):
        if endpoint is Endpoint.JOBS:
            return False
        if cache_state is CacheState.WARM:
            return False

    # 2. GTF and bigBed require external tools that may not be installed
    # on every dev host; drop those rows up front so the suite stays
    # green even on a partial-toolchain laptop.
    if fmt is Format.GTF and not tool_available("gffread"):
        return False
    if fmt is Format.BIGBED:
        if not tool_available("bedToBigBed"):
            return False
        if not tool_available("bigBedToBed"):
            return False

    # 3. /data falls through to direct streaming for pre-sorted BAMs;
    # the cold-cache scenario for that combination is meaningless and
    # covered by a dedicated assertion test.
    if endpoint is Endpoint.DATA and fmt is Format.BAM and cache_state is CacheState.COLD:
        return False

    # 4. HEAD never dispatches a workflow, so HEAD × COLD × DATA|INDEX
    # would always be a 404 with no dispatch side-effect — not worth
    # sweeping over.
    if (
        method is Method.HEAD
        and cache_state is CacheState.COLD
        and endpoint in (Endpoint.DATA, Endpoint.INDEX)
    ):
        return False

    # 5. mongomock-motor + N10 only runs when mongomock-motor is
    # importable; falls out as ``True`` when it is, ``False`` when it
    # is not, so the row is skipped on hosts without it.
    if mutex_backend is MutexBackend.MONGOMOCK and concurrency is Concurrency.N10:
        try:
            import mongomock_motor  # noqa: F401
        except ImportError:
            return False

    return True


# ---------------------------------------------------------------------------
# KNOWN_BUGS + xfail_known_bugs fixture.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _KnownBug:
    """Single entry in ``KNOWN_BUGS``.

    Carries a predicate on Scenario, the exception classes to retry on,
    the human-readable reason xfail uses, the retry budget, and a finer
    ``retryable`` predicate over the actual raised exception (so two
    exceptions of the same type but different shapes can be discriminated).
    """

    id: str
    match: Callable[["Scenario"], bool]
    raises: tuple[type[BaseException], ...]
    reason: str
    retries: int = 0
    retryable: Callable[[BaseException], bool] = staticmethod(lambda exc: True)


KNOWN_BUGS: tuple[_KnownBug, ...] = (
    _KnownBug(
        id="bigbed_macos_zero_byte_stdout",
        match=(
            lambda s: s.format is Format.BIGBED
            and s.endpoint is Endpoint.DATA
            and s.cache_state is CacheState.WARM
        ),
        raises=(RuntimeError, AssertionError),
        reason=(
            "bigBedToBed occasionally writes a 0-byte stdout on macOS "
            "dev hosts under load"
        ),
        retries=2,
        retryable=lambda exc: True,
    ),
    _KnownBug(
        id="mongomock_n10_insert_reorder",
        match=(
            lambda s: s.concurrency is Concurrency.N10
            and s.mutex_backend is MutexBackend.MONGOMOCK
        ),
        raises=(AssertionError,),
        reason=(
            "mongomock-motor's insert_one occasionally re-orders the "
            "partial-index check with the insert under high concurrency"
        ),
        retries=1,
        retryable=lambda exc: True,
    ),
    _KnownBug(
        id="tabix_macos_sigpipe_under_load",
        match=(
            lambda s: s.format
            in {
                Format.BED,
                Format.NARROWPEAK,
                Format.BROADPEAK,
                Format.VCF,
                Format.GFF3,
                Format.GTF,
                Format.BIGBED,
                Format.BAM,
                Format.SAM,
            }
        ),
        raises=(RuntimeError, AssertionError),
        reason=(
            "macOS dev hosts intermittently SIGPIPE the tabix/samtools "
            "subprocess from inside a wool worker — grpc poll FDs "
            "inherited across the worker fork race with the subprocess "
            "pipe write. Pre-existing wool/grpc/subprocess interaction; "
            "retries clear it"
        ),
        retries=3,
        retryable=lambda exc: (
            isinstance(exc, RuntimeError)
            and "exited -13" in str(exc)
        )
        or isinstance(exc, AssertionError),
    ),
)


def _find_known_bug(scenario: Scenario) -> _KnownBug | None:
    """Return the first KNOWN_BUGS entry matching ``scenario``."""
    for bug in KNOWN_BUGS:
        try:
            if bug.match(scenario):
                return bug
        except Exception:
            # A buggy predicate shouldn't mask a test failure.
            continue
    return None


@pytest_asyncio.fixture()
async def xfail_known_bugs():
    """Return a callable that retries a body, xfailing on a matching bug.

    Use as ``await xfail_known_bugs(scenario, body)`` per the Async
    Integration Testing guide. The body is an async no-arg callable
    (typically a small async closure). When an exception in the bug's
    ``raises`` tuple is raised AND ``retryable`` returns True, the body
    is retried after a short backoff up to ``retries`` extra attempts;
    on the final failure ``pytest.xfail(reason)`` is called so the
    fixture marks the test as expectedly-failing rather than failing
    the run.

    Non-matching exceptions propagate as real failures so a regression
    that produces a different shape of error is not silently masked.
    """

    async def _runner(
        scenario: Scenario,
        body: Callable[[], Awaitable[Any]],
    ) -> Any:
        bug = _find_known_bug(scenario)
        if bug is None:
            return await body()

        attempts = bug.retries + 1
        last_exc: BaseException | None = None
        for attempt in range(attempts):
            try:
                return await body()
            except bug.raises as exc:
                if not bug.retryable(exc):
                    raise
                last_exc = exc
                if attempt < attempts - 1:
                    # Linear backoff — 0.1s, 0.2s, ... — keeps the
                    # cumulative wait bounded for small retry budgets.
                    await asyncio.sleep(0.1 * (attempt + 1))
                    continue
                pytest.xfail(f"{bug.id}: {bug.reason} (raised {type(exc).__name__})")
        # Unreachable: either return / raise / xfail above terminates.
        if last_exc is not None:  # pragma: no cover - defensive
            raise last_exc
        return None  # pragma: no cover - defensive

    return _runner


# ---------------------------------------------------------------------------
# Shared request / wait helpers (promoted from test files).
# ---------------------------------------------------------------------------


class _Request:
    """Minimal stand-in for a FastAPI Request object.

    Router handlers only read ``request.method`` in the integration
    tests' flows, so a tiny shim with that attribute is enough to drive
    them without bringing the ASGI layer into the test loop.
    """

    def __init__(self, method: str = "GET") -> None:
        self.method = method


async def _wait_for_terminal(
    db, job_id: str, *, timeout: float = 90.0
) -> None:
    """Poll ``get_job`` until the record reaches a terminal status."""
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        record = await get_job(db, job_id)
        if record is not None and record.status not in ACTIVE_STATUSES:
            return
        await asyncio.sleep(0.1)
    raise AssertionError(
        f"Job {job_id} did not reach terminal status within {timeout}s"
    )


# ---------------------------------------------------------------------------
# Pytest fixtures.
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def _session_samples(tmp_path_factory) -> dict[str, SampleFile | None]:
    """Materialize the sample-data set once per session and cache it."""
    _require_tools()
    root = tmp_path_factory.mktemp("cfdb_samples")
    return generate_all(root)


@pytest.fixture(scope="session")
def sample_data_root(_session_samples) -> Path:
    """Return the directory that holds the generated sample files."""
    for sample in _session_samples.values():
        if sample is not None:
            return sample.path.parent
    raise RuntimeError("generate_all produced no samples")


@pytest.fixture(scope="session")
def samples(_session_samples) -> dict[str, SampleFile | None]:
    """Return the sample-file mapping produced once by ``generate_all``."""
    return _session_samples


@pytest.fixture(scope="session")
def sample_server(sample_data_root) -> str:
    """Serve the sample directory over real HTTP on 127.0.0.1.

    The Wool worker runs in a separate process and must reach the
    sample files over the network — ``aiohttp`` does not accept
    ``file://`` URIs, and mocking ``download_source`` in the test
    process doesn't cross the Wool boundary. A background
    ``http.server`` thread is the simplest way to bridge that gap.
    """
    directory = str(sample_data_root)

    class _Handler(http.server.SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=directory, **kwargs)

        def log_message(self, *_args, **_kwargs) -> None:  # pragma: no cover
            # Silence the default stderr access logging; the test output
            # stays clean.
            return

    server = socketserver.ThreadingTCPServer(("127.0.0.1", 0), _Handler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        server.shutdown()
        server.server_close()


@pytest_asyncio.fixture()
async def wool_pool():
    """Start a Wool worker for the duration of a single test.

    Function-scoped so every test gets a clean pool — prevents any
    cross-test state bleeding through worker process memory. The
    executor opens a single dispatch attempt and, on overflow,
    reschedules the job onto its durable retry scheduler — so the brief
    window between pool startup and the worker being surfaced needs no
    explicit sleep here.
    """
    async with wool.WorkerPool(spawn=1):
        yield


@pytest.fixture()
def integration_cache_root(tmp_path) -> Path:
    root = tmp_path / "cache"
    root.mkdir()
    return root


@pytest.fixture()
def integration_workdir_root(tmp_path) -> Path:
    root = tmp_path / "jobs"
    root.mkdir()
    return root


@pytest.fixture()
def install_jobs_index(mock_db):
    """Seed the partial-unique mutex index on the FakeDB jobs collection."""
    mock_db.jobs.create_index(
        {"workflow_key": 1},
        unique=True,
        partialFilterExpression={"active": True},
    )
    return mock_db


@pytest_asyncio.fixture()
async def integration_executor(
    install_jobs_index,
    integration_cache_root,
    integration_workdir_root,
    wool_pool,
):
    """Wire a ``WoolExecutor`` with the real BAM and tabix processors.

    Drains in-flight workflow tasks on teardown so a test that returns
    before its ``ensure_workflow`` background task completes does not
    leave an orphan coroutine racing the ``wool_pool`` shutdown.
    """
    registry = ProcessorRegistry()
    registry.register(BamIndexProcessor())
    registry.register(TabixIntervalProcessor())

    cache = LocalFsCache(integration_cache_root)
    executor = WoolExecutor(
        install_jobs_index,
        cache,
        registry,
        workdir_root=integration_workdir_root,
    )
    try:
        yield executor
    finally:
        await executor.drain(timeout=10.0)


def make_file_meta(
    sample: SampleFile,
    *,
    base_url: str,
    dcc: str = "ENCODE",
    local_id: str | None = None,
    extra_files: list[dict] | None = None,
) -> dict[str, Any]:
    """Build a Mongo-style file_meta dict whose access_url resolves.

    ``base_url`` comes from the ``sample_server`` fixture so the URL
    points at the per-session HTTP server serving the sample files.

    ``extra_files`` is an optional list mirroring 4DN's
    ``extra.extra_files`` sidecar array. When supplied, the returned
    dict carries an ``extra.extra_files`` field so router-level tests
    can exercise the sidecar fast path.
    """
    meta: dict[str, Any] = {
        "dcc": {"dcc_abbreviation": dcc},
        "submission": dcc.lower(),
        "local_id": local_id or f"ENCFF-{sample.format}",
        "md5": sample.md5,
        "access_url": f"{base_url}/{sample.path.name}",
        "file_format": {"name": sample.format},
    }
    if extra_files is not None:
        meta["extra"] = {"extra_files": list(extra_files)}
    return meta


# ---------------------------------------------------------------------------
# Worker mutual-TLS material.
#
# Shared across the mTLS integration modules because the *shape* of this
# material is what those tests assert on: the worker leaf's only SAN is
# the logical identity (so it is unverifiable by address, which is what
# makes an identity-carried handshake provable), and the API leaf has no
# SAN at all and clientAuth only (so a worker authenticating its client
# by chain alone is exercised, and the API's certificate cannot
# terminate a server side). A second hand-copied minter is how two test
# modules start disagreeing about what a worker certificate is.
# ---------------------------------------------------------------------------


def _generate_key() -> rsa.RSAPrivateKey:
    # 2048 rather than the 4096 the shipped script uses: these certs live
    # for one test, and key generation dominates its runtime.
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


def _common_name(value: str) -> x509.Name:
    return x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, value)])


def _sign(subject, subject_key, issuer, issuer_key, *, sans=None, eku=None, ca=False):
    """Mint a certificate, self-signed when issuer and subject coincide."""
    now = datetime.datetime.now(datetime.timezone.utc)
    builder = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(subject_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=5))
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=ca, path_length=None), critical=True)
    )
    if sans is not None:
        builder = builder.add_extension(
            x509.SubjectAlternativeName(sans), critical=False
        )
    if eku is not None:
        builder = builder.add_extension(x509.ExtendedKeyUsage(eku), critical=False)
    return builder.sign(issuer_key, hashes.SHA256())


def _write(path, data: bytes) -> str:
    path.write_bytes(data)
    return str(path)


def _write_leaf(tmp_path, stem, ca_name, ca_key, *, common_name, sans, eku):
    """Mint a CA-signed leaf and return its ``(cert_path, key_path)``."""
    key = _generate_key()
    cert = _sign(_common_name(common_name), key, ca_name, ca_key, sans=sans, eku=eku)
    return (
        _write(
            tmp_path / f"{stem}-cert.pem",
            cert.public_bytes(serialization.Encoding.PEM),
        ),
        _write(
            tmp_path / f"{stem}-key.pem",
            key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=serialization.NoEncryption(),
            ),
        ),
    )


@pytest.fixture
def worker_certs(tmp_path):
    """Mint a CA plus the separate worker and API leaves it signs.

    Returns ``(ca_path, worker_cert, worker_key, api_cert, api_key)``,
    mirroring the deployed arrangement where each process holds its own
    leaf and only the CA is shared.

    The worker leaf's sole SAN is the logical identity — no
    ``localhost``, no ``127.0.0.1`` — so it is unverifiable by address.
    The API leaf gets none at all, which is the point: a worker
    authenticates its client by chain, with no name check.

    The extended key usages mirror what ``certs/generate-worker-certs.sh``
    mints, so these tests fail if a ``clientAuth``-only API leaf turns
    out not to work as a client. Withholding ``serverAuth`` from the API
    is what stops its certificate doubling as a worker's.
    """
    from cfdb.workflows.credentials import DEFAULT_TLS_IDENTITY

    ca_key = _generate_key()
    ca_name = _common_name("cfdb-worker-ca")
    ca_cert = _sign(ca_name, ca_key, ca_name, ca_key, ca=True)

    worker_cert, worker_key = _write_leaf(
        tmp_path,
        "worker",
        ca_name,
        ca_key,
        common_name="cfdb-worker",
        sans=[x509.DNSName(DEFAULT_TLS_IDENTITY)],
        # serverAuth to terminate dispatch; clientAuth because wool's
        # graceful-stop RPC dials the worker's own subprocess.
        eku=[ExtendedKeyUsageOID.SERVER_AUTH, ExtendedKeyUsageOID.CLIENT_AUTH],
    )
    api_cert, api_key = _write_leaf(
        tmp_path,
        "api",
        ca_name,
        ca_key,
        common_name="cfdb-api",
        sans=None,
        eku=[ExtendedKeyUsageOID.CLIENT_AUTH],
    )

    return (
        _write(tmp_path / "ca.pem", ca_cert.public_bytes(serialization.Encoding.PEM)),
        worker_cert,
        worker_key,
        api_cert,
        api_key,
    )


@pytest.fixture
def offline_aws_env(monkeypatch, tmp_path):
    """Let a wool worker subprocess build a boto3 client without AWS.

    Dispatch cloudpickles the proxy into the worker, so an
    ``EcsDiscovery`` riding along has its ``__setstate__`` rebuild a
    real boto3 ECS client *inside the worker* — even though nothing
    there ever calls it. Without resolvable credentials botocore's
    chain falls through to the login provider and raises
    ``MissingDependencyException`` (this venv carries no
    ``botocore[crt]``); with a developer's ``AWS_PROFILE`` exported it
    raises ``ProfileNotFound``. Both surface as a failed dispatch,
    indistinguishable from an mTLS fault — hence the belt-and-braces.

    MUST be set before the worker spawns: the subprocess inherits
    ``os.environ`` at spawn time.
    """
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "absent-aws-config"))
    monkeypatch.setenv(
        "AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "absent-aws-credentials")
    )
