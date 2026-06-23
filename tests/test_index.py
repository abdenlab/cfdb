"""Tests for the /index router's sidecar + workflow + dispatch paths."""

from __future__ import annotations

import pytest
from fastapi import HTTPException
from fastapi.responses import JSONResponse
from wool.runtime.routine.task import do_dispatch

from cfdb import api
from cfdb.api.routers.index import stream_index_file, stream_index_file_status
from cfdb.services import drs, locks
from cfdb.workflows.executor import WoolExecutor
from cfdb.workflows.models import ArtifactKind
from cfdb.workflows.processors.bam import BamIndexProcessor
from cfdb.workflows.processors.registry import ProcessorRegistry, default_registry
from tests.test_workflows import FIXTURE_MD5


def _make_request(method: str = "GET"):
    """Return a minimal mock request object."""

    class FakeRequest:
        def __init__(self):
            self.method = method

    return FakeRequest()


def _make_file_doc(*, extra=None, **fields) -> dict:
    """Return a minimal /index file_doc, overridable per test."""
    doc = {
        "submission": "4dn",
        "id_namespace": "tag:4dn.org,2015:",
        "local_id": "4DNFIBED01",
        "filename": "x.bed.gz",
        "md5": FIXTURE_MD5,
        "dcc": {"dcc_abbreviation": "4DN_DCIC"},
        "file_format": {"name": "BED"},
    }
    if extra is not None:
        doc["extra"] = extra
    doc.update(fields)
    return doc


class TestStreamIndexFileFourdnSidecar:
    @pytest.mark.asyncio
    async def test_stream_index_file_should_prefer_tbi_sidecar_when_multiple_present(
        self, mock_db, mocker
    ):
        """Test (IX-001) that the sidecar picker prefers index-format entries.

        Given:
            A 4DN file with ``extra.extra_files`` containing both a
            ``data`` entry (first) and a ``tbi`` entry (second).
        When:
            ``stream_index_file`` is called.
        Then:
            The streamed URL should end with the ``.tbi`` entry's href —
            ``_SIDECAR_INDEX_FORMATS`` preference must win over array
            ordering.
        """
        # Arrange
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)

        captured_urls: list[str] = []

        async def fake_stream(url, _range):
            captured_urls.append(url)
            yield b"tbi-bytes"

        mocker.patch.object(drs, "stream_from_url", fake_stream)

        mock_db.files.docs = []
        mock_db.file.docs = [
            _make_file_doc(
                extra={
                    "extra_files": [
                        {"file_format": "data", "href": "/x/data"},
                        {"file_format": "tbi", "href": "/x/abc.tbi", "file_size": 100},
                    ]
                }
            )
        ]

        # Act
        resp = await stream_index_file(
            "4dn", "4DNFIBED01", _make_request(), range=None
        )

        # Assert
        assert resp.status_code == 200
        body = b"".join([chunk async for chunk in resp.body_iterator])
        assert body == b"tbi-bytes"
        assert captured_urls[0].endswith("/x/abc.tbi")

    @pytest.mark.asyncio
    async def test_stream_index_file_should_resolve_nested_fourdn_extra_files(
        self, mock_db, mocker
    ):
        """Test (IX-002) that the nested ``extra.fourdn.extra_files`` is read.

        Given:
            A 4DN file with no top-level ``extra.extra_files`` but a
            populated ``extra.fourdn.extra_files``.
        When:
            ``stream_index_file`` is called.
        Then:
            It should return a streaming response sourced from the
            nested array.
        """
        # Arrange
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)

        async def fake_stream(_url, _range):
            yield b"nested-bytes"

        mocker.patch.object(drs, "stream_from_url", fake_stream)

        mock_db.files.docs = []
        mock_db.file.docs = [
            _make_file_doc(
                extra={
                    "fourdn": {
                        "extra_files": [
                            {"href": "/x/abc.tbi", "file_size": 50},
                        ]
                    }
                }
            )
        ]

        # Act
        resp = await stream_index_file(
            "4dn", "4DNFIBED01", _make_request(), range=None
        )

        # Assert
        assert resp.status_code == 200
        body = b"".join([chunk async for chunk in resp.body_iterator])
        assert body == b"nested-bytes"

    @pytest.mark.asyncio
    async def test_stream_index_file_should_502_when_sidecar_href_fails_allowlist(
        self, mock_db, mocker
    ):
        """Test (IX-003) that a poisoned sidecar href yields a 502.

        Given:
            A 4DN sidecar entry with
            ``href="https://attacker.example.com/x.tbi"``.
        When:
            ``stream_index_file`` is called.
        Then:
            It should raise ``HTTPException(502)`` matching "allowlist
            validation" and the upstream stream should never be opened.
        """
        # Arrange
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)
        stream_calls: list = []

        async def fake_stream(url, _range):
            stream_calls.append(url)
            yield b"should-never-stream"

        mocker.patch.object(drs, "stream_from_url", fake_stream)

        mock_db.files.docs = []
        mock_db.file.docs = [
            _make_file_doc(
                extra={
                    "extra_files": [
                        {
                            "file_format": "tbi",
                            "href": "https://attacker.example.com/x.tbi",
                            "file_size": 10,
                        }
                    ]
                }
            )
        ]

        # Act & assert
        with pytest.raises(HTTPException) as exc_info:
            await stream_index_file(
                "4dn", "4DNFIBED01", _make_request(), range=None
            )
        assert exc_info.value.status_code == 502
        assert "allowlist validation" in (exc_info.value.detail or "").lower()
        assert stream_calls == []

    @pytest.mark.asyncio
    async def test_stream_index_file_should_502_when_sidecar_href_missing(
        self, mock_db, mocker
    ):
        """Test (IX-004) that a malformed sidecar (no href) returns 502.

        Given:
            A sidecar entry with no ``href`` key.
        When:
            ``stream_index_file`` is called.
        Then:
            It should raise ``HTTPException(502)`` matching the
            missing-``href`` message.
        """
        # Arrange
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)

        mock_db.files.docs = []
        mock_db.file.docs = [
            _make_file_doc(extra={"extra_files": [{"file_format": "tbi"}]})
        ]

        # Act & assert
        with pytest.raises(HTTPException) as exc_info:
            await stream_index_file(
                "4dn", "4DNFIBED01", _make_request(), range=None
            )
        assert exc_info.value.status_code == 502
        assert "href" in (exc_info.value.detail or "").lower()

    @pytest.mark.asyncio
    async def test_stream_index_file_should_serve_206_with_content_range_on_sidecar_range(
        self, mock_db, mocker
    ):
        """Test (IX-005) that the sidecar path supports Range requests.

        Given:
            A 4DN sidecar with ``file_size=100`` and a ``Range: bytes=0-49``
            header.
        When:
            ``stream_index_file`` is called.
        Then:
            It should return a 206 response with
            ``Content-Range: bytes 0-49/100``.
        """
        # Arrange
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)

        async def fake_stream(_url, _range):
            yield b"a" * 50

        mocker.patch.object(drs, "stream_from_url", fake_stream)

        mock_db.files.docs = []
        mock_db.file.docs = [
            _make_file_doc(
                extra={
                    "extra_files": [
                        {"file_format": "tbi", "href": "/x/abc.tbi", "file_size": 100}
                    ]
                }
            )
        ]

        # Act
        resp = await stream_index_file(
            "4dn", "4DNFIBED01", _make_request(), range="bytes=0-49"
        )

        # Assert
        assert resp.status_code == 206
        assert resp.headers["content-range"] == "bytes 0-49/100"


    @pytest.mark.asyncio
    async def test_stream_index_file_should_serve_fourdn_sidecar_when_extra_files_present(
        self, mock_db, mocker
    ):
        """Test that the existing 4DN sidecar fast path still works.

        Given:
            A 4DN BED with an extra.extra_files entry (an href + file_size).
        When:
            stream_index_file is called.
        Then:
            It should return a streaming response by resolving the entry's
            href against the DCC's api_base, without touching the
            workflow subsystem.
        """
        # Arrange
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)

        captured_urls: list[str] = []

        async def fake_stream(url, _range):
            captured_urls.append(url)
            yield b"idx-bytes"

        mocker.patch.object(drs, "stream_from_url", fake_stream)

        mock_db.files.docs = []
        mock_db.file.docs = [
            _make_file_doc(
                extra={
                    "extra_files": [
                        {"href": "/x/abc.tbi", "file_size": 100},
                    ]
                }
            )
        ]

        # Act
        resp = await stream_index_file(
            "4dn", "4DNFIBED01", _make_request(), range=None
        )

        # Assert
        assert resp.status_code == 200
        body = b"".join([chunk async for chunk in resp.body_iterator])
        assert body == b"idx-bytes"
        assert len(captured_urls) == 1
        assert captured_urls[0].endswith("/x/abc.tbi")

    @pytest.mark.asyncio
    async def test_stream_index_file_should_select_dict_shaped_file_format_sidecar(
        self, mock_db, mocker
    ):
        """Test (IX-007) that a 4DN dict-shaped file_format sidecar is read.

        Given:
            A 4DN file whose ``extra.fourdn.extra_files`` carries
            ``file_format`` as the CV object the materializer emits — a
            non-index ``bw`` entry first and the ``beddb`` index second.
        When:
            ``stream_index_file`` is called.
        Then:
            It should read the token from ``display_title``, prefer the
            ``beddb`` entry, and stream it rather than crashing on the
            dict (the #26 500).
        """
        # Arrange
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)

        captured_urls: list[str] = []

        async def fake_stream(url, _range):
            captured_urls.append(url)
            yield b"beddb-bytes"

        mocker.patch.object(drs, "stream_from_url", fake_stream)

        mock_db.files.docs = []
        mock_db.file.docs = [
            _make_file_doc(
                extra={
                    "fourdn": {
                        "extra_files": [
                            {
                                "file_format": {"display_title": "bw"},
                                "href": "/x/data.bw",
                            },
                            {
                                "file_format": {"display_title": "beddb"},
                                "href": "/x/abc.beddb",
                                "file_size": 100,
                            },
                        ]
                    }
                }
            )
        ]

        # Act
        resp = await stream_index_file(
            "4dn", "4DNFIBED01", _make_request(), range=None
        )

        # Assert
        assert resp.status_code == 200
        body = b"".join([chunk async for chunk in resp.body_iterator])
        assert body == b"beddb-bytes"
        assert captured_urls[0].endswith("/x/abc.beddb")

    @pytest.mark.asyncio
    async def test_stream_index_file_should_fall_back_when_dict_file_format_lacks_display_title(
        self, mock_db, mocker
    ):
        """Test (IX-008) that a dict file_format with no display_title falls back.

        Given:
            A 4DN file with two non-index entries: one whose ``file_format``
            dict carries no ``display_title`` key (first) and a ``bw`` entry
            (second), so nothing matches the index allowlist.
        When:
            ``stream_index_file`` is called.
        Then:
            The missing token resolves to no-match, the first-entry fallback
            serves the first entry (not the second), and no AttributeError
            is raised (the #26 crash line).
        """
        # Arrange
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)

        captured_urls: list[str] = []

        async def fake_stream(url, _range):
            captured_urls.append(url)
            yield b"fallback-bytes"

        mocker.patch.object(drs, "stream_from_url", fake_stream)

        mock_db.files.docs = []
        mock_db.file.docs = [
            _make_file_doc(
                extra={
                    "fourdn": {
                        "extra_files": [
                            {
                                "file_format": {"@id": "/file-formats/beddb/"},
                                "href": "/x/first.beddb",
                                "file_size": 50,
                            },
                            {
                                "file_format": {"display_title": "bw"},
                                "href": "/x/second.bw",
                            },
                        ]
                    }
                }
            )
        ]

        # Act
        resp = await stream_index_file(
            "4dn", "4DNFIBED01", _make_request(), range=None
        )

        # Assert
        assert resp.status_code == 200
        body = b"".join([chunk async for chunk in resp.body_iterator])
        assert body == b"fallback-bytes"
        assert captured_urls[0].endswith("/x/first.beddb")

    @pytest.mark.asyncio
    async def test_stream_index_file_should_reject_non_index_dict_token(
        self, mock_db, mocker
    ):
        """Test (IX-009) that a non-index dict display_title does not match.

        Given:
            A 4DN file with two dict-shaped sidecar entries: a non-index
            ``bw`` entry first and a ``bai`` index entry second.
        When:
            ``stream_index_file`` is called.
        Then:
            The non-index dict token must not match the canonical index
            formats; the ``bai`` index entry is selected and streamed.
        """
        # Arrange
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)

        captured_urls: list[str] = []

        async def fake_stream(url, _range):
            captured_urls.append(url)
            yield b"bai-bytes"

        mocker.patch.object(drs, "stream_from_url", fake_stream)

        mock_db.files.docs = []
        mock_db.file.docs = [
            _make_file_doc(
                extra={
                    "fourdn": {
                        "extra_files": [
                            {
                                "file_format": {"display_title": "bw"},
                                "href": "/x/data.bw",
                            },
                            {
                                "file_format": {"display_title": "bai"},
                                "href": "/x/y.bai",
                                "file_size": 80,
                            },
                        ]
                    }
                }
            )
        ]

        # Act
        resp = await stream_index_file(
            "4dn", "4DNFIBED01", _make_request(), range=None
        )

        # Assert
        assert resp.status_code == 200
        body = b"".join([chunk async for chunk in resp.body_iterator])
        assert body == b"bai-bytes"
        assert captured_urls[0].endswith("/x/y.bai")

    @pytest.mark.asyncio
    async def test_stream_index_file_should_match_bare_string_index_alongside_dict_entry(
        self, mock_db, mocker
    ):
        """Test (IX-010) that a bare-string index token is matched.

        Given:
            A 4DN file with a non-index dict ``bw`` entry first and a
            bare-string ``tbi`` index entry second.
        When:
            ``stream_index_file`` is called.
        Then:
            The bare-string entry is normalized and matched, so the ``tbi``
            entry is selected and streamed — exercising the string branch
            alongside the dict branch in one pass.
        """
        # Arrange
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)

        captured_urls: list[str] = []

        async def fake_stream(url, _range):
            captured_urls.append(url)
            yield b"tbi-bytes"

        mocker.patch.object(drs, "stream_from_url", fake_stream)

        mock_db.files.docs = []
        mock_db.file.docs = [
            _make_file_doc(
                extra={
                    "fourdn": {
                        "extra_files": [
                            {
                                "file_format": {"display_title": "bw"},
                                "href": "/x/data.bw",
                            },
                            {
                                "file_format": "tbi",
                                "href": "/x/abc.tbi",
                                "file_size": 100,
                            },
                        ]
                    }
                }
            )
        ]

        # Act
        resp = await stream_index_file(
            "4dn", "4DNFIBED01", _make_request(), range=None
        )

        # Assert
        assert resp.status_code == 200
        body = b"".join([chunk async for chunk in resp.body_iterator])
        assert body == b"tbi-bytes"
        assert captured_urls[0].endswith("/x/abc.tbi")

    @pytest.mark.asyncio
    async def test_stream_index_file_should_match_dict_token_case_insensitively(
        self, mock_db, mocker
    ):
        """Test (IX-011) that a dict display_title token matches regardless of case.

        Given:
            A 4DN file with a non-index ``bw`` entry first and a dict index
            entry whose ``display_title`` is upper-case (``TBI``).
        When:
            ``stream_index_file`` is called.
        Then:
            The token is lowercased before matching, so the ``TBI`` entry
            is selected and streamed.
        """
        # Arrange
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)

        captured_urls: list[str] = []

        async def fake_stream(url, _range):
            captured_urls.append(url)
            yield b"tbi-bytes"

        mocker.patch.object(drs, "stream_from_url", fake_stream)

        mock_db.files.docs = []
        mock_db.file.docs = [
            _make_file_doc(
                extra={
                    "fourdn": {
                        "extra_files": [
                            {
                                "file_format": {"display_title": "bw"},
                                "href": "/x/data.bw",
                            },
                            {
                                "file_format": {"display_title": "TBI"},
                                "href": "/x/abc.tbi",
                                "file_size": 100,
                            },
                        ]
                    }
                }
            )
        ]

        # Act
        resp = await stream_index_file(
            "4dn", "4DNFIBED01", _make_request(), range=None
        )

        # Assert
        assert resp.status_code == 200
        body = b"".join([chunk async for chunk in resp.body_iterator])
        assert body == b"tbi-bytes"
        assert captured_urls[0].endswith("/x/abc.tbi")

    @pytest.mark.parametrize("bad_format", [None, 123, ["tbi"]])
    @pytest.mark.asyncio
    async def test_stream_index_file_should_tolerate_non_string_file_format(
        self, mock_db, mocker, bad_format
    ):
        """Test (IX-012) that an unexpected file_format type does not 500.

        Given:
            A 4DN file with two non-matching entries: one whose
            ``file_format`` is a non-dict, non-string value (None, an int,
            or a list) first, and a ``bw`` entry second.
        When:
            ``stream_index_file`` is called.
        Then:
            The value is treated as a non-match, the first-entry fallback
            serves the first entry, and no AttributeError/500 occurs.
        """
        # Arrange
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)

        captured_urls: list[str] = []

        async def fake_stream(url, _range):
            captured_urls.append(url)
            yield b"fallback-bytes"

        mocker.patch.object(drs, "stream_from_url", fake_stream)

        mock_db.files.docs = []
        mock_db.file.docs = [
            _make_file_doc(
                extra={
                    "fourdn": {
                        "extra_files": [
                            {
                                "file_format": bad_format,
                                "href": "/x/first.dat",
                                "file_size": 40,
                            },
                            {
                                "file_format": {"display_title": "bw"},
                                "href": "/x/second.bw",
                            },
                        ]
                    }
                }
            )
        ]

        # Act
        resp = await stream_index_file(
            "4dn", "4DNFIBED01", _make_request(), range=None
        )

        # Assert
        assert resp.status_code == 200
        body = b"".join([chunk async for chunk in resp.body_iterator])
        assert body == b"fallback-bytes"
        assert captured_urls[0].endswith("/x/first.dat")

    @pytest.mark.parametrize("index_token", ["pairs_px2", "bg_px2"])
    @pytest.mark.asyncio
    async def test_stream_index_file_should_match_pairix_index_tokens(
        self, mock_db, mocker, index_token
    ):
        """Test (IX-013) that 4DN pairix index tokens are matched by format.

        Given:
            A 4DN file with a non-index ``bw`` entry first and a pairix
            index entry (``pairs_px2`` or ``bg_px2``) second.
        When:
            ``stream_index_file`` is called.
        Then:
            The pairix token matches the index allowlist and wins over array
            order, so the index entry is selected and streamed rather than
            the first-entry fallback serving the ``bw``.
        """
        # Arrange
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)

        captured_urls: list[str] = []

        async def fake_stream(url, _range):
            captured_urls.append(url)
            yield b"px2-bytes"

        mocker.patch.object(drs, "stream_from_url", fake_stream)

        mock_db.files.docs = []
        mock_db.file.docs = [
            _make_file_doc(
                extra={
                    "fourdn": {
                        "extra_files": [
                            {
                                "file_format": {"display_title": "bw"},
                                "href": "/x/data.bw",
                            },
                            {
                                "file_format": {"display_title": index_token},
                                "href": "/x/abc.px2",
                                "file_size": 100,
                            },
                        ]
                    }
                }
            )
        ]

        # Act
        resp = await stream_index_file(
            "4dn", "4DNFIBED01", _make_request(), range=None
        )

        # Assert
        assert resp.status_code == 200
        body = b"".join([chunk async for chunk in resp.body_iterator])
        assert body == b"px2-bytes"
        assert captured_urls[0].endswith("/x/abc.px2")


class TestStreamIndexFileWorkflowPaths:
    @pytest.mark.asyncio
    async def test_stream_index_file_should_serve_cached_index_without_dispatch(
        self, mock_db, mocker, tmp_path
    ):
        """Test (IX-006) that an INDEX cache hit short-circuits dispatch.

        Given:
            A BAM file with a registered ``BamIndexProcessor`` and an
            INDEX cache pre-populated with the BAI bytes.
        When:
            ``stream_index_file`` is called (GET).
        Then:
            It should return a 200 ``StreamingResponse`` and
            ``ensure_workflow`` MUST NOT be called.
        """
        # Arrange
        from cfdb.workflows import keys as key_utils
        from cfdb.workflows.cache import LocalFsCache
        from starlette.responses import StreamingResponse

        mocker.patch.object(locks, "wait_for_cutover", return_value=None)
        cache = LocalFsCache(tmp_path / "cache")
        src = tmp_path / "src"
        src.write_bytes(b"bai-cached")
        processor = BamIndexProcessor()
        # extract_identity reads dcc.dcc_abbreviation → "4DN_DCIC" →
        # normalized to "4dn_dcic" — that's the dcc value baked into
        # the cache key.
        index_key = key_utils.cache_key(
            dcc="4dn_dcic",
            local_id="4DNFIBAM01",
            artifact_kind=ArtifactKind.INDEX,
            md5=FIXTURE_MD5,
            processor_version=processor.processor_version,
        )
        await cache.put(index_key, src)

        class _FailIfDispatched:
            async def ensure_workflow(self, _file_doc):
                raise AssertionError("cache hit must NOT dispatch a workflow")

        registry = ProcessorRegistry()
        registry.register(processor)
        mocker.patch.object(api, "cache", cache)
        mocker.patch.object(api, "processor_registry", registry)
        mocker.patch.object(api, "executor", _FailIfDispatched())

        bam_doc = _make_file_doc(
            file_format={"name": "BAM"},
            filename="x.bam",
            local_id="4DNFIBAM01",
        )
        bam_doc.pop("extra", None)
        mock_db.files.docs = []
        mock_db.file.docs = [bam_doc]

        # Act
        resp = await stream_index_file(
            "4dn", "4DNFIBAM01", _make_request(), range=None
        )

        # Assert
        assert isinstance(resp, StreamingResponse)
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_stream_index_file_should_return_404_when_no_sidecar_and_no_processor(
        self, mock_db, mocker, tmp_path
    ):
        """Test that files with no index path return 404 (subsystem wired).

        Given:
            A CSV file with no sidecar, the workflow subsystem WIRED but
            with no processor that produces an INDEX artifact for CSV.
        When:
            stream_index_file is called.
        Then:
            It should raise HTTPException(404) since there's no index to
            serve or build for this format. The 503 branch (subsystem
            disabled) is exercised separately by
            ``test_stream_index_file_should_return_503_when_subsystem_disabled``.
        """
        # Arrange
        from cfdb.workflows.cache import LocalFsCache

        mocker.patch.object(locks, "wait_for_cutover", return_value=None)
        doc = _make_file_doc(file_format={"name": "CSV"})
        doc.pop("extra", None)
        mock_db.files.docs = []
        mock_db.file.docs = [doc]
        # Wire the workflow subsystem with an empty registry so the
        # 503-disabled branch does not fire and the 404-no-processor
        # branch under test is reachable.
        mocker.patch.object(api, "cache", LocalFsCache(tmp_path / "cache"))
        mocker.patch.object(api, "processor_registry", ProcessorRegistry())
        mocker.patch.object(
            api,
            "executor",
            WoolExecutor(
                mock_db,
                api.cache,
                api.processor_registry,
                workdir_root=tmp_path / "jobs",
            ),
        )

        # Act & assert
        with pytest.raises(HTTPException) as exc_info:
            await stream_index_file("4dn", "4DNFIBED01", _make_request(), range=None)
        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_stream_index_file_should_return_503_when_subsystem_disabled(
        self, mock_db, mocker
    ):
        """Test that an index request returns 503 when the subsystem is disabled.

        Given:
            A BAM file with no upstream sidecar and the workflow
            subsystem unwired (``api.executor is None``).
        When:
            stream_index_file is called.
        Then:
            It should raise HTTPException(503) with a clear "subsystem
            disabled" detail and a Retry-After header — matching /data's
            graceful-degradation philosophy rather than masquerading as
            a per-file 404.
        """
        # Arrange
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)
        mocker.patch.object(api, "executor", None)
        mocker.patch.object(api, "cache", None)
        mocker.patch.object(api, "processor_registry", None)
        doc = _make_file_doc(file_format={"name": "BAM"})
        doc.pop("extra", None)
        mock_db.files.docs = []
        mock_db.file.docs = [doc]

        # Act & assert
        with pytest.raises(HTTPException) as exc_info:
            await stream_index_file("4dn", "4DNFIBED01", _make_request(), range=None)
        assert exc_info.value.status_code == 503
        assert "subsystem disabled" in (exc_info.value.detail or "").lower()

    @pytest.mark.asyncio
    async def test_stream_index_file_should_return_404_on_head_cache_miss(
        self, mock_db, mocker
    ):
        """Test that a HEAD probe on an un-cached index does not dispatch.

        Given:
            A BAM file with no sidecar, a registered processor emitting an
            INDEX artifact, and an empty cache.
        When:
            stream_index_file is called with method HEAD.
        Then:
            It should raise HTTPException(404) without claiming the
            workflow mutex, so monitoring probes cannot trigger
            preprocessing.
        """
        # Arrange
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)

        class _MissCache:
            async def head(self, _k):
                return None

            def get(self, _k, _r=None):
                raise AssertionError

            async def put(self, *_a, **_kw):
                raise AssertionError

            async def delete(self, _k):
                return False

        class _FailIfDispatched:
            async def ensure_workflow(self, _file_doc):
                raise AssertionError("HEAD must not dispatch a workflow")

        registry = ProcessorRegistry()
        registry.register(BamIndexProcessor())
        mocker.patch.object(api, "cache", _MissCache())
        mocker.patch.object(api, "processor_registry", registry)
        mocker.patch.object(api, "executor", _FailIfDispatched())

        bam_doc = _make_file_doc(
            file_format={"name": "BAM"},
            filename="x.bam",
            local_id="4DNFIBAM01",
        )
        bam_doc.pop("extra", None)
        mock_db.files.docs = []
        mock_db.file.docs = [bam_doc]

        # Act & assert
        with pytest.raises(HTTPException) as exc_info:
            await stream_index_file(
                "4dn", "4DNFIBAM01", _make_request("HEAD"), range=None
            )
        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_stream_index_file_should_serve_sidecar_when_raw_true(
        self, mock_db, mocker
    ):
        """Test that ?raw=true returns an upstream sidecar when present.

        Given:
            A 4DN BED with an extra_files sidecar and a client passing raw=True.
        When:
            stream_index_file is called.
        Then:
            It should return the upstream-served sidecar response.
        """
        # Arrange
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)

        async def fake_stream(_url, _range):
            yield b"sidecar-bytes"

        mocker.patch.object(drs, "stream_from_url", fake_stream)

        mock_db.files.docs = []
        mock_db.file.docs = [
            _make_file_doc(
                extra={"extra_files": [{"href": "/x/abc.tbi", "file_size": 42}]}
            )
        ]

        # Act
        resp = await stream_index_file(
            "4dn", "4DNFIBED01", _make_request(), range=None, raw=True
        )

        # Assert
        assert resp.status_code == 200
        body = b"".join([chunk async for chunk in resp.body_iterator])
        assert body == b"sidecar-bytes"

    @pytest.mark.asyncio
    async def test_stream_index_file_should_return_404_when_raw_true_and_no_sidecar(
        self, mock_db, mocker
    ):
        """Test that ?raw=true 404s when no upstream sidecar exists.

        Given:
            A BAM file with no extra_files sidecar and a client passing
            raw=True; the workflow subsystem is wired up.
        When:
            stream_index_file is called.
        Then:
            It should raise HTTPException(404) without touching the
            workflow cache.
        """
        # Arrange
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)

        class _FailIfConsulted:
            async def head(self, _k):
                raise AssertionError("raw=true must not consult the cache")

            def get(self, *_a, **_k):
                raise AssertionError

            async def put(self, *_a, **_k):
                raise AssertionError

            async def delete(self, _k):
                return False

        class _FailIfDispatched:
            async def ensure_workflow(self, _file_doc):
                raise AssertionError("raw=true must not dispatch a workflow")

        registry = ProcessorRegistry()
        registry.register(BamIndexProcessor())
        mocker.patch.object(api, "cache", _FailIfConsulted())
        mocker.patch.object(api, "processor_registry", registry)
        mocker.patch.object(api, "executor", _FailIfDispatched())

        bam_doc = _make_file_doc(
            file_format={"name": "BAM"},
            filename="x.bam",
            local_id="4DNFIBAM01",
        )
        bam_doc.pop("extra", None)
        mock_db.files.docs = []
        mock_db.file.docs = [bam_doc]

        # Act & assert
        with pytest.raises(HTTPException) as exc_info:
            await stream_index_file(
                "4dn", "4DNFIBAM01", _make_request(), range=None, raw=True
            )
        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_stream_index_file_should_return_202_when_processor_applies_and_cache_miss(
        self, mock_db, mocker
    ):
        """Test that a missing index artifact triggers workflow dispatch.

        Given:
            A BED file, a registered processor with the INDEX artifact
            kind, a cache backend reporting a miss, and an executor that
            returns a fresh job record.
        When:
            stream_index_file is called.
        Then:
            It should return a 202 JSONResponse with the job's
            ``Location`` header set to ``/jobs/{job_id}``.
        """
        # Arrange
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)
        mock_db.jobs.create_index(
            {"workflow_key": 1},
            unique=True,
            partialFilterExpression={"active": True},
        )

        # Register a real processor + cache + executor on the module.
        class _NoopCache:
            async def head(self, _k):
                return None

            def get(self, _k, _r=None):  # pragma: no cover - unused here
                raise AssertionError

            async def put(self, *_a, **_kw):  # pragma: no cover
                raise AssertionError

            async def delete(self, _k):  # pragma: no cover
                return False

        registry = ProcessorRegistry()
        registry.register(BamIndexProcessor())
        mocker.patch.object(api, "cache", _NoopCache())
        mocker.patch.object(api, "processor_registry", registry)
        mocker.patch.object(
            api,
            "executor",
            WoolExecutor(
                mock_db,
                api.cache,
                registry,
                workdir_root="/tmp/cfdb/jobs",
            ),
        )

        # Prevent the fire-and-forget task from running any real dispatch
        # via the module-top-imported do_dispatch context manager.
        bam_doc = _make_file_doc(
            file_format={"name": "BAM"},
            filename="x.bam",
            local_id="4DNFIBAM01",
        )
        bam_doc.pop("extra", None)
        mock_db.files.docs = []
        mock_db.file.docs = [bam_doc]

        # Act
        with do_dispatch(False):
            resp = await stream_index_file(
                "4dn", "4DNFIBAM01", _make_request(), range=None
            )

        # Assert
        assert isinstance(resp, JSONResponse)
        assert resp.status_code == 202
        assert resp.headers["location"].startswith("/jobs/")


class TestStreamIndexFileStatus:
    @pytest.mark.asyncio
    async def test_stream_index_file_status_should_raise_400_when_dcc_invalid(
        self, mock_db, mocker
    ):
        """Test that an unknown DCC is rejected the same as /index.

        Given:
            A status probe for a DCC name not in the registry.
        When:
            stream_index_file_status is called.
        Then:
            It should raise HTTPException(400).
        """
        # Arrange
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)

        # Act & assert
        with pytest.raises(HTTPException) as exc_info:
            await stream_index_file_status("nope", "file-1")
        assert exc_info.value.status_code == 400

    @pytest.mark.asyncio
    async def test_stream_index_file_status_should_raise_404_when_file_missing(
        self, mock_db, mocker
    ):
        """Test that a missing file mirrors /index's 404.

        Given:
            A valid DCC but no matching file document.
        When:
            stream_index_file_status is called.
        Then:
            It should raise HTTPException(404).
        """
        # Arrange
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)
        mock_db.files.docs = []
        mock_db.file.docs = []

        # Act & assert
        with pytest.raises(HTTPException) as exc_info:
            await stream_index_file_status("4dn", "missing")
        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_stream_index_file_status_should_raise_403_when_hubmap_not_public(
        self, mock_db, mocker
    ):
        """Test that a protected HuBMAP file mirrors /index's 403.

        Given:
            A HuBMAP file with ``data_access_level="consortium"``.
        When:
            stream_index_file_status is called.
        Then:
            It should raise HTTPException(403) before any sidecar or
            workflow lookup.
        """
        # Arrange
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)
        mock_db.files.docs = []
        mock_db.file.docs = [
            {
                "submission": "hubmap",
                "id_namespace": "tag:hubmapconsortium.org,2023:",
                "local_id": "HBM-1",
                "filename": "x.bed",
                "data_access_level": "consortium",
                "file_format": {"name": "BED"},
            }
        ]

        # Act & assert
        with pytest.raises(HTTPException) as exc_info:
            await stream_index_file_status("hubmap", "HBM-1")
        assert exc_info.value.status_code == 403

    @pytest.mark.asyncio
    async def test_stream_index_file_status_should_report_ready_when_sidecar_present(
        self, mock_db, mocker
    ):
        """Test that an upstream sidecar reports ready immediately.

        Given:
            A 4DN BED with an ``extra.extra_files`` sidecar entry.
        When:
            stream_index_file_status is called.
        Then:
            It should return ``{"ready": True}`` without opening any
            upstream stream.
        """
        # Arrange
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)

        stream_calls: list = []

        async def fake_stream(url, _range):
            stream_calls.append(url)
            yield b"should-not-stream"

        mocker.patch.object(drs, "stream_from_url", fake_stream)

        mock_db.files.docs = []
        mock_db.file.docs = [
            _make_file_doc(
                extra={"extra_files": [{"href": "/x/abc.tbi", "file_size": 100}]}
            )
        ]

        # Act
        result = await stream_index_file_status("4dn", "4DNFIBED01")

        # Assert
        assert result == {"ready": True}
        assert stream_calls == []

    @pytest.mark.asyncio
    async def test_stream_index_file_status_should_502_when_sidecar_href_missing(
        self, mock_db, mocker
    ):
        """Test that a malformed sidecar mirrors /index's 502.

        Given:
            A 4DN sidecar entry with no ``href`` key.
        When:
            stream_index_file_status is called.
        Then:
            It should raise HTTPException(502).
        """
        # Arrange
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)
        mock_db.files.docs = []
        mock_db.file.docs = [
            _make_file_doc(extra={"extra_files": [{"file_format": "tbi"}]})
        ]

        # Act & assert
        with pytest.raises(HTTPException) as exc_info:
            await stream_index_file_status("4dn", "4DNFIBED01")
        assert exc_info.value.status_code == 502

    @pytest.mark.asyncio
    async def test_stream_index_file_status_should_report_ready_on_cache_hit(
        self, mock_db, mocker, tmp_path
    ):
        """Test that a cached INDEX artifact reports ready without dispatch.

        Given:
            A BAM file with no sidecar, a registered ``BamIndexProcessor``,
            and the INDEX cache pre-populated with the BAI bytes.
        When:
            stream_index_file_status is called.
        Then:
            It should return ``{"ready": True}`` and ``ensure_workflow``
            MUST NOT be called.
        """
        # Arrange
        from cfdb.workflows import keys as key_utils
        from cfdb.workflows.cache import LocalFsCache

        mocker.patch.object(locks, "wait_for_cutover", return_value=None)
        cache = LocalFsCache(tmp_path / "cache")
        src = tmp_path / "src"
        src.write_bytes(b"bai-cached")
        processor = BamIndexProcessor()
        index_key = key_utils.cache_key(
            dcc="4dn_dcic",
            local_id="4DNFIBAM01",
            artifact_kind=ArtifactKind.INDEX,
            md5=FIXTURE_MD5,
            processor_version=processor.processor_version,
        )
        await cache.put(index_key, src)

        class _FailIfDispatched:
            async def ensure_workflow(self, _file_doc):
                raise AssertionError("status probe must NOT dispatch a workflow")

        registry = ProcessorRegistry()
        registry.register(processor)
        mocker.patch.object(api, "cache", cache)
        mocker.patch.object(api, "processor_registry", registry)
        mocker.patch.object(api, "executor", _FailIfDispatched())

        bam_doc = _make_file_doc(
            file_format={"name": "BAM"},
            filename="x.bam",
            local_id="4DNFIBAM01",
        )
        bam_doc.pop("extra", None)
        mock_db.files.docs = []
        mock_db.file.docs = [bam_doc]

        # Act
        result = await stream_index_file_status("4dn", "4DNFIBAM01")

        # Assert
        assert result == {"ready": True}

    @pytest.mark.asyncio
    async def test_stream_index_file_status_should_report_not_ready_on_cache_miss(
        self, mock_db, mocker
    ):
        """Test that a processable-but-uncached index reports not-ready.

        Given:
            A BAM file with no sidecar, a registered processor emitting an
            INDEX artifact, an empty cache, and an executor that fails if
            dispatched.
        When:
            stream_index_file_status is called.
        Then:
            It should return ``{"ready": False}`` and ``ensure_workflow``
            MUST NOT be called — the probe never dispatches.
        """
        # Arrange
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)

        class _MissCache:
            async def head(self, _k):
                return None

            def get(self, _k, _r=None):
                raise AssertionError

            async def put(self, *_a, **_kw):
                raise AssertionError

            async def delete(self, _k):
                return False

        class _FailIfDispatched:
            async def ensure_workflow(self, _file_doc):
                raise AssertionError("status probe must NOT dispatch a workflow")

        registry = ProcessorRegistry()
        registry.register(BamIndexProcessor())
        mocker.patch.object(api, "cache", _MissCache())
        mocker.patch.object(api, "processor_registry", registry)
        mocker.patch.object(api, "executor", _FailIfDispatched())

        bam_doc = _make_file_doc(
            file_format={"name": "BAM"},
            filename="x.bam",
            local_id="4DNFIBAM01",
        )
        bam_doc.pop("extra", None)
        mock_db.files.docs = []
        mock_db.file.docs = [bam_doc]

        # Act
        result = await stream_index_file_status("4dn", "4DNFIBAM01")

        # Assert
        assert result == {"ready": False}

    @pytest.mark.asyncio
    async def test_stream_index_file_status_should_raise_404_for_passthrough_format(
        self, mock_db, mocker, tmp_path
    ):
        """Test that a passthrough format mirrors /index's no-index 404.

        Given:
            A CSV file with no sidecar and the default registry wired, so
            the matched PassthroughProcessor declares no index artifact.
        When:
            stream_index_file_status is called.
        Then:
            It should raise HTTPException(404) — CSV has no index in any
            state of the world.
        """
        # Arrange
        from cfdb.workflows.cache import LocalFsCache

        mocker.patch.object(locks, "wait_for_cutover", return_value=None)
        doc = _make_file_doc(file_format={"name": "CSV"})
        doc.pop("extra", None)
        mock_db.files.docs = []
        mock_db.file.docs = [doc]
        # Wire the DEFAULT registry (which includes PassthroughProcessor)
        # so CSV resolves to a processor that produces no artifacts —
        # exercising the no-index-format branch rather than the generic
        # "no processor matched" fall-through.
        mocker.patch.object(api, "cache", LocalFsCache(tmp_path / "cache"))
        mocker.patch.object(api, "processor_registry", default_registry())
        mocker.patch.object(
            api,
            "executor",
            WoolExecutor(
                mock_db,
                api.cache,
                api.processor_registry,
                workdir_root=tmp_path / "jobs",
            ),
        )

        # Act & assert
        with pytest.raises(HTTPException) as exc_info:
            await stream_index_file_status("4dn", "4DNFIBED01")
        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_stream_index_file_status_should_raise_503_when_subsystem_disabled(
        self, mock_db, mocker
    ):
        """Test that a processable format with the subsystem off mirrors 503.

        Given:
            A BAM file with no sidecar and the workflow subsystem unwired
            (``api.executor is None``).
        When:
            stream_index_file_status is called.
        Then:
            It should raise HTTPException(503).
        """
        # Arrange
        mocker.patch.object(locks, "wait_for_cutover", return_value=None)
        mocker.patch.object(api, "executor", None)
        mocker.patch.object(api, "cache", None)
        mocker.patch.object(api, "processor_registry", None)
        doc = _make_file_doc(file_format={"name": "BAM"})
        doc.pop("extra", None)
        mock_db.files.docs = []
        mock_db.file.docs = [doc]

        # Act & assert
        with pytest.raises(HTTPException) as exc_info:
            await stream_index_file_status("4dn", "4DNFIBED01")
        assert exc_info.value.status_code == 503
