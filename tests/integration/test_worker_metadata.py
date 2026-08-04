"""Integration test for the worker-metadata admission contract.

The ECS metadata-publishing pipeline (worker tags its task →
``EcsDiscovery`` reads the tags back) carries one value end to end:
whatever ``wool.LocalWorker`` authors as its metadata. Every unit test
on that pipeline mocks the worker, so none of them would notice if a
wool upgrade renamed the field, stopped populating it, or started
returning something wool's own admission gate cannot parse — which
would reproduce the original fleet-wide rejection (issue #90) with a
green suite. This test starts a real worker and closes the loop against
wool's actual predicates.
"""

from __future__ import annotations

import pytest
import wool
import wool.protocol
from wool.runtime.worker.proxy import is_version_compatible, parse_version


pytestmark = pytest.mark.integration


class TestLocalWorkerMetadata:
    @pytest.mark.asyncio
    async def test_metadata_should_be_admissible_by_wools_version_gate(self):
        """Test that a real worker authors an admissible version.

        Given:
            A started ``wool.LocalWorker`` — the same object whose
            ``metadata.version`` the ECS entrypoint publishes as the
            ``wool.version`` task tag.
        When:
            Its authored version is checked against wool's own
            version-compatibility predicate, as a same-version proxy
            would at admission.
        Then:
            It should parse and be compatible, pinning the contract the
            tag pipeline round-trips: what a real worker publishes is
            what a real proxy admits.
        """
        # Arrange
        worker = wool.LocalWorker(host="127.0.0.1", port=0)
        await worker.start()

        # Act
        try:
            metadata = worker.metadata
        finally:
            await worker.stop()

        # Assert
        assert metadata is not None
        published = parse_version(metadata.version)
        proxy_side = parse_version(wool.protocol.__version__)
        assert published is not None
        assert is_version_compatible(proxy_side, published)
