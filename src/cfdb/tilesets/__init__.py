"""Matrix tile serving for Hi-C contact maps (issue #82).

Layered so that only ``service`` and ``store`` ever touch a tile backend:

- :mod:`cfdb.tilesets.formats` — which files are contact maps, and of what
  kind. Stdlib-only, so the worker-side processor can import it.
- :mod:`cfdb.tilesets.errors` — the failure vocabulary the routers translate
  into status codes.
- :mod:`cfdb.tilesets.backend` — the single import gate for clodius.
- :mod:`cfdb.tilesets.store` — cached artifact to local filesystem path.
- :mod:`cfdb.tilesets.service` — open-tileset and tile caches, thread offload.
- :mod:`cfdb.tilesets.wire` — HiGlass response payload assembly.
"""
