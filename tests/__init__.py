"""Test package marker.

Required so that ``tests.integration.routines`` (used by the wool
cloudpickle-boundary integration tests) is importable from the wool
worker subprocess. Without this file, the worker's ``loads(task.args)``
fails with ``ModuleNotFoundError: No module named 'tests'`` when
unpickling a ``StubProcessor`` instance whose class is qualified by
that module path.
"""
