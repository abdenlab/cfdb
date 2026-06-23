"""Shared test fixtures."""

from __future__ import annotations

from typing import Any

import pytest

from cfdb import api


def _resolve(doc: dict, key: str):
    """Resolve a possibly dot-notated key against a nested dict."""
    value = doc
    for part in key.split("."):
        if isinstance(value, dict):
            value = value.get(part)
        else:
            return None
    return value


def _match(doc: dict, query: dict) -> bool:
    """Minimal MongoDB query matcher supporting a small operator subset."""
    for key, cond in query.items():
        # AND $and/$or with the rest of the top-level keys rather than
        # short-circuiting the whole match, so a query that mixes them with
        # sibling field conditions (e.g. {active, updated_at, $or}) is
        # evaluated faithfully regardless of key order.
        if key == "$and":
            if not all(_match(doc, sub) for sub in cond):
                return False
            continue
        if key == "$or":
            if not any(_match(doc, sub) for sub in cond):
                return False
            continue

        value = _resolve(doc, key)

        if isinstance(cond, dict):
            for op, operand in cond.items():
                if op == "$in":
                    if value not in operand:
                        return False
                elif op == "$ne":
                    if value == operand:
                        return False
                elif op == "$exists":
                    if operand and key not in doc:
                        return False
                    if not operand and key in doc:
                        return False
                elif op == "$regex":
                    import re

                    flags = 0
                    if cond.get("$options", "") == "i":
                        flags = re.IGNORECASE
                    if not re.search(operand, str(value or ""), flags):
                        return False
                elif op == "$options":
                    pass  # handled by $regex branch
                elif op == "$lt":
                    if value is None or not (value < operand):
                        return False
                elif op == "$lte":
                    if value is None or not (value <= operand):
                        return False
                elif op == "$gt":
                    if value is None or not (value > operand):
                        return False
                elif op == "$gte":
                    if value is None or not (value >= operand):
                        return False
                else:
                    return False
        else:
            if value != cond:
                return False
    return True


class _FakeCursor:
    """Async-iterable cursor backed by a plain list."""

    def __init__(self, docs: list[dict]) -> None:
        self._docs = list(docs)
        self._skip = 0
        self._limit = 0

    def skip(self, n: int) -> "_FakeCursor":
        self._skip = n
        return self

    def limit(self, n: int) -> "_FakeCursor":
        self._limit = n
        return self

    async def to_list(self, *, length: Any = None) -> list[dict]:
        sliced = self._docs[self._skip :]
        if self._limit:
            sliced = sliced[: self._limit]
        return sliced

    def __aiter__(self):
        self._iter_docs = iter(
            self._docs[self._skip :]
            if not self._limit
            else self._docs[self._skip : self._skip + self._limit]
        )
        return self

    async def __anext__(self):
        try:
            return next(self._iter_docs)
        except StopIteration:
            raise StopAsyncIteration


class _DeleteResult:
    def __init__(self, count: int) -> None:
        self.deleted_count = count


class _BulkWriteResult:
    def __init__(self, count: int) -> None:
        self.modified_count = count


class _UpdateResult:
    def __init__(self, matched: int, modified: int) -> None:
        self.matched_count = matched
        self.modified_count = modified


class _InsertOneResult:
    def __init__(self, inserted_id) -> None:
        self.inserted_id = inserted_id


def _set_nested(doc: dict, key: str, value: Any) -> None:
    """Assign ``value`` at a dot-notated path within ``doc``."""
    parts = key.split(".")
    cursor: dict = doc
    for part in parts[:-1]:
        nxt = cursor.get(part)
        if not isinstance(nxt, dict):
            nxt = {}
            cursor[part] = nxt
        cursor = nxt
    cursor[parts[-1]] = value


def _apply_update(doc: dict, update: dict, *, is_insert: bool = False) -> bool:
    """Apply a MongoDB-style update to ``doc`` in place. Return True if changed."""
    changed = False
    for op, fields in update.items():
        if op == "$set":
            for k, v in fields.items():
                current = doc
                for part in k.split(".")[:-1]:
                    current = current.get(part, {}) if isinstance(current, dict) else {}
                existing = current.get(k.split(".")[-1]) if isinstance(current, dict) else None
                if existing != v:
                    _set_nested(doc, k, v)
                    changed = True
        elif op == "$setOnInsert":
            if is_insert:
                for k, v in fields.items():
                    _set_nested(doc, k, v)
                    changed = True
        elif op == "$unset":
            for k in fields:
                if k in doc:
                    doc.pop(k)
                    changed = True
        elif op == "$addToSet":
            for k, v in fields.items():
                existing = doc.setdefault(k, [])
                if v not in existing:
                    existing.append(v)
                    changed = True
        elif op == "$inc":
            for k, v in fields.items():
                _set_nested(doc, k, (_resolve(doc, k) or 0) + v)
                changed = True
        else:
            raise NotImplementedError(f"FakeCollection update op: {op}")
    return changed


class FakeCollection:
    """In-memory MongoDB collection stub."""

    def __init__(self) -> None:
        self.docs: list[dict] = []
        # Each entry: (key_spec: dict, opts: dict). Supports opts["unique"]
        # and opts["partialFilterExpression"] to exercise Mongo's partial
        # unique index behavior in tests.
        self._indexes: list[tuple[dict, dict]] = []

    def create_index(self, spec: dict, **opts) -> None:
        self._indexes.append((spec, opts))

    def with_options(self, **_kwargs):
        """No-op shim mirroring Motor's ``Collection.with_options``.

        Production code calls ``db[collection].with_options(write_concern=...)``
        to pin a per-collection write concern (see ``cfdb.workflows.lock._jobs``).
        Tests don't model write-concern semantics — this stub returns the
        same FakeCollection so downstream operations land on the same
        in-memory document set.
        """
        return self

    def _violates_unique(self, new_doc: dict) -> bool:
        for spec, opts in self._indexes:
            if not opts.get("unique"):
                continue
            partial_filter = opts.get("partialFilterExpression")
            if partial_filter and not _match(new_doc, partial_filter):
                continue
            key_fields = list(spec.keys())
            new_tuple = tuple(new_doc.get(k) for k in key_fields)
            for existing in self.docs:
                if partial_filter and not _match(existing, partial_filter):
                    continue
                existing_tuple = tuple(existing.get(k) for k in key_fields)
                if existing_tuple == new_tuple:
                    return True
        return False

    def find(self, query: dict | None = None, projection: dict | None = None) -> _FakeCursor:
        if query is None:
            query = {}
        matched = [d for d in self.docs if _match(d, query)]
        if projection:
            fields = {k for k, v in projection.items() if v and k != "_id"}
            if projection.get("_id", 1):
                fields.add("_id")
            matched = [{k: d[k] for k in fields if k in d} for d in matched]
        return _FakeCursor(matched)

    async def find_one(self, query: dict, projection: dict | None = None) -> dict | None:
        # ``projection`` is accepted for Motor signature parity but the
        # in-memory store is small enough that we always return the full
        # document — projecting fields would require modeling Mongo's
        # inclusion-vs-exclusion rules and dotted-path semantics, which
        # the tests don't actually exercise.
        for d in self.docs:
            if _match(d, query):
                return dict(d)
        return None

    async def delete_many(self, query: dict) -> _DeleteResult:
        before = len(self.docs)
        self.docs = [d for d in self.docs if not _match(d, query)]
        return _DeleteResult(before - len(self.docs))

    async def count_documents(self, query: dict) -> int:
        return sum(1 for d in self.docs if _match(d, query))

    async def insert_many(self, docs: list[dict]) -> None:
        self.docs.extend(docs)

    async def insert_one(self, doc: dict) -> _InsertOneResult:
        from pymongo.errors import DuplicateKeyError

        if self._violates_unique(doc):
            raise DuplicateKeyError("fake unique-index violation")
        self.docs.append(dict(doc))
        return _InsertOneResult(inserted_id=doc.get("_id"))

    async def find_one_and_update(
        self,
        query: dict,
        update: dict,
        *,
        upsert: bool = False,
        return_document=None,
        sort: list | None = None,
        **_kwargs,
    ) -> dict | None:
        candidates = [d for d in self.docs if _match(d, query)]
        if sort:
            # Single-key sort is all the production queries use; honoring it
            # lets tests exercise ordered claims (e.g. oldest-due-first).
            field, direction = sort[0]
            candidates.sort(key=lambda d: d.get(field), reverse=direction < 0)
        if candidates:
            d = candidates[0]
            _apply_update(d, update, is_insert=False)
            return dict(d)
        if upsert:
            seed: dict = {}
            for k, v in query.items():
                if k.startswith("$"):
                    continue
                if isinstance(v, dict):
                    continue
                seed[k] = v
            _apply_update(seed, update, is_insert=True)
            if self._violates_unique(seed):
                from pymongo.errors import DuplicateKeyError

                raise DuplicateKeyError("fake unique-index violation on upsert")
            self.docs.append(dict(seed))
            return dict(seed)
        return None

    async def bulk_write(self, operations: list, ordered: bool = True) -> _BulkWriteResult:
        count = 0
        for op in operations:
            # Support UpdateOne
            if hasattr(op, "_filter") and hasattr(op, "_doc"):
                filt = op._filter
                update = op._doc
                for d in self.docs:
                    if _match(d, filt):
                        for k, v in update.get("$set", {}).items():
                            d[k] = v
                        count += 1
                        break
        return _BulkWriteResult(count)

    async def update_many(self, query: dict, update: dict, **kwargs) -> _UpdateResult:
        matched = 0
        modified = 0
        for d in self.docs:
            if _match(d, query):
                matched += 1
                # Row-level modified count, matching real Mongo semantics
                # (number of documents changed, not number of field writes).
                if _apply_update(d, update, is_insert=False):
                    modified += 1
        return _UpdateResult(matched, modified)

    async def update_one(self, query: dict, update: dict, **kwargs) -> _UpdateResult:
        for d in self.docs:
            if _match(d, query):
                modified = 1 if _apply_update(d, update, is_insert=False) else 0
                return _UpdateResult(1, modified)
        if kwargs.get("upsert"):
            seed: dict = {}
            for k, v in query.items():
                if k.startswith("$") or isinstance(v, dict):
                    continue
                seed[k] = v
            _apply_update(seed, update, is_insert=True)
            self.docs.append(seed)
            return _UpdateResult(0, 0)
        return _UpdateResult(0, 0)

    async def distinct(self, field: str, query: dict | None = None) -> list:
        if query is None:
            query = {}
        matched = [d for d in self.docs if _match(d, query)]
        seen: list = []
        for doc in matched:
            value = _resolve(doc, field)
            if value is not None and value not in seen:
                seen.append(value)
        return seen


class FakeDB:
    """In-memory database with named collections accessible by attribute or item."""

    def __init__(self) -> None:
        self._collections: dict[str, FakeCollection] = {}

    def __getattr__(self, name: str) -> FakeCollection:
        if name.startswith("_"):
            raise AttributeError(name)
        return self._collections.setdefault(name, FakeCollection())

    def __getitem__(self, name: str) -> FakeCollection:
        return self._collections.setdefault(name, FakeCollection())

    async def list_collection_names(self) -> list[str]:
        return list(self._collections.keys())


@pytest.fixture()
def mock_db(monkeypatch):
    """Patch ``cfdb.api.db`` with an in-memory fake database."""
    db = FakeDB()
    monkeypatch.setattr(api, "db", db)
    return db
