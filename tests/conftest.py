"""Shared test fixtures."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

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
        if key == "$and":
            return all(_match(doc, sub) for sub in cond)
        if key == "$or":
            return any(_match(doc, sub) for sub in cond)

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


class FakeCollection:
    """In-memory MongoDB collection stub."""

    def __init__(self) -> None:
        self.docs: list[dict] = []

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

    async def find_one(self, query: dict) -> dict | None:
        for d in self.docs:
            if _match(d, query):
                return d
        return None

    async def delete_many(self, query: dict) -> _DeleteResult:
        before = len(self.docs)
        self.docs = [d for d in self.docs if not _match(d, query)]
        return _DeleteResult(before - len(self.docs))

    async def count_documents(self, query: dict) -> int:
        return sum(1 for d in self.docs if _match(d, query))

    async def insert_many(self, docs: list[dict]) -> None:
        self.docs.extend(docs)

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
                for k, v in update.get("$set", {}).items():
                    if d.get(k) != v:
                        d[k] = v
                        modified += 1
        return _UpdateResult(matched, modified)

    async def update_one(self, query: dict, update: dict, **kwargs) -> _UpdateResult:
        for d in self.docs:
            if _match(d, query):
                modified = 0
                for k, v in update.get("$set", {}).items():
                    if d.get(k) != v:
                        d[k] = v
                        modified = 1
                return _UpdateResult(1, modified)
        if kwargs.get("upsert"):
            new_doc = {**query}
            for k, v in update.get("$set", {}).items():
                new_doc[k] = v
            self.docs.append(new_doc)
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
