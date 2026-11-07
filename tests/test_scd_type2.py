"""Tests for customer Type 2 slowly changing dimension behavior."""

from contextlib import contextmanager
from datetime import date

import pandas as pd
import pytest

from src.warehouse.scd_type2 import (
    CustomerDimensionError,
    apply_customer_changes,
)


class _Mappings:
    def __init__(self, value):
        self.value = value

    def one_or_none(self):
        return self.value


class _Result:
    def __init__(self, value=None, rowcount: int = 1) -> None:
        self.value = value
        self.rowcount = rowcount

    def mappings(self):
        return _Mappings(self.value)


class _Connection:
    def __init__(self) -> None:
        self.versions: list[dict[str, object]] = []
        self.next_key = 1
        self.fail_on_insert = False
        self.watermarks: dict[str, object] = {}

    def execute(self, statement, parameters):
        sql = str(statement).lower()
        if "select customer_key" in sql:
            current = next(
                (
                    version
                    for version in reversed(self.versions)
                    if version["customer_id"] == parameters["customer_id"]
                    and version["is_current"]
                ),
                None,
            )
            return _Result(current.copy() if current else None)

        if "update dim_customer" in sql:
            if "source_updated_at" in sql:
                current = next(
                    (
                        version
                        for version in self.versions
                        if version["customer_key"] == parameters["customer_key"]
                        and version["is_current"]
                    ),
                    None,
                )
                if current is None:
                    return _Result(rowcount=0)
                current["source_updated_at"] = parameters["source_updated_at"]
                return _Result(rowcount=1)
            current = next(
                (
                    version
                    for version in self.versions
                    if version["customer_key"] == parameters["customer_key"]
                    and version["is_current"]
                ),
                None,
            )
            if current is None:
                return _Result(rowcount=0)
            current["effective_end_date"] = parameters["effective_end_date"]
            current["is_current"] = False
            return _Result(rowcount=1)

        if "insert into dim_customer" in sql:
            if self.fail_on_insert:
                raise RuntimeError("simulated insert error")
            version = dict(parameters)
            version.update(
                {
                    "customer_key": self.next_key,
                    "effective_end_date": None,
                    "is_current": True,
                }
            )
            self.next_key += 1
            self.versions.append(version)
            return _Result()

        if "insert into pipeline_watermark" in sql:
            self.watermarks[parameters["pipeline_name"]] = parameters["watermark_at"]
            return _Result()

        raise AssertionError(f"Unexpected SQL: {statement}")


class _Engine:
    def __init__(self) -> None:
        self.connection = _Connection()

    @contextmanager
    def begin(self):
        yield self.connection


def _customer(city: str = "Hyderabad", updated_at: str = "2025-01-02T10:00:00Z"):
    return pd.DataFrame(
        [
            {
                "customer_id": "C001",
                "customer_name": "Aarav Sharma",
                "email": "aarav@example.com",
                "city": city,
                "state": "Telangana" if city == "Hyderabad" else "Karnataka",
                "updated_at": updated_at,
            }
        ]
    )


def test_scd2_inserts_first_customer_version() -> None:
    engine = _Engine()

    result = apply_customer_changes(_customer(), engine=engine)

    assert result.inserted == 1
    assert result.changed == result.unchanged == result.stale == 0
    assert len(engine.connection.versions) == 1
    assert engine.connection.versions[0]["effective_start_date"] == date(2025, 1, 2)
    assert engine.connection.versions[0]["is_current"] is True
    assert engine.connection.versions[0]["source_updated_at"].isoformat() == (
        "2025-01-02T10:00:00+00:00"
    )
    assert engine.connection.watermarks["dim_customer_scd2"] == result.watermark_at


def test_scd2_expires_old_version_and_inserts_new_city() -> None:
    engine = _Engine()
    apply_customer_changes(_customer(), engine=engine)

    result = apply_customer_changes(
        _customer("Bengaluru", "2025-02-10T09:00:00Z"), engine=engine
    )

    old_version, current_version = engine.connection.versions
    assert result.changed == 1
    assert old_version["city"] == "Hyderabad"
    assert old_version["effective_end_date"] == date(2025, 2, 10)
    assert old_version["is_current"] is False
    assert current_version["city"] == "Bengaluru"
    assert current_version["effective_start_date"] == date(2025, 2, 10)
    assert current_version["is_current"] is True


def test_scd2_supports_same_day_versions_with_exclusive_end_date() -> None:
    engine = _Engine()
    apply_customer_changes(_customer(), engine=engine)

    result = apply_customer_changes(
        _customer("Bengaluru", "2025-01-02T18:00:00Z"), engine=engine
    )

    old_version, current_version = engine.connection.versions
    assert result.changed == 1
    assert old_version["effective_end_date"] == old_version["effective_start_date"]
    assert current_version["effective_start_date"] == date(2025, 1, 2)


def test_scd2_rejects_stale_change_from_same_calendar_day() -> None:
    engine = _Engine()
    apply_customer_changes(_customer("Bengaluru", "2025-01-02T18:00:00Z"), engine=engine)

    result = apply_customer_changes(
        _customer("Pune", "2025-01-02T12:00:00Z"), engine=engine
    )

    assert result.stale == 1
    assert len(engine.connection.versions) == 1
    assert engine.connection.versions[0]["city"] == "Bengaluru"


def test_scd2_skips_unchanged_and_stale_customer_rows() -> None:
    engine = _Engine()
    apply_customer_changes(_customer("Bengaluru", "2025-02-10T09:00:00Z"), engine=engine)

    unchanged = apply_customer_changes(
        _customer("Bengaluru", "2025-02-11T09:00:00Z"), engine=engine
    )
    stale = apply_customer_changes(_customer("Pune", "2025-01-15T09:00:00Z"), engine=engine)

    assert unchanged.unchanged == 1
    assert stale.stale == 1
    assert len(engine.connection.versions) == 1
    assert engine.connection.versions[0]["city"] == "Bengaluru"
    assert engine.connection.versions[0]["source_updated_at"].isoformat() == (
        "2025-02-11T09:00:00+00:00"
    )


def test_scd2_rejects_missing_customer_fields() -> None:
    with pytest.raises(ValueError, match="missing columns"):
        apply_customer_changes(pd.DataFrame([{"customer_id": "C001"}]), engine=_Engine())


def test_scd2_returns_empty_result_without_database_call() -> None:
    result = apply_customer_changes(pd.DataFrame(), engine=_Engine())

    assert result.inserted == result.changed == result.unchanged == result.stale == 0