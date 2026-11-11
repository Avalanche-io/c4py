"""A zone abbreviation in a pretty timestamp names an offset only if this
process knows it: UTC/GMT/Z, or the local zone's own. Any other abbreviation
is refused rather than read as offset zero (which would be hours wrong while
claiming success). Mirrors c4 core #80 and libc4's ParseTimestamp.
"""

import time
from datetime import datetime, timezone

import pytest

from c4py.decoder import _parse_timestamp


def _line_time(zone: str) -> str:
    return f"Jan  2 03:04:05 2026 {zone}"


def test_utc_names_parse(monkeypatch: pytest.MonkeyPatch) -> None:
    # In a zone other than UTC: through 1.0.24 a time marked UTC was read
    # as the reader's local time (hours wrong, no error).
    monkeypatch.setenv("TZ", "America/Los_Angeles")
    time.tzset()
    try:
        want = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
        for zone in ("UTC", "GMT", "Z"):
            assert _parse_timestamp(_line_time(zone)) == want
    finally:
        monkeypatch.undo()
        time.tzset()


def test_numeric_offset_parses() -> None:
    assert _parse_timestamp("Jan  2 03:04:05 2026 -0600") == datetime(
        2026, 1, 2, 9, 4, 5, tzinfo=timezone.utc
    )


@pytest.mark.parametrize("zone", ["XYZ", "PST", "CST", "EDT"])
def test_unknown_abbreviation_is_refused(zone: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TZ", "UTC")
    time.tzset()
    with pytest.raises(ValueError, match="unknown time zone abbreviation"):
        _parse_timestamp(_line_time(zone))


def test_local_abbreviation_parses_in_local_time(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TZ", "America/Chicago")
    time.tzset()
    try:
        assert _parse_timestamp(_line_time("CST")) == datetime(
            2026, 1, 2, 9, 4, 5, tzinfo=timezone.utc
        )
    finally:
        monkeypatch.undo()
        time.tzset()
