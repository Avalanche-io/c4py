"""Fail when a test skipped that the allow-list does not name.

Reads pytest's JUnit XML (``python -m pytest --junitxml=FILE``), prints
every skipped test with the reason it gave, and exits 1 when one is not
named in the allow-list for this platform, so a test that runs nowhere
cannot go unnoticed. An expected failure (xfail) counts as a skip: the
test did not pass.

    python -m pytest --junitxml=junit.xml
    python scripts/check_skips.py junit.xml --allow tests/allowed-skips.txt

The allow-list holds one skip per line: the platform it is allowed on
(``sys.platform``: linux, darwin, win32; or ``*`` for any) and the test id
(``<classname>::<name>`` as the XML gives it), with the reason after
``#``. A listed skip that did not happen is reported, not failed on, since
a skip depends on what the machine has. ``--report-only`` prints the same
summary and always exits 0 unless the XML cannot be read.

Standard library only.
"""

from __future__ import annotations

import argparse
import sys
import xml.etree.ElementTree as ET


def read_allow(path: str, platform: str) -> set[str]:
    """Return the test ids the list allows to skip on platform."""
    allowed: set[str] = set()
    with open(path, encoding="utf-8") as f:
        for n, raw in enumerate(f, 1):
            fields = raw.split("#", 1)[0].split()
            if not fields:
                continue
            if len(fields) != 2:
                raise ValueError(
                    f"{path}:{n}: want a platform and a test id, have {raw.rstrip()!r}"
                )
            if fields[0] in ("*", platform):
                allowed.add(fields[1])
    return allowed


def read_skips(path: str) -> tuple[int, list[tuple[str, str]]]:
    """Return the number of test cases and every (test id, reason) that skipped."""
    root = ET.parse(path).getroot()
    cases = 0
    skips: list[tuple[str, str]] = []
    for case in root.iter("testcase"):
        cases += 1
        node = case.find("skipped")
        if node is None:
            continue
        test = f"{case.get('classname', '')}::{case.get('name', '')}"
        reason = node.get("message") or (node.text or "").strip() or "(no reason given)"
        skips.append((test, " ".join(reason.split())))
    return cases, sorted(skips)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("junit", help="pytest's --junitxml output")
    p.add_argument("--allow", default="tests/allowed-skips.txt", help="the allow-list")
    p.add_argument("--platform", default=sys.platform, help="the platform the tests ran on")
    p.add_argument("--report-only", action="store_true", help="list skips, never fail on them")
    a = p.parse_args(argv)

    try:
        allowed = read_allow(a.allow, a.platform)
        cases, skips = read_skips(a.junit)
    except (OSError, ValueError, ET.ParseError) as e:
        print(f"check_skips: {e}")
        return 2
    if cases == 0:
        print(f"check_skips: {a.junit} holds no test cases")
        return 1

    unlisted = [(t, r) for t, r in skips if t not in allowed]
    print(f"{cases} tests on {a.platform}, {len(skips)} skipped")
    for t, r in skips:
        mark = "allowed " if t in allowed else "UNLISTED"
        print(f"  {mark} {t}  # {r}")
    for t in sorted(allowed - {t for t, _ in skips}):
        print(f"  listed, did not skip: {t}")
    if not unlisted:
        return 0
    verdict = "report only" if a.report_only else "failing"
    print(f"check_skips: {len(unlisted)} skip(s) not in {a.allow} for {a.platform} ({verdict})")
    return 0 if a.report_only else 1


if __name__ == "__main__":
    sys.exit(main())
