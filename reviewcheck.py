"""Checks reproducible observations and line references, not proposal correctness."""
import contextlib
import io
import json
import re
import sys
from pathlib import Path
from coverage_observe import check, replay


def verify():
    with contextlib.redirect_stdout(io.StringIO()):
        check()
    text = Path("DESIGN.md").read_text(encoding="utf-8")
    blocks = re.findall(r"```json coverage-evidence\n(.*?)\n```", text, re.S)
    assert len(blocks) == 1, "one coverage-evidence JSON block required"
    actual = replay()
    assert json.loads(blocks[0]) == actual, "report observation differs from actual calls"
    refs = re.findall(r"((?:intervaljoin/(?:model|worker|cluster)|coverage_ref|coverage_observe)\.py):([0-9]+)", text)
    assert refs, "source line references required"
    for path, line in refs:
        assert 1 <= int(line) <= len(Path(path).read_text(encoding="utf-8").splitlines()), (path, line)
    print(json.dumps({"report_evidence": "REPORT_REPLAY_OK",
                      "scenario_keys": [s["key"] for s in actual["scenarios"]]}, sort_keys=True))


if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in ("replay", "verify"):
        raise SystemExit("usage: python3 reviewcheck.py replay|verify")
    if sys.argv[1] == "replay":
        print(json.dumps(replay(), ensure_ascii=False, sort_keys=True))
    else:
        verify()
