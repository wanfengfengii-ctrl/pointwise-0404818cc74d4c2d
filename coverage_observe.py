"""Real join calls plus a full-snapshot coverage oracle, never a proposed maintainer."""
import contextlib
import io
import json
import sys
from pathlib import Path
from checks import run
from intervaljoin import Cluster
from coverage_ref import coverage


def replay():
    data = json.loads(Path(__file__).with_name("coverage_samples.json").read_text(encoding="utf-8"))
    c = Cluster([":memory:", ":memory:"])
    scenarios = []
    try:
        for scenario in data["scenarios"]:
            key = scenario["key"]
            worker = c.workers[c.owner(key)]
            steps = []
            initial = coverage(worker.export_key(key), "missing")
            assert initial == {"seq": 0, "covered": None, "duration": None, "ratio": None}
            for tx in scenario["transactions"]:
                args = {k: v for k, v in tx.items() if k != "expected"}
                response = c.apply(key, **args)
                snapshot = worker.export_key(key)
                values = {ident: coverage(snapshot, ident) for ident in data["probes"]}
                for ident, expected in zip(data["probes"], tx["expected"]):
                    got = values[ident]
                    assert [got["covered"], got["duration"], got["ratio"]] == expected, (key, tx, got)
                    assert got["seq"] == tx["seq"]
                if key == "denominator" and tx["seq"] == 2:
                    assert response["remove"] == response["add"] == []
                steps.append({"response": response, "join": c.read(key), "coverage_reference": values})
            before = worker.export_key(key)
            first = {k: v for k, v in scenario["transactions"][0].items() if k != "expected"}
            repeated = c.apply(key, **first)
            assert repeated == steps[0]["response"]
            assert worker.export_key(key) == before
            failures = []
            seq = before["seq"] + 1
            # An accepted source change makes rollback observable, rather than testing an empty batch.
            change = {"side": "R", "id": "fault", "rev": 1, "start": 100,
                      "end": 104, "value": {}, "deleted": False}
            for point in ("after_state", "after_receipt"):
                try:
                    c.apply(key, seq, "failed", [change], fail_at=point)
                except RuntimeError as exc:
                    assert str(exc) == "INJECTED"
                else:
                    raise AssertionError("fault did not fire")
                assert worker.export_key(key) == before
                values = {ident: coverage(worker.export_key(key), ident) for ident in data["probes"]}
                assert values == steps[-1]["coverage_reference"]
                failures.append({"point": point, "error": "INJECTED", "coverage_reference": values})
            scenarios.append({"key": key, "steps": steps, "repeated_first": repeated,
                              "head_after_replay": c.read(key)["seq"], "failures": failures})
        return {"scenarios": scenarios}
    finally:
        c.close()


def check():
    with contextlib.redirect_stdout(io.StringIO()):
        run("normal")
    actual = replay()
    print(json.dumps({"baseline": "COVERAGE_BASELINE_OK",
                      "scenario_keys": [s["key"] for s in actual["scenarios"]]}, sort_keys=True))


if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in ("replay", "check"):
        raise SystemExit("usage: python3 coverage_observe.py replay|check")
    if sys.argv[1] == "check":
        check()
    else:
        print(json.dumps(replay(), ensure_ascii=False, sort_keys=True))
