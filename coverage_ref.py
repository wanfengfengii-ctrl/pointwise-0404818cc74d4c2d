"""Full-snapshot oracle only; no incremental state or transaction integration."""
from fractions import Fraction


def coverage(snapshot, left_id):
    rows = snapshot["rows"]
    left = next((r for r in rows if r["side"] == "L" and r["id"] == left_id), None)
    result = {"seq": snapshot["seq"], "covered": None, "duration": None, "ratio": None}
    if left is None or left["deleted"]:
        return result
    start, end = left["start"], left["end"]
    right = [r for r in rows if r["side"] == "R" and not r["deleted"]]
    cuts = sorted({start, end} | {max(start, min(end, r[k]))
                                for r in right for k in ("start", "end")})
    covered = sum(b - a for a, b in zip(cuts, cuts[1:])
                  if any(r["start"] <= a and b <= r["end"] for r in right))
    duration = end - start
    ratio = Fraction(covered, duration)
    result.update(covered=covered, duration=duration,
                  ratio=[ratio.numerator, ratio.denominator])
    return result
