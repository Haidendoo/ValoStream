"""Daily volume reports shared by the batch monitor and API metrics reader."""

from datetime import date, datetime, time, timedelta, timezone
import json
from pathlib import Path

STREAMS = ("interactions", "couriers")


def assess_volume(counts, target_day):
    """Require seven observed calendar days; a missing target day means zero."""
    days = [target_day - timedelta(days=offset) for offset in range(1, 8)]
    allowed = {day.isoformat() for day in [target_day, *days]}
    if not isinstance(counts, dict) or any(
        day not in allowed or type(value) is not int or value < 0
        for day, value in counts.items()
    ):
        raise ValueError("Invalid daily counts")
    missing = sorted(day.isoformat() for day in days if day.isoformat() not in counts)
    current = counts.get(target_day.isoformat(), 0)
    if missing:
        return {"status": "insufficient_baseline", "current_count": current, "missing_days": missing}
    total = sum(counts[day.isoformat()] for day in days)
    # Integer arithmetic preserves the strict >50% boundary, including large counts.
    anomaly = abs(current * 7 - total) * 2 > total
    return {"status": "anomaly" if anomaly else "ok", "current_count": current,
            "baseline_mean": total / 7,
            "relative_change": (current * 7 - total) / total if total else None}


def summarize_report(report, now=None):
    now = now or datetime.now(timezone.utc)
    if report["version"] != 1 or report["source_ref"] != "main":
        raise ValueError("Unsupported statistical report")
    target = date.fromisoformat(report["target_day"])
    end = datetime.combine(target + timedelta(days=1), time(), timezone.utc)
    checked = datetime.fromisoformat(report["checked_at"])
    if checked.tzinfo is None or checked < end or checked > now + timedelta(minutes=5):
        raise ValueError("Invalid report timestamps")
    if report["state"] == "error":
        return {"status": "error", "streams": {}, "window_end": end.timestamp()}
    if report["state"] != "complete" or not report.get("source_hash"):
        raise ValueError("Incomplete statistical report")
    if set(report["counts"]) != set(STREAMS):
        raise ValueError("Missing statistical streams")
    streams = {name: assess_volume(report["counts"][name], target) for name in STREAMS}
    statuses = {result["status"] for result in streams.values()}
    status = ("anomaly" if "anomaly" in statuses else
              "insufficient_baseline" if "insufficient_baseline" in statuses else "ok")
    return {"status": status, "streams": streams, "window_end": end.timestamp()}


def read_report(path):
    return summarize_report(json.loads(Path(path).read_text()))
