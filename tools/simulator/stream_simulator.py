"""Bounded synthetic Kinesis producer. Preview by default; --send writes data."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import random
import time
import uuid

ACCOUNT = "455646290306"
REGION = "us-east-1"
STREAM = "ring-events"
HOUSEHOLD = "household-001"
DEVICES = (("camera-001", 74), ("camera-002", 75), ("doorbell-001", 82))


def make_events(session_id, tick, now, scenario="normal"):
    """Keep event identity stable during retries; create new identity per tick."""
    events = []
    for device, battery in DEVICES:
        if scenario == "silence" and tick > 0 and device == "camera-002":
            continue
        event = {
            "event_id": f"sim-{session_id}-{tick:04d}-{device}",
            "household_id": HOUSEHOLD,
            "device_id": device,
            "event_time": now.astimezone(timezone.utc).isoformat(),
            "battery_percent": 150 if scenario == "invalid-battery" and tick == 0
            and device == "camera-002" else battery,
        }
        events.append(event)
    if scenario == "duplicate" and tick == 0:
        events.append(dict(events[0]))
    return events


def wire_record(event):
    return {
        "PartitionKey": f"{event['household_id']}#{event['device_id']}",
        "Data": json.dumps(event, sort_keys=True, separators=(",", ":")).encode(),
    }


def send_batch(client, events, emit, sleep=time.sleep):
    """Retry only explicitly failed entries; stop on uncertain whole-call errors."""
    pending = list(events)
    for attempt in range(1, 6):
        try:
            response = client.put_records(
                StreamName=STREAM, Records=[wire_record(e) for e in pending]
            )
        except Exception as error:
            details = getattr(error, "response", {})
            aws_error = details.get("Error", {}) if isinstance(details, dict) else {}
            if aws_error.get("Code") in {"AccessDeniedException", "AccessDenied", "KMSAccessDeniedException"}:
                emit({"status": "send_denied", "error_code": aws_error["Code"],
                      "message": aws_error.get("Message", "AWS denied this request"),
                      "event_ids": [e["event_id"] for e in pending]})
                raise RuntimeError("AWS denied this batch; check the recorded permission error before retrying") from error
            # A lost response can hide successful writes. Preserve evidence and stop.
            emit({"status": "delivery_unconfirmed", "error_type": type(error).__name__,
                  "event_ids": [e["event_id"] for e in pending]})
            raise RuntimeError("Delivery uncertain; inspect the session log before resending") from error
        results = response.get("Records")
        if not isinstance(results, list) or len(results) != len(pending):
            emit({"status": "delivery_unconfirmed", "error_type": "InvalidResponse",
                  "event_ids": [e["event_id"] for e in pending]})
            raise RuntimeError("Unexpected Kinesis response; inspect the session log")
        failed = []
        for event, result in zip(pending, results):
            if result.get("ErrorCode"):
                failed.append(event)
                emit({"status": "send_failed", "event_id": event["event_id"],
                      "attempt": attempt, "error_code": result["ErrorCode"]})
            elif result.get("SequenceNumber") and result.get("ShardId"):
                emit({"status": "sent_to_kinesis", "event_id": event["event_id"],
                      "sequence_number": result["SequenceNumber"], "shard_id": result["ShardId"]})
            else:
                emit({"status": "delivery_unconfirmed", "error_type": "InvalidEntry",
                      "event_ids": [e["event_id"] for e in pending]})
                raise RuntimeError("Incomplete Kinesis result; inspect the session log")
        if not failed:
            return
        pending = failed
        if attempt < 5:
            sleep(random.uniform(0, min(4, 0.5 * 2 ** (attempt - 1))))
    raise RuntimeError(f"Five attempts exhausted for {len(pending)} record(s); see log")


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--send", action="store_true", help="Actually publish to Kinesis")
    p.add_argument("--profile", default="ring-dev")
    p.add_argument("--duration-seconds", type=int, default=180)
    p.add_argument("--interval-seconds", type=int, default=30)
    p.add_argument("--scenario", choices=("normal", "duplicate", "invalid-battery", "silence"), default="normal")
    return p


def main():
    p = parser()
    args = p.parse_args()
    if not 10 <= args.interval_seconds <= 300:
        p.error("interval-seconds must be 10..300")
    if not args.interval_seconds <= args.duration_seconds <= 900:
        p.error("duration-seconds must be at least the interval and at most 900")
    session_id = uuid.uuid4().hex
    first = make_events(session_id, 0, datetime.now(timezone.utc), args.scenario)
    planned_ticks = (args.duration_seconds + args.interval_seconds - 1) // args.interval_seconds
    planned_records = sum(len(make_events(session_id, i, datetime.now(timezone.utc), args.scenario))
                          for i in range(planned_ticks))
    print(json.dumps({"mode": "send" if args.send else "preview", "session_id": session_id,
                      "stream": STREAM, "region": REGION, "profile": args.profile,
                      "scenario": args.scenario, "duration_seconds": args.duration_seconds,
                      "interval_seconds": args.interval_seconds, "maximum_planned_records": planned_records,
                      "first_batch": first}), flush=True)
    if not args.send:
        print("Preview only. No AWS calls, files, or background process created.")
        return 0

    # Imports and AWS credential lookup occur only on explicit send.
    import boto3
    from botocore.config import Config
    session = boto3.Session(profile_name=args.profile, region_name=REGION)
    config = Config(connect_timeout=3, read_timeout=5,
                    retries={"mode": "standard", "total_max_attempts": 1})
    sts = session.client("sts", config=config)
    try:
        identity = sts.get_caller_identity()
    finally:
        sts.close()
    if identity["Account"] != ACCOUNT:
        raise RuntimeError("Wrong AWS account; nothing was sent")
    print(json.dumps({"status": "identity_checked", "arn": identity["Arn"]}), flush=True)
    log_dir = Path(__file__).resolve().parent / "sessions"
    log_dir.mkdir(exist_ok=True)
    log_path = log_dir / f"{session_id}.jsonl"
    client = session.client("kinesis", config=config)
    tick = 0
    try:
        with log_path.open("x", encoding="utf-8") as log:
            def emit(item):
                item = {"logged_at": datetime.now(timezone.utc).isoformat(), **item}
                line = json.dumps(item)
                log.write(line + "\n")
                log.flush()
                print(line, flush=True)

            emit({"status": "session_started", "session_id": session_id,
                  "scenario": args.scenario, "actor_arn": identity["Arn"],
                  "duration_seconds": args.duration_seconds, "interval_seconds": args.interval_seconds})
            started = time.monotonic()
            deadline = started + args.duration_seconds
            try:
                while time.monotonic() < deadline and tick < planned_ticks:
                    events = make_events(session_id, tick, datetime.now(timezone.utc), args.scenario)
                    emit({"status": "batch_prepared", "tick": tick, "events": events})
                    send_batch(client, events, emit)
                    tick += 1
                    # No catch-up bursts after slow calls. Deadline stops new batches.
                    delay = min(args.interval_seconds, max(0, deadline - time.monotonic()))
                    if delay:
                        time.sleep(delay)
                emit({"status": "generation_finished", "completed_batches": tick,
                      "ingestion_verified": False})
            except KeyboardInterrupt:
                emit({"status": "stopped_by_user", "completed_batches": tick,
                      "message": "An interrupted request may have succeeded; inspect prepared events and receipts."})
                return 130
            except Exception as error:
                emit({"status": "session_failed", "error_type": type(error).__name__,
                      "message": str(error), "completed_batches": tick})
                return 1
    finally:
        client.close()
        print(f"Session evidence: {log_path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
