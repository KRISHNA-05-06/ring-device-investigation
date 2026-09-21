"""Local, read-only Ring investigation dashboard. Credentials stay in Python."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import importlib
import json
import os
from pathlib import Path
import secrets
import sys
import threading
from urllib.parse import parse_qs, urlsplit

ROOT = Path(__file__).resolve().parent
PROJECT = ROOT.parent.parent


def request_options(query):
    values = parse_qs(query, keep_blank_values=True, max_num_fields=6)
    if set(values) - {"as_of", "silence_minutes", "report_day"} or any(len(v) != 1 for v in values.values()):
        raise ValueError("Unsupported or repeated query parameters")
    silence = values.get("silence_minutes", ["10"])[0]
    if not silence.isascii() or not silence.isdigit() or not 1 <= int(silence) <= 1440:
        raise ValueError("Silence threshold must be 1..1440 minutes")
    day = values.get("report_day", [datetime.now(timezone.utc).date().isoformat()])[0]
    if len(day) != 10 or date.fromisoformat(day).isoformat() != day:
        raise ValueError("Choose a valid UTC report date")
    date.fromisoformat(day) + timedelta(days=1)
    cutoff = values.get("as_of", [""])[0]
    if cutoff:
        timestamp = datetime.fromisoformat(cutoff.replace("Z", "+00:00"))
        if timestamp.tzinfo is None:
            raise ValueError("Cutoff requires a timezone")
        cutoff = timestamp.astimezone(timezone.utc).isoformat()
    return {"as_of": cutoff or None, "silence_minutes": int(silence), "report_day": day}


def collect_snapshot(adapter, options):
    health_args = {"silence_minutes": options["silence_minutes"]}
    if options["as_of"]:
        health_args["as_of"] = options["as_of"]
    calls = {
        "health": ("get_device_health", health_args),
        "pipeline": ("get_pipeline_status", {}),
        "rejections": ("get_rejection_summary", {}),
        "daily": ("get_daily_device_metrics", {"report_day": options["report_day"], "device_limit": 100}),
    }
    panels = {}
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {name: pool.submit(adapter.invoke_query, op, args) for name, (op, args) in calls.items()}
        for name, future in futures.items():
            try:
                body = future.result()
                if (body.get("status") != "ok" or body.get("household_id") != "household-001"
                        or body.get("database_user") != "ring_reader" or body.get("operation") != calls[name][0]
                        or not isinstance(body.get("result"), dict)):
                    raise ValueError("Unexpected query response scope")
                panels[name] = {"ok": True, "generated_at": body.get("generated_at"), "result": body["result"]}
            except Exception as error:
                print(f"{name}: {type(error).__name__}: {error}", file=sys.stderr, flush=True)
                # Keep raw service errors (which may contain configuration) out of the UI.
                panels[name] = {"ok": False, "error": "Query unavailable. Check the dashboard terminal and AWS login."}
    return {"mode": "aws", "fetched_at": datetime.now(timezone.utc).isoformat(),
            "household_id": "household-001", "options": options, "panels": panels}


def demo_snapshot(options):
    """Explicitly labeled offline layout fixture; never presented as AWS evidence."""
    now = datetime.now(timezone.utc)
    cutoff = datetime.fromisoformat(options["as_of"]) if options["as_of"] else now
    devices = []
    for device, battery, age in [("camera-001", 74, .47), ("camera-002", 75, 3), ("doorbell-001", 82, .47)]:
        devices.append({"device_id": device, "device_type": "camera" if device.startswith("camera") else "doorbell",
                        "battery_percent": battery, "minutes_since_heartbeat": age,
                        "health_status": "suspected_offline" if age > options["silence_minutes"] else "reporting",
                        "last_heartbeat": (cutoff-timedelta(minutes=age)).isoformat(),
                        "event_id": "sample-event", "run_id": "sample-run"})
    results = {
        "health": {"devices": devices, "as_of": cutoff.isoformat(), "truncated": False},
        "pipeline": {"scope": "all recorded ingestion attempts", "summary": {
            "total_runs": 42, "completed_runs": 41, "failed_runs": 1, "running_runs": 0,
            "input_records": 48, "accepted_records": 39, "duplicate_records": 3, "rejected_records": 6,
            "latest_success_at": now.isoformat()}, "recent_runs": [
                {"run_id": "sample-run", "status": "completed", "source_key": "raw/kinesis/sample.jsonl",
                 "started_at": now.isoformat(), "accepted_count": 1, "duplicate_count": 0, "rejected_count": 0}]},
        "rejections": {"scope": "all recorded rejection occurrences", "errors": [
            {"error_code": "INVALID_BATTERY", "rejection_count": 2},
            {"error_code": "EVENT_ID_CONFLICT", "rejection_count": 1},
            {"error_code": "HOUSEHOLD_MISMATCH", "rejection_count": 1},
            {"error_code": "INVALID_JSON", "rejection_count": 1},
            {"error_code": "UNKNOWN_DEVICE", "rejection_count": 1}], "truncated": False},
        "daily": {"report_day": options["report_day"], "truncated": False, "devices": [
            {"device_id": d["device_id"], "accepted_heartbeat_count": 12 if i != 1 else 7,
             "min_battery_percent": d["battery_percent"], "max_battery_percent": d["battery_percent"],
             "average_battery_percent": d["battery_percent"]} for i, d in enumerate(devices)]},
    }
    return {"mode": "demo", "fetched_at": now.isoformat(), "household_id": "household-001", "options": options,
            "panels": {k: {"ok": True, "generated_at": now.isoformat(), "result": v} for k, v in results.items()}}


def handler_factory(adapter=None, demo=False):
    token = secrets.token_urlsafe(32)
    gate = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def respond(self, code, content, kind="application/json"):
            if isinstance(content, dict):
                content = json.dumps(content).encode()
            self.send_response(code)
            self.send_header("Content-Type", kind)
            self.send_header("Content-Length", str(len(content)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Referrer-Policy", "no-referrer")
            self.send_header("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'")
            self.end_headers()
            self.wfile.write(content)

        def do_GET(self):
            expected_host = f"127.0.0.1:{self.server.server_port}"
            if self.headers.get("Host") != expected_host:
                return self.respond(403, {"error": "Use the printed loopback URL"})
            url = urlsplit(self.path)
            if url.path == "/api/snapshot":
                origin = self.headers.get("Origin")
                if (not secrets.compare_digest(self.headers.get("X-Dashboard-Token", ""), token)
                        or origin not in (None, f"http://{expected_host}")):
                    return self.respond(403, {"error": "Local dashboard request required"})
                try:
                    if len(url.query) > 512:
                        raise ValueError("Query too long")
                    options = request_options(url.query)
                except (ValueError, OverflowError):
                    return self.respond(400, {"error": "Invalid date, cutoff, or silence threshold"})
                if not gate.acquire(blocking=False):
                    return self.respond(429, {"error": "A refresh is already running. Try again shortly."})
                try:
                    result = demo_snapshot(options) if demo else collect_snapshot(adapter, options)
                    return self.respond(200, result)
                finally:
                    gate.release()
            paths = {"/": ("index.html", "text/html; charset=utf-8"),
                     "/app.js": ("app.js", "text/javascript; charset=utf-8"),
                     "/style.css": ("style.css", "text/css; charset=utf-8")}
            if url.path not in paths:
                return self.respond(404, {"error": "Not found"})
            filename, kind = paths[url.path]
            content = (ROOT / filename).read_bytes()
            if url.path == "/":
                content = content.replace(b"DASHBOARD_TOKEN", token.encode())
                content = content.replace(b"DASHBOARD_MODE", b"Offline sample data" if demo else b"AWS / read only")
            self.respond(200, content, kind)

        def do_POST(self):
            self.respond(405, {"error": "This dashboard has no write operations"})

        def log_message(self, format, *args):
            pass

    return Handler


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--demo", action="store_true", help="Offline sample data for UI review only")
    parser.add_argument("--profile", default="ring-mcp")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()
    if not 1024 <= args.port <= 65535:
        parser.error("Port must be 1024..65535")
    adapter = None
    if not args.demo:
        os.environ["RING_AWS_PROFILE"] = args.profile
        sys.path.insert(0, str(PROJECT))
        adapter = importlib.import_module("aws_mcp_server")
    server = ThreadingHTTPServer(("127.0.0.1", args.port), handler_factory(adapter, args.demo))
    server.daemon_threads = True
    print(f"Dashboard: http://127.0.0.1:{args.port}/", flush=True)
    print("OFFLINE SAMPLE DATA" if args.demo else f"AWS read-only queries; profile {args.profile}", flush=True)
    print("Open the URL. Refresh sends four read requests. Ctrl+C stops this local server.", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
