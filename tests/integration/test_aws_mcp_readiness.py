"""Final read-only live smoke check; does not submit proposals or replay events.

Run after renewing ring-mcp credentials. Historical mutation suites remain separate.
"""
import argparse
import asyncio
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

TOOLS = {
    "get_device_health", "get_pipeline_status", "get_rejection_summary",
    "list_rejected_records", "inspect_rejected_record", "preview_record_correction",
    "propose_record_correction", "get_operation_status", "get_audit_history",
    "preview_replay", "apply_record_correction", "replay_records", "get_device_details",
    "get_event_history", "get_quality_results", "recompute_device_health", "get_daily_device_metrics",
}


async def main(profile):
    root = Path(__file__).resolve().parents[2]
    parameters = StdioServerParameters(command=sys.executable, args=[str(root / "aws_mcp_server.py")],
        cwd=str(root), env={**os.environ, "RING_AWS_PROFILE": profile})
    async with stdio_client(parameters) as (read, write):
        async with ClientSession(read, write, read_timeout_seconds=timedelta(seconds=120)) as session:
            await session.initialize()
            names = {tool.name for tool in (await session.list_tools()).tools}
            if names != TOOLS:
                raise AssertionError(f"Tool mismatch: missing={TOOLS - names}, extra={names - TOOLS}")
            print("PASS: exactly 17 MCP tools discovered")
            uris = {str(r.uri) for r in (await session.list_resources()).resources}
            if uris != {"ring://schema", "ring://runbook", "ring://definitions"}:
                raise AssertionError(f"Unexpected resources: {uris}")
            for uri in sorted(uris):
                if not (await session.read_resource(uri)).contents:
                    raise AssertionError(f"Empty resource {uri}")
            prompt = await session.get_prompt("investigate_devices", arguments={"device_id": "camera-002", "silence_minutes": "10"})
            if not prompt.messages:
                raise AssertionError("Empty investigation prompt")
            print("PASS: resources and investigation prompt accessible")
            requests = [
                ("get_device_health", {"silence_minutes": 10}),
                ("get_pipeline_status", {}),
                ("get_daily_device_metrics", {"report_day": "2026-09-18", "device_limit": 100}),
            ]
            for name, args in requests:
                response = await session.call_tool(name, args)
                if response.isError:
                    raise AssertionError(f"{name}: {response.content}")
                body = json.loads(next(block.text for block in response.content if block.type == "text"))
                if (body.get("status") != "ok" or body.get("operation") != name or
                    body.get("household_id") != "household-001" or body.get("database_user") != "ring_reader"):
                    raise AssertionError(f"Unexpected scope or response for {name}")
                print(json.dumps({"check": name, "status": "PASS", "generated_at": body.get("generated_at"),
                                  "result": body["result"]}))
            print(json.dumps({"status": "PASS", "checked_at": datetime.now(timezone.utc).isoformat(),
                              "scope": "discovery, resources, prompt and three read-only AWS operations",
                              "recovery_tested": False, "writes_requested": False}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", default="ring-mcp")
    asyncio.run(main(parser.parse_args().profile))
