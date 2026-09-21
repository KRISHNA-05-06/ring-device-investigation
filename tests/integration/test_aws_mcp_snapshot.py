"""Opt-in live test: intentionally creates one health report, then checks retry/conflict."""
import argparse
import asyncio
import json
import os
import sys
from datetime import timedelta
from pathlib import Path
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


async def main(key):
    parameters = StdioServerParameters(command=sys.executable,
        args=[str(Path(__file__).resolve().parents[2] / "aws_mcp_server.py")],
        env={**os.environ, "RING_AWS_PROFILE": "ring-mcp"})
    arguments = {"as_of": "2026-09-18T12:05:00Z", "silence_minutes": 10, "idempotency_key": key}
    async with stdio_client(parameters) as (read, write):
        async with ClientSession(read, write, read_timeout_seconds=timedelta(seconds=120)) as session:
            await session.initialize()
            receipts = []
            for _ in range(2):
                response = await session.call_tool("recompute_device_health", arguments)
                if response.isError:
                    raise AssertionError(str(response.content))
                body = json.loads(next(c.text for c in response.content if c.type == "text"))
                assert body["status"] == "ok" and body["operation"] == "recompute_device_health"
                receipts.append(body["result"])
            first, second = receipts
            assert first["snapshot_sha256"] == second["snapshot_sha256"]
            assert first["snapshot"] == second["snapshot"] and first["storage"] == second["storage"]
            assert second["created"] is False and second["recomputed_this_request"] is False
            assert first["database_modified"] is False and first["replayed"] is False
            conflict = await session.call_tool("recompute_device_health", {**arguments, "silence_minutes": 11})
            assert conflict.isError and "IDEMPOTENCY_CONFLICT" in str(conflict.content)
            print("PASS: snapshot persisted; identical retry returned the original report; changed inputs were blocked.")
            print(json.dumps({"first_call_created": first["created"], "snapshot_sha256": first["snapshot_sha256"],
                              "snapshot_id": first["snapshot"]["snapshot_id"], "summary": first["snapshot"]["summary"],
                              "storage": first["storage"]}, indent=2))
            if not first["created"]:
                print("NOT EXERCISED: first-time creation in this run; this key already had a matching report.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--save", action="store_true", required=True, help="Intentionally save an S3 report")
    parser.add_argument("--idempotency-key", required=True, help="Keep the same key when retrying this test")
    options = parser.parse_args()
    asyncio.run(main(options.idempotency_key))
