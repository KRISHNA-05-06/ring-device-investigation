import asyncio
import hashlib
import json
import os
import sys
from datetime import timedelta
from pathlib import Path

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


PROJECT_DIR = Path(__file__).resolve().parents[2]
REASON = "Synthetic demo: explicitly choose 75 as the replacement value."


async def call(session, name, arguments):
    result = await session.call_tool(name, arguments)
    if result.isError:
        raise AssertionError(f"{name} failed: {result.content}")
    body = json.loads(next(block.text for block in result.content if block.type == "text"))
    if (
        body.get("status") != "ok" or body.get("operation") != name
        or body.get("backend") != "aws" or body.get("household_id") != "household-001"
    ):
        raise AssertionError(f"Unexpected response: {body}")
    return body["result"]


async def expect_error(session, arguments, code):
    result = await session.call_tool("propose_record_correction", arguments)
    text = " ".join(block.text for block in result.content if block.type == "text")
    if not result.isError or code not in text:
        raise AssertionError(f"Expected {code}, received: {text}")
    print(f"PASS: {code}")


async def main():
    parameters = StdioServerParameters(
        command=sys.executable,
        args=[str(PROJECT_DIR / "aws_mcp_server.py")],
        env={**os.environ, "RING_AWS_PROFILE": "ring-mcp"},
    )
    async with stdio_client(parameters) as (read, write):
        async with ClientSession(read, write, read_timeout_seconds=timedelta(seconds=120)) as session:
            await session.initialize()
            tools = await session.list_tools()
            tool = next(t for t in tools.tools if t.name == "propose_record_correction")
            if tool.annotations is None or tool.annotations.readOnlyHint is not False:
                raise AssertionError("Proposal tool must be marked as a write")
            print("PASS: proposal tool is marked as a write")

            before = await call(session, "inspect_rejected_record", {"rejection_id": 6})
            preview_arguments = {"rejection_id": 6, "battery_percent": 75, "reason": REASON}
            preview = await call(session, "preview_record_correction", preview_arguments)
            if preview["preview_status"] != "ready":
                raise AssertionError("A ready preview is required")
            arguments = {
                **preview_arguments,
                "expected_preview_sha256": preview["preview_sha256"],
                "idempotency_key": "mcp-proposal-test-v1",
            }

            print("Saving one proposal in S3, or reusing the proposal from an earlier run.")
            first = await call(session, "propose_record_correction", arguments)
            second = await call(session, "propose_record_correction", arguments)
            for result in (first, second):
                proposal = result["proposal"]
                canonical = json.dumps(
                    proposal, sort_keys=True, separators=(",", ":"),
                    ensure_ascii=True, allow_nan=False,
                ).encode("utf-8")
                if (
                    result["persisted"] is not True
                    or result["applied"] is not False or result["replayed"] is not False
                    or result["proposal_status"] not in {"pending_approval", "expired"}
                    or proposal["approval_status"] != "pending"
                    or proposal["preview"] != preview["preview"]
                    or proposal["preview_sha256"] != preview["preview_sha256"]
                    or result["proposal_sha256"] != hashlib.sha256(canonical).hexdigest()
                ):
                    raise AssertionError("Saved proposal differs from the reviewed preview")
            if (
                second["created"] is not False
                or first["proposal"] != second["proposal"]
                or first["storage"] != second["storage"]
            ):
                raise AssertionError("A repeated request changed the saved proposal")
            print("PASS: saved proposal matches preview and repeated save is unchanged")
            print(json.dumps({
                "proposal_id": second["proposal"]["proposal_id"],
                "proposal_status": second["proposal_status"],
                "expires_at": second["proposal"]["expires_at"],
                "storage": second["storage"],
            }, indent=2))

            await expect_error(session, {
                **arguments, "expected_preview_sha256": "0" * 64,
            }, "PREVIEW_CHANGED")

            changed_arguments = {**preview_arguments, "reason": "Different synthetic demo reason."}
            changed_preview = await call(session, "preview_record_correction", changed_arguments)
            await expect_error(session, {
                **arguments, **changed_arguments,
                "expected_preview_sha256": changed_preview["preview_sha256"],
            }, "IDEMPOTENCY_CONFLICT")

            after_failures = await call(session, "propose_record_correction", arguments)
            if after_failures["proposal"] != first["proposal"]:
                raise AssertionError("Failed requests changed the saved proposal")
            after = await call(session, "inspect_rejected_record", {"rejection_id": 6})
            if after != before:
                raise AssertionError("The original rejection changed")
            print("PASS: proposal and original rejection preserved after failed requests")
            print("AWS MCP proposal test passed")


if __name__ == "__main__":
    asyncio.run(main())
