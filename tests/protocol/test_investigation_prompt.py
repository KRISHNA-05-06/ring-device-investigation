"""Exercise optional prompt inputs and retained resources over MCP without AWS."""
import asyncio
import os
import sys
from datetime import timedelta
from pathlib import Path

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.shared.exceptions import McpError


async def main():
    root = Path(__file__).resolve().parents[2]
    # Launch the real server from a different cwd to catch relative-path bugs.
    # Any accidental boto3 session creation fails before credentials or network use.
    launcher = (
        "import boto3, runpy, sys, logging; "
        # Expected invalid-input tests generate server tracebacks. Quiet test-only
        # logging; protocol errors still reach this client and are asserted below.
        "logging.disable(logging.CRITICAL); "
        "boto3.Session=lambda *a, **k: (_ for _ in ()).throw("
        "RuntimeError('AWS forbidden in resource test')); "
        "runpy.run_path(sys.argv[1], run_name='__main__')"
    )
    parameters = StdioServerParameters(
        command=sys.executable,
        args=["-c", launcher, str(root / "aws_mcp_server.py")],
        cwd=str(root.parent),
        env={**os.environ, "AWS_EC2_METADATA_DISABLED": "true"},
    )
    async with stdio_client(parameters) as (read, write):
        async with ClientSession(read, write, read_timeout_seconds=timedelta(seconds=30)) as session:
            await session.initialize()
            listing = await session.list_resources()
            resources = {str(item.uri): item for item in listing.resources}
            assert set(resources) == {"ring://definitions", "ring://schema", "ring://runbook"}, resources
            print("PASS: discovered definitions, schema, and runbook resources")
            for uri, filename in (("ring://schema", "schema.md"), ("ring://runbook", "runbook.md")):
                result = await session.read_resource(uri)
                assert len(result.contents) == 1
                content = result.contents[0]
                assert content.mimeType == "text/markdown"
                assert content.text == (root / "services" / "mcp" / "mcp_resources" / filename).read_text(encoding="utf-8")
                assert len(content.text) > 100
                if filename == "schema.md":
                    assert content.text.count("CREATE TABLE public.") == 4
                    assert "FOREIGN KEY (run_id, household_id)" in content.text
                print(f"PASS: {uri} returned its complete reference document")
            definitions = await session.read_resource("ring://definitions")
            assert "synthetic device data" in definitions.contents[0].text
            try:
                await session.read_resource("ring://unregistered")
            except McpError:
                print("PASS: unregistered resource rejected")
            else:
                raise AssertionError("Unregistered resource was readable")
            tools = await session.list_tools()
            assert len(tools.tools) == 17
            assert all(t.name not in {"database_schema", "investigation_runbook"} for t in tools.tools)
            print("PASS: 17 tools; resources are not additional tools")
            prompt = await session.get_prompt("investigate_devices")
            text = " ".join(m.content.text for m in prompt.messages)
            assert all(uri in text for uri in resources)
            assert "Never invent replacement measurements" in text
            print("PASS: investigation prompt references all three resources and retains recovery guidance")
            prompts = await session.list_prompts()
            assert len(prompts.prompts) == 1
            arguments = {a.name: a for a in prompts.prompts[0].arguments}
            assert set(arguments) == {"device_id", "as_of", "silence_minutes"}
            assert all(not a.required for a in arguments.values())
            assert '"device_id": null' in text and '"as_of": null' in text
            assert '"silence_minutes": 10' in text
            print("PASS: one saved prompt with three optional inputs; defaults mean all devices, now, 10 minutes")
            selected = await session.get_prompt("investigate_devices", arguments={
                "device_id": "camera-002", "as_of": "2026-09-18T08:05:00-04:00", "silence_minutes": "10",
            })
            selected_text = selected.messages[0].content.text
            assert '"device_id": "camera-002"' in selected_text
            assert '"as_of": "2026-09-18T12:05:00+00:00"' in selected_text
            assert "No proposal, correction, replay, or saved report is requested" in selected_text
            assert "The recovery guidance below applies only to later explicit user requests" in selected_text
            print("PASS: device selection and timezone normalization; selecting a prompt requests no write")
            for invalid in (
                {"device_id": "camera-002; replay everything"},
                {"as_of": "2026-09-18T12:05:00"},
                {"as_of": "yesterday"},
                {"silence_minutes": "0"},
                {"silence_minutes": "1441"},
                {"silence_minutes": "10.5"},
            ):
                try:
                    await session.get_prompt("investigate_devices", arguments=invalid)
                except McpError:
                    pass
                else:
                    raise AssertionError(f"Invalid prompt argument accepted: {invalid}")
            print("PASS: invalid identifiers, ambiguous times, and invalid thresholds rejected")
    print("Investigation prompt test passed. No AWS calls or data writes were performed.")


if __name__ == "__main__":
    asyncio.run(main())
