"""Exercise resource discovery/reads over MCP stdio, with AWS access blocked."""
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
        "import boto3, runpy, sys; "
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
    print("MCP resource test passed. No AWS calls or data writes were performed.")


if __name__ == "__main__":
    asyncio.run(main())
