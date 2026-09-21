import asyncio
import os
import sys
from datetime import timedelta
from pathlib import Path

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


PROJECT_DIR = Path(__file__).resolve().parents[2]
LOG_PATH = PROJECT_DIR / "mcp-diagnostic.log"

BOOTSTRAP = """
import faulthandler
import runpy
import sys

faulthandler.enable()
faulthandler.dump_traceback_later(20, repeat=True)

runpy.run_path(sys.argv[1], run_name="__main__")
"""


async def diagnose():
    parameters = StdioServerParameters(
        command=sys.executable,
        args=[
            "-u",
            "-c",
            BOOTSTRAP,
            str(PROJECT_DIR / "aws_mcp_server.py"),
        ],
        cwd=str(PROJECT_DIR),
        env={
            **os.environ,
            "RING_AWS_PROFILE": "ring-mcp",
        },
    )

    with LOG_PATH.open("w", encoding="utf-8") as log:
        async with stdio_client(parameters, errlog=log) as (read, write):
            async with ClientSession(
                read,
                write,
                read_timeout_seconds=timedelta(seconds=60),
            ) as session:
                try:
                    await session.initialize()
                    print("PASS: MCP initialized", flush=True)

                    resource = await session.read_resource(
                        "ring://definitions"
                    )
                    print(
                        f"PASS: resource returned "
                        f"{len(resource.contents)} item(s)",
                        flush=True,
                    )

                    print("Calling get_device_health...", flush=True)

                    result = await session.call_tool(
                        "get_device_health",
                        {
                            "as_of": "2026-09-18T12:05:00Z",
                            "silence_minutes": 10,
                        },
                    )

                    print(
                        f"Tool response received. isError={result.isError}",
                        flush=True,
                    )

                    for block in result.content:
                        if block.type == "text":
                            print(block.text, flush=True)

                except Exception as error:
                    print(
                        f"ORIGINAL ERROR: "
                        f"{type(error).__name__}: {error}",
                        flush=True,
                    )
                    raise


if __name__ == "__main__":
    try:
        asyncio.run(diagnose())
    except Exception as error:
        print(f"Session ended: {type(error).__name__}", flush=True)
        print(f"Diagnostic log: {LOG_PATH}", flush=True)
        sys.exit(1)