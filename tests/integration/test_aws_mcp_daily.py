"""Live read-only MCP verification against the previously verified daily fixtures."""
import asyncio
import json
import os
import sys
from datetime import timedelta
from pathlib import Path

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


async def main():
    params = StdioServerParameters(command=sys.executable,
        args=[str(Path(__file__).resolve().parents[2] / "aws_mcp_server.py")],
        env={**os.environ, "RING_AWS_PROFILE": "ring-mcp"})
    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write, read_timeout_seconds=timedelta(seconds=120)) as session:
            await session.initialize()
            listing = await session.list_tools()
            assert len(listing.tools) == 17
            tool = next(t for t in listing.tools if t.name == "get_daily_device_metrics")
            assert set(tool.inputSchema["properties"]) == {"report_day", "device_limit"}
            assert tool.inputSchema["required"] == ["report_day"]
            assert tool.annotations.readOnlyHint is True
            print("PASS: discovered 17 tools; daily metrics has the expected read-only contract")
            for day, limit in (("2026-09-18", 100), ("2026-09-17", 100), ("2026-09-18", 1)):
                response = await session.call_tool("get_daily_device_metrics", {"report_day": day, "device_limit": limit})
                assert not response.isError, response.content
                body = json.loads(next(c.text for c in response.content if c.type == "text"))
                result = body["result"]
                assert body["database_user"] == "ring_reader" and body["household_id"] == "household-001"
                assert result["returned_devices"] == min(3, limit) and result["truncated"] == (limit == 1)
                expected_battery = {"camera-001": 74, "camera-002": 75, "doorbell-001": 82}
                for row in result["devices"]:
                    expected_count = 0 if day == "2026-09-17" else 1
                    assert row["accepted_heartbeat_count"] == expected_count, "Fixture changed: inspect current accepted events"
                    assert row["average_battery_percent"] == (None if not expected_count else expected_battery[row["device_id"]])
                print(f"PASS: daily fixture {day}, device_limit={limit}, truncated={result['truncated']}")
            for arguments in ({"report_day": "2026-02-30"}, {"report_day": "2026-09-18", "device_limit": 101}):
                response = await session.call_tool("get_daily_device_metrics", arguments)
                assert response.isError, "Invalid daily request unexpectedly succeeded"
            print("PASS: invalid daily inputs rejected")
    print("Daily metrics MCP test passed. No data was saved or changed.")


if __name__ == "__main__":
    asyncio.run(main())
