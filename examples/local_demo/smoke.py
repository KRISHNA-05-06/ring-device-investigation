import asyncio
import json
import os
import sys

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


async def main():
    params = StdioServerParameters(command=sys.executable, args=['server.py'], env=dict(os.environ))
    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            tools = await session.list_tools()
            assert {tool.name for tool in tools.tools} == {
                'get_device_health', 'get_pipeline_status', 'get_rejection_summary'}
            results = {}
            for tool in tools.tools:
                result = await session.call_tool(tool.name, {})
                assert not result.isError, result
                results[tool.name] = result.structuredContent
            devices = results['get_device_health']['devices']
            assert {d['device_id']: d['status'] for d in devices} == {
                'camera-001': 'suspected_offline', 'camera-002': 'unknown', 'doorbell-001': 'reporting'}
            run = results['get_pipeline_status']['latest_run']
            assert (run['input_count'], run['accepted_count'], run['duplicate_count'], run['rejected_count']) == (8, 3, 1, 4)
            assert len(results['get_rejection_summary']['errors']) == 4
            resources = await session.list_resources()
            assert len(resources.resources) == 1
            resource = await session.read_resource('device-health://definitions')
            assert 'Source completeness' in resource.contents[0].text
            prompts = await session.list_prompts()
            assert len(prompts.prompts) == 1
            prompt = await session.get_prompt('investigate_devices')
            assert prompt.messages
            invalid = await session.call_tool('get_device_health', {'silence_minutes': 0})
            assert invalid.isError
            invalid_time = await session.call_tool('get_device_health', {'as_of': 'not-a-date'})
            assert invalid_time.isError
            forbidden = await session.call_tool('execute_sql', {'sql': 'DELETE FROM devices'})
            assert forbidden.isError
            print(json.dumps({'mcp_check': 'passed', 'results': results}, indent=2))


if __name__ == '__main__':
    asyncio.run(asyncio.wait_for(main(), timeout=30))
