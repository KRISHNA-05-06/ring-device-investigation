"""Run mock-based unit suites and real MCP protocol checks, without AWS calls."""
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
SOURCES = {
    'query': 'services/lambdas/query', 'recovery': 'services/lambdas/recovery',
    'approval': 'services/lambdas/approval', 'dashboard': 'apps/dashboard',
    'simulator': 'tools/simulator', 'monitoring': 'infra/monitoring',
}


def main():
    for suite, source in SOURCES.items():
        env = {**os.environ, 'AWS_EC2_METADATA_DISABLED': 'true',
               'RING_APPROVAL_URL': 'https://example.lambda-url.us-east-1.on.aws/',
               'AWS_ACCESS_KEY_ID': 'offline-test-key', 'AWS_SECRET_ACCESS_KEY': 'offline-test-secret',
               'AWS_DEFAULT_REGION': 'us-east-1',
               'PYTHONPATH': os.pathsep.join([str(ROOT), str(ROOT / source)])}
        print(f'\nUnit suite: {suite}', flush=True)
        subprocess.run([sys.executable, '-m', 'unittest', 'discover', '-s',
                        str(ROOT / 'tests/unit' / suite), '-p', 'test_*.py', '-v'],
                       cwd=ROOT, env=env, check=True)
    for name in ['test_mcp_resources.py', 'test_investigation_prompt.py']:
        print(f'\nProtocol check: {name}', flush=True)
        subprocess.run([sys.executable, str(ROOT / 'tests/protocol' / name)], cwd=ROOT, check=True)
    print('\nPASS: all offline suites and MCP protocol checks. Live AWS integration tests were not run.')


if __name__ == '__main__':
    main()
