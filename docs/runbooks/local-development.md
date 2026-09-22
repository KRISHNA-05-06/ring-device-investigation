# Local development

Use Python 3.11 and run commands from the repository root. Install `requirements.txt` in a
virtual environment before starting the clients or tests.

```powershell
python -m venv .venv-aws
.\.venv-aws\Scripts\python.exe -m pip install -r requirements.txt
.\.venv-aws\Scripts\python.exe scripts/test_offline.py
.\.venv-aws\Scripts\python.exe apps/dashboard/dashboard.py --demo
```

The test runner uses mocked AWS services and real local MCP protocol sessions. Dashboard demo
mode serves labeled sample data. To preview producer events without sending them:

```powershell
.\.venv-aws\Scripts\python.exe tools/simulator/stream_simulator.py
```

On macOS/Linux, use `.venv-aws/bin/python` instead.

## AWS clients

Configure `ring-mcp` for query/recovery, `ring-dev` for event production, and `ring-operator` for
approvals. Verify the identity behind each profile. Profile names alone do not enforce separation;
the operator and MCP client must authenticate as different, appropriately restricted principals.

For a configured deployment, run `tests/integration/test_aws_mcp_readiness.py` or start the dashboard
without `--demo`. Renew expired credentials before retrying. Each dashboard refresh performs four
read operations. Simulator `--send` writes events and stops after three minutes by default.

## Claude configuration

Use `config/claude_desktop_config.example.json` as a template. Set absolute paths to the virtual
environment's Python executable and root `aws_mcp_server.py`. The launcher loads the implementation
in `services/mcp/`. Restart the Claude desktop application after changing its configuration.

## Operator configuration

Set `RING_APPROVAL_URL`, or put an `approval_url` property in the ignored
`local-only/config/operator.json`. Credentials come from the operator's AWS profile. The repository
does not include a live approval URL. The root `approval_client.py` loads `tools/operator/approval_client.py`.

## Code and tests

Edit implementations under `services/`, `apps/`, `tools/`, and `infra/`. Test suites live under
`tests/unit/`, `tests/protocol/`, and `tests/integration/`. Live integration checks are separate from
the offline runner; inspect their requirements and write behavior before running them.
