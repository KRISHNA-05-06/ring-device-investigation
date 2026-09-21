# Local development

Run commands from the repository root. Existing `.venv-aws` and AWS profiles remain on your laptop.

```powershell
.\.venv-aws\Scripts\python.exe scripts/test_offline.py
.\.venv-aws\Scripts\python.exe apps/dashboard/dashboard.py --demo
.\.venv-aws\Scripts\python.exe tools/simulator/stream_simulator.py
```

These run offline checks, serve labelled UI fixtures, and preview a producer session respectively.
For AWS, renew the `ring-mcp` login and run `tests/integration/test_aws_mcp_readiness.py`, or start
the dashboard without `--demo`. Simulator `--send` writes events using `ring-dev` and stops after
three minutes by default. Each dashboard refresh performs four reads.

Claude can keep pointing at root `aws_mcp_server.py`; its implementation is now under `services/mcp/`.
Root `approval_client.py` is also preserved. Old root test commands remain as ignored local launchers;
fresh clones should use `tests/integration/` and `tests/protocol/`.

Set `RING_APPROVAL_URL` for the operator client, or put an `approval_url` property in
`local-only/config/operator.json`. The existing endpoint was migrated there. Credentials still come
from the AWS profile. A new clone deliberately has no configured live approval endpoint.

Use the new canonical paths when editing. Historical drafts and learning PDFs are under `local-only/`;
they are not runtime dependencies. Existing convenience commands in the old dashboard and simulator
folders forward to the canonical implementations.
