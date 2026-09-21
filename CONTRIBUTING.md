# Working on this repository

Use Python 3.11 for the local clients and tests. Install `requirements.txt` in a virtual environment.
Run `python scripts/test_offline.py` before committing. AWS tests under `tests/integration/` are opt-in;
some create S3 artifacts and depend on historical proposal IDs. Read their headers before running.

Edit implementations under `services/`, `apps/`, `tools/`, or `infra/`. The root
`aws_mcp_server.py` and `approval_client.py` are compatibility entry points, not duplicate implementations.
New automated checks belong in `tests/unit/` or `tests/protocol/`. Keep database verification SQL in
`infra/database/queries/` and operating instructions in `docs/runbooks/`.

Keep credentials, real customer data, operator proposal exports, logs, PDFs, session evidence and
old code under the ignored `local-only/` folder. Never force-add this folder. Use synthetic fixtures
in committed tests. Review `git diff --cached` before creating a commit.

Change locally, test, commit, then push. Pushing does not deploy AWS resources; the CI workflow only
runs offline checks. Review deployment changes separately. See `docs/runbooks/deployment.md`.
