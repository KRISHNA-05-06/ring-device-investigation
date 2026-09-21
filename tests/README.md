# Test layers

| Directory | Purpose | AWS writes |
|---|---|---|
| `unit/` | Mock-based query, recovery, approval, dashboard, simulator and monitoring checks | None |
| `protocol/` | Real local stdio discovery, resource reads and prompt validation; AWS sessions blocked | None |
| `integration/` | Explicit live AWS checks against the demonstration deployment | Some suites write; inspect first |

Run `python scripts/test_offline.py` for CI and local checks. Each unit group runs in its own process,
so similarly named modules and mocks cannot leak between service suites.
`test_aws_mcp_readiness.py` is the current read-only live smoke check. Older integration files bind to
historical proposals and may require refreshed fixtures; do not run every integration file blindly.
Successful offline checks do not prove current AWS credentials, IAM, triggers or database connectivity.
