# Repository reorganization verification

Verified locally on 2026-09-21 after consolidating the canonical implementations.

| Check | Result |
|---|---|
| Query unit suite | 59 passed |
| Recovery unit suite | 102 passed |
| Approval unit suite | 32 passed |
| Dashboard unit suite | 4 passed |
| Simulator unit suite | 10 passed |
| Monitoring unit suite | 5 passed |
| MCP resource protocol check | Passed; AWS session creation blocked |
| MCP prompt protocol check | Passed; AWS session creation blocked |
| Clean publication export | Same suites passed without local archives, PDFs, virtual environment or private configuration inside the export |
| Publication checks | No configured secret-pattern or forbidden-path matches |

Total: 212 unit tests plus two protocol scripts. Two historical discovery assertions were updated
from 16 to the final 17 tools. Tests target the canonical server and backend source, not old staged copies.

The export used the existing Python environment outside its directory; this does not prove a fresh
dependency installation on every OS. The GitHub Actions workflow has not run yet. The existing
Pydantic lifespan warning remains visible, but protocol checks finish successfully.

No live AWS calls, deployments, approvals, ingestion writes or replay operations were performed
during reorganization. Earlier live verification remains described in the project checklist; original
account evidence is retained locally under `local-only/evidence/`.
