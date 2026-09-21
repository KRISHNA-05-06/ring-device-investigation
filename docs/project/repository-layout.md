# Repository organization

The existing `ring-device-investigation` folder is the working repository. The final query, recovery,
approval, simulator, dashboard and monitoring implementations were consolidated from step-by-step
work folders. Tests now target these canonical versions.

Root MCP and approval launchers keep the existing client configuration working. Older test commands
remain as ignored local launchers. New commands use `tests/`. Previous dashboard and simulator
commands in sibling `work` folders forward to the canonical code; edit only the new paths.

`local-only/` collects old drafts, learning PDFs, session logs, evidence, proposal exports and a backup.
It is ignored, as are virtual environments and generated outputs. No deployed AWS settings changed.
The operator endpoint moved from source into ignored local configuration. Fixed AWS identity/resource
bindings remain in source and test fixtures and are documented in SECURITY.md.

Local CI checks run mocked suites and MCP protocol tests. The GitHub workflow runs the same offline
checks; it never deploys AWS. Live tests remain explicit. This repository is prepared locally only:
no GitHub repository was created and no code was pushed.
