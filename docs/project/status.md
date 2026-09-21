# Current project progress

Reconciled 2026-09-21. This supersedes the accumulated, sometimes contradictory status notes retained in local-only/before-reorganization/docs/history-before-closeout. No exact completion percentage is assigned without a fixed weighted baseline.

## Built and demonstrated

- End-to-end Kinesis -> archive Lambda -> S3 -> validator -> four-table PostgreSQL ingestion.
- Read-only query service, 17 MCP tools, resources/prompts and Claude integration.
- Rejection inspection, correction preview, saved proposal, independent operator approval, correction artifact, replay and database readback.
- Duplicate suppression and conflicting-content rejection; original rejection preserved after approved recovery.
- Daily metrics, consistency checks, S3 health and quality reports with retry protection.
- Bounded synthetic streaming: normal, silence, invalid battery and duplicate scenarios.
- Local read-only dashboard: live health, aging after producer stop, rejection and duplicate counts observed by the user.

## Final verification completed

1. Operational monitoring: stack ring-operational-monitoring deployed and all ten alarm configurations verified. The deliberate validator NoSuchKey failure produced Errors=1 at 2026-09-21T20:34:00Z, and alarm history confirms OK -> ALARM at 20:36:31.840Z for that datapoint. This verifies one real detection path; the other nine alarms were configuration-checked, not fault-injected. SQL confirms the exact test run failed with NoSuchKey, has a completion timestamp, and has zero input/outcome counts and zero stored accepted/rejected rows. The controlled monitoring test is complete.
2. Final read-only MCP smoke PASSED at 2026-09-21T20:41:10.117446Z: all 17 tools discovered, three resources and the investigation prompt accessible, plus successful live health, pipeline-status and daily-metrics calls. No writes requested; recovery was not rerun. Evidence is retained in local-only/evidence/2026-09-21-mcp-readiness.txt and .json.
3. The agreed functional prototype and final verification are complete. The operating/interview guide is provided and final evidence saved. Interview rehearsal and checking current account spend are follow-up activities, not claims of additional completed tests.

Monitoring instructions: docs/runbooks/monitoring.md. Main learning guide: docs/interview/guide.md. Evidence and limitations: docs/project/checklist.md. No subscriptions/payments or other business domain is being added.

## Known limits to disclose

- Concurrent database inserts, hard timeouts, mid-commit failures and complete crash recovery remain unverified. The two-object duplicate test ran sequentially.
- S3 exports and PostgreSQL commits are not one atomic transaction; interrupted runs or orphan exports need investigation.
- Audit output records artifact lifecycle, not reads, failed requests, retries or automatic expiry events. Proposal-creator identity is not recorded; approval actor is.
- No end-to-end exactly-once transport, full delivery-completeness guarantee, production multi-tenant security, or real Ring integration is claimed.
- Producer is a foreground bounded simulator; dashboard is a local application. Always-on hosting, notifications and schedules are not implemented.
- Old PDF trackers remain historical. Use the closeout guide and these updated Markdown files for current status.
