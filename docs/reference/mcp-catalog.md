# MCP tool catalog

The local MCP server exposes 17 tools over stdio. Queries read household-scoped evidence;
recovery tools preview or persist specific artifacts. Approval is handled by a separate operator interface.

| # | Tool | Effect | Purpose |
| --- | --- | --- | --- |
| 1 | `get_device_health` | Read | Latest accepted heartbeat and age-based classification for registered devices. |
| 2 | `get_pipeline_status` | Read | Ingestion attempts, recent runs and counters; totals are not unique-device counts. |
| 3 | `get_rejection_summary` | Read | Recorded rejection occurrences grouped by error code. |
| 4 | `list_rejected_records` | Read | Bounded rejection details, optional error-code filter, explicit truncation. |
| 5 | `inspect_rejected_record` | Read | Original rejected content, validation reason and source evidence. |
| 6 | `preview_record_correction` | Preview | Construct a proposed battery correction; no persistence or approval. |
| 7 | `propose_record_correction` | Write | Persist an exact pending proposal, bound to source and content digests. |
| 8 | `get_operation_status` | Read | Combine proposal, approval, correction, submission and observed ingestion evidence. |
| 9 | `get_audit_history` | Read | Recorded artifact lifecycle; not a complete log of every request. |
| 10 | `preview_replay` | Preview | Predict the saved candidate's outcome against current database state. |
| 11 | `apply_record_correction` | Approved write | Create the exact approved correction artifact in S3; no database update. |
| 12 | `replay_records` | Approved write | Create fixed replay input in raw/replays; observe ingestion separately. |
| 13 | `get_device_details` | Read | One device, latest accepted heartbeat and source lineage. |
| 14 | `get_event_history` | Read | Accepted events in a bounded, start-inclusive/end-exclusive time window. |
| 15 | `get_quality_results` | Read | Four consistency checks over selected completed ingestion runs. |
| 16 | `recompute_device_health` | Write | Save a health observation in S3; identical retries reuse the original report. |
| 17 | `get_daily_device_metrics` | Read | UTC daily counts and battery statistics; zero events preserve null readings. |

## Resources and prompts

- ring://schema: reference schema and household joins; reading it does not introspect or migrate RDS.
- ring://runbook: investigation, recovery and report interpretation guidance.
- ring://definitions: statuses, evidence boundaries and operational meanings.
- investigate_devices: parameterized investigation workflow; device ID, as-of cutoff and silence threshold are optional.
- investigation_guidance is an internal helper composing that prompt, not another advertised prompt. Prompts guide the host model; they do not execute tools or authorize writes themselves.

Approval is an independent IAM-authenticated operator interface, not an MCP tool. Internal get_replay_ingestion_status and backend save_quality_report do not add advertised tools. Tool annotations describe intent; server-side validation and AWS/IAM authorization enforce it. The dashboard calls the shared AWS adapter directly, not the MCP protocol.

## Boundaries

Household is configured server-side. No arbitrary SQL, S3 key or Lambda function is exposed to the model. Read queries use ring_reader. Approval binds reviewed content and expiry to an independently authenticated operator. Hashes identify content but are not credentials. Accepted rows are not overwritten. A saved correction, replay submission and accepted database event are three different outcomes.
