# Bounded streaming simulator

This is a separate producer for synthetic events in the existing ring-events stream. It is not connected to Ring devices or the Ring app. The original CloudShell producer was not available in the local workspace, so this file implements its own small, tested sender rather than modifying that remote script.

## First run from the project PowerShell directory

Preview (immediate, no AWS access):

```powershell
.\.venv-aws\Scripts\python.exe 'tools/simulator/stream_simulator.py'
```

Send normal heartbeats for up to three minutes, every 30 seconds:

```powershell
.\.venv-aws\Scripts\python.exe 'tools/simulator/stream_simulator.py' --send
```

Default profile ring-dev, region us-east-1, account 455646290306. Identity is checked before sending; an unexpected account is rejected. The maintainer needs kinesis:PutRecords for ring-events. Do not broaden ring-mcp's permissions. A failed or expired login must be resolved separately. No new AWS resources are created.

Normal schedule: up to six batches, three registered devices per batch, 18 events total. Each event has the existing five-field contract, current UTC event_time, and a new session/tick/device ID. Partition key is household#device. Battery readings are fixed synthetic values; they do not model battery drain. A slow send can reduce the batch count; no catch-up bursts occur. Duration caps generation, not a hard process-kill deadline: an in-flight bounded request/retry may finish after the deadline. Maximum allowed generation window is 900 seconds; minimum interval is 10 seconds. Ctrl+C stops early; it does not retract events already sent.

The pipeline remains Kinesis -> archive Lambda -> raw S3 -> validator -> PostgreSQL. A sent_to_kinesis receipt is not evidence that PostgreSQL accepted the event. No model or MCP call is needed to trigger this ingestion path.

## Session evidence

Every send run generates a new random session ID and an exclusive sessions/<id>.jsonl file beside the script. Each batch's full events are written and flushed before sending; responses record shard and sequence numbers. Logs are excluded from Git and contain no AWS credentials. This is local troubleshooting evidence, not a crash-proof transactional outbox.

Explicit per-record failures are retried alone, using the same ID and bytes, up to five attempts with jitter. SDK request retries are disabled. A whole-request failure may conceal a successful write; generation stops and logs unconfirmed delivery instead of automatically making a fresh event. A terminal retry failure stops the run with nonzero exit status. Restarting this program starts a new session with new events, not a recovery of the previous session. Inspect uncertain delivery before restarting; do not claim exactly-once transport. PutRecords does not guarantee strict order.

AWS reference: https://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecords.html

## Other scenarios (run normal first)

--scenario duplicate repeats camera-001's first event once, with identical ID and content. Later batches are normal. Expect one duplicate transport entry in addition to normal events, possibly more ingestion attempts due to AWS redelivery.

--scenario invalid-battery emits 150 for camera-002 in the first batch only. Later camera-002 events are distinct valid events with 75. The first invalid event should remain rejected; later events are not corrections of it.

--scenario silence sends camera-002 once and then omits it while the other two continue. This simulates missing telemetry, not proven hardware failure. To see the transition in a three-minute demonstration, explicitly query get_device_health with silence_minutes=1 and omit as_of for current time. The normal ten-minute threshold will not expire during this short test. The dashboard should later show the chosen threshold clearly.

All live scenarios add database history and S3 objects. They may change latest-health and current-day reports; older September 17/18 fixtures remain unchanged only if newly generated event timestamps are outside those days. Nothing is automatically deleted when the producer stops. This is a local foreground demo, not an always-on cloud service.

## Functions to understand

- make_events: constructs five-field events and applies the selected scenario.
- wire_record: serializes the event and supplies its Kinesis partition key.
- send_batch: reads individual Kinesis outcomes and retries failed entries without changing identity.
- main: validates runtime bounds, previews or verifies AWS identity, saves local evidence, schedules batches and closes the client.

## Verify the first live run

Keep the session ID printed in send mode (preview uses a different ID). In ring-db-shell as ring_reader, paste verify_session.sql after replacing PASTE_SEND_SESSION_ID. Run again if ingestion is still catching up, without restarting the producer. For a normal uninterrupted six-batch session expect 18 accepted event rows, six per device. If fewer batches were completed, compare with the producer's batch receipts. SQL accepted counts alone cannot diagnose whether a missing event is in transit, rejected, or unsent: correlate source/sequence receipts and ingestion evidence before concluding failure.

Local tests mock AWS; no live stream has been started by the assistant.

## Short invalid-data demonstration

After normal and silence scenarios, run a single batch:

```powershell
.\.venv-aws\Scripts\python.exe 'tools/simulator/stream_simulator.py' --scenario invalid-battery --duration-seconds 30 --interval-seconds 30 --send
```

This generates exactly one planned batch: camera-001 battery 74, camera-002 battery 150, doorbell-001 battery 82, then stops after about 30 seconds. Expected accepted rows for that session are two, one per valid device. camera-002 should have no accepted event for this session. At least one INVALID_BATTERY rejection occurrence should be recorded for the invalid input; AWS redelivery may produce repeated occurrences. Verify with verify_invalid_battery.sql, substituting the SEND session ID.

In the live dashboard select Current time, refresh after ingestion, and inspect the rejection summary and recent runs. INVALID_BATTERY should increase from its pre-test count by at least one. Pipeline counters include attempts and may exceed the three unique inputs if delivery repeats. A completed validator run may contain a rejected record; invalid data is different from the missing-source infrastructure test that marked the run failed. camera-002 retains its prior accepted heartbeat and battery value; rejecting this new event does not erase earlier accepted history.

## Initial live access denial (details)

The user's first send session 05d93e11da6a4568bb179c156328f6a6 authenticated as ring-project-developer but its first PutRecords call returned AccessDeniedException, with zero completed batches. The previous sender labeled all whole-call errors unconfirmed; it now distinguishes explicit access denial as send_denied and includes AWS's error message. Timeouts and other uncertain errors retain the conservative unconfirmed status.

Using an existing IAM-administration session, add the inline policy RingSimulatorStreamWrite.json to the IAM user ring-project-developer, named RingSimulatorStreamWrite, if that scoped grant is missing. Keep existing policies. This grants only kinesis:PutRecords on the one project stream; PutRecord is a different action. Do not attach this to ring-mcp-client or the Lambda execution roles. This policy is prepared locally, not applied by the assistant. If access is still denied, inspect the full AWS error now logged: an explicit deny, permissions boundary, session restriction or KMS denial needs specific diagnosis rather than a broader grant.
