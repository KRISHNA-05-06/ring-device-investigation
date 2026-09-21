# Ring-inspired MCP Investigation and Recovery

## 01. What you built and why it matters

You built a synthetic product/support investigation system. It receives device heartbeat events, separates valid data from invalid input, preserves processing evidence, and lets an AI assistant investigate through constrained MCP tools. A human operator can approve an exact correction; the system then reprocesses the corrected event and exposes evidence of the result.

The business question is: "Why is this device missing from accepted data, and can we safely repair the data problem?" The project addresses an investigation workflow. It does not connect to the real Ring app, observe real cameras, diagnose mechanical failures, or implement customer billing.

Your strongest interview evidence is the complete MCP workflow, not the number of tools. You demonstrated discovery, typed inputs, resources, reusable prompts, AWS service calls, scoped reads, independent approval, safe retries and post-action verification. The pipeline supplies realistic data and failure states for that workflow.

Suggested introduction: "I built an AWS-backed MCP investigation and recovery prototype for synthetic device telemetry. An assistant can inspect rejected events and propose a correction, but a separate authenticated operator must approve the exact content. Recovery preserves the original evidence and reuses the ingestion validator. I verified the resulting PostgreSQL row and repeat-request behavior."

For the Ring Agent Platforms role, position this as one product/customer-support use case demonstrating agent-accessible data services. Your previous work can provide evidence for other data-engineering domains. Do not describe this as a complete subscription, finance or enterprise platform. The domain was deliberately bounded to make the MCP workflow demonstrable within the available time.

Status on 2026-09-21: the agreed functional prototype is complete. The pipeline, MCP, approved recovery, reports, simulator and dashboard have shared test evidence. Ten alarms are deployed; one controlled validator failure triggered its alarm and was verified in SQL. The final read-only MCP smoke passed at 20:41 UTC. Interview rehearsal remains a learning follow-up.

## 02. Follow one event through the system

The simulator creates a five-field event: event_id, household_id, device_id, event_time and battery_percent. For example, camera-002 belongs to household-001 and reports a synthetic battery percentage. A stable event ID lets the consumer recognize repeated delivery; a content digest distinguishes an identical repeat from conflicting content.

Step 1: the local Python producer sends a record to Kinesis ring-events. The partition key combines household and device. Kinesis returns a shard ID and sequence number. These identify stream placement and ordering context; they are not application event IDs or proof of database acceptance. PutRecords can partially succeed, so the producer retries only explicitly failed entries with unchanged IDs and bytes.

Step 2: ring-stream-archive consumes the stream and saves the input under S3 raw/. Source paths retain stream, shard and sequence lineage. This durable input allows later inspection independently of the original producer.

Step 3: an S3 object-created notification invokes ring-device-validator. It validates each record, checks registration and household scope, and compares existing event identity/content. Accepted events go to heartbeat_events. Invalid input gets rejected_records evidence. Duplicate attempts increase the run's duplicate count without another accepted row. ingestion_runs records the attempt and its outcome. S3 exports retain curated and rejected output.

Step 4: ring-device-query reads the database as ring_reader. The dashboard or an MCP tool requests a supported operation and receives structured evidence. The assistant summarizes that evidence; it should not invent causes or treat raw record text as instructions.

The downstream pipeline is automatic once input arrives. The current producer is manually started, generates a bounded stream, then stops. That is different from an unattended, always-on producer. Stopping it does not delete registered devices or stored events.

## 03. Why these AWS services and four tables

Kinesis decouples event production from consumption and provides a stream buffer. The tradeoff is stream capacity/cost, ordering constraints and retry handling. For a tiny batch-only system, direct S3 upload would be simpler; streaming was included to exercise a realistic event path.

S3 retains raw input, exports, proposals, approvals, correction artifacts and reports. It makes investigations reproducible and separates evidence from an assistant conversation. The tradeoff is extra processing stages, many small objects and no shared transaction with PostgreSQL. Content hashes detect differences; they do not grant authority or provide S3 Object Lock.

Lambda provides event-driven processing and isolates archive, validation, queries, recovery and approval permissions. The tradeoff is deployment/configuration overhead, cold starts, execution limits and careful database connection management. A managed service still needs monitoring and tested failure behavior.

RDS PostgreSQL supplies relational constraints, transactions and SQL joins. devices contains one registration per household/device. ingestion_runs contains one processing attempt. heartbeat_events contains accepted application events, unique by household/event ID. rejected_records contains rejection occurrences, linked to an ingestion run. A retry can add another run or rejection occurrence; these counts are not always counts of unique source events.

rejected_records does not carry a direct household_id in this schema. Queries join run_id to ingestion_runs and constrain the run's household. Accepted events also retain device and run relationships, supporting source lineage and reconciliation. Exactly four tables remain; report and recovery artifacts are in S3.

IAM controls who may invoke AWS APIs or read/write particular prefixes. VPC subnets and security groups provide private network reachability to RDS. A Lambda execution role describes what that function may do; it does not automatically let every person invoke the function. PostgreSQL privileges separately limit database operations. Network access, IAM authorization and database authorization must all be correct.

CloudWatch supplies execution logs, service metrics and the operational alarms prepared in the final stage. It complements application-level run/rejection evidence rather than replacing it.

## 04. MCP: client, server, tools, resources and prompts

Claude is the host application. Its MCP client starts the local aws_mcp_server.py process and communicates over stdio using MCP messages. A Python test can act as a client too; an LLM is not required to initialize a session, discover tools or exercise a call.

The server advertises tools with schemas. The host model can request a supported tool, but the implementation decides what the call actually does. A typical read travels from Claude to the local MCP server, through boto3 to ring-device-query, then to PostgreSQL using ring_reader. The response travels back as structured tool output.

Tools perform operations. Resources provide reference context: ring://schema, ring://runbook and ring://definitions. They are served from local reference content; reading the schema resource does not inspect the live database. The host must support and load resources, or the user must select them. Simply registering a resource does not guarantee the model has read it.

The investigate_devices prompt is a reusable workflow with optional device_id, as_of and silence_minutes. Selecting it supplies instructions and validated parameters to the host. It does not run every tool, automatically approve writes or replace backend authorization. investigation_guidance is an internal helper used to compose the prompt, not another advertised prompt.

There are 17 advertised tools: the original 15 plus preview_record_correction and get_daily_device_metrics. Reads cover health, device details, history, runs, rejections, quality, operation status and audit. Previews calculate correction/replay candidates. Writes save proposals/reports and perform approved correction/replay steps. Full names and effects are in docs/reference/mcp-catalog.md.

Approval remains outside the model's tool list. The browser dashboard also remains separate: it reuses the Python AWS adapter directly and does not speak MCP. This distinction explains what MCP contributes: a discoverable, typed interface for an AI host, not a replacement for the underlying data pipeline or API implementation.

## 05. The recovery workflow and its evidence

Your verified example started with rejection 6: evt-batch-003, camera-002, battery_percent 150. The validator rejected it because the allowed integer range is 0 through 100. For the synthetic demonstration, you explicitly chose 75 as the replacement. That is a declared test value, not a battery value inferred by AI.

inspect_rejected_record exposes original content and source context. preview_record_correction constructs the proposed before/after change without saving it. propose_record_correction saves the exact proposal, source digest, preview digest and expiry. preview_replay predicts whether that saved candidate would be accepted, duplicated or rejected against the current database snapshot. Prediction is not authorization or execution.

The operator separately reviews the proposal with approval_client.py, authenticates as ring-project-approver, and types the exact approval confirmation. The AWS_IAM function URL enforces the independent identity boundary. Approval is saved in a separate S3 object bound to the proposal hash and original expiry. Repeating approval returns existing identical evidence without extending the lifetime.

apply_record_correction checks the stored evidence and creates a correction artifact. It does not update heartbeat_events. replay_records creates fixed input under raw/replays/ for the normal validator. Source evidence and authorization are checked before submission. A saved replay object is submission evidence; ingestion can still be pending, fail or reject the record.

The successful replay used proposal 744e5f5447a1df812bdd323a2df276662ffac250f0a4f3d92dc65a5238480f19. PostgreSQL showed exactly one evt-batch-003 with battery 75 and replay source lineage. Original rejection 6 still contained battery 150. The second replay call returned the existing object. These facts support the specific retry behavior tested; they do not prove exactly-once transport for every failure scenario.

The original proposal still says pending because it preserves its creation state. Current status combines that document with separate approval and execution evidence. Audit history records lifecycle artifacts; it is not a complete journal of all reads, denied requests or retries.

## 06. Important semantics to explain accurately

Reporting means the latest accepted heartbeat is within the chosen silence threshold. Suspected_offline means it is older. Unknown means no accepted heartbeat exists at the requested event-time cutoff. None of these alone proves a hardware failure. Device issues, rejected input, delays or pipeline faults can all affect observed data.

At a ten-minute threshold, exactly ten minutes still reports; eleven minutes is suspected_offline. The live dashboard recomputes age when refreshed, so devices can change status even without a new event. The bounded simulator stopping is enough to cause this expected transition.

event_time and received_at answer different questions. The former describes the synthetic observation; the latter records database arrival. A report for September 18 can include a correction accepted on September 19 if its event_time is September 18. It describes currently stored accepted data for that period, not what the system knew at the original cutoff.

Daily windows include their starting midnight and exclude the next midnight. This avoids counting a boundary event twice in adjacent days. A day with zero accepted events has null battery statistics, not battery zero or confirmed downtime. An average of one event is just that reading; it does not establish battery stability, drain or uptime.

Quality checks reconcile input = accepted + duplicate + rejected, accepted row counts, rejected row counts and limited accepted-value validity. A pass can include rejected input because correct rejection is expected behavior. A truncated selection is incomplete. These checks do not prove source delivery completeness, independently verify every duplicate, scan all orphan records or rerun the entire validator.

Saved report retries return the original observation for the same idempotency key. To intentionally measure again, use a new key. This makes retry semantics clear while preserving the earlier evidence. The current live queries and saved reports serve different purposes.

## 07. File map and how to study the code

Project paths below are relative to ring-device-investigation; work/ is its sibling folder. Preserve both directories when transferring this demo. Older staged files show incremental development and should not be deployed interchangeably.

aws_mcp_server.py is the current MCP adapter. Start with OPERATIONS and RECOVERY_OPERATIONS, then invoke_query/invoke_recovery, one decorated tool, response validators, resource readers and investigate_devices. Follow an argument from schema to Lambda payload to returned evidence. finally blocks close response streams and clients even if validation raises an exception.

services/lambdas/validator/validator_lambda.py handles ingestion. Study validate_event and process_file, then the handler's direct-test versus S3-notification branches. Validation exceptions become rejected rows; infrastructure exceptions roll back pending database work, attempt to mark the run failed, and are re-raised. S3 exports occur outside the database transaction guarantee.

services/lambdas/query/query_lambda.py is the current query reference. parse_request rejects unsupported fields and bounds inputs; connect_database requires ring_reader and TLS; query functions use parameterized SQL and household constraints; serialization converts dates and numeric values to JSON-compatible forms.

services/lambdas/recovery/recovery_lambda.py is the current recovery reference. Study canonical_bytes/digest, load_verified_approval, verify_original_source, apply_record_correction, replay_records and observe_replay_ingestion. Then study report functions. Conditional S3 creation and subsequent verification distinguish a new object from a safe identical retry.

approval_client.py reviews a proposal in an interactive terminal and signs the approval request. services/lambdas/approval/approval_lambda.py validates trusted caller context and saves approval evidence. The caller's display name or a model-supplied approved=true flag is not sufficient authority.

tools/simulator/stream_simulator.py constructs synthetic events, publishes batches and keeps session receipts. apps/dashboard/dashboard.py exposes four fixed read operations to a loopback browser; app.js renders data, style.css controls appearance, and index.html contains the interface.

tests/integration/test_aws_mcp_readiness.py is the read-only live smoke test. Other tests cover individual features and some deliberately call write endpoints. examples/local_demo/ contains the earlier SQLite prototype and its Docker files. The older query source is retained only in local-only/. The archive producer code developed in AWS should be exported and compared before any future redeployment.

## 08. Problems encountered and what the fixes taught us

The archive failure destination initially lacked S3 PutObject permission. The execution role was verified and a prefix-scoped permission was added. Lesson: identify the actual actor and resource; logging into an AWS account does not imply the function role can write an object.

The simulator authenticated correctly as ring-project-developer but PutRecords was denied. A narrow grant on the one stream resolved it. PutRecord and PutRecords are different actions. The sender now distinguishes explicit denial from uncertain delivery rather than treating every exception as proof that nothing arrived.

MCP discovery succeeded while later calls failed due to AWS credentials. A credential process needed the region; later sessions expired and required renewal. Lesson: tool discovery exercises the local protocol, whereas a call to AWS also requires valid credentials and service authorization. The full path must be tested separately.

A pg8000 import failure showed that a function receiving database code also needed the database dependency layer. Query and recovery deployment files must not be interchanged. Recovery intentionally delegates reads rather than taking a database dependency itself.

The first operator profile resolved to the developer identity. Checking the STS ARN revealed the issue; signing into the intended approver identity fixed it. Profile names are local labels, not proof of independent authority. The access tests then exercised allowed operator URL calls and denied MCP/anonymous access.

Several approvals expired during the staged build. The system correctly rejected new work with expired authorization. Fresh intent requires a fresh proposal and review; editing expiry or silently renewing an approval would defeat the boundary.

The dashboard initially did not fetch data on page reload; an initial read fixed it. A stopped simulator later made devices suspected_offline, which was expected aging behavior. Some AI summaries confused daily counts with health and invented temporal conclusions. Tool descriptions, prompts and the runbook now emphasize evidence scope, late arrivals, null values and uncertainty. A fluent summary still needs checking against tool output.

## 09. Test evidence and honest reliability limits

Normal streaming session 71d5f5fddfa74e59ad11f3cf104a9139 produced six accepted events per device, 18 total. Silence session 1da48988b6c8499285400e64b9171052 produced counts 6/1/6. At the fixed end cutoff and one-minute threshold, camera-002 was suspected_offline while the other two reported.

Invalid-battery session dd3ff42652e24796940b2f46ebdff6e0 increased the dashboard rejection count; you confirmed the expected SQL results. Duplicate session 6709a6e4d9f247e595b8f29249fe3b2d increased the duplicate count by one, while SQL showed one stored event per device. The original approved replay also has direct SQL evidence of one corrected event and preserved rejection.

The missing-source test recorded a failed NoSuchKey run with zero input, accepted, duplicate and rejected counts, and no event/rejection rows. It verifies failure bookkeeping for that condition. It does not cover a timeout in the middle of a database commit or every stream retry outcome.

The two-object duplicate test produced one accepted event and one duplicate, but the attempts did not overlap. Therefore concurrent database insert behavior is not demonstrated. Hard timeouts can leave a running row; S3 exports and PostgreSQL are not atomic. Production hardening would need fault injection, reconciliation and stronger end-to-end observability.

The final local checks passed for monitoring guardrails (five), dashboard behavior/security boundaries (four), simulator behavior (ten), and real MCP resource/prompt protocol tests with AWS calls blocked. These are local passes, not new AWS deployment evidence. Earlier recovery tests and live user-supplied outputs remain separately recorded in docs/project/checklist.md.

Final live evidence: the validator produced Errors=1 at 20:34 UTC on September 21; its alarm changed OK to ALARM at 20:36:31 UTC. SQL verified failed/NoSuchKey with zero event rows. At 20:41 UTC, MCP discovery, resources, prompt and three live read calls passed. The query reported 69 runs: 67 completed, two intentional failure-test runs and zero running; 73 inputs balanced as 62 accepted, four duplicates and seven rejected. These are observed attempt totals, not a full independent reconciliation. Production load, crash and adversarial coverage remain outside the demonstrated scope.

## 10. Operational monitoring and daily operation

The monitoring stack is deployed. For a repeat deployment, use infra/monitoring/monitoring.py from regular CloudShell with the existing setup identity. Preview is local; inspect reads the environment; deploy creates a separate ten-alarm CloudFormation stack; verify compares the actual alarm configuration with the intended metrics and dimensions. It does not change existing Lambda code, IAM policies, database contents or log retention.

The alarms cover function errors for all five Lambdas, throttles for archive/validator, archive iterator age, archive failure-destination delivery failures and stream write throttling. They use existing service metrics. The archive-age threshold is two minutes in two of three one-minute periods, a prototype choice rather than a measured production objective.

Alarm actions are disabled and no recipient is configured. These are console-visible operational indicators, not customer notifications. Missing data is treated as not breaching because the producer is often idle; therefore OK is not proof that the stream is flowing. A disabled mapping or stopped producer requires direct checks.

The runbook includes a deliberate unique missing-source invocation. Confirm NoSuchKey and Lambda FunctionError, then correlate the validator Errors alarm history and database failed-run evidence. A normal JSON error return is not the same thing as a Lambda function error. Setting an alarm state manually would test the display, not metric-driven detection.

For a real incident, begin with signal, resource and UTC time. Correlate CloudWatch request IDs, source keys, ingestion runs and producer receipts. Investigate the cause before submitting a replay. Use the supplied stale_runs.sql for old running attempts; it flags candidates, not proof of a crash. Preserve original evidence and use the established approval workflow for recovery.

Before a demo, confirm the three AWS identities and sessions. Start the local dashboard separately from the producer. The producer stops after its bounded window; the dashboard continues until Ctrl+C. Stop auto-refresh when finished. Current totals include prior deliberate failure tests, so a historical failed-run count is not automatically a new incident.

## 11. A repeatable interview demonstration

Prepare two paths: a short read-only demonstration using preserved evidence, and an optional fresh write demonstration if time, credentials and independent approval are available. Keep a saved screenshot or query result for each important claim in case AWS or the host application is unavailable.

Minute 0-1: state the use case and draw the ingestion path plus the MCP/approval path. Explain that the data is synthetic and the app is not an official Ring integration. Separate the model's decision to request a tool from backend authorization.

Minute 1-3: show tool discovery, the schema/runbook resources and the investigate_devices prompt. Select camera-002 with historical cutoff 2026-09-18T12:05:00Z. Its accepted battery 75 comes from later replay; say that the query uses current stored data filtered by event time. Do not call it proof of what the database knew on September 18.

Minute 3-5: inspect rejection 6 with original battery 150. Show the preserved successful proposal, approval identity, correction artifact, replay source, audit events and accepted row. Explain why approved, artifact saved, submission created and ingestion accepted are separate states. The historical proposal is now expired; reading evidence remains useful, but do not present it as current authorization for new work.

Minute 5-7: optionally run a bounded normal or silence simulator and refresh the dashboard. State the threshold and expected transition. Acknowledged Kinesis writes are not enough; use session-prefixed SQL counts to verify acceptance. The silence fixture can be queried read-only using 2026-09-20T03:29:55.398942Z and threshold one minute.

For a fresh correction demonstration, deliberately send a new invalid synthetic event, identify its new rejection ID, choose a replacement with a stated reason, create a fresh proposal and obtain independent approval. Then apply, replay, retry the same operation and verify the new event. Do not reuse evt-batch-003 and expect a new first acceptance; it already exists and should produce duplicate/conflict semantics as applicable.

Finish by showing the evidence checklist and one tradeoff. Avoid claiming all production failure cases are solved. Do not spend the interview reading every line; use a small code trace to explain validation, authorization, idempotency and evidence flow.

## 12. Interview questions and answers to practice

Why MCP rather than letting an agent run SQL? MCP gives the host a discoverable interface with named operations and schemas. The server exposes bounded domain operations rather than arbitrary SQL. Parameterized queries, configured household scope and ring_reader enforce the read boundary underneath that interface.

Why keep approval separate? If the same model-accessible identity could approve its own proposal, the approval gate would add little protection. A separate authenticated operator reviews the exact content. Tool annotations and prompt instructions alone are not access controls.

What is idempotency here? An identical event retry does not create another accepted event. Repeated artifact requests reuse fixed identities and verified existing content. The same report key returns the original observation. These are specific application guarantees; transport can redeliver and attempts are still counted.

What happens if the preview says accepted but replay later fails? Preview is a read against a particular database snapshot. State, source evidence, permissions or infrastructure can change. Replay rechecks required evidence and the normal validator processes the submitted event. The client reports submission and observed ingestion separately.

Why not update the rejected row or overwrite the event? Original rejection evidence explains what arrived and why it failed. A separate correction and replay preserve that history, reuse validation and provide source lineage. Already accepted conflicting content is not silently replaced.

What is the main tradeoff? More evidence, approval and recovery safety add services, states and latency. That is useful for a support investigation prototype, but a simpler read-only system would need fewer components. S3 plus PostgreSQL also creates reconciliation work because they are not one transaction.

What would you improve for production? Test concurrent and crash cases, improve request-level attribution, move configuration/secret handling into managed deployment controls, add reconciliation and notifications, load-test database connections, and implement authenticated multi-user hosting. Prioritize based on actual risk and service objectives instead of adding unrelated tools.

How does this generalize? The MCP boundary, scoped queries, evidence inspection and approval pattern can support subscription or finance investigations. Those domains still require their own schemas, correctness rules, authorization and tests. Adding names to a tool list is not enough to implement a new use case.

## 13. Completion, costs, cleanup and references

The bounded prototype is complete: functional workflow, monitoring deployment, controlled failure detection, database verification and the final live read-only smoke all have evidence. Documentation is reconciled. This does not mean every production extension in the old aspirational checklist was implemented. docs/project/checklist.md explicitly identifies deferred coverage. Interview rehearsal and checking actual account spend remain follow-up activities.

For current spend, use AWS Billing/Cost Explorer and inspect remaining credits. RDS, Kinesis, S3 requests/storage, Lambda, logs and monitoring can incur charges. The monitoring package adds ten alarm metrics and no custom metric publisher or separate dashboard. Free-tier eligibility and credits are account-specific; no current balance or zero-cost promise is available from this workspace.

When the demonstration is finished, stop the simulator and local dashboard, and disable browser auto-refresh. Save the needed test evidence and decide whether to retain a database backup. Stopping local processes does not remove cloud costs. Review the monitoring stack, stream, RDS, retained snapshots, S3 objects/versions, log groups and any networking resources in the actual account before cleanup.

Delete resources only after deciding which evidence must survive. Deleting the dedicated ring-operational-monitoring stack removes only its alarms. Retained S3 objects, database backups and logs can continue costing money. Do not run blanket account cleanup or publish credentials with an interview portfolio. Keep secrets and session logs out of a public repository, and use synthetic examples.

Primary technical references used for the monitoring design:

- AWS Lambda metric definitions: https://docs.aws.amazon.com/lambda/latest/dg/monitoring-metrics-types.html
- Kinesis monitoring: https://docs.aws.amazon.com/streams/latest/dev/monitoring-with-cloudwatch.html
- CloudWatch missing-data handling: https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/alarms-and-missing-data.html
- CloudFormation alarm reference: https://docs.aws.amazon.com/AWSCloudFormation/latest/TemplateReference/aws-resource-cloudwatch-alarm.html
- CloudWatch pricing: https://aws.amazon.com/cloudwatch/pricing/
- PutRecords behavior: https://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecords.html

Evidence basis: the local implementation, user-supplied AWS/SQL/Claude outputs through September 21, and the explicitly identified local checks. Final monitoring and smoke evidence is retained with the project. Live state can change after any observation. Keep the generated-at timestamps, session IDs and source references alongside conclusions.
