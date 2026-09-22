# Operational monitoring

CloudWatch monitors Lambda failures, throttling, stream processing delay, and failure-destination delivery. The deployment tool manages a dedicated CloudFormation stack and verifies its alarm configurations. The failure exercise below checks that a real validator exception produces a metric-driven alarm transition.

## What is added

One CloudFormation stack, `ring-operational-monitoring`, containing ten standard-resolution CloudWatch alarms:

| Signal | Resources | Condition | Meaning |
| --- | --- | --- | --- |
| Errors | All five project Lambdas | Sum >= 1 in one 60-second period | A function/runtime exception occurred |
| Throttles | Archive and validator Lambdas | Sum >= 1 in one 60-second period | An invocation was refused for capacity |
| IteratorAge | Archive Lambda | Maximum >= 120,000 ms in 2 of 3 one-minute periods | Stream processing is delayed |
| DestinationDeliveryFailures | Archive Lambda | Sum >= 1 in one 60-second period | Delivery to the configured failure destination failed |
| WriteProvisionedThroughputExceeded | ring-events | Sum >= 1 in one 60-second period | Kinesis writes encountered throttling |

These are prototype thresholds, not measured production service-level objectives. Missing data is treated as not breaching because the bounded producer is often idle. An OK alarm therefore does not prove traffic exists or all records arrived. All alarm actions are disabled and empty: no email, SMS, automatic replay, or scaling is configured. Alarms and existing metrics are visible in the CloudWatch console. This stack adds no dashboard, custom metrics, IAM permissions, or log-retention change.

The dedicated project dashboard already displays application outcomes. Its accepted/rejected/duplicate counts complement infrastructure metrics. A validation rejection or an application JSON `{status: error}` returned normally does not by itself increment Lambda Errors. Conversely, the validator's raised infrastructure exception does. These layers must be interpreted separately.

## Deploy in regular CloudShell

Use an existing setup/admin session in account 455646290306, us-east-1. Use regular CloudShell rather than the VPC database shell: deployment needs AWS control-plane access, not private PostgreSQL access. Upload infra/monitoring/monitoring.py with Actions > Upload file. If an old copy exists, choose a new filename and use that filename in the commands; do not accidentally execute the old upload.

```bash
python3 monitoring.py preview
python3 monitoring.py inspect
python3 monitoring.py deploy
python3 monitoring.py verify
```

Preview is local and makes no AWS calls. Inspect is read-only and checks identity, all five functions, the stream, its enabled mapping, and log-group retention. Deploy validates the template with AWS and deploys only the separate alarm stack. It waits for CloudFormation and then verifies metric names, dimensions, thresholds, periods and actions. Re-running an unchanged deploy is safe. Any future edits to managed alarms should be made in the template; deploying overwrites their stack-managed configuration. Never attach notification actions manually and assume a later deploy preserves them.

CloudShell already provides the AWS CLI; Python uses only its standard library. Locally, append `--profile <setup-profile>`; no local profile is assumed. A wrong AWS account aborts. AccessDenied means the setup identity needs the specific described service action; do not broaden the MCP identity to administer monitoring. If a stack operation fails, inspect its CloudFormation Events before retrying.

## Prove the alarm detects a real failure

Do this only after deploy/verify succeeds. This deliberately invokes the existing validator with a unique missing S3 input. It creates a failed ingestion-run record, not a rejected device event. It must not upload that missing object. It does not change existing heartbeat rows. Run in regular CloudShell with the setup identity:

```bash
python3 - <<'PY'
import json, uuid
from pathlib import Path
key = 'raw/reliability/monitoring-missing-source/' + uuid.uuid4().hex + '.jsonl'
Path('monitoring-missing-source.json').write_text(json.dumps({'key': key}), encoding='utf-8')
print('Keep this deliberately absent source key:', key)
PY

aws lambda invoke --function-name ring-device-validator \
  --payload fileb://monitoring-missing-source.json \
  --region us-east-1 --no-cli-pager monitoring-failure-response.json
cat monitoring-failure-response.json
```

Expected: invocation metadata contains FunctionError and the response identifies NoSuchKey. HTTP 200 from the invocation API alone is not a successful function run. A different error requires investigation and is not a pass of this missing-source test.

After metrics arrive (allow a few minutes), open CloudWatch > Alarms in us-east-1 and inspect `ring-operational-monitoring-ring-device-validator-errors`. A transient ALARM can recover to OK before you look; inspect history:

```bash
aws cloudwatch describe-alarm-history \
  --alarm-name ring-operational-monitoring-ring-device-validator-errors \
  --history-item-type StateUpdate --max-records 10 \
  --region us-east-1 --no-cli-pager
```

Save the invocation response and the history entry whose time matches this test. In ring-db-shell query ingestion_runs for the printed source key and confirm failed/NoSuchKey, zero counts and zero accepted/rejected rows. Do not use SetAlarmState as proof of metric-driven detection. Do not rerun a failure just because the console is delayed. This verifies one alarm path, not every threshold or downstream incident response.

## Incident response

1. Identify the alarm, metric dimensions, UTC interval and whether a deliberate test caused it.
2. Open that function's CloudWatch log group (normally /aws/lambda/<function>) and correlate the request ID with the ingestion run or recovery receipt. Custom log groups, if configured, appear in inspect output.
3. For archive delay, compare IteratorAge, Lambda throttles/errors and the mapping state. An idle producer, disabled mapping or missing telemetry can evade a notBreaching alarm; inspect the mapping and producer receipts as well.
4. For validator errors, distinguish source absence, permissions, database connectivity, import failure and timeout. A failed run is different from a completed run with rejected input. A hard timeout may leave a running row.
5. Use MCP read tools for affected records and run evidence. Preserve original inputs. Recovery still requires an exact proposal, independent approval, revalidation, and post-ingestion verification.
6. After resolving the cause, confirm fresh processing and document the observation. Never clear alarms by deleting evidence.

Useful Logs Insights query (select only the relevant project log group and a narrow time interval):

```text
fields @timestamp, @requestId, @message
| filter @message like /ERROR|Processing failed|Task timed out|ImportModuleError/
| sort @timestamp desc
| limit 50
```

No match is not proof of no incident: logging formats and handled application errors differ. The query scans billable log data. Retention is only reported, not changed, to preserve existing evidence. Select a retention policy deliberately after exporting any evidence you need.

## Cost and cleanup

This adds ten alarm metrics. Account credits/free-tier eligibility and regional pricing determine charges; no free-cost guarantee is made. Existing RDS, Kinesis, S3, Lambda, logs and reads can also incur charges. Check Billing and Cost Explorer for actual spend. Stopping a local simulator does not stop RDS or delete a stream. CloudWatch alarm charges accrue while alarms exist.

When deliberately retiring monitoring, delete only the `ring-operational-monitoring` CloudFormation stack in the console. It owns only these ten alarms. That does not remove the pipeline, log groups or data. Do not delete the pipeline until the demonstration evidence and any desired database backup are saved.

References:
- https://docs.aws.amazon.com/lambda/latest/dg/monitoring-metrics-types.html
- https://docs.aws.amazon.com/streams/latest/dev/monitoring-with-cloudwatch.html
- https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/alarms-and-missing-data.html
- https://docs.aws.amazon.com/AWSCloudFormation/latest/TemplateReference/aws-resource-cloudwatch-alarm.html
- https://aws.amazon.com/cloudwatch/pricing/
