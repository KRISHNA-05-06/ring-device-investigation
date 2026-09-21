"""Small, reviewable CloudWatch deployment. Preview is entirely local.

CloudShell: python3 monitoring.py preview | deploy | verify
Only deploy writes to AWS, creating a separate CloudFormation alarm stack.
No IAM, source, database, Lambda configuration, or log-retention changes.
"""
import argparse
import json
import subprocess
import tempfile
from pathlib import Path

ACCOUNT = "455646290306"
REGION = "us-east-1"
STACK = "ring-operational-monitoring"
FUNCTIONS = ["ring-stream-archive", "ring-device-validator", "ring-device-query",
             "ring-device-recovery", "ring-device-approval"]


def specifications():
    items = []
    for index, function in enumerate(FUNCTIONS):
        items.append((f"FunctionErrors{index}", function + "-errors", "AWS/Lambda",
                      "Errors", "FunctionName", function, "Sum", 1, 1, 1))
    for index, function in enumerate(FUNCTIONS[:2]):
        items.append((f"IngestionThrottles{index}", function + "-throttles", "AWS/Lambda",
                      "Throttles", "FunctionName", function, "Sum", 1, 1, 1))
    items.extend([
        ("ArchiveLag", "archive-age", "AWS/Lambda", "IteratorAge", "FunctionName",
         "ring-stream-archive", "Maximum", 120000, 3, 2),
        ("FailureDestination", "archive-destination-failures", "AWS/Lambda",
         "DestinationDeliveryFailures", "FunctionName", "ring-stream-archive", "Sum", 1, 1, 1),
        ("ProducerThrottle", "stream-write-throttles", "AWS/Kinesis",
         "WriteProvisionedThroughputExceeded", "StreamName", "ring-events", "Sum", 1, 1, 1),
    ])
    return items


def template():
    resources = {}
    for logical, suffix, namespace, metric, dim, target, stat, threshold, periods, points in specifications():
        resources[logical] = {
            "Type": "AWS::CloudWatch::Alarm",
            "Properties": {
                "AlarmName": f"{STACK}-{suffix}",
                "AlarmDescription": (
                    f"Ring synthetic prototype: {metric} on {target}. "
                    "Console-only alarm; consult the operational runbook. Missing data is not health proof."
                ),
                "Namespace": namespace, "MetricName": metric,
                "Dimensions": [{"Name": dim, "Value": target}],
                "Statistic": stat, "Period": 60,
                "EvaluationPeriods": periods, "DatapointsToAlarm": points,
                "Threshold": threshold, "ComparisonOperator": "GreaterThanOrEqualToThreshold",
                "TreatMissingData": "notBreaching", "ActionsEnabled": False,
                "AlarmActions": [], "OKActions": [], "InsufficientDataActions": [],
                "Tags": [{"Key": "Project", "Value": "ring-device-investigation"}],
            },
        }
    return {"AWSTemplateFormatVersion": "2010-09-09",
            "Description": "Ten console-only alarms for the existing Ring synthetic prototype.",
            "Resources": resources}


def aws(arguments, profile=None, parse=True):
    command = ["aws", *arguments, "--region", REGION, "--no-cli-pager"]
    if profile:
        command += ["--profile", profile]
    if parse:
        command += ["--output", "json"]
    completed = subprocess.run(command, capture_output=True, text=True, check=False)
    if completed.returncode:
        raise RuntimeError(completed.stderr.strip() or completed.stdout.strip())
    return json.loads(completed.stdout) if parse else completed.stdout.strip()


def identity(profile):
    actor = aws(["sts", "get-caller-identity"], profile)
    if actor.get("Account") != ACCOUNT:
        raise RuntimeError(f"Wrong account; expected {ACCOUNT}. Nothing deployed.")
    print(json.dumps({"account": actor["Account"], "actor": actor["Arn"], "region": REGION}))


def inspect_resources(profile):
    # Select only operational fields; never retrieve environment-variable contents in output.
    for function in FUNCTIONS:
        item = aws(["lambda", "get-function-configuration", "--function-name", function,
                    "--query", "{name:FunctionName,state:State,timeout:Timeout,logging:LoggingConfig}"], profile)
        if item.get("state") != "Active":
            raise RuntimeError(f"{function} is not Active; inspect its deployment before monitoring.")
        print(json.dumps(item))
    stream = aws(["kinesis", "describe-stream-summary", "--stream-name", "ring-events",
                  "--query", "StreamDescriptionSummary.{status:StreamStatus,arn:StreamARN}"], profile)
    if stream.get("status") != "ACTIVE":
        raise RuntimeError("ring-events is not ACTIVE")
    print(json.dumps(stream))
    mappings = aws(["lambda", "list-event-source-mappings", "--function-name", "ring-stream-archive",
                    "--query", "EventSourceMappings[].{uuid:UUID,state:State,source:EventSourceArn,destination:DestinationConfig}"], profile)
    expected = f"arn:aws:kinesis:{REGION}:{ACCOUNT}:stream/ring-events"
    if not any(m.get("source") == expected and m.get("state") == "Enabled" for m in mappings):
        raise RuntimeError("No enabled archive mapping for ring-events; fix the mapping first.")
    print(json.dumps({"mappings": mappings}))
    groups = aws(["logs", "describe-log-groups", "--log-group-name-prefix", "/aws/lambda/ring-",
                  "--query", "logGroups[].{name:logGroupName,retention_days:retentionInDays,stored_bytes:storedBytes}"], profile)
    print(json.dumps({"log_groups": groups,
                      "note": "Null retention means no expiry. This script does not delete logs or change retention."}))


def verify(profile):
    stack = aws(["cloudformation", "describe-stacks", "--stack-name", STACK,
                 "--query", "Stacks[0].{status:StackStatus}"], profile)
    if stack["status"] not in {"CREATE_COMPLETE", "UPDATE_COMPLETE"}:
        raise RuntimeError(f"Stack not successfully deployed: {stack['status']}")
    expected = template()["Resources"]
    names = [item["Properties"]["AlarmName"] for item in expected.values()]
    response = aws(["cloudwatch", "describe-alarms", "--alarm-names", *names], profile)
    actual = {item["AlarmName"]: item for item in response["MetricAlarms"]}
    fields = ["Namespace", "MetricName", "Dimensions", "Statistic", "Period", "EvaluationPeriods",
              "DatapointsToAlarm", "Threshold", "ComparisonOperator", "TreatMissingData", "ActionsEnabled",
              "AlarmActions", "OKActions", "InsufficientDataActions"]
    for resource in expected.values():
        props = resource["Properties"]
        item = actual.get(props["AlarmName"])
        if item is None:
            raise RuntimeError(f"Missing alarm: {props['AlarmName']}")
        for field in fields:
            if item.get(field) != props[field]:
                raise RuntimeError(f"Configuration mismatch: {props['AlarmName']} / {field}")
        print(json.dumps({"alarm": item["AlarmName"], "state": item["StateValue"],
                          "reason": item.get("StateReason")}))
    print("PASS: all ten alarm configurations match. This does NOT prove a live failure was detected.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["preview", "inspect", "deploy", "verify"], nargs="?", default="preview")
    parser.add_argument("--profile", help="Omit in CloudShell to use its signed-in identity")
    args = parser.parse_args()
    if args.action == "preview":
        print(json.dumps(template(), indent=2))
        return
    identity(args.profile)
    if args.action == "inspect":
        inspect_resources(args.profile)
    elif args.action == "deploy":
        inspect_resources(args.profile)
        with tempfile.TemporaryDirectory(prefix="ring-monitoring-") as directory:
            path = Path(directory) / "alarms.json"
            path.write_text(json.dumps(template()), encoding="utf-8")
            aws(["cloudformation", "validate-template", "--template-body", "file://" + str(path)], args.profile)
            print(aws(["cloudformation", "deploy", "--stack-name", STACK, "--template-file", str(path),
                       "--no-fail-on-empty-changeset", "--tags", "Project=ring-device-investigation"],
                      args.profile, parse=False))
        verify(args.profile)
    else:
        verify(args.profile)


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, OSError, ValueError, KeyError) as error:
        raise SystemExit("FAIL: " + str(error))
