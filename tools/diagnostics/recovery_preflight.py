"""Collect scoped recovery configuration for review without changing AWS resources."""

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError


ACCOUNT = "455646290306"
REGION = "us-east-1"
BUCKET = "ring-device-investigation-455646290306-us-east-1"
USERS = ("ring-project-developer", "ring-project-approver")
ROLES = ("ring-recovery-role", "ring-approval-role")
FUNCTIONS = ("ring-device-recovery", "ring-device-approval")


class Collector:
    def __init__(self, session):
        config = Config(connect_timeout=5, read_timeout=15,
                        retries={"mode": "standard", "total_max_attempts": 2})
        self.clients = {name: session.client(name, region_name=REGION, config=config)
                        for name in ("sts", "iam", "lambda", "s3")}
        self.errors = []
        self.managed = {}

    def read(self, service, operation, **arguments):
        try:
            response = getattr(self.clients[service], operation)(**arguments)
            return {key: value for key, value in response.items() if key != "ResponseMetadata"}
        except (ClientError, BotoCoreError) as error:
            code = error.response["Error"]["Code"] if isinstance(error, ClientError) else type(error).__name__
            # These responses mean the optional configuration is absent, not unreadable.
            if (operation == "get_bucket_policy" and code == "NoSuchBucketPolicy") or (
                    operation in {"get_policy", "get_function_url_config"}
                    and service == "lambda" and code == "ResourceNotFoundException"):
                return {"present": False}
            self.errors.append({"service": service, "operation": operation,
                                "arguments": arguments, "error_code": code})
            return {"unavailable": True, "error_code": code}

    def items(self, operation, result_key, **arguments):
        values = []
        marker = None
        while True:
            page = self.read("iam", operation, **arguments, **({"Marker": marker} if marker else {}))
            if page.get("unavailable"):
                return values
            values.extend(page.get(result_key, []))
            if not page.get("IsTruncated"):
                return values
            next_marker = page.get("Marker")
            if not next_marker or next_marker == marker:
                self.errors.append({"operation": operation, "error_code": "INVALID_PAGINATION"})
                return values
            marker = next_marker

    def managed_policy(self, arn):
        if arn not in self.managed:
            policy = self.read("iam", "get_policy", PolicyArn=arn)
            version = policy.get("Policy", {}).get("DefaultVersionId")
            self.managed[arn] = {
                "metadata": policy,
                "default_version": self.read("iam", "get_policy_version", PolicyArn=arn, VersionId=version)
                if version else {"unavailable": True},
            }

    def identity(self, kind, name):
        argument = {kind.title() + "Name": name}
        metadata = self.read("iam", "get_" + kind, **argument)
        if kind == "group" and not metadata.get("unavailable"):
            metadata = {"Group": metadata.get("Group", {})}
        inline_names = self.items("list_" + kind + "_policies", "PolicyNames", **argument)
        attached = self.items("list_attached_" + kind + "_policies", "AttachedPolicies", **argument)
        inline = {policy: self.read("iam", "get_" + kind + "_policy", **argument, PolicyName=policy)
                  for policy in inline_names}
        for policy in attached:
            self.managed_policy(policy["PolicyArn"])
        boundary = metadata.get(kind.title(), {}).get("PermissionsBoundary", {}).get("PermissionsBoundaryArn")
        if boundary:
            self.managed_policy(boundary)
        result = {"metadata": metadata, "inline_policies": inline, "attached_policies": attached}
        if kind == "user":
            groups = self.items("list_groups_for_user", "Groups", UserName=name)
            result["groups"] = {group["GroupName"]: self.identity("group", group["GroupName"])
                                for group in groups}
        return result

    def collect(self):
        caller = self.read("sts", "get_caller_identity")
        if caller.get("Account") != ACCOUNT:
            raise RuntimeError("Cannot verify the expected AWS account; no project configuration was read.")
        report = {
            "collected_at": datetime.now(timezone.utc).isoformat(),
            "account": ACCOUNT, "region": REGION, "caller": caller,
            "mode": "read_only_configuration_collection",
            "users": {name: self.identity("user", name) for name in USERS},
            "roles": {name: self.identity("role", name) for name in ROLES},
            "functions": {}, "bucket": BUCKET,
            "limitations": [
                "This report is evidence for policy review, not proof that access is denied.",
                "Organization policies, session policies, other assumable roles, and KMS key policies are not collected.",
                "Missing read permissions make this report incomplete; they do not prove the target action is denied.",
            ],
        }
        for name in FUNCTIONS:
            config = self.read("lambda", "get_function_configuration", FunctionName=name)
            # Never include Lambda environment variables, which may contain credentials.
            public_config = {key: config[key] for key in (
                "FunctionName", "FunctionArn", "Role", "Runtime", "Handler", "LastModified",
                "RevisionId", "CodeSha256", "VpcConfig", "unavailable", "error_code") if key in config}
            report["functions"][name] = {
                "configuration": public_config,
                "resource_policy": self.read("lambda", "get_policy", FunctionName=name),
                "function_url": self.read("lambda", "get_function_url_config", FunctionName=name),
            }
        report["bucket_configuration"] = {
            "policy": self.read("s3", "get_bucket_policy", Bucket=BUCKET),
            "versioning": self.read("s3", "get_bucket_versioning", Bucket=BUCKET),
            "notifications": self.read("s3", "get_bucket_notification_configuration", Bucket=BUCKET),
        }
        report["managed_policies"] = self.managed
        report["errors"] = self.errors
        report["collection_complete"] = not self.errors
        return report

    def close(self):
        for client in self.clients.values():
            client.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", help="Optional AWS profile; omit in administrator CloudShell")
    parser.add_argument("--output", default="recovery_preflight_report.json")
    args = parser.parse_args()
    collector = Collector(boto3.Session(profile_name=args.profile, region_name=REGION))
    try:
        report = collector.collect()
        output = Path(args.output).resolve()
        output.write_text(json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8")
        print(f"Saved read-only permissions report: {output}")
        print(f"Collection complete: {report['collection_complete']}")
        print(f"Unresolved read errors: {len(report['errors'])}")
        print("No approval, correction, replay, or AWS configuration change was performed.")
    finally:
        collector.close()


if __name__ == "__main__":
    main()
