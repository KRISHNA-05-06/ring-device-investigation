# Deployment boundaries

AWS deployment is separate from Git commits and pushes. This repository contains service source,
database definitions, IAM policies, and monitoring deployment tooling. It does not include complete
infrastructure provisioning or the archive Lambda source.

| Component | Canonical source | Requirements |
|---|---|---|
| MCP | `services/mcp/aws_mcp_server.py` | Local Python and AWS profile; root launcher preserves Claude setup |
| Query Lambda | `services/lambdas/query/query_lambda.py` | Python 3.13, pg8000, RDS CA bundle, VPC, ring_reader |
| Validator Lambda | `services/lambdas/validator/validator_lambda.py` | Python 3.13, pg8000, CA bundle, VPC, ingestion database credentials |
| Recovery Lambda | `services/lambdas/recovery/recovery_lambda.py` | Scoped S3 and query-Lambda access |
| Approval Lambda | `services/lambdas/approval/approval_lambda.py` | AWS_IAM URL, separate operator, APPROVER_ARN |
| Archive Lambda | Not exported here | Inspect deployed artifact before recreating or replacing |
| Monitoring | `infra/monitoring/monitoring.py` | Generates/deploys a ten-alarm CloudFormation stack |

Deployed Lambdas use `lambda_function.lambda_handler`. Place the selected source at the ZIP root as
`lambda_function.py`, or deliberately change the configured handler. Dependency files describe
required packages; a Python file alone does not supply a dependency layer or CA bundle.

Database configuration uses DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, DB_SSLROOTCERT and
HOUSEHOLD_ID. Supply real values through AWS, never Git. Review each function's exact requirements.
The policies under `infra/iam/` were used incrementally; they are not a complete IAM stack. Review
their composition with existing policies. Preserve the operator/MCP/ingestion permission boundaries.

Review `infra/database/schema.sql` before executing. No database migration runs automatically.
For a new AWS account, update deployment bindings consistently across source, policies and tests.
Changing only an AWS profile name is insufficient. Offline tests do not prove a new deployment works.
