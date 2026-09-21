# Module scope

One completed functional use case: investigate synthetic device telemetry through MCP and perform an independently approved correction/replay. Operational alarm deployment, one controlled alarm-detection test with SQL verification, and the final read-only MCP smoke check are complete; see docs/project/status.md.

Included: existing AWS ingestion pipeline, four PostgreSQL tables, 17 MCP tools, resources/prompts, independent operator approval, S3 reports, bounded streaming simulator and local read-only dashboard.

Excluded: official Ring app/hardware integration; subscriptions/payments; other business domains; always-on cloud simulator; automatic customer notifications; production hosting or production-readiness certification.

Accepted events are immutable under the application workflow. Recovery writes a separate artifact and submits through the validator. Source/recovery evidence is protected by scoped permissions and conditional writes where implemented; this is not a claim of S3 Object Lock or privileged-administrator immutability.

Remaining unverified production failure cases are disclosed in docs/project/checklist.md. We will not expand those into new product scope merely to claim 100 percent.
