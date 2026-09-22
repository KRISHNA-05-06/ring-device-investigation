# Security boundaries and publication

This is a single-household application using synthetic events. It is not an official Ring
integration or a production service. MCP annotations describe tools; backend validation and IAM
enforce access. Approval requires a separate operator identity. Secrets must never be supplied by
the host model or committed to Git.

Original AWS account IDs, bucket names, IAM principal IDs and fixed resource ARNs occur in
source and test fixtures. These are deployment identifiers, not credentials. They are retained
because several integrity and identity checks bind to this demonstration deployment. A new AWS
deployment must update these bindings consistently; changing a profile name alone is insufficient.
The approval URL is loaded from private local configuration or an environment variable.

Do not publish AWS configuration directories, access keys, passwords, signed URLs, tokens,
database exports or local session logs. `local-only/`, environments and generated outputs are ignored.
The repository's pattern scan is a publication aid, not a guarantee that every secret has been detected.

For a security issue, contact the repository owner privately. Do not include credentials in public issues.
