# Local investigation dashboard

The dashboard displays device health, ingestion totals, rejection summaries, daily metrics, and the latest 20 ingestion runs. Its Python server invokes the Query Lambda through the shared AWS adapter. Claude uses the MCP protocol; the dashboard calls the adapter directly. All dashboard operations are read-only.

## Start from the project PowerShell directory

```powershell
.\.venv-aws\Scripts\python.exe 'apps/dashboard/dashboard.py'
```

Open http://127.0.0.1:8765/ in your browser. Data loads automatically once on page open or browser reload; Refresh data requests another observation. Continuous polling remains opt-in. The default profile is ring-mcp; a nonfatal Pydantic warning may appear when the existing MCP module is imported. If credentials expire, renew the corresponding AWS login and restart the dashboard. Keep this terminal open. Ctrl+C stops the server. Unlike the simulator this server does not auto-stop. Each initial load or refresh makes four read-only query calls. Stopping the simulator does not delete devices or accepted events.

Use --demo for an explicitly labeled offline layout preview (no AWS imports or calls), or --port 8766 if the default port is occupied. Offline sample results are fictional and not deployment evidence.

## First live check

Click Load verified silence example. This sets health cutoff to 2026-09-20T03:29:55Z and threshold 1 minute. The previously verified data should yield camera-001 reporting, camera-002 suspected offline, doorbell-001 reporting. Current data includes late arrivals and replay; this is not a historical database reconstruction. Historical health selection does not filter current pipeline totals or rejection occurrences. The daily report has its own UTC date selector.

All-recorded failed runs include the intentional missing-source test. Do not infer a newly broken pipeline from that historical count. Independent panels use separate query invocations, not one shared database transaction. Each panel shows query time, unavailable errors, and relevant truncation. The latest successful ingestion timestamp is database evidence, not a Kinesis iterator-age measurement or delivery guarantee.

## Live simulator demonstration

Keep the dashboard server running in one terminal. In another terminal run the three-minute stream simulator. In the dashboard select Current time and optionally Refresh every 15s. That checkbox sends four read-only Lambda invocations on each refresh, roughly 16 invocations/minute while the page is visible. It defaults off and pauses when the page is hidden. It is disabled for historical mode. Reads can incur AWS usage. No overlap is allowed per server refresh; stale data and partial query failures are labeled.

The simulator stops generating automatically, but AWS processes already-submitted records afterward. The dashboard can continue observing this. With no new events, device heartbeat ages grow; after the selected threshold, devices become suspected offline. This is expected demo behavior.

## Design and boundaries

Sidebar navigation smoothly scrolls to Overview, Daily metrics or the actual ingestion evidence table. The active heading tracks manual scrolling, preserves the requested selection during animated navigation, and exposes aria-current for assistive technology. Reduced-motion settings disable animations. After changing these static files, refresh the browser page to load the new navigation; restarting Python is unnecessary.

- dashboard.py validates three query parameters, runs exactly four fixed read operations, and serves three fixed assets. It binds only 127.0.0.1, checks Host/Origin and a per-process request token, and exposes no arbitrary operation or filesystem path.
- app.js renders returned strings as text, never as executable markup. AWS credentials stay in Python. The browser gets only query results and a local request token, not AWS credentials.
- index.html and style.css provide the presentation, accessible controls and responsive layout.
- test_dashboard.py checks request validation, scoped response handling, partial failures, and HTTP restrictions with mocked AWS.

Loopback controls are demo safeguards, not a production multi-user authentication system. Do not expose this server through a public tunnel. Production hosting needs an authenticated web service, appropriate identity/household authorization, rate limits and an application server.

Correction and replay use the MCP recovery tools, with approval handled by the independent operator client. The dashboard has no write operations or embedded model.
