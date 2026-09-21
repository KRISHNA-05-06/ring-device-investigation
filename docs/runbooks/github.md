# Publish when you are ready

The local repository is initialized on `main`. No remote, commit or push was created by this
reorganization. Continue working in this same folder.

Before publishing:

```powershell
.\.venv-aws\Scripts\python.exe scripts/test_offline.py
.\.venv-aws\Scripts\python.exe scripts/check_publication.py
git status --short
git add .
git diff --cached --stat
```

Inspect the staged files. `local-only/`, PDFs, environments, session logs and personal configuration
must not appear. Do not use `git add -f`. AWS deployment identifiers remain in source as documented
in SECURITY.md; they are not credentials. The live approval endpoint is excluded.

Create an empty GitHub repository using your chosen visibility. Then, if your Git author identity is
already configured, commit and connect the remote. Replace YOUR_GITHUB_USERNAME before using:

```powershell
git commit -m "Add MCP device investigation and recovery prototype"
git remote add origin https://github.com/YOUR_GITHUB_USERNAME/ring-device-investigation.git
git push -u origin main
```

Alternatively, use the source-control panel in your editor to review and commit, and publish through
your preferred GitHub client. Do not upload the whole folder as an unfiltered ZIP: manual uploads
can bypass `.gitignore`. Publish through Git so the local-only files stay on your machine.

After publishing, keep editing, testing, committing and pushing from this folder. The GitHub workflow
runs offline tests only. Lambda changes still need a separate deployment. No open-source license has
been selected for you; choose one deliberately if you want to grant reuse rights.
