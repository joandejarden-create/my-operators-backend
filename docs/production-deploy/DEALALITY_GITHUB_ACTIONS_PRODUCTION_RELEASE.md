# Dealality production release — GitHub Actions (canonical)

**Sole live production entry point** for ADP, AVA, and GDI.

Workflow: `.github/workflows/dealality-production-release.yml`  
Name: **Dealality production release**

## Why Actions

Independent Cursor sessions cannot share a durable local filesystem lock. GitHub Actions `concurrency.group: dealality-production-release` with `cancel-in-progress: false` queues overlapping releases and **never auto-cancels** a running Railway deployment.

## Dispatch

```bash
gh workflow run dealality-production-release.yml \
  -f product=AVA \
  -f approved_sha=8eabf043cbc350322a5aca5f985bcc8a8ff75715 \
  -f authorization_phrase=I_AUTHORIZE_PRODUCTION_RELEASE \
  -f dry_run=true
```

Live deploy: set `dry_run=false` only after explicit founder authorization of the **exact artifact SHA**.

Required repo secret for live path: `RAILWAY_TOKEN`.

## Gates

1. Authorization phrase must be exactly `I_AUTHORIZE_PRODUCTION_RELEASE`
2. Exact 40-char `approved_sha` checkout (clean tree)
3. SHA must be on `origin/main` ancestry
4. In-process release lock + Actions concurrency
5. Non-terminal Railway → fail `RAILWAY_BUSY` (wait; do not cancel)
6. Live channel: `DEALALITY_RELEASE_CHANNEL=github-actions`

## Local CLI

- **Allowed:** `npm run deploy:production:dry-run` (no railway up)
- **Blocked:** local live `npm run deploy:production`, raw `railway up`
- Helper: `scripts/Run-Dealality-Production-Release.ps1` prints the `gh workflow run` command; local path is dry-run only

## Rollback

Re-dispatch the same workflow with the prior known-good artifact SHA. Do not cancel in-flight Railway builds.

## AVA V4 note

Control-plane commits may advance `main` after this workflow lands. The **artifact SHA** to deploy for AVA V4 remains the certified SHA authorized by the founder (e.g. `8eabf043cbc350322a5aca5f985bcc8a8ff75715`), which the workflow checks out explicitly.
