# Inter-prompt non-regression enforcement — Founder Report

**Status:** `DEALALITY_INTER_PROMPT_NON_REGRESSION_ENFORCED — INTEGRATED_ON_8080 — AWAITING_FOUNDER_REVIEW`  
**Date:** 2026-11-27  
**Workspace:** `C:\Dev\dealality\ws-gdi`  
**Branch (pre-merge):** `feat/gdi-inter-prompt-non-regression-enforcement-20261127`

## Exact cause of repeated regression

Port **8080** was owned by a **foreign process**:

```text
node -r dotenv/config C:\Dev\dealality\_verify_pr627_main\server.js
```

while claiming authorized ownership via **Integration cwd** (`my-operators-backend`) + `DEALALITY_8080_OWNER=integration`.

| Field | Observed |
|---|---|
| Live PID (pre-restore) | 49704 |
| Command line | `_verify_pr627_main\server.js` |
| runtime-identity basename | `my-operators-backend` |
| ownership code | `AUTHORIZED_INTEGRATION_HOME` (false positive) |
| Git branch (script tree) | `feat/he-contact-column-gdi-parity-v1` |
| Served HTML bust | `gdi-contact-coverage-scorecard-v1` (regressed cumulative UX) |
| ws-gdi meta PID | stale `48564` claiming AUTHORIZED_GDI_HOME |

**Why inventory appeared gone:** STAGED tip `…46de4104` remained on the durable GDI package root, but the **UI shell** came from the foreign public tree (missing/incomplete cumulative ES/List chrome). Founder saw 3-tab-era behavior / 0 UX even with `preview=staged`.

## Why permanent controls failed

| Control | Failure mode |
|---|---|
| `DEALALITY_CANONICAL_PORT_8080_OWNERSHIP_V1` | Checked **cwd only**, not **server.js directory** |
| Launcher meta PID | Written once; not re-validated against live OS listener |
| Integrated delivery docs/rules | Documented but not executed as mandatory pre-prompt gate |
| Feature HE verify tree | Could bind `:8080` by running foreign `server.js` from Integration cwd |
| Relative `server.js` spawn | Command line lacked absolute path → audits ambiguous |

Documentation existed; **runtime path-identity + live OS audit + assignment preflight** did not fully close the mismatch class.

## Corrective runtime enforcement

1. Ownership requires **cwd AND serverScriptDir** match (`PORT_8080_SERVER_SCRIPT_MISMATCH`).
2. Launcher spawns **absolute** `…\ws-gdi\server.js`; verifies ES HTML + identity script basename.
3. `auditLiveCanonical8080Listener` — OS command-line audit.
4. `npm run dealality:canonical-8080-guard` — reclaim foreign listeners.
5. `npm run dealality:assignment-preflight` — mandatory pre-implementation gate (`DEALALITY_INTER_PROMPT_NON_REGRESSION_V1`).
6. Cumulative manifest: `lib/dealality-dev-os/gdi-cumulative-functionality-manifest-v1.json`.

## Mandatory pre-prompt validation evidence

```powershell
cd C:\Dev\dealality\ws-gdi
npm run dealality:assignment-preflight -- --stream gdi
# PASS workspace_home, origin_main, git_preflight, manifest, live_8080_listener, runtime_identity
```

## Canonical integrated environment (post-restore)

| Field | Value |
|---|---|
| Path | `C:\Dev\dealality\ws-gdi` |
| Ownership | `AUTHORIZED_GDI_HOME` |
| Script basename | `ws-gdi` |
| cwdMatchesServerScript | true |
| Wyndham STAGED | `gdi_hotel_wide_fqo_recnPB3Q02NO8Ma1o_20261127_46de4104` |
| Count | **26** (validator locked-ID parity) |
| Competitive | **3** (UI Market Scope) |
| CURRENT | unpublished |
| HTML bust | `gdi-es-outlook-12mo-kpis-20261127` |

## Screenshots

- `screenshots/01-restored-opportunities-26-competitive-3.png` — four tabs + All(26) + Competitive(3)
- `screenshots/02-four-tabs-es-present-26.png` — ES tab present in compact left cluster

## Exact PowerShell commands

```powershell
cd C:\Dev\dealality\ws-gdi
npm run dealality:assignment-preflight -- --stream gdi
npm run test:canonical-port-8080-ownership-v1
npm run test:dealality-inter-prompt-non-regression-v1
npm run test:gdi-cumulative-integrated-staging-non-regression-v1
npm run dealality:canonical-8080-guard -- --once
npm run dealality:gdi-local-8080
npm run dealality:validate-gdi-cumulative-8080
# Feature work (never 8080):
#   $env:PORT=8095; $env:DEALALITY_ALLOW_WORKTREE_SERVER=1; node server.js
```

## Permanent regression tests

- `test:canonical-port-8080-ownership-v1` (includes Integration-cwd + foreign script denial)
- `test:dealality-inter-prompt-non-regression-v1`
- `test:gdi-cumulative-integrated-staging-non-regression-v1`
- `dealality:validate-gdi-cumulative-8080`

No paid research. No CURRENT. No production deploy. Dirty WIP preserved.
