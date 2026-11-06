# AVA Permanent Audit Runner — Operator Guide (V1)

## Purpose

Run Dealality’s certified **AI Visibility Audit & Action Plan (AVA) V4** for any eligible hotel on demand, archive immutable results, and expose version history in AI Admin — without redesigning ADP measurement or creating a second methodology.

## Prerequisites

- Windows 11 + repository-supported Node.js on PATH
- Repo checkout with certified AVA payloads + ADP lean baselines
- Optional: Playwright browsers for PDF generation (`npx playwright install chromium` if missing)
- **Do not** set `AVA_AUDIT_PAID_RESEARCH_AUTHORIZED=1` without founder authorization
- **Do not** set Airtable live sync flags without schema authorization (see migration plan)

## PowerShell entrypoint

```powershell
cd C:\Dev\dealality\my-operators-backend-ava-v4-quality

# Single hotel (propertyId, payload slug, or mapped Airtable rec…)
.\scripts\Run-AVA-Audit.ps1 -HotelId "adp_cambridge_beaches_bermuda"

# Preview only (no archive / Admin / Airtable writes)
.\scripts\Run-AVA-Audit.ps1 -HotelId "adp_cambridge_beaches_bermuda" -DryRun

# Multiple
.\scripts\Run-AVA-Audit.ps1 -HotelIds "adp_cambridge_beaches_bermuda","adp_ac_hotel_a_coruna","adp_hilton_times_square" -DryRun

# Full certified portfolio (dry-run recommended first)
.\scripts\Run-AVA-Audit.ps1 -AllHotels -DryRun

# Certification + archive without paid research re-collection
.\scripts\Run-AVA-Audit.ps1 -HotelId "adp_cambridge_beaches_bermuda" -CertifyOnly

# Exact reproduction from frozen inputs
.\scripts\Run-AVA-Audit.ps1 -AuditRunId "AVA-RUN-..." -Reproduce
```

Equivalent Node CLI: `node scripts/run-ava-permanent-audit-v1.mjs --help`

## Execution sequence (enforced)

1. Resolve hotel identity  
2. Validate config  
3. Resolve authoritative customer ADP package  
4. Freeze input manifest  
5. Run AVA Full E2E (unless dry-run / certify-only / reproduce)  
6–9. Findings package, V4 PDF, CQA, ADP↔AVA certification  
10–11. Immutable hashes + local archive  
12. Airtable sync (mirror by default; live blocked without auth)  
13. Register Admin draft version (**never** publishes)  
14. Execution report JSON  

Blocking certification failure ⇒ status `FAILED_CERTIFICATION` / uncertified. Never customer-ready.

## Lifecycle separation

| State | Meaning |
|---|---|
| `AUDIT_CERTIFIED` | Internal quality + cross-report gates passed |
| `ARCHIVE_COMPLETE` | Local archive + verified Airtable index |
| `FOUNDER_APPROVED` | Explicit founder action only |
| `CUSTOMER_PUBLISHED` | Separate publication authorization |

Generating an audit does **not** publish, rotate share tokens, or replace the published customer PDF.

## Artifacts

- Runs: `data/ai-visibility-audit-action-plan/audit-runs/{propertyId}/{auditRunId}/`
- Airtable mirror: `data/ai-visibility-audit-action-plan/airtable-history-mirror/`
- Hotel→Airtable map scaffold: `data/ai-visibility-audit-action-plan/ava-hotel-identity-map-v1.json`
- Schema proposal: `docs/ava-permanent-audit-runner-airtable-schema-migration-v1.md`

## AI Admin

Open `http://localhost:8080/app#/admin/ai-demand?tab=visibility-audit`

- **Current Audit** — latest permanent run metadata + preview/download  
- **Audit History (Permanent runner)** — prior runs; exact archived PDF bytes; SHA header  
- **Compare** — select two permanent runs  

Historical download verifies SHA-256 and never regenerates.

## Regression

```bash
npm run test:ava-permanent-audit-runner
```
