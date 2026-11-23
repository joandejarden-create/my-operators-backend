# GDI Portfolio Processing Certification V1 — Operator Runbook

**Contract:** `gdi-portfolio-processing-certification-v1`  
**Canonical engine:** `scripts/gdi-canonical-full-e2e-entry-v1.mjs`  
**PowerShell portfolio entry:** `scripts/Invoke-GdiPortfolioProcessing.ps1`  
**Single-hotel ops wrapper:** `scripts/gdi-workflow.ps1` (Research/FullRun → canonical dry-run)

This runbook standardizes how to process additional GDI hotels. It does **not** create a second research pipeline.

---

## Prerequisites

1. Registered GDI workspace (or authorized temp worktree) with repo checkout.
2. Node.js on PATH; PowerShell 5.1+.
3. Canonical HPC hotel IDs (`rec…`) — never invent IDs.
4. Local GDI hotel artifacts under `data/group-demand-intelligence/hotels/<hotelId>/` when refreshing existing inventories.
5. No production Airtable writes, paid research, customer publication, or weekly automation unless the founder issues an explicit separate GO.

---

## Standardized PowerShell commands

### Safe preview (one hotel)

```powershell
.\scripts\Invoke-GdiPortfolioProcessing.ps1 `
  -HotelId recLuxvwwxID7U2B8 `
  -Mode Preview
```

### Hotel batch from file (rehearsal / certification — $0)

```powershell
.\scripts\Invoke-GdiPortfolioProcessing.ps1 `
  -Mode Rehearsal `
  -HotelsFile .\reports\gdi-portfolio-processing-certification-v1\cert-cohort-hotels.txt
```

### Hotel batch inline

```powershell
.\scripts\Invoke-GdiPortfolioProcessing.ps1 `
  -Mode Preview `
  -Hotels "recLuxvwwxID7U2B8,recnPB3Q02NO8Ma1o"
```

### Single-hotel canonical Full E2E dry-run (existing path)

```powershell
.\scripts\gdi-workflow.ps1 -HotelId recLuxvwwxID7U2B8 -Action FullRun -DryRun
```

### Audit-only (capability matrix + scores)

```powershell
.\scripts\Invoke-GdiPortfolioProcessing.ps1 -Mode AuditOnly
```

---

## Hotel list input

- **Text file:** one `rec…` ID per line (see `cert-cohort-hotels.txt`).
- **JSON file:** `{ "hotelIds": ["rec…", …] }` or a bare string array.
- **Inline:** `-Hotels` comma-separated; `-HotelId` for one.
- Duplicates are skipped; invalid IDs fail closed before spend.

---

## Safe preview mode

- `-Mode Preview` / `Rehearsal` → eligibility preflight + offline quality gates only.
- Budget remains `$0`.
- No paid providers, no Airtable apply, no publication, no weekly activation.
- Review `reports/gdi-portfolio-processing-certification-v1/*.summary.json`.

---

## How to review results

| Artifact | Purpose |
| --- | --- |
| `<portfolioRunId>.summary.json` | Per-hotel pass/fail, spend, safety flags |
| `<portfolioRunId>.state.json` | Resume checkpoint |
| `capability-audit-matrix.json` | CERTIFIED / FUNCTIONAL_NOT_CERTIFIED / PARTIAL / BLOCKED |
| `fourteen-segment-coverage.json` | 14-segment registry coverage |
| `hotel-certification-scores.json` | Hotel-by-hotel commercial scores |
| `readiness-verdict.json` | `LIMITED_PILOT_ONLY` / `NOT_READY` / (rare) `CERTIFIED_FOR_SCALING` |
| `FOUNDER_REPORT.json` | Founder closeout payload |
| `hotels/<hotelId>/<hotelRunId>/` | Independent per-hotel run directory |

Exit code `0` is **not** Full E2E CERTIFIED. Read `readiness-verdict.json`.

---

## Authorize paid research separately

Paid research is **independent** of publication.

```powershell
# Shape only — requires explicit founder GO + -AllowLivePilot
.\scripts\Invoke-GdiPortfolioProcessing.ps1 `
  -Mode FullE2E `
  -HotelsFile .\path\to\hotels.txt `
  -BudgetUSD 25 `
  -AuthorizePaidResearch `
  -AllowLivePilot
```

Without `-AllowLivePilot`, the launcher fail-closes even if paid flags are present.

Prefer the canonical Node entry for a single-hotel live research pilot:

```bash
npm run gdi:canonical-full-e2e -- --hotel <recId> --mode live \
  --authorize-paid-discovery --authorize-paid-research \
  --authorize-exa-agent-live --budget-ceiling-usd 15
```

---

## Publication authorization (independent)

Publication is **not** implied by paid research.

```powershell
.\scripts\Invoke-GdiPortfolioProcessing.ps1 `
  -Mode FullE2E `
  -HotelId <recId> `
  -AuthorizePublication `
  -AllowLivePilot
```

Customer publication and Airtable production writes still require their own engine gates (approval reference / exact write-set / env unlock).

---

## Resume after partial failure

```powershell
.\scripts\Invoke-GdiPortfolioProcessing.ps1 `
  -Mode Resume `
  -HotelsFile .\reports\gdi-portfolio-processing-certification-v1\cert-cohort-hotels.txt `
  -ResumeState .\reports\gdi-portfolio-processing-certification-v1\<portfolioRunId>.state.json
```

Completed hotels are skipped; failed hotels retry with a new independent `hotelRunId` under the same portfolio run when state is reused.

---

## Verify certification

```bash
npm run test:gdi-portfolio-processing-certification-v1
npm run gdi:portfolio-processing-certification -- --mode rehearsal
```

Confirm:

1. `spentUsd === 0` for rehearsal
2. `safety.paidResearchExecuted === false`
3. `safety.publicationExecuted === false`
4. `safety.airtableProductionWrites === false`
5. Opportunity ID snapshots unchanged vs local stores
6. Verdict is honest (`LIMITED_PILOT_ONLY` until paid+customer evidence exists)

---

## Diagnose failures

| Symptom | Check |
| --- | --- |
| `INVALID_HOTEL_LIST` | Non-`rec` IDs or empty list |
| `FAILED_PREFLIGHT` | `hotels/<id>/<run>/result.json` → `eligibility.errors` |
| `REHEARSAL_REFUSES_PAID` | Drop paid flags in Rehearsal |
| `LIVE_PILOT_NOT_ALLOWED` | Missing `-AllowLivePilot` / `--allow-live-pilot` |
| `BLOCKED_BUDGET` | Aggregate ceiling exhausted — raise budget only with founder GO |
| Canonical dry-run errors | Run single-hotel `gdi-workflow.ps1 -Action FullRun -DryRun` |

Per-hotel errors are isolated — one failure does not abort siblings (sequential continue).

---

## Process the next group of hotels

1. Build a new hotels file with canonical HPC IDs only.
2. Run `-Mode Preview` and review eligibility + commercial scores.
3. Run `-Mode Rehearsal` (still $0) to validate orchestration/resume/budget gates.
4. **STOP for founder review** before paid research, live Airtable writes, customer publication, or weekly activation.
5. After founder GO, run a **controlled paid pilot** on 1–2 hotels with explicit budget + independent publication decision.
6. Preserve prior opportunity inventories — do not wipe stores; rely on idempotent reconcile / weekly-change classification.

---

## Classification legend

| Status | Meaning |
| --- | --- |
| `CERTIFIED` | Full E2E evidence under stated constraints |
| `FUNCTIONAL_NOT_CERTIFIED` | Works in harness/dry-run; not Full E2E certified for scaling claim |
| `PARTIAL` | Incomplete path or missing evidence class |
| `BLOCKED` | Cannot proceed without missing module/auth/data |

Script existence and unit tests alone never equal `CERTIFIED`.
