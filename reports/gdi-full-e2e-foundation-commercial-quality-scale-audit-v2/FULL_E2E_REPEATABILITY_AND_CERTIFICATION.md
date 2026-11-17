# Full E2E Repeatability and Certification

## Automation status (summary)

| Stage | Status |
|-------|--------|
| Hotel configuration | Partial — HPC + hotel config; many script flags |
| Segment planning | Automated (14-segment plan) |
| Discovery | Partial — Exa when authorized; cache/fixture otherwise; Apify mock default |
| Deep research | Partial / campaign-dependent |
| Qualification | Automated modules + gates |
| Staging / fingerprint | Automated |
| Founder approval | Manual explicit |
| Publication / parity | Gated scripts |
| Monitoring | Observability present; historical UNKNOWN gaps |

## Gate mapping (proposed G0–G8 ↔ existing)

| Gate | Existing controls | Gap |
|------|-------------------|-----|
| G0 Preflight | Hotel template, auth flags, budgets | Unify entry command |
| G1 Discovery | Fourteen-segment execution + ledger | Emit per-segment EXECUTED/SKIP |
| G2 Qualification | Commercial gates / CMI contracts | Preserve honesty |
| G3 Drawer QA | Remediations + audits | Rubric automation |
| G4 Persistence | Packages + optional Airtable | Read-back when claiming AT writes |
| G5 Staging | STAGED + fingerprint | OK |
| G6 Founder approval | Activate scripts / phrases | OK |
| G7 Parity | Local-external gate | Extend beyond YOTEL |
| G8 Monitoring | Execution observability | Wire live runs always |

## Verdict vocabulary (keep)

FULL_E2E complete / partial / blocked / failed — operational ≠ commercial quality ≠ publish authorization (ledger certificate note already states this).

## Required result

**One orchestrator to harden:** fourteen-segment discovery execution + research-to-page full-e2e mode + mandatory ledger writes. Deprecate reliance on hotel-only campaign scripts for “definition of done,” or make those scripts call the same instrumented chain.
