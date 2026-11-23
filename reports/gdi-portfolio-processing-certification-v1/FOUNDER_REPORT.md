# GDI Portfolio Processing Certification V1 — Founder Report

**Date:** 2026-11-23  
**Contract:** `gdi-portfolio-processing-certification-v1`  
**Rehearsal run:** `gdi_portfolio_cert_v1_founder_rehearsal`  
**Spend:** `$0`  
**Readiness verdict:** **LIMITED_PILOT_ONLY**

**STOP** before paid research, live Airtable writes, customer publication, weekly automation, or production activation.

---

## 1. Current runner inventory

| Runner | Role | Classification |
| --- | --- | --- |
| `scripts/gdi-workflow.ps1` | Universal ops; Research/FullRun → canonical dry-run | FUNCTIONAL_NOT_CERTIFIED |
| `scripts/Invoke-GdiScalingCompanies.ps1` | Scaling-companies segment launcher | FUNCTIONAL_NOT_CERTIFIED |
| `scripts/gdi-canonical-full-e2e-entry-v1.mjs` | Authoritative Node Full E2E engine | FUNCTIONAL_NOT_CERTIFIED |
| `scripts/gdi-full-e2e-fourteen-segment-orchestrator-v1.mjs` | Fourteen-segment orchestrator | FUNCTIONAL_NOT_CERTIFIED |
| `scripts/gdi-full-e2e-powershell-deep-research-certification-v1.mjs` | Bethesda-centric $0 deep-research harness | PARTIAL |
| `scripts/Invoke-GdiPortfolioProcessing.ps1` | **New** standardized portfolio interface | FUNCTIONAL_NOT_CERTIFIED |

Script existence / unit tests alone are never Full E2E CERTIFIED.

---

## 2. Canonical execution-path recommendation

1. **Portfolio / batch (founder/operator):**  
   `.\scripts\Invoke-GdiPortfolioProcessing.ps1 -Mode Rehearsal|Preview|Resume …`
2. **Single hotel ops dry-run:**  
   `.\scripts\gdi-workflow.ps1 -HotelId <rec…> -Action FullRun -DryRun`
3. **Engine:**  
   `npm run gdi:canonical-full-e2e -- --hotel <rec…> --mode dry-run|preflight|live`

Do **not** use hotel-specific one-off Full E2E scripts as the scaling path.

---

## 3. Certification gaps

- Fresh paid discovery not certified in this run
- Airtable production writes not executed
- Authenticated customer delivery not executed
- Weekly automation not activated
- Per-segment live deep research not portfolio-certified (14/14 executors present offline → FUNCTIONAL_NOT_CERTIFIED)

---

## 4. Standardized PowerShell command

```powershell
.\scripts\Invoke-GdiPortfolioProcessing.ps1 `
  -Mode Rehearsal `
  -HotelsFile .\reports\gdi-portfolio-processing-certification-v1\cert-cohort-hotels.txt
```

Runbook: `docs/data-intelligence/gdi-portfolio-processing-certification-v1-runbook.md`

---

## 5. Multi-hotel execution results ($0 rehearsal)

| Metric | Value |
| --- | --- |
| Portfolio run ID | `gdi_portfolio_cert_v1_founder_rehearsal` |
| Hotels | 7 |
| Passed | 7 |
| Failed | 0 |
| Spent USD | 0 |
| Sequential | yes |
| Uncontrolled concurrency | no |
| Authoritative store mutations | none |

Resume replay skipped completed hotels; opportunity ID snapshots unchanged.

---

## 6. Fourteen-segment coverage matrix

| Status | Count |
| --- | --- |
| CERTIFIED | 0 |
| FUNCTIONAL_NOT_CERTIFIED | 14 |
| PARTIAL | 0 |
| BLOCKED | 0 |

All 14 segment executors are on disk. Live discovery + deep research per segment still requires paid/auth certification.

---

## 7. Commercial quality results (local inventory gates)

Offline store quality only — not customer-delivery certification.

| Hotel | ID | Score | Local status | Opps |
| --- | --- | ---: | --- | ---: |
| Bethesda Marriott | `recLuxvwwxID7U2B8` | 100 | CERTIFIED | 86 |
| Wyndham San Diego Bayside | `recnPB3Q02NO8Ma1o` | 100 | CERTIFIED | 82 |
| Renaissance NY Times Square | `recG66DQJKP2c0UNh` | 90 | CERTIFIED | 29 |
| Sheraton Guadalajara Expo | `recCAGqkERSXgiku4` | 80 | CERTIFIED | 43 |
| YOTEL Geneva Lake | `recrPQcZg7SFARRb2` | 90 | CERTIFIED | 42 |
| Spice Island Beach Resort | `recKRJjcPnb4tVDDS` | 100 | CERTIFIED | 77 |
| Mayflower (empty store) | `rec0eTEnCqlsGJJt9` | 15 | PARTIAL | 0 |

Sales Ready speculation reject gate: PASS on assessed inventories.

---

## 8. Budget and publication safety results

| Gate | Result |
| --- | --- |
| Paid research executed | **false** |
| Publication executed | **false** |
| Airtable production writes | **false** |
| Weekly automation activated | **false** |
| Rehearsal refuses paid flags | **pass** |
| Nonzero budget without paid auth | **fail-closed** |
| Live pilot without `-AllowLivePilot` | **fail-closed** |
| Persistence apply without auth | **blocked** |

---

## 9. Repeated-run and Weekly Change validation

- First rehearsal: 7/7 PASSED, `$0`
- Resume: completed hotels skipped (`resumedSkipped`)
- Opportunity IDs stable vs local stores (no overwrite)
- Weekly Change durability annotation demonstrated offline; authoritative stores **not** mutated
- Weekly automation **not** activated

---

## 10. Hotel-by-hotel certification scores

See §7 and `hotel-certification-scores.json`.  
Empty-store hotel (Mayflower) proves net-new admission path is eligible for preflight but **cannot** be Full-E2E certified without fresh discovery.

---

## 11. Remaining blockers

1. Controlled paid discovery pilot (explicit budget + provider auth)
2. Airtable research-inventory persistence apply + read-back (exact write-set + env gate)
3. Authenticated customer drawer / share delivery verification
4. Optional weekly activation GO (separate from research/publication)
5. International hotels with thin local profile metadata (e.g. Sheraton GDL missing local `profile.json`) should be enriched before claiming geographic CERTIFIED beyond inventory gates

---

## 12. Exact approval required for a controlled paid pilot

Issue **all that apply** as separate founder phrases (do not bundle):

1. `FOUNDER_GO_PAID_RESEARCH` — hotel list + `BudgetUSD` ceiling + providers allowed (e.g. Exa only)
2. `FOUNDER_GO_PUBLICATION` — only if customer publication is intended (independent)
3. `FOUNDER_GO_AIRTABLE_RESEARCH_PERSISTENCE` — exact write-set authorization + env unlock
4. Explicit **no** weekly automation unless a fourth GO is issued

Suggested first paid pilot: **1–2 hotels** (Bethesda + one international), budget ≤ `$25`, publication **off**.

---

## 13. Readiness verdict

# LIMITED_PILOT_ONLY

Portfolio orchestration, $0 preview/replay, resume, budget/publication fail-closed, sequential isolation, and local commercial gates are demonstrated and repeatable.

**Not** `CERTIFIED_FOR_SCALING` until a controlled paid pilot + persistence read-back + authenticated customer delivery succeed under the same PowerShell contract.

---

## Capability audit (Phase 1 summary)

| Capability | Status |
| --- | --- |
| New hotels (net-new discovery) | PARTIAL |
| Existing hotels (refresh / replay) | FUNCTIONAL_NOT_CERTIFIED |
| All 14 demand categories | FUNCTIONAL_NOT_CERTIFIED |
| Discovery | FUNCTIONAL_NOT_CERTIFIED |
| Deep research | PARTIAL |
| Contact identification | PARTIAL |
| Opportunity qualification | FUNCTIONAL_NOT_CERTIFIED |
| Canonical persistence | PARTIAL |
| Customer drawer generation | FUNCTIONAL_NOT_CERTIFIED |
| Weekly Change | FUNCTIONAL_NOT_CERTIFIED |
| Repeated runs | FUNCTIONAL_NOT_CERTIFIED |

---

## Tests run (this certification)

```bash
npm run test:gdi-portfolio-processing-certification-v1
npm run gdi:portfolio-processing-certification -- --mode rehearsal
```

No production deployment. No paid research. No customer publication. No live Airtable writes. No weekly automation activation.
