# GDI Next-Generation Discovery Architecture — Founder Report

**Contract:** `gdi-nextgen-discovery-architecture-v1`  
**Branch:** `feature/gdi-nextgen-discovery-architecture-v1`  
**Paid research authorized this wave:** **No** (`$0`)

---

## 1. Current capability audit

| Surface | Status |
|---|---|
| Fourteen demand strategies (`NEED_DATE_RESEARCH_STRATEGY`) | Preserved (14) |
| RESEARCH_SEGMENT / ten-bases | Preserved — hunters map into them |
| Apify actor registry V1 | Recovered — **13** actors |
| Fourteen-workflow Apify coverage matrix | Reused |
| Unified Discovery V7 commercial prequalify | Party Bus / leisure gates retained |
| Full E2E supervisor | Soft-wired; **not** forked |
| HCP V3 / weekly soft signals | Hotel-own profile only |

Machine-readable matrix: `reports/gdi-nextgen-discovery-architecture-v1/capability-gap-matrix.json`

### Gap summary (10 hunters)

| Capability | Count | Action |
|---|---|---|
| EXISTS | 4 | Reuse + PowerShell wrap |
| PARTIAL | 5 | Extend existing workflow |
| NEW | 1 | Emergency / Displacement only |

---

## 2. Prioritized gap analysis

**First wave (implemented):** Government Procurement · Project Workforce · Sports Housing · Corporate Meeting Triggers · University Demand  

**Second wave (entry points only):** Association Rotation · Clinical Trials / Medical · Crew / Production  

**Third wave (entry points only):** Legal / Regulatory · Emergency / Displacement  

No fifteenth demand segment. No parallel supervisor.

---

## 3. Ecosystem graph architecture

`Organization → Division/Affiliate → Program/Project → Demand Trigger → Event/Activity → Commercial Contact → Hotel Demand Hypothesis`

Module: `lib/group-demand-intelligence/nextgen-discovery/organization-demand-ecosystem-v1.js`

- Canonical org IDs, incremental merge, evidence URLs, confidence, hotel-demand relevance  
- Dedup keys for accounts and opportunities  
- **Hypothesis ≠ Sales Ready** — max disposition hint is research continuation / account development handoff to ORS

---

## 4. PowerShell module design

Shared: `scripts/gdi-nextgen-hunters/Invoke-GdiSpecialistHunter.ps1`  
Node runner: `scripts/gdi-nextgen-hunter-run.mjs`  
Ten wrappers under `scripts/gdi-nextgen-hunters/Invoke-Gdi*Hunter.ps1`

Common flags: `-HotelId`, `-BudgetUSD`, `-DryRun`, `-Execute`, `-AuthorizePaidResearch` (fail-closed)

---

## 5. First-wave implementation

| Hunter | Node module | Providers (plan) |
|---|---|---|
| government_procurement | `hunters/government-procurement-hunter-v1.js` | public_api + exa; Apify supplemental |
| project_workforce | `hunters/project-workforce-hunter-v1.js` | public_api + exa |
| sports_housing | `hunters/sports-housing-hunter-v1.js` | **Apify primary** + exa |
| corporate_meeting_triggers | `hunters/corporate-meeting-triggers-hunter-v1.js` | exa + apify supplemental |
| university_demand | `hunters/university-demand-hunter-v1.js` | exa + apify supplemental |

Cache fixture: `fixtures/gdi/nextgen-discovery/first-wave-cache-v1.json`

---

## 6. Four-hotel zero-paid pilot

| Hotel | Hotel ID | Incremental hypotheses | Rejected |
|---|---|---|---|
| Bethesda Marriott | `recLuxvwwxID7U2B8` | 5 | 1 |
| Wyndham San Diego Bayside | `recnPB3Q02NO8Ma1o` | 5 | 1 |
| Westin Tampa Waterside | `recdmo0CnhIZAwHGC` | 5 | 1 |
| Renaissance NY Times Square | `recG66DQJKP2c0UNh` | 5 | 1 |

**Portfolio:** 20 incremental hypotheses · 4 rejections (Party Bus gate on sports) · **0** Sales Ready · **0** auto-publish · **$0** spend

Artifacts: `reports/gdi-nextgen-discovery-architecture-v1/pilot/`

---

## 7. Incremental qualified-opportunity results by module

Zero-paid cache pilot produces **demand hypotheses for ORS**, not newly certified Sales Ready / Account Development rows.

| Module | Incremental orgs (portfolio) | Ready-for-ORS | Sales Ready |
|---|---|---|---|
| government_procurement | 4 | 0 | 0 |
| project_workforce | 4 | 0 | 0 |
| sports_housing | 4 (+4 leisure rejects) | 0 | 0 |
| corporate_meeting_triggers | 4 | 0 | 0 |
| university_demand | 4 | 0 | 0 |

Success metric remains **incremental commercially credible opportunities after ORS/qualification**, not page/org counts. Next authorized step: named-buyer enrichment on READY_FOR_ORS candidates only.

---

## 8. Commercial contact coverage

Fixture wave deliberately omits fabricated buyers (`contactName: null`). Contact nodes = **0** in pilot. Named buyer research remains ORS / existing commercial research path.

---

## 9. Evidence and deduplication audit

- Evidence URLs required for `ACTIVITY_EVIDENCED`  
- Opportunity dedup keys: hotel + org + activity + date  
- Apify `commercialRelevanceScreen` reused  
- V7 Party Bus / nightlife rejection retained and proven (4/4 sports runs)

---

## 10. Cost and scaling recommendations

| Item | Recommendation |
|---|---|
| Near-term | Stay cache/fixture + historical Apify/Exa outputs |
| Per-hotel weekly | Adaptive selection max 3–4 first-wave hunters from **that hotel’s** HCP V3 |
| Paid Apify | Enable only for PRIMARY sports/event schedules after yield proof; keep `GDI_APIFY_LIVE` fail-closed |
| Paid Exa | Buyer verify / ORS only on hotel-thesis candidates |
| Cost KPI | Cost per newly qualified Account Development / Sales Ready vs Exa-only baseline |
| Do not | Run all ten hunters weekly; do not reset exhausted budgets without new auth |

Expected incremental paid cost until authorization: **$0**.

---

## 11. Regression tests and publication safeguards

| Gate | Result |
|---|---|
| `npm run test:gdi-nextgen-discovery-architecture-v1` | PASS |
| `npm run test:gdi-bethesda-67-sales-baseline-cmi-v1` | PASS (67 + 7 CMI) |
| `npm run test:gdi-bethesda-67-sales-team-inventory-display-v1` | PASS |
| `npm run test:gdi-hcp-v3-multi-hotel-isolation-v1` | PASS |
| Auto-publish | OFF |
| Sales Ready from hunter | Forbidden |
| Sales Opportunities filter row | Untouched |
| CSV 64-column export | Untouched this PR |
| Fifteenth segment | None |

Weekly supervisor soft plan: `specialistHunterPlan` on `buildWeeklyWorkQueues` with `executePaid: false`.

---

## 12. Apify addendum acceptance

1. **Recovered** — `apify-actor-registry-v1` (13), fourteen-workflow coverage, Full E2E orchestration, V7 prequalify.  
2. **Previously vs new** — Apify/event discovery EXISTS; organization-first ecosystem + ten hunter entry points + adaptive selection are the new layer.  
3. **Working actor inventory** — registry-enabled actors; sports PRIMARY; gov/scaling EXA_PRIMARY.  
4. **Provider routing** — `hunter-provider-routing-v1.js` / matrix in pilot summary.  
5. **Organization-first** — ecosystem graph; org alone ≠ opportunity.  
6. **V7 lessons** — Party Bus gate, evidence≠opportunity, actor replaceability.  
7. **Zero-paid validation** — pilot above.  
8. **Expected incremental cost** — $0 until founder auth.  
9. **Yield comparison framework** — incremental qualified AD/SR + contacts + dup/exclusion rates vs Exa-only baseline.  
10. **Rollout** — first wave live (cache); second/third after hotel-specific yield.

**Core principle held:** Apify is a discovery provider inside GDI, not a separate opportunity engine.

---

## PR / CI / merge

Open PR from `feature/gdi-nextgen-discovery-architecture-v1`. Merge only after CI green.  
Related: PR #220 (67 sales-team inventory) CI was green; merge separately when founder confirms.
