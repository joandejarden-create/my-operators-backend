# GDI Portfolio Certification V1.1 — Controlled Paid Discovery Founder Report

**Date:** 2026-11-23  
**Contract:** `gdi-portfolio-cert-v1-1-paid-pilot`  
**Verdict:** **ITERATE**  
**Aggregate spend:** **$2.00** / $25 authorized (Bethesda $1.00 · YOTEL $1.00)

**STOP** before live Airtable writes, customer publication, broader paid research, weekly automation, or production deployment.

---

## 1. PR #562 integration result

| Item | Result |
| --- | --- |
| PR | [#562](https://github.com/joandejarden-create/my-operators-backend/pull/562) |
| CI | All required checks **SUCCESS** |
| Merge | **MERGED_REMOTE** @ `fba237863` (2026-11-23T10:30:17Z) |
| Method | `dealality-remote-pr-merge` — no local IC checkout mutation |
| Dirty trees | Preserved (`ws-gdi` dirty unrelated branch; IC detached dirty; BE overlap blocked FF) |
| Post-merge sync | Dry-run **BLOCKED** (expected preserve) — continued on exceptional clean temp WT @ `origin/main` |

Canonical runner remains authoritative. PowerShell only orchestrates Node. Spend/publication gates fail closed. No unrelated product changes in #562.

---

## 2. Exact PowerShell / Node commands executed

```powershell
# Preflight ($0)
.\scripts\Invoke-GdiPortfolioProcessing.ps1 -Mode PaidPilotPreflight -MaxCandidates 4

# Paid execute (after plan OK)
$env:GDI_EXA_AGENT_LIVE = "1"
.\scripts\Invoke-GdiPortfolioProcessing.ps1 `
  -Mode PaidPilotExecute `
  -BudgetUSD 25 `
  -PerHotelBudgetUSD 12.5 `
  -AuthorizePaidResearch `
  -AllowLivePilot `
  -FounderSpendConfirmation FOUNDER_GDI_PORTFOLIO_CERT_V1_1_PAID_25 `
  -MaxCandidates 4

# Equivalent Node used in this run:
node scripts/gdi-portfolio-cert-v1-1-paid-pilot.mjs --mode preflight
node scripts/gdi-portfolio-cert-v1-1-paid-pilot.mjs --mode execute-paid `
  --confirm-founder-spend-go FOUNDER_GDI_PORTFOLIO_CERT_V1_1_PAID_25 `
  --max-candidates 4 --portfolio-run-id gdi_portfolio_cert_v1_1_paid_pilot
node scripts/gdi-portfolio-cert-v1-1-paid-pilot.mjs --mode replay
```

---

## 3. Hotels processed

| Hotel | HPC | Auth ledger |
| --- | --- | --- |
| Bethesda Marriott | `recLuxvwwxID7U2B8` | `FOUNDER_GDI_PORTFOLIO_CERT_V1_1_BETHESDA_12_50` |
| YOTEL Geneva Lake | `recrPQcZg7SFARRb2` | `FOUNDER_GDI_PORTFOLIO_CERT_V1_1_YOTEL_12_50` |

Baseline inventories preserved: Bethesda **86** opps · YOTEL **42** opps · IDs **unchanged** after paid + replay.

---

## 4. Actual research spend by hotel and provider

| Hotel | Provider | Discovery | Deep research | Total | Cap | Remaining |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Bethesda | Exa Agent (medium @ $0.10) | 6 × $0.10 = $0.60 | 4 × $0.10 = $0.40 | **$1.00** | $12.50 | $11.50 |
| YOTEL | Exa Agent (medium @ $0.10) | 6 × $0.10 = $0.60 | 4 × $0.10 = $0.40 | **$1.00** | $12.50 | $11.50 |
| **Aggregate** | | | | **$2.00** | **$25.00** | **$23.00** |

Surfe / Findymail / PDL / FullEnrich / Prospeo: **$0** (not authorized).  
Durable ledgers: `data/group-demand-intelligence/paid-budget-ledgers/FOUNDER_GDI_PORTFOLIO_CERT_V1_1_*.json`

---

## 5. Fourteen-segment execution matrix

Full 14-segment architecture retained. Per hotel, **6 priority segments** ran live; **8** skipped for cost/relevance (UNVERIFIED — not certified).

### Bethesda (live)

| Segment | Live discovery | Yield | Deep processed | Notes |
| --- | --- | --- | ---: | --- |
| associations_conferences_events | YES | LIVE_NEW 12 | 1 | CAP_TRUNCATED |
| corporate_accounts_scaling | YES | LIVE_NEW 10 | 1 | CAP_TRUNCATED |
| government_public_sector | YES | LIVE_NEW 15 | 1 | CAP_TRUNCATED |
| education_scientific | YES | LIVE_NEW 10 | 0 | zero retained |
| sports_entertainment | YES | (executed) | 0 | zero / truncated |
| corporate_meetings_incentives | YES | LIVE_NEW | 1 | CAP_TRUNCATED |
| *8 other segments* | NO | — | — | skipped cost/relevance → **UNVERIFIED** |

### YOTEL (live)

| Segment | Live discovery | Deep processed | Notes |
| --- | --- | ---: | --- |
| associations_conferences_events | YES | 1 | |
| corporate_meetings_incentives | YES | 1 | |
| preferred_lodging_partnerships | YES | 0 | ZERO_YIELD_EVALUATED |
| travel_trade_group_intermediaries | YES | 1 | |
| education_scientific | YES | 1 | |
| professional_event_planners | YES | 0 | |
| *8 other segments* | NO | — | **UNVERIFIED** |

Do **not** treat adapter presence as certification. Unexecuted segments remain unverified.

---

## 6. New unique candidates discovered

| Hotel | Screened | Eligible deep | Duplicates | Selected for deep | LIVE_NEW selected |
| --- | ---: | ---: | ---: | ---: | ---: |
| Bethesda | 72 | 61 | 11 | 4 | 4 |
| YOTEL | 64 | 53 | 11 (+1 enrich) | 4 | 4 |

---

## 7. Independently verified demand opportunities (deep-selected)

All 8 deep-processed rows finished commercial disposition **EXCLUDE** (honest fail-closed — **not** Sales Ready).

### Bethesda sample (strongest intake)

1. **ASGSR 2026 Annual Meeting** — evidence: asgsr.org — unresolved: named buyer, sourcing  
2. **AAEI 106th Annual Conference & Trade Day 2027** — aaei.org — unresolved: named buyer, sourcing  
3. **AHA Annual Membership Meeting 2027** — annualmeeting.aha.org/hotel-travel — unresolved: named buyer, sourcing  

### Bethesda rejected / weak for Sales Ready

4. **Liatris facility expansion (Rockville)** — scaling-company news; weak lodging demand thesis → correctly not Sales Ready  

### YOTEL sample

1. **IRF 2026 Annual Conference** — irfofficial.org  
2. **ESCP 2027 Geneva** — escp.eu.com venue + YOTEL meetings page  
3. **SETAC Europe 2027** — palexpo.ch  
4. **GDPM 2027 / Agroscope** — agroscope.admin.ch  

Economics honesty: Bethesda `verifiedCommercialContactsGained=1`, `opportunitiesBecomingActionable=1`; YOTEL both **0**. Deep EXCLUDE disposition means these are **not** certified commercially actionable opportunities.

---

## 8. Named buyers and contact coverage

| Metric | Bethesda | YOTEL |
| --- | ---: | ---: |
| Named stakeholders on selected rows | 0 | 0 |
| Sales-ready contacts on processed | 0 | 0 |
| Contact research complete | false (typical) | false |
| Surfe paid | **no** | **no** |
| Public-source contact verification | incomplete / unresolved `named_buyer_not_verified` | same |

Paid Surfe/Findymail/PDL/FullEnrich correctly **not** called. Contact path needs iteration (public verification depth) before commercial certification.

---

## 9. Rejected speculative / duplicate candidates

- **Duplicates blocked:** 11 Bethesda + 11 YOTEL (`DUPLICATE_NO_NEW_EVIDENCE`) — e.g. AAN, AMWA, EPFL, SETAC, IAS  
- **Unsupported rejects:** 0 auto-reject at screen (downstream EXCLUDE after deep)  
- **Speculative Sales Ready:** none promoted  
- Cap-deferred: 57 Bethesda + 49 YOTEL (maxCandidates=4)

---

## 10. Evidence quality and drawer quality

- Intake rows carry source URLs / evidence count ≥ 1  
- Deep `investigatedUrls` often empty; `namedStakeholders` empty  
- Presentation sections generated (10) but `commercialResearchComplete=false`  
- Customer publication ready: **false**  
- Drawer completeness for Sales Ready: **NOT MET**

---

## 11. Repeated-run and deduplication results

| Check | Result |
| --- | --- |
| Zero-paid replay | **PASS** — `spentUsdDelta=0` both hotels |
| Opportunity ID stability | **PASS** |
| Weekly Change probe | **UNCHANGED** |
| Durable ledger resume | **PASS** (spent retained; ceiling raise refused) |
| Authoritative store overwrite | **none** |

---

## 12. Airtable persistence dry-run results

Plan-only persistence ledger written. Apply **blocked**:

- `AUTHORIZE_RESEARCH_PERSISTENCE_REQUIRED`  
- `EXACT_WRITE_SET_AUTHORIZATION_REQUIRED`  
- `ENV_PRODUCTION_GATE_OFF` (`GDI_RESEARCH_INVENTORY_PRODUCTION_WRITES`)  

No live Airtable writes. No GDO writes. No publication.

---

## 13. Authenticated customer-delivery test status

**NOT EXECUTED / SEPARATE GATE** — founder excluded customer publication; authenticated delivery remains a distinct certification.

---

## 14. Remaining certification blockers

1. Deep research → commercial EXCLUDE on all selected rows (qualification / hotel-fit / buyer evidence)  
2. Named buyer / public contact verification incomplete without paid enrichment  
3. Only 6/14 segments live-tested per hotel  
4. Airtable persistence apply not authorized  
5. Authenticated customer delivery not tested  
6. Portfolio live path required V1.1 controls (#562 rehearsal launcher was dry-run-only)

---

## 15. Recommended per-hotel research budget

| Mode | Suggested ceiling | Notes |
| --- | ---: | --- |
| Discovery smoke | **$1–2** | Proven this run |
| Discovery + selective deep (4–8) | **$3–6** | Current shape |
| Deep + public contact push | **$8–12.50** | Next iterate focus |
| Do not default to burning full $12.50 | — | Stop when unresolved buyer/sourcing dominates |

Aggregate portfolio pilots: keep **sum(per-hotel caps) ≤ aggregate** via durable ledgers.

---

## 16. Verdict

# ITERATE

**Proven:** standardized PowerShell → canonical Node path; dual durable spend caps ($12.50 / $25); sequential isolation; Exa discovery + bounded deep; dedupe; no store overwrite; no Surfe/publish/Airtable apply; replay at $0.

**Not proven:** repeatable production of Sales Ready / commercially actionable opportunities with named buyers and drawer completeness under this budgeted path.

Do not equate `FULL_E2E_EXECUTION_COMPLETE` with commercial certification.

---

## Safety checklist

- [x] PR #562 merged before spend  
- [x] Provider-call plan + dual caps verified before spend  
- [x] Aggregate ≤ $25 · per hotel ≤ $12.50  
- [x] No Surfe/Findymail/PDL/FullEnrich  
- [x] No Airtable apply  
- [x] No customer publication  
- [x] No weekly automation  
- [x] No production deployment  
