# DEALALITY — Bethesda 67→19 Forensic Reconciliation & Discovery Path

**Hotel:** Bethesda Marriott (`recLuxvwwxID7U2B8`)  
**Main includes:** PRs #201–#210  
**Paid research:** **not executed** (pilot plan only)  
**Auto-publication:** **none**

---

## Founder decisions (evidence-backed)

### Why did Bethesda fall from 67 commercial records to 19 customer-facing opportunities?

**Because PR #209 enforced local = published parity.** The historical **67** was the primary *commercial-surface / hotel-sales review list* (including MONITOR/WATCHLIST and unapproved research). The **19** is the *qualified + appropriately approved customer opportunity feed*.

The 48 rows were **not deleted**. They remain in the 88-ID research union / commercial-surface research lanes. They were removed from the **hotel customer opportunity list** so incomplete research no longer appears as sellable inventory.

Provenance is **VERIFIED** from the frozen scorecard:  
`reports/gdi-bethesda-68-hotel-sales-commercial-acceptance-v1/01-commercial-sales-scorecard.json` (post-ACCP Salt Lake City removal; 68→67).

### How many of the 48 were genuinely valuable business we incorrectly lost?

| Category | Count | % of 48 | Meaning |
|---|---:|---:|---|
| **A** Correctly excluded | **2** | 4.2% | Past events (SHOW 2026; RIC Sep 2026) — correctly inactive |
| **B** Legitimate research continuation | **45** | 93.8% | MONITOR / QUALIFY_FIRST / WATCHLIST — valuable research, not customer-feed ready |
| **C** Potential false negative (filter bug) | **0** | 0% | No customer-feed false negatives found |
| **D** Publication/technical defect | **0** | 0% | No approved rows hidden by bug |
| **E** Pending founder approval | **1** | 2.1% | Arlington Spring Tournament — AD-eligible, never founder-published |

**Incorrectly lost from the customer feed via filter defect: 0.**  
**Valuable but unfinished / unapproved (still recoverable as research → AD → founder review): ~14–16** (1 Category E + 13 MONITOR with engine AD-eligible + 2 QUALIFY_FIRST).  
**Do not count Category E as failed qualification** — it is correctly withheld until founder publication approval.

### Is qualification too strict, research insufficient, or filters hiding legitimate opportunities?

**Primary answer: research under-completion + publication lifecycle separation — not an over-strict customer-feed filter bug.**

1. **Filters (PR #209):** Doing what the founder required — hide WATCHLIST / incomplete / unapproved from the hotel feed. **Correct.**
2. **Qualification standard for customer publication:** Remains appropriate (named commercial entry point + evidence-grounded hotel demand). **Not too strict for the customer feed.**
3. **Research quality (paid $6 pilot):** **Insufficient hotel-specific demand evidence** produced 0 new publication-review-ready bodies (12 WATCHLIST / 34 EXCLUDE). That is the binding constraint on *new* opportunities.
4. **Process gap:** Engine often stamps `ACCOUNT_DEVELOPMENT_ELIGIBLE` while hotel-sales scores MONITOR and no founder maturity/publication stamp is applied — so AD-capable accounts never enter the 19.

### What changes improve valuable opportunity count without sacrificing quality?

1. **Keep customer-feed parity** (do not restore the 67 indiscriminately).
2. **Stage AD completion packages** for Arlington Spring + strongest MONITOR/AD-eligible locals (SHRM, PMI, PDA, Cohera, IWLCA, NADO overflow) for founder publication review — without auto-publish.
3. **Finish QUALIFY_FIRST host/fit research** (NADO 2028, AMWA 2027) before AD promotion.
4. **Run a narrowly scoped $8 fresh-discovery pilot only after separate authorization**, using post-#201–#209 targeting/dedupe/CHD — not a broad restart of exhausted queries.
5. **Do not** treat missing RFP / unknown room nights as automatic commercial irrelevance for Account Development — preserve that bar in research continuation (already the intent; customer feed still requires approval).

### Is the updated Full E2E engine ready for a controlled $8 pilot?

**Conditionally YES — for a narrow four-segment pilot after explicit founder spend authorization.**  
Targeting/dedupe/CHD fixes from #201–#209 are on main, but the closed $6 run never live-verified the post-fix binary. Success target: ≥1 Sales Ready **or** ≥2 Account Development publication-review-ready. Stop early on yield failure. **Do not spend without a separate authorization message.**

---

## A. Reconciliation

| Metric | Value |
|---|---:|
| Original frozen commercial cohort | **67** (verified) |
| Current customer-facing | **19** |
| Missing from customer feed | **48** |
| Facing IDs not in frozen 67 | **0** |
| Previously approved rows hidden by bug | **0** |
| Current commercial surface (research-inclusive) | **71** |
| Research union | **88** |
| Canonical store | **59** |

Full row file: `03-full-67-reconciliation.json` / `.csv`.

### 48 by exclusion reason (primary)

| Subcategory | Count |
|---|---:|
| MONITOR_WATCHLIST | 30 |
| MONITOR_ENGINE_AD_ELIGIBLE | 13 |
| QUALIFY_FIRST_HOST_OR_FIT_OPEN | 2 |
| PAST_EVENT_SURFACE_INACTIVE | 2 |
| AD_ELIGIBLE_NEVER_FOUNDER_APPROVED | 1 |

Dominant gate chain: `customerVisible=false` and/or `priority=WATCHLIST` and/or `lacks_approved_commercial_class` → `isBlockedFromQualifiedCustomerOpportunityFeed` / `isCustomerFacingOpportunity`.

---

## B. Commercial recovery (this pass)

| Item | Count |
|---|---:|
| Customer-feed false negatives fixed in code | **0** (none substantiated) |
| Recoverable / stageable candidates identified | **16** |
| Newly auto-published | **0** |
| New Sales Ready applied | **0** |
| New Account Development applied | **0** |
| Publication-review staged (applied) | **0** (package only — see below) |
| Still requiring research | **45** Category B |
| Correctly rejected / past | **2** Category A |

**Top stage-for-founder candidate (Category E):**  
`gdi_opp_arlington_spring_tournament_2027` — Arlington Soccer Association — Lizzy Stell — engine AD-eligible — hotel-sales ACCOUNT_DEVELOPMENT — `customerVisible=false` (correct until approval).

**Do not re-inject MONITOR rows into the customer feed.** Route via Qualified → Publication Review → Founder Approval → Published.

---

## C. Root-cause analysis (three biggest)

### 1. Customer-list definition change (PR #209) — explains 67→19

- **Evidence:** Frozen 67 scorecard vs live facing 19; parity audit; all 48 lack approved commercial class; 42 WATCHLIST priority.
- **Impact:** −48 visible customer cards; **0** inventory loss.
- **Type:** UI/publication filtering (intentional).
- **Fix status:** Implemented and correct for founder parity directive.
- **Regression:** `test:gdi-bethesda-67-customer-feed-reconciliation-v1`.

### 2. Paid pilot demand-evidence failure — explains 0 new pub-ready from $6

- **Evidence:** Live campaign `FOUNDER-COMPLETION-REPORT.md` — $6.00 / 170 fair-queue / 46 deep / 12 WATCHLIST / 34 EXCLUDE / 0 pub-ready. Handoff trace: content existed; demand evidence failed.
- **Impact:** No incremental customer opportunities from that wave.
- **Type:** Research quality + segment contamination (PEP/LCM) + avoidable deep spend.
- **Fix status:** Targeting/dedupe/CHD on main (#201–#209); **not live-verified** on a new paid run.
- **Remaining:** Narrow authorized pilot with improved queries; do not re-deep KnowBe4/Ameresco without Bethesda-side evidence.

### 3. AD eligibility without founder publication stamping — explains “lost” valuable accounts

- **Evidence:** 13 MONITOR rows with `ACCOUNT_DEVELOPMENT_ELIGIBLE` + Arlington Spring AD usability never received `commercialMaturity` / registry / `customerVisible` approval.
- **Impact:** Valuable accounts stay invisible on the 19-card feed despite engine AD signals.
- **Type:** Process / lifecycle (not a filter bug).
- **Fix:** Founder AD publication-review packages; keep feed closed until approval.

---

## D. Recommended system corrections

| Root cause | Code / process | Status | Expected impact |
|---|---|---|---|
| Parity filter | `gdi-qualified-customer-opportunity-feed-v1.js` + `customer-visibility.js` | **Keep** | Honest customer feed |
| Frozen 67 accounting | fixture + `test:gdi-bethesda-67-customer-feed-reconciliation-v1` | **Added** | Prevent silent approved loss |
| Paid targeting waste | PRs #201–#205 PEP/LCM/dedupe | **On main** | Better $ per deep |
| AD never staged | Founder publication-review package workflow | **Process** — no auto-stamp | +1–few AD on feed after approval |
| Geography honesty | CHD contested-metro (#207/#209) | **Keep** | Avoid false Bethesda promotions |

No broad relaxation of Ready/RFP/room-night rules. No customer-feed reopening of WATCHLIST.

---

## E. Discovery plan ($8 ceiling — **NOT AUTHORIZED**)

### Scope (4 segments)

1. Associations & Conferences — open housing / future host  
2. Sports & Entertainment — overflow / future-cycle displacement  
3. Local Catering & Day Meetings — local grain only (reject destination conferences)  
4. Corporate / Scaling — Bethesda-side travel/meeting evidence required  

### Budget model

| Stage | Est. |
|---|---|
| Discovery | $2.50–$4.00 |
| Deep | $3.00–$5.00 |
| Hard cap | **$8.00** |

### Controls

- Pre-deep inventory + cross-segment org dedupe  
- No re-deep of closed KnowBe4/Ameresco/NRL without new Bethesda-side trigger  
- No unchanged exhausted Exa queries  
- CHD + hotel-fit + named contact before publication-review  

### Stop conditions (earliest)

1. 3 new publication-review-ready opportunities  
2. Hard budget  
3. 2 consecutive zero-yield segment waves  

### Success target

≥1 Sales Ready **or** ≥2 Account Development publication-review-ready — **without** lowering standards.

**Separate explicit founder authorization required before any paid call.**

---

## F. Local / published verification

- Customer feed = 19 approved rows; WATCHLIST = 0 on feed  
- Local parity path uses same `filterCustomerFacingOpportunities`  
- Research remains on `?surface=research|full` / admin — not hotel feed  
- URL: `http://localhost:8080/group-demand-intelligence.html?hotelId=recLuxvwwxID7U2B8`

---

## G. GitHub

- Forensic package + regression on branch (this deliverable)  
- Prior: #209 parity MERGED; #208 feedback persistence MERGED  
- Tests: `test:gdi-bethesda-67-customer-feed-reconciliation-v1`, qualified-feed, customer-visibility  

---

## H. Safety

- No unauthorized paid research  
- No customer auto-publication  
- No destructive Airtable opportunity writes  
- 88 research IDs / 59 canonical preserved  
- Hotel feedback preserved  
- No parallel GDI engine  

---

## Artifacts

| File | Role |
|---|---|
| `00-summary.json` | Counts + categories |
| `03-full-67-reconciliation.json` | Full 67-row forensic table |
| `03-full-67-reconciliation.csv` | Spreadsheet export |
| `fixtures/gdi-bethesda-frozen-commercial-67-customer-feed-v1.json` | Frozen regression cohort |
| `reports/gdi-bethesda-fourteen-segment-live-20261112/FOUNDER-COMPLETION-REPORT.md` | $6 pilot SoT |
