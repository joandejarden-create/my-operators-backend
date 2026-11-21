# FOUNDER REPORT — HIF V5 Consolidated Commercial Target Discovery

**PR:** [#503](https://github.com/joandejarden-create/my-operators-backend/pull/503)  
**Implementation HEAD (pre-this-commit tip):** see push notes  
**Research horizon filter:** **NONE**  
**Geographic research restriction:** **NONE**  
**Publication:** CUSTOMER_BLOCKED · No HPC · No unauthorized Meta schema

---

## Phase 0 — Baseline / safety

| Check | Result |
|---|---|
| PR #503 vs `origin/main` | Diverged (neither is ancestor of the other). Continue on #503; IC owns merge/rebase. |
| Date-semantic gate regressions | **8/8 PASS** (opening years / loan maturities blocked) |
| Dirty HE / peer worktrees | Preserved (commit-tree onto PR branch; no checkout switch) |
| V4 protections | Intact — opening/year-built never Tier A/B |

---

## Phase 1 — Inventory reconciliation

### Prior scale-pilot snapshot (`OPPORTUNITIES.json` / `PILOT_METRICS.json`)

| Metric | Count |
|---|---:|
| Distinct hotel observations | 446 |
| Tier B verified contractual expirations | 76 |
| Tier C research candidates | 71 |
| HOLD / quarantine | 299 |
| Brand Conversion list rows | 136 |
| Management Contract list rows | **0** |

### Why Brand Conversion (136) ≠ Tier B (76) + Tier C (71)

1. **Tier B (76)** = hotel-specific **verified contractual expiration** rows — all `FRANCHISE` in the CMBS-heavy scale pilot.
2. **Tier C (71)** = hotel + commercial signal, **no verified end date**, mostly `UNKNOWN` agreement type.
3. **Brand Conversion projection** includes:
   - all Franchise Tier A/B rows (76), **plus**
   - Tier C rows **only when** `brand` / `conversionSignals.brand` is present (~60),
   - and **excludes** HOLD/quarantine (299).
4. Therefore **136 ≈ 76 Franchise Tier B + ~60 branded Tier C** — not 76+71.

### Why Management Contract list was 0

The scale pilot’s high-yield source was **CMBS franchise expiration schedules**.  
`agreementType` distribution was `FRANCHISE: 375`, `UNKNOWN: 71`, **`MANAGEMENT: 0`**.  
Dual-list projection only places `MANAGEMENT` (or explicit list hints) on List B → **0 rows**.

### After reconcile (this wave)

| Metric | Count |
|---|---:|
| Opportunities (after EX-10 merge) | 451 |
| Distinct hotels | 446 |
| Tier A | 2 |
| Tier B | 77 |
| Tier C | 73 |
| HOLD | 299 |
| Research-eligible | 152 |
| Verified expirations | 79 (**60 historical** + **19 future**) as of **2026-11-21** |
| Future franchise (Brand filter) | **17** |
| Future management | **2** (PHL Tier A + Denver SpringHill Tier B) |
| Brand Conversion (deduped hotels) | **136** |
| Management Contract (deduped hotels) | **4** (was 0) |
| Notice periods verified/calculated | 2 |

**Identity check:** `60 + 19 = 79` and `17 franchise future + 2 management future = 19`. Tier B enrichment queue future count (**17**) is franchise Tier B only — not the full 19.

---

## Phase 2–3 — No research horizon; date classes

- `within36Months` / `nearTermOutreachPriority` are **application filter metadata only**.
- Historical Tier B rows remain **`researchEligible: true`**.
- App filters available: ALL · HISTORICAL · FUTURE · NEXT_12/24/36/60 · UNKNOWN · custom range · country/region/brand/owner/operator/tier.
- Contract date classes: `EXPLICIT_CONTRACT_EXPIRATION`, `HISTORICAL_EXPIRATION_STATUS_UNKNOWN`, `CURRENT_CONTRACT_EXPIRATION_VERIFIED`, `NON_CONTRACTUAL_PROPERTY_DATE`, etc.
- Opening/loan dates stay HOLD — never prospect inflation.

### App filter examples (Brand list)

| Filter | Count |
|---|---:|
| ALL | 136 |
| HISTORICAL_EXPIRATIONS | 60 |
| FUTURE_EXPIRATIONS | 17 |
| NEXT_36_MONTHS | 5 |
| nearTermOnly | 5 |

Brand `FUTURE_EXPIRATIONS = 17` is correct and does **not** contradict inventory `future = 19` (the extra 2 are management-list hotels).

---

## Phase 5–7 — Tier B + Management Contract

### Tier B — all 76+ remain in scope

| Bucket | Count | Action |
|---|---:|---|
| Future-dated | 16–19 | Priority enrichment (owner/operator/notice/status) |
| Historically expired | 60 | Status resolution research — **do not discard** |

Classification for historical without successor evidence:  
`HISTORICAL_EXPIRATION_STATUS_UNKNOWN` / `REQUIRES_FOLLOW_UP` — **not** claimed as active conversion windows.

### Management Contract list (strict)

Named EX-10 MANAGEMENT hotels merged (portfolio/form-of-agreement excluded):

| Hotel | End date | Notice | Tier |
|---|---|---:|---|
| Hilton Philadelphia Airport | 2035-03-30 | 180 | A |
| SpringHill Suites Denver Airport | 2035-12-30 | — | B |
| Embassy Suites Las Vegas | undisclosed | 90 | C |
| Hilton San Francisco Financial District | undisclosed | 30 | C |

**Note:** Prior `MANAGEMENT_CONTRACT_TARGETS_V5.json` (80) inflated UNKNOWN/OTHER into List B. That path is corrected — only true `MANAGEMENT` (or explicit hints) qualify.

---

## Phase 8–9 — Dual lists (permanent)

1. **BRAND_CONVERSION_TARGETS** — franchisors / reflagging / franchise expirations (historical + future).
2. **MANAGEMENT_CONTRACT_TARGETS** — owner–operator / HMA expirations & transitions.

Same hotel may appear on both without duplicating property identity.  
CLOSED/SUPERSEDED archived separately; HOLD never inflates lists.

---

## Phase 11 — Exa Search / Agent / Jev (status)

| Layer | Role | Status |
|---|---|---|
| Exa Search + free EDGAR | High-volume discovery | Primary |
| Exa Agent | Optional deep verification | Pilot done — **MANUAL_ESCALATION_ONLY** ($0.762 of $15) |
| Jev | Typed routing | Live shadow — **DETERMINISTIC_ONLY** (insufficient measured benefit vs deterministic) |

Budgets remain separate (V5 Search $25 · Agent $15 · Jev eval ~$0.001).

---

## Founder review examples

### 1) Historical franchise — Tier B (Brand Conversion) — Joplin correction

- **Hotel:** Hilton Garden Inn Joplin (Joplin, MO)  
- **Brand:** Hilton / HGI  
- **Agreement:** FRANCHISE  
- **Verified end:** 2026-10-31  
- **As of 2026-11-21:** **HISTORICAL** (`monthsToExpiration = -1`) — prior narrative that labeled this “future” was wrong; **date preserved**  
- **Notice:** not disclosed  
- **Status:** HISTORICAL_EXPIRATION_STATUS_UNKNOWN / STATUS_UNCONFIRMED — **not** an active decision window  
- **Next:** VERIFY_RENEWAL_OR_SUCCESSOR_STATUS  
- **Evidence:** SEC CMBS schedule URL on prospect row  

### 2) Future management — Tier A (Management Contract)

- **Hotel:** Hilton Philadelphia Airport  
- **Agreement:** MANAGEMENT  
- **End:** 2035-03-30 · **Notice:** 180 days → calculated deadline **2034-10-01** (end − 180d)  
- **Notice relevance:** **Yes** — the 180-day clause applies to the 2035 expiration; it is **not** a near-term outreach trigger as of 2026-11-21 (~95 months to notice deadline)  
- **Tier:** A_VERIFIED_DECISION_WINDOW (verified end + notice window terms) — retained  
- **Current applicability:** HISTORICALLY_DISCLOSED_ONLY / STATUS_UNCONFIRMED (later-filing corroboration still open)  
- **Near-term outreach:** false · **Research eligible:** true  
- **Owner/operator:** enrichment pending (no unauthorized HPC)  
- **Evidence:** EX-10 persist source URL  

### 3) Historical franchise — Tier B retained (additional example)

- **Hotel:** Hampton Inn Mount Vernon  
- **End:** 2020-10-31 (historical)  
- **Status class:** HISTORICAL_EXPIRATION_STATUS_UNKNOWN  
- **Research eligible:** yes · **Near-term outreach:** no  
- **Next:** VERIFY_RENEWAL_OR_SUCCESSOR_STATUS  

### 4) Rejected false positive (HOLD)

- Opening/year-built misread as franchise expiration → quarantined (264 such false-expiration samples in metrics).

---

## Economics / coverage (this reconcile wave)

- Incremental paid research for reconcile: **$0** (offline reclassify + EX-10 merge).
- Prior V5/Agent spend unchanged and separately ledgered.
- Issuers beyond original 32-registry: 10 (from prior metrics).
- Global eligibility: **enabled in code** (country/region fields + no geo exclusion). International source adapters remain progressive — US SEC/CMBS still highest yield today.

---

## Remaining gaps / next research waves

1. Owner/operator enrichment for all Tier B (76) — reuse HE/HI org graph; no paid people enrichment.  
2. Historical status resolution for 60 expired Tier B (renewed / reflagged / terminated / unknown).  
3. Expand EX-10 / REIT HMA harvest for additional **named** MANAGEMENT agreements (strict attribution).  
4. Large-annex CMBS extraction repairs for missed legitimate expirations.  
5. International adapters (registries, exchanges, multilingual) — prioritize by yield; never geo-exclude.  
6. Optional UI: date/geo filters only — do not delete research inventory.

---

## Approvals / IC

- No customer publication · no HPC creates · CUSTOMER_BLOCKED  
- Schema proposals only if new Airtable fields required (none applied this wave)  
- **Integration Controller** owns merge of PR #503  

`AWAITING_INTEGRATION_CONTROLLER`
