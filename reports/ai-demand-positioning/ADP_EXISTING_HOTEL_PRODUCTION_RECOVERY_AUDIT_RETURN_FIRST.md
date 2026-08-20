# Existing Hotel ADP — Production Recovery Audit (RETURN FIRST)

**Mode:** Product Builder MODE B — FINISH / RECOVER  
**Generated:** 2026-08-21  
**Status:** Audit complete — **no production payload/Airtable mutations applied yet**  
**Artifacts:**  
- `reports/ai-demand-positioning/adp-existing-hotel-production-recovery-audit-v1.json`  
- `reports/ai-demand-positioning/adp-existing-hotel-recovery-deep-audit-v1.json`  
- Scripts: `scripts/run-adp-existing-hotel-production-recovery-audit-v1.mjs`, `scripts/run-adp-recovery-deep-audit-v1.mjs`

---

## A. CURRENT PRODUCTION INVENTORY

**Production read SoT (proven from code):** local published JSON under `data/ai-demand-positioning/published/`.  
Airtable is an **optional mirror/overlay** (`ADP_AIRTABLE_READ_LIVE=1` or `ADP_PUBLISHED_READ_SOURCE=airtable`). Default env has neither set.

| Property | Period (Live manifest) | Classification | Contract hash | Demand capture | Providers OK/Fail |
|----------|------------------------|----------------|---------------|----------------|-------------------|
| Waterstone Boca Raton | `…20260820131042_a69590` | **ACTIVE_OFFICIAL_BASELINE** (`ADP_OFFICIAL_BASELINE_PERIOD_001`) | `e4d85401…` | 70.5% | 307/5 of 312 |
| Renaissance Times Square | `…20260820131042_18731c` | **ACTIVE_OFFICIAL_BASELINE** | same | 43.1% | 256/4 of 260 |
| Cambridge Beaches Bermuda | `…20260820141258_547a7b` | **ACTIVE_OFFICIAL_BASELINE** | same | 98.3% | 233/7 of 240 |
| NOW NOW NOHO | `…20260820141258_2fe3a3` | **ACTIVE_OFFICIAL_BASELINE** | same | 88.9% | 243/9 of 252 |
| Hotel Phillips Kansas City | `…20260820194028_21bf47` | **CERTIFIED_STANDALONE** (`ADP_HOTEL_PHILLIPS_BASELINE_PERIOD_001`) — not portfolio four | same | 77.8% | 244/8 of 252 |

**Official portfolio baseline release:** `reports/ai-demand-positioning/adp-measurement-contract-v1-baseline-001-production-release.json` (2026-08-20).  
**Pre-baseline / test runtime periods retained locally:** 45 classified `TEST` / pre-baseline (not customer-trend-eligible). Archive ≠ delete.

---

## B. NUMERICAL RECONCILIATION

Independent rebuild of `buildOwnerPayload` from runtime observations vs published payload:

| Metric family | Result |
|---------------|--------|
| Demand Capture overall | **MATCH** (all 5) |
| AI Consideration Rate (`executiveMetrics.considerationRate`) | **MATCH** where present |
| AI Scenario Presence | **MATCH** |
| Presence Index (`intentPresenceIndex.*.index`) + CORE rates | **MATCH** on rebuild (field = `index` / `myRate` / `coreBenchmarkRatePct`) |
| Cross-metric (#1 > Top-3, etc.) | **No violations** on published executiveMetrics |

**Note:** #1 / Top-3 live under `executiveMetrics.rankMetrics.numberOneAppearanceRate` / `topThreeAppearanceRate` (not top-level aliases). Waterstone example: #1 = 12.3%, Top-3 = 78.9%, Consideration = 48.9% (observation grain), Scenario Presence = 70.5% (scenario grain) — contract-consistent different grains.

**Extreme Index values (formula correct, presentation later):** e.g. Cambridge Leisure 561, Celebration 598; Waterstone Couples 531, Leisure 469 → classify `CORRECT_BUT_PRESENTATION_REVIEW_PENDING`. **Do not change formula.**

**Unsupported action impact claims (published):** **11** across all 5 properties (e.g. “Could improve capture in 9+ scenarios”, “3 new demand scenarios captured”). Generator: `lib/ai-demand-positioning/customer/owner-payload.js`.

---

## C. ENTITY ERRORS

| Property | Issue |
|----------|--------|
| Waterstone | Alias clusters: Boca Raton Resort / Boca Raton Resort & Club / The Boca Raton; Eau Palm Beach Resort / Eau Palm Beach Resort & Spa; Four Seasons Resort / Four Seasons Resort Palm Beach |
| Cambridge | Prose fragments as hotels: “Many suites”, “This resort”, “this iconic hotel”, “Located in Tucker's Point, this resort” |
| Phillips | “This hotel”; Museum Hotel vs 21c Museum Hotel alias risk; Ambassador variants |
| Renaissance | “This hotel” |
| NOW NOW NOHO | Walker Hotel vs Walker Hotel Greenwich Village (near-dup) |

**Required fix:** one canonical customer-facing entity resolution path applied to competitive set, displacement, top alternative, exec references — preserve raw extraction internally.

---

## D. PROVIDER COMPLETENESS (incl. Phillips)

| Property | Expected | OK | Fail | Rule |
|----------|----------|----|------|------|
| Phillips | 252 | 244 | 8 Gemini | **Do not fill.** Failures = missing, not zero. Period immutable. |
| Others | see inventory | | Gemini/partial fails | Same omit-missing rule |

No evidence in this audit that published rates treat failed cells as zero for these certified periods (rebuild MATCH). Still add regression tests locking omit-missing.

---

## E. ROOT CAUSES

| Area | Cause type | Detail |
|------|------------|--------|
| Trend blank despite baseline | **Code (UI)** | Published `trends.length === 1` (correct). UI requires `length >= 2` → empty copy. |
| Unsupported impact numbers | **Code (payload builder)** | Hardcoded expectedImpact strings in `owner-payload.js` |
| Entity duplicates / junk | **Code (entity resolution incomplete on customer path)** | Extraction aliases not collapsed; prose fragments leak |
| Extreme Index optics | **Contract math + thin CORE** | Formula correct; presentation guardrail deferred |
| Evidence “inconsistency” | **Code (routing grain)** | Intent links only `type=missing`; evidence index capped (5/intent); no general scenario-ID hyperlink path in customer UI; displacement path exists separately |
| Airtable not reconciled | **Config / auth** | `ADP_AIRTABLE_BASE_ID` unset; PAT against default base returns **not authorized** for ADP table |
| Railway/production visibility | **Deploy** | Local publish ≠ Railway data unless deploy includes published artifacts or Airtable read-live |

---

## F. PROPOSED FIXES (bounded)

1. **Trend UI:** If exactly one comparable official period → show baseline date + Consideration / Scenario Presence (and demand capture if present) + “Awaiting next comparable period” / no delta / no fake chart series pretending change.  
2. **Action impacts:** Replace unsupported numeric expectedImpact with null-safe neutral copy; keep section structure.  
3. **Entity resolution:** Single customer canonicalizer (Eau Palm Beach, The Boca Raton, junk/prose suppression, near-dup collapse) on customer-facing lists.  
4. **Evidence:** Centralize query params (`propertyId`, `periodId`, `intent`, `scenarioId`, `competitor`, `type`); explicit empty state; ensure intent Missing links filter to that intent only; add scenario drill where UI already surfaces scenarios; do not redesign drawer chrome.  
5. **Baseline consumers:** Confirm only `ADP_OFFICIAL_BASELINE_PERIOD_001` + `customerTrendEligible` feed Trend/exec deltas; pre-baseline stay archived for provenance.  
6. **Republish:** Rebuild published payloads after (2)(3); OLD vs NEW diff; apply only after reconcile + tests.  
7. **Airtable:** Dry-run upsert/archive plan once `ADP_AIRTABLE_BASE_ID` + authorized token available — no mass delete.  
8. **Tests:** baseline singleton, one-period Trend, omit-missing, entity canon, evidence filter, impact neutralization.

---

## G. EXPECTED NUMBER CHANGES (customer-visible)

| Change | Direction |
|--------|-----------|
| Core KPI rates / Index | **No material change expected** if republish from same observations (rebuild already MATCH) |
| Action `expectedImpact` strings | Numeric promises → neutral / null |
| Competitive set / displacement names | Alias collapse → fewer rows, cleaner names (counts may drop) |
| Trend section | Baseline values become **visible** (not a delta invention) |
| Index display | Unchanged numerically until later presentation guardrail |

---

## H. SAFE PUBLISH PLAN

1. Fix code paths (Trend UI, impacts, entity, evidence routing/tests).  
2. Dry-run `buildPublishedSnapshotBundle` per property → diff OLD vs NEW JSON (metrics + entities + actions).  
3. Founder ack if entity/count deltas look right.  
4. Write local published snapshots (immutable raw runtime untouched).  
5. Verify API read from local SoT.  
6. Deploy / sync Railway published data (or enable governed Airtable Live read after Airtable dry-run upsert).  
7. Playwright correctness QA on current UI.  
8. Airtable: dry-run create/update/archive only; never delete certified active; archive superseded Live duplicates if found.

---

## I. BLOCKERS / FOUNDER DECISIONS

1. **Airtable base + token scope** — set `ADP_AIRTABLE_BASE_ID` and ensure PAT can read/write `AI Demand Positioning - Published Reports`. Until then Airtable reconcile is **BLOCKED** (local file remains production SoT).  
2. **Confirm Railway serves local published files vs Airtable** for customer ADP — needed before claiming production-visible fix.  
3. **Phillips** — keep out of official four-property baseline? (Audit treats as standalone; recommend **yes stay standalone**.)  
4. **Entity merge aggressiveness** — OK to collapse Eau / Boca / Four Seasons clusters and drop prose fragments as proposed?  
5. Go / no-go on **republish** after dry-run diffs.

---

## BASELINE INVENTORY

| Class | Count (local runtime) |
|-------|----------------------|
| ACTIVE_OFFICIAL_BASELINE | **4** (one per portfolio property, marker `ADP_OFFICIAL_BASELINE_PERIOD_001`) |
| CERTIFIED_STANDALONE | **1** (Phillips) |
| TEST / pre-baseline retained | **45** |
| Multiple active official baselines per property | **None** |

**Canonical active official baseline marker:** `ADP_OFFICIAL_BASELINE_PERIOD_001` (epoch release 2026-08-20). Older periods remain on disk as non-trend-eligible history.

### TREND DEFECT

- Payload correctly emits **one** baseline trend point.  
- UI: `renderTrends` treats `trends.length < 2` as empty → “Additional comparable monitoring periods…”.  
- **Fix:** one-period baseline display + awaiting next period; no delta.

### EVIDENCE ROUTING AUDIT

| Path | Status |
|------|--------|
| Demand intent “Missing” → `/evidence?intent=&type=missing` | Works; intent-filtered; capped at 5 excerpts |
| Displacement competitor → evidence API | Separate path exists |
| Scenario hyperlink → scenario-specific evidence | **No dedicated customer scenario-ID link path found** in ADP JS (scenario labels appear inside drawer items only) |
| Present / captured intent evidence | Not opened by current Missing buttons |
| Empty / error | Partial — some empty messages exist; needs consistent “Evidence unavailable…” |
| Grain mismatch risk | Provider-observation “missing” vs scenario-level “missed” can disagree; counts must not be presented as interchangeable |

### AIRTABLE ROLE

| Question | Answer |
|----------|--------|
| Is Airtable production SoT today? | **No** — optional mirror; default read = published files |
| Credentials in this audit env | PAT present; `ADP_AIRTABLE_BASE_ID` **unset**; queries to default base → **not authorized** |
| Schema | Table `AI Demand Positioning - Published Reports`; statuses Draft/Validated/Live/Archived |
| Repair | Dry-run only after base ID + auth fixed |

---

## BOUNDED FIXES APPLIED (code only — published payloads NOT replaced yet)

| Fix | Status |
|-----|--------|
| Trend one-period baseline UI | **Done** (`ai-demand-positioning.js`) — shows baseline + awaiting next period; no delta |
| Unsupported action impact claims | **Done** in `owner-payload.js` — `expectedImpact: null` + neutral `impactNote` |
| Customer entity resolution path | **Done** — `customer-entity-resolution-v1.js` wired into competitive set + displacement |
| NYC cross-market leak on non-NYC | **Done** — NYC canonical map gated by market; cross-market reject |
| Evidence empty copy | **Done** — explicit unavailable message |
| Regression test | **Done** — `npm run test:adp-existing-hotel-production-recovery-v1` |
| Republish local snapshots | **Pending founder ack** of OLD vs NEW entity/impact diffs |
| Airtable reconcile | **Blocked** — `ADP_AIRTABLE_BASE_ID` + authorized token |
| Scenario-ID evidence hyperlinks | **Partial** — intent Missing + displacement paths exist; dedicated scenario links still missing |
| Index presentation guardrail | **Deferred** (as ordered) |

Dry-run rebuild: Demand Capture rates unchanged. Impacts → null. Waterstone aliases collapse to The Boca Raton / Eau Palm Beach Resort & Spa. Cambridge prose largely suppressed.

## OPENAI + TABLE STABILITY (2026-08-21 addendum)

See `reports/ai-demand-positioning/ADP_OPENAI_AND_TABLE_STABILITY_RECOVERY_ADDENDUM.md`.

- Provider conclusion: **predominantly REAL** (Phillips extreme); **2** OpenAI alias false negatives fixed via subject-match v2 + runtime reparse.
- CORE note moved to Competitive Overview title row; Competitive Overview table uses fixed column geometry.

## COMPLETION GATE (updated)

Not production-correct until: one active official baseline per portfolio property (✓ local), archived not contaminating trends (✓ eligibility), Trend shows baseline with one period (✓ code; needs deploy), no fake delta (✓), contextual evidence for displayed intents (partial), scenario links where claimed (✗), omit-missing tests (partial), entity canon (✓ code; needs republish), impact neutralization (✓ code; needs republish), OpenAI subject aliases (✓ reparse applied; needs republish), table/CORE structural consistency (✓ code), publish/read consistency (pending republish + Railway), Airtable reconcile or explicit file-SoT ops note (blocked on credentials).

