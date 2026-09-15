# Bethesda GDI — Qualification Precision Iteration

**Date:** 2026-09-15  
**Hotel:** Bethesda Marriott (`recLuxvwwxID7U2B8`)  
**Mode:** MODE B — finish / recover (no rebuild, no ADP changes, no second research engine)  
**Webhound spend this iteration:** **$0** (hard cap remains $15; prior pilot spend unchanged)  
**Verdict:** **PARTIAL GO** → production commercial QA + redeploy: **SAFE TO SHARE** (2026-09-15)

---

## Commercial QA addendum (pre-share, 2026-09-15)

### Bethesda Premier Cup — DOWNGRADED High → Medium
Official field map places age groups across **Alexandria VA, Poolesville, Olney, Upper Marlboro (U15), and SoccerPlex Boyds** — not Bethesda-centered lodging. HBC stay-to-play is real, but partner hotels cluster near assigned fields. Organizer name “Bethesda” is insufficient for Core High. Territory corrected to **DMV Competitive**.

### Potomac Memorial — RETAINED High
Primary venue **Maryland SoccerPlex, Boyds** (Montgomery). Stay-to-play via HBC for teams >100 miles. Named director + ~25–40 min to Bethesda Marriott is commercially credible. Overflow/Housing + Verified Open.

### AMWA / NICE / ACTS Medium review
| Opp | Bucket | Note |
|-----|--------|------|
| AMWA | A — weak/incomplete qualification | Venue TBD but room demand UNKNOWN + generic inbox |
| NICE | B — strong open, missing named contact | STRONG qual; High blocked only by generic `nice@nist.gov` — appropriately strict |
| ACTS | B — strong open, missing named contact | STRONG qual; unnamed staff desk — deepen contact, do not auto-promote |

High contact rule is **appropriately strict** for “act this week”; Medium still surfaces exceptional open opportunities.

### Final production counts (verified live)
| | Pre-precision | Post-precision | Post commercial QA (LIVE) |
|--|-------------:|---------------:|--------------------------:|
| High | 5 | 2 | **1** |
| Medium | 13 | 12 | **12** |
| Watchlist | 11 | 15 | **16** |

Deploy `58968cbb` SUCCESS · share resolve OK · token unchanged · cache bust `gdi-qual-20260915`

Client URL (unchanged):  
https://my-operators-backend-production.up.railway.app/group-demand-intelligence-share.html?share=gdishare.v1.eyJ2IjoxLCJ0aWQiOiJnZGlzaHRfNDdjMjVkNzRjNzkyMTYwMjFmYjM2MTUwIiwiaG90ZWxJZCI6InJlY0x1eHZ3d3hJRDdVMkI4Iiwic3VyZmFjZXMiOlsiYnJpZWYiLCJvcHBvcnR1bml0aWVzIiwib3Bwb3J0dW5pdHlfZGV0YWlsIiwic3VtbWFyeSJdLCJtb2RlIjoicmVhZF9vbmx5IiwiaWF0IjoxNzg5MjE0OTMyLCJleHAiOjE3OTg2NzUyMDB9.DcmVDLtAQD_XkAvgI8mn6ir8YREmAwJ0uG3VOQXY-gc

---

## What Changed

1. **Opportunity Type** (mandatory): `PRIMARY_PURSUIT` · `OVERFLOW_HOUSING` · `REACTIVATION` · `FUTURE_CYCLE` · `CLOSED_DISQUALIFIED`
2. **Venue / Sourcing Status** gate (evidence-based, before priority): Open / RFP / Hotel TBD / Partially placed / Primary+overflow / Primary+no overflow / Fully placed / Current cycle closed / Unknown
3. **Opportunity Qualification** (separate from Hotel Fit): `VERIFIED_OPEN` · `STRONG` · `MODERATE` · `WEAK` · `CLOSED`
4. **Hard priority rules:** Fully placed → not High (normally Closed/DQ); Primary+no overflow → not High; Primary+overflow → type must be Overflow/Housing; high Hotel Fit cannot rescue closed qualification
5. **Research method** `GDI-VENUE-HOUSING-01` (`verify_event_venue_and_housing_status`) registered; contact research remains post-qualification (`GDI-CONTACT-01`)
6. **Event geography > organization name/HQ** for Demand Territory Fit
7. **Event Location Status:** Verified Venue / City / Region Only / Estimated / Unknown
8. **Room demand ≠ attendance:** Room Demand Status + published vs estimated attendance/rooms fields; High blocked when room demand unknown/local-limited without structure evidence
9. **Hotel Opportunity Thesis** required for High
10. **Contact Quality** + role relevance fields; High requires named decision maker / named meetings / housing-sourcing contact (generic inbox → Medium at best)
11. **Reactivation Signal** taxonomy (hotel validation / history can mark reactivation without CRM)
12. **Qualification Failure Reason** on feedback form (evaluation only — no auto-retrain)
13. **Scoring order:** Venue → Opportunity validity → Room demand → Geography → Hotel Fit → Timing → Contactability → Priority
14. **UI:** table + detail drawer + weekly brief show Type / Qualification / Room Demand / Venue; share page updated
15. **Reprocessed existing Bethesda/DMV corpus only** — no broad discovery pass

### Key modules

| Module | Role |
|--------|------|
| `lib/group-demand-intelligence/qualification-precision.js` | Classifiers + High quality gate |
| `lib/group-demand-intelligence/qualification-precision-pass.js` | Re-score existing opportunities |
| `lib/group-demand-intelligence/scoring.js` | `classifyPriority` respects qualification gates |
| `lib/group-demand-intelligence/opportunity-factory.js` | Persists new fields at choke point |
| `scripts/gdi-apply-qualification-precision.mjs` | Offline apply (`npm run gdi:apply-qualification-precision`) |

---

## Before vs After

| Priority | Before | After |
|----------|-------:|------:|
| High | 5 | **2** |
| Medium | 13 | 12 |
| Watchlist | 11 | 15 |
| Disqualified | 9 | 9 |
| Salesperson-visible | 29 | 29 |

Snapshot: `reports/group-demand-intelligence/bethesda-qualification-precision-before-after.json`  
Applied run: `gdi_run_mu1vqpr8_ea6e6350` (re-apply after overflow false-match fix)

---

## High Priority Review (after)

### 1. Bethesda Premier Cup 2026
| Field | Value |
|-------|--------|
| Opportunity Type | Overflow / Housing |
| Venue Status | Primary Venue Selected / Overflow Possible |
| Room Demand | Verified Housing Program |
| Qualification | Verified Open |
| Hotel Fit | 79 |
| Evidence Confidence | 71 |
| Contact Quality | Named Decision Maker (Brad Roos) |
| Why Now | Tournament dates sanctioned; official hotel program via HBC |
| Recommended Action | Contact housing provider / tournament housing lead; request housing-list inclusion |

### 2. Potomac Memorial Tournament 2027
| Field | Value |
|-------|--------|
| Opportunity Type | Overflow / Housing |
| Venue Status | Primary Venue Selected / Overflow Possible |
| Room Demand | Verified Housing Program |
| Qualification | Verified Open |
| Hotel Fit | 79 |
| Evidence Confidence | 66 |
| Contact Quality | Named Decision Maker (Kathy Hauschild) |
| Why Now | 2027 housing list planning before RFP season |
| Recommended Action | Housing / stay-to-play path — not primary host sell |

Both High rows are **housing/overflow pursuits with named directors and verified housing evidence** — not “found an event title” false positives.

---

## Downgraded Opportunities (former High)

| Opportunity | Before → After | Why |
|-------------|----------------|-----|
| AMWA 2027 Annual | High → **Medium** | Generic inbox (`associatedirector@…`); room demand Unknown → Qualification **Weak**; Hotel Fit stayed strong but gate blocked High |
| NICE 2027 | High → **Medium** | Venue open / estimated rooms / Strong qualification, but `nice@nist.gov` treated as **generic inbox** — fails High contact checklist |
| ACTS TS27 | High → **Medium** | Unnamed “meetings staff” desk → generic / non-named contact; cannot be High until a named meetings contact is found |

These remain **credible Medium** pursue/qualify items — not deleted. Precision improved by refusing to call them “act this week as High.”

---

## False-Positive Patterns Fixed

| Pattern | Fix |
|---------|-----|
| Event found but host already selected | Venue gate → Fully Placed / Closed; High blocked even if Hotel Fit ≥ 75 |
| “No overflow” text matching overflow | Regex requires affirmative overflow; excludes `no overflow` |
| Org name “Bethesda” ⇒ Core geography | Event destination/venue first; org HQ is weak signal only |
| Large attendance ⇒ large rooms | Room Demand Status; local/day-meeting → Local Limited; attendance alone ≠ High |
| Fields/housing events scored as primary host | Opportunity Type = Overflow/Housing; score overflow path |
| Generic inbox = strong contactability | Contact Quality enum; High requires named / housing contact |
| Hotel Fit overrides closed deal | `classifyPriority` closes on Qualification Closed / Fully Placed before fit thresholds |

Regression coverage: `qualification_precision_false_positive_patterns` in `npm run test:gdi-foundation` — **PASS**

---

## Contact Quality

| Metric | Before (approx) | After |
|--------|-----------------|-------|
| High with named relevant contact | Mixed (several High used generic / unnamed desks) | **2 / 2 (100%)** |
| Former High with generic inbox | AMWA, NICE, ACTS-style | Downgraded to Medium |

Contact enrichment remains **post-qualification** — no deep contact spend on closed/placed candidates.

---

## Venue Verification

- Method `GDI-VENUE-HOUSING-01` applied as L2 classification over existing evidence (no new Webhound).
- Sports/housing opportunities correctly moved to **Primary + Overflow Possible**.
- Priority changes driven by venue+contact+room gates: **3 of 5 former High** reclassified.

---

## Room Demand

- Attendance without room-block / housing / multi-day overnight structure no longer implies guestrooms High.
- Verified housing programs (Premier Cup, Potomac) correctly elevated room-demand confidence.
- NICE/ACTS keep cautious **Estimated** room demand from conference structure — enough for Medium, not automatic High without named contact.

---

## Reactivation

- Taxonomy + thesis fields shipped.
- No CRM required; hotel validation can supply prior Marriott/hotel history later.
- Current corpus mostly `UNKNOWN` / market-presence — no forced “already known” label from public web alone.

---

## Economics

| Item | Amount |
|------|--------|
| New broad discovery | **Not run** (per brief) |
| Incremental Webhound | **$0** |
| Global Webhound cap | **$15** unchanged |

---

## Product Assessment

1. **Are High Priority opportunities materially more reliable?** Yes — High shrank from 5 → 2 and both clear a venue + housing + named-contact + thesis gate.
2. **Would a DOS trust the top list enough to act?** More than before for the two High rows; Medium still needs contact deepening (NICE/AMWA/ACTS).
3. **Did venue verification eliminate false positives?** Partially — overflow vs primary is explicit; fully-placed patterns are gated in code/tests. Live web re-verify of every Medium still deferred ($0 spend).
4. **Did event-geography correction improve fit?** Yes — org-name-only Bethesda no longer forces Core.
5. **Did room-demand validation improve usefulness?** Yes — attendance ≠ rooms is enforced in qualification.
6. **Did contact quality improve?** On High, yes (100% named). Overall corpus still needs post-qual contact enrichment for Medium High-candidates.
7. **Ready for another fresh research cycle?** **Not yet as GO.** Qualification model is materially better; next cycle should deepen contacts + selective venue verification on Medium Strong candidates before expanding discovery.

---

## Success question

> When GDI tells a hotel salesperson to pursue something now, how often is that recommendation actually right?

**Structural proxy:** High list now only contains opportunities with verified-open qualification, overflow/housing or primary path, room-demand support, and named contacts.  
**True High Priority Precision (>80%)** still needs fresh hotel-sales validation labels (not the foundation-test feedback stubs). Until that review, claim **precision improved**, not certified.

---

## Final Verdict

# PARTIAL GO

**Ship the qualification model + reprocessed corpus + UI.**  
**Do not** run another broad discovery pass until hotel validation confirms High precision and Medium contact gaps are closed.

### Next recommended slice (not this iteration)
1. Selective L3 venue/housing verify on Medium Strong (NICE, AMWA, ACTS) — document cost before any Webhound
2. Named contact enrichment only after venue gate passes
3. Hotel validation pass to measure Discovery / Actionability / High Precision rates
4. Only then: fresh discovery cycle

---

## Test checklist

- [x] `npm run test:gdi-foundation` — all PASS (including new false-positive pattern gate)
- [x] Persisted Bethesda opportunities reprocessed (`gdi:apply-qualification-precision --apply`)
- [x] No ADP / Census / CRM paths touched
- [x] Webhound spend $0

### Manual QA
1. Open `/group-demand-intelligence` — Opportunities table shows Type, Qualification, Room Demand, Venue Status
2. Open High rows — detail sections 1–18 render; Hotel Fit note clarifies it is not availability
3. Share URL — Type / Qualification / Room Demand visible; no admin feedback
4. Confirm High count = 2 (Premier Cup, Potomac Memorial)
