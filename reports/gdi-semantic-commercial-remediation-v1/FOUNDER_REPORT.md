# GDI Semantic Commercial Remediation V1 — Founder Deliverable

**Generated:** 2026-11-13T02:12:44.982Z  
**Branch:** `fix/gdi-semantic-commercial-remediation-v1`  
**Immutable baseline:** `reports/gdi-semantic-commercial-remediation-v1/00-immutable-baseline/`  
**Post-remediation:** `reports/gdi-semantic-commercial-remediation-v1/01-post-remediation/`  
**As-of clock:** 2026-11-13  
**Policy:** No merge / deploy without founder authorization. Research integrity preserved.

---

## 1. Scores — original vs post-remediation

| Dimension | Baseline | Post-remediation | Δ |
|---|---:|---:|---:|
| Overall | 85.1 | 91.6 | 6.5 |
| Semantic consistency | 81.05293276108726 | 91.20886981402003 | 10.2 |
| Commercial accuracy | 78.58369098712447 | 87.55221745350501 | 9.0 |
| Readability | 73.77110157367667 | 86 | 12.2 |
| Content quality | 73.72961373390558 | 88.5722460658083 | 14.8 |

**90+ claim:** NOT claimed — scorecard below 90 on one or more dimensions

---

## 2. Findings corrected

| Metric | Baseline | Post | Corrected (net) |
|---|---:|---:|---:|
| Total contradiction rows | 644 | 55 | 589 |
| P0 | 6 | 0 | 6 |
| P1 | 207 | 4 | 203 |
| P2 | 317 | 26 | 291 |
| P3 | 114 | 25 | 89 |

---

## 3. Remaining findings by severity

- **P0:** 0
- **P1:** 4
- **P2:** 26
- **P3:** 25

Machine-readable remaining list: `01-post-remediation/02-contradictions.json` (each row includes hotelId, opportunityId, code, severity, passages).

---

## 4. Before / after examples

### SEM_DUPLICATE_EVIDENCE_URL
- **Opportunity:** `gdi_opp_5282e7a3223f2741` @ `rec35fExUxCClpOP6`
- **Before:** {"urls":"https://www.scbwi.org/annual-events"}
- **After:** Cleared on re-audit. Sample drawer action: Recommended next step
 Contact the housing provider / housing / hotel-reservation lead (not only the event brand), request housing-list inclusion, and confirm o
- **Fix:** Shared generator remediation

### AGENT_URGENCY_VS_MONITOR_STANCE
- **Opportunity:** `gdi_opp_9ace8bee94848595` @ `recG66DQJKP2c0UNh`
- **Before:** {"why_act_now":"Why act now Lodging / travel signal is publicly evidenced — confirm Midtown partner acceptance now.","recommended_next_step":"Recommended next step Do not aggressively sell the current cycle. Monitor venu
- **After:** Cleared on re-audit. Sample drawer action: Recommended next step
 Do not aggressively sell the current cycle. Monitor venue/housing announcements, capture a future trigger date, and keep a light relation
- **Fix:** Shared generator remediation

### SEM_POTENTIAL_BUSINESS_COPIED_TO_WHY_ACT_NOW
- **Opportunity:** `gdi_opp_incentive_travel_events_7_b8fede1i` @ `rec82D9zpB8fede1I`
- **Before:** {"text":"Incentive travel is gaining traction as companies plan future events."}
- **After:** Cleared on re-audit. Sample drawer action: Recommended next step
 Do not aggressively sell the current cycle. Monitor venue/housing announcements, capture a future trigger date, and keep a light relation
- **Fix:** Shared generator remediation

### SEM_TOURNAMENT_HOUSING_TEMPLATE_LEAK
- **Opportunity:** `gdi_opp_ycamp_art_geneve_2027_pr_teams_sponsor` @ `recrPQcZg7SFARRb2`
- **Before:** {"commercial_recommendation":"Contact the housing provider / tournament housing lead (not only the event brand), request housing-list inclusion, and confirm overflow peak-room needs and stay dates","recommended_next_step
- **After:** Cleared on re-audit. Sample drawer action: Recommended next step
 Contact the housing provider / housing / hotel-reservation lead (not only the event brand), request housing-list inclusion, and confirm o
- **Fix:** Shared generator remediation

### SEM_PIPELINE_SLUG_CUSTOMER_SUBJECT
- **Opportunity:** `gdi_opp_ycamp_art_geneve_2027_pr_teams_sponsor` @ `recrPQcZg7SFARRb2`
- **Before:** {"displayTitle":"PR Teams — Art Genève 2027 (SPONSOR)","what_is_happening":"PR teams is tied to PR teams — Art Genève 2027 (SPONSOR) (2027-01-28 – 2027-01-31), creating a potential overflow/housing hypothesis (unconfirme
- **After:** Cleared on re-audit. Sample drawer action: Recommended next step
 Contact the housing provider / housing / hotel-reservation lead (not only the event brand), request housing-list inclusion, and confirm o
- **Fix:** Shared generator remediation


---

## 5. Classification remediation

| Status | Count |
|---|---:|
| Recovered / confirmed from research evidence | 34 |
| Explicitly flagged unresolved | 30 |
| Total reviewed | 64 |

Details: `01-post-remediation/04-classification-remediation.json`  
Unresolved rows remain `OTHER_UNCLASSIFIED` / `FLAGGED_UNRESOLVED` — no generic categories assigned to clear the audit.

---

## 6. New regression tests

- `scripts/test-gdi-semantic-commercial-remediation-v1.mjs`
- `npm run test:gdi-title-sourcing-commercial-consistency-v1`
- `npm run test:gdi-regression-recovery-v1`

Run: `npm run test:gdi-semantic-commercial-remediation-v1`

---

## 7. Shared fixes integrated

1. Host-hotel-contracted wording scrub on `whyHotelDemand` / hotel-relevance thesis (sanitizer)
2. Closed / monitor-only sourcing → Monitor (not Win Room Block) via sales-action resolver + remediation scrub
3. Internal `gdi_hotel_*`, `rec…`, demand-enum scrubbers in sales presentation
4. New shared module `gdi-semantic-commercial-remediation-v1.js` (URL dedupe, whyActNow≠potentialBusiness, tournament-housing leak, pipeline slug titles, urgency↔monitor alignment)
5. Past-cycle vs future-date detector now uses as-of clock (eliminates false P1s on past 2026 dates)
6. Research-title classification recovery for congress / sports / education / government contracts

---

## 8. Release gates

| Gate | Status |
|---|---|
| Immutable baseline preserved | PASS |
| Re-render all 699 opportunities | PASS |
| Browser verify (Bethesda + Wyndham toolbars) | PASS — heights aligned; Waterstone still 0 inventory |
| Merge / deploy | **NOT AUTHORIZED** |
| Paid research | **NOT AUTHORIZED** |

---

## 9. Why not a full 90+ dimensional claim

Weighted **overall is 91.6**, and **semantic consistency is 91.2**, but:

| Dimension | Post | Notes |
|---|---:|---|
| Commercial accuracy | 87.6 | Remaining P1/P2 commercial judgment rows (disqualified urgency, attendance claims, exhibitor contact quality) |
| Readability | 86.0 | Rubric ceilings clean rows at 86 in `scoreFromFindings` (same agent-batch formula as baseline) |
| Content quality | 88.6 | Pulled down by remaining stub / contact / timing presentation rows |

Do **not** treat overall ≥90 as all-dimension clearance.

---

## 10. Remaining open clusters (exact IDs in contradictions JSON)

| Code | Count | Sev | Notes |
|---|---:|---|---|
| `SEM_IMPLAUSIBLE_MONTH_LONG_TIMING` | 22 | P3 | Card timing spans ≥20 days — often research date-range packaging |
| `SEM_PEAK_ROOMS_PUBLISHED_VS_SIZE_SIGNAL` | 11 | P2 | Size-signal remnants in advantages lists |
| `SEM_EXHIBITOR_CONTACT_QUALITY` | 10 | P2 | Placeholder exhibitor contact paths |
| `SEM_DISQUALIFIED_VS_URGENT_DEMAND_FRAMING` | 4 | P1 | Residual urgency framing on past-cycle / DISQUALIFIED |
| Others | 8 | P2–P3 | Attendance claims, advance-planning, stale evidence year |

---

## 11. Unresolved research / policy questions

1. Should month-long card timing windows be normalized to event start-only when end is a research placeholder?
2. Authorize paid research for the 30 `FLAGGED_UNRESOLVED` classification rows?
3. Persist recovered demand-segment IDs into opportunity stores, or keep presentation-only recovery?
4. Hide Hotel Validation placeholder sections entirely until workflow populates them (still harness-excluded from product counts)?
