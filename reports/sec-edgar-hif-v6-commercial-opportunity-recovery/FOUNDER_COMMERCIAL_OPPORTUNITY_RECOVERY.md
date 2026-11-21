# HIF V6 — Commercial Opportunity Recovery & Global Research Intelligence

**As of:** 2026-11-21  
**Publication:** CUSTOMER_BLOCKED  
**Research cost:** $0  

## 1. Executive commercial summary

- Canonical inventory analyzed: **1259** contractual research records
- Qualified near-term opportunities (first-100 gate): **0** / 100
- Priority A: **0** · Priority B: **92** · C: **175** · D: **672** · HOLD: **320**
- Verified commercial decision windows: **0**
- Outreach-ready: **0**
- Qualified BCT / MCT: **74** / **48**

### Main commercial limitations
- CMBS schedules overwhelmingly yield EXPIRATION_ONLY_PROXY — not confirmed commercial decision windows
- Absence of renewal evidence must not be treated as proof no renewal occurred
- Priority B ≤24m cohort is a research queue, not an automatic commercial pipeline
- Physical geography often REVIEW_REQUIRED; filing jurisdiction never substitutes for hotel location
- First-100 milestone excludes expiration-only proxies by design
- Nearly all current CMBS-derived rows use EXPIRATION_ONLY_PROXY — confirmed notice deadlines are rare in schedule extracts
- Physical country often REVIEW_REQUIRED when locality cues are weak; filing jurisdiction alone never verifies country
- Unresolved combined License/Franchise/Operating/Management columns cannot auto-qualify as BCT/MCT
- Priority A requires confirmed decision window or strong inferred active signal — expiration-only near-term is Priority B at best
- 8-K undated management signals contribute UNKNOWN timing unless a date is extracted

## 2. Country × timing matrix

See `MATRIX_COUNTRY_TIMING.json`. Physical verification classes:
- VERIFIED: 226
- REVIEW_REQUIRED: 1033
- UNRESOLVED: 0

### Research-gap analysis

- Geographic verification gap: 1033
- Timing gap: 320
- Agreement classification gap: 884
- Current-status gap: 1259
- Commercial qualification gap: 1259
- Coverage gap: NOT_COMPUTED_WITHOUT_CREDIBLE_DOCUMENTED_HOTEL_UNIVERSE_DENOMINATOR

## 3. Source vintage × timing matrix

See `MATRIX_SOURCE_VINTAGE_TIMING.json`. Research direction mix:
- OTHER_UNKNOWN: 16.9% (213)
- CURRENT_BACKWARD: 17.3% (218)
- HISTORICAL_FORWARD: 65.8% (828)

### Evidence freshness
- UNKNOWN: 137
- SUPERSESSION_RISK_HIGH: 931
- SOURCE_RECENT_UNCHECKED: 133
- SOURCE_AGED_UNCHECKED: 58

## 4. Research direction comparison

### HISTORICAL_FORWARD — CMBS / Related License name-free SEC
```json
{
  "label": "HISTORICAL_FORWARD — CMBS / Related License name-free SEC",
  "sourceDocumentsInspected": 80,
  "incrementalContractualRecords": 0,
  "nearTermDecisionCandidates": "see Priority B cohort from timing audit",
  "newlyQualifiedBctMct": "primarily unresolved → research; see agreement-type-resolution",
  "priorityAPromotions": 0,
  "outreachReadyPromotions": 0,
  "researchCostUsd": 0,
  "commercialYieldNote": "Highest raw contractual yield; commercial yield low — expiration-only proxies dominate"
}
```

### CURRENT_BACKWARD — 8-K / commercial-event then contract trace
```json
{
  "label": "CURRENT_BACKWARD — 8-K / commercial-event then contract trace",
  "sourceDocumentsInspected": 21,
  "incrementalSignals": 161,
  "newVerifiedManagementContractualDates": 0,
  "newQualifiedMct": 0,
  "priorityAPromotions": 0,
  "researchCostUsd": 0,
  "commercialYieldNote": "Adds contemporary signals but often undated — timing UNKNOWN until contract dates linked"
}
```

### CONTRACT_WINDOW_TARGETED — Priority B ≤24m deep audit
```json
{
  "label": "CONTRACT_WINDOW_TARGETED — Priority B ≤24m deep audit",
  "cohortSize": 92,
  "reviewed": 92,
  "noticeDeadlineFound": 0,
  "promotedPriorityA": 0,
  "remainingPriorityB": 16,
  "rejectedOrInvalid": 4,
  "hold": 72,
  "researchCostUsd": 0,
  "commercialYieldNote": "Directly tests near-term expiration cohort; promotions require confirmed notice/decision evidence"
}
```

## 5. Priority B recovery results

- Reviewed: 92
- Geography verified: 24
- Agreement type verified (document cues): 13
- Current status corroborated: 0
- Notice/termination deadline found: 0
- Promoted to Priority A: 0
- Remaining Priority B: 16
- Superseded/rejected: 4
- HOLD: 72

### Hotel-specific examples
- **turity Date of the Related Mortgage Loan Residence Inn Toledo Maumee** — REJECTED_OR_INVALID: Hotel identity fails noise/loan-maturity parse quality gate
- **turity Date of the Related Mortgage Loan Hyatt Place Portfolio** — REJECTED_OR_INVALID: Hotel identity fails noise/loan-maturity parse quality gate
- **turity Date of the Related Mortgage Loan Embassy Suites Glendale** — REJECTED_OR_INVALID: Hotel identity fails noise/loan-maturity parse quality gate
- **Days Inn Iselin** — PRIORITY_B_RESEARCH: Document-level FRANCHISE cues found but no confirmed notice deadline — remains research candidate
- **TownePlace Suites Redwood City** — PRIORITY_B_RESEARCH: Document-level MANAGEMENT cues found but no confirmed notice deadline — remains research candidate
- **Hilton Garden Inn Albuquerque Uptown** — PRIORITY_B_RESEARCH: Document-level MANAGEMENT cues found but no confirmed notice deadline — remains research candidate
- **Holiday Inn Express Savannah - Historic District** — PRIORITY_B_RESEARCH: Document-level MANAGEMENT cues found but no confirmed notice deadline — remains research candidate
- **Holiday Inn Express King of Prussia** — PRIORITY_B_RESEARCH: Document-level MANAGEMENT cues found but no confirmed notice deadline — remains research candidate
- **Hampton Inn Suites Waco South** — HOLD_INSUFFICIENT_EVIDENCE: Expiration-only proxy + unresolved type + aged source supersession risk — current status not corroborated
- **Hampton Inn Suites Bloomington Normal** — HOLD_INSUFFICIENT_EVIDENCE: Expiration-only proxy + unresolved type + aged source supersession risk — current status not corroborated
- **Waco Residence Inn** — HOLD_INSUFFICIENT_EVIDENCE: Expiration-only proxy + unresolved type + aged source supersession risk — current status not corroborated

## 6. First 100 commercial milestone

**Qualified count: 0 / 100**

Standards were not weakened. Expiration-only Priority B proxies are excluded.

Zero opportunities currently meet the milestone gate (verified decision window within 24 months + franchise/management relevance + identity/geo). Near-term inventory remains Priority B research candidates pending notice/EX-10/current-status evidence.

## 7. Discovery continuation

- Name-free SEC CMBS harvest remains the primary HISTORICAL_FORWARD engine.
- CURRENT_BACKWARD (8-K management / conversion signals) complements with undated commercial events.
- CONTRACT_WINDOW_TARGETED recovery operates on the ≤24m Priority B cohort.
- Worldwide eligibility unchanged — priority markets are concentration only.

## 8. Integration

- Modules: research-direction-freshness, research-gap-visibility, first-100-milestone
- Scripts: commercial-opportunity-recovery, priority-b-recovery-audit, current-backward-discovery
- Tests: `test:hif-v6-commercial-opportunity-recovery-v6`
- Integration Controller owns merge; Track A/B and HE/ADP/GDI/BE dirty worktrees preserved
- Actual spending: **$0**

---

**WHERE × WHEN × HOW CURRENT** — physical geography, decision windows (not expiration proxies alone), and evidence freshness are first-class and separately measured.