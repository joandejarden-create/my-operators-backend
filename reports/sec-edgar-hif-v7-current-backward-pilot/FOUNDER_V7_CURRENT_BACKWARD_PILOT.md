# HIF V7 — Current-Backward Commercial Opportunity Discovery

**As of:** 2026-11-21  
**Publication:** CUSTOMER_BLOCKED  
**Research cost:** $0  
**Integration dependency:** PRs #528 → #529 → #531 still OPEN; this pilot does not compete for canonical inventory merges.

## A. Executive results

- **distinctHotelsInvestigated:** 10
- **candidateSignalsDiscovered:** 10
- **priorityAQualified:** 4
- **priorityBRemaining:** 18
- **priorityBFromCurrentBackward:** 2
- **priorityBFromPriorRecoveryCohort:** 16
- **qualifiedBct:** 1
- **qualifiedMct:** 3
- **outreachReady:** 0
- **rejectedOrCompleted:** 3
- **hold:** 1
- **first100Progress:** 4/100
- **actualSpendUsd:** 0
- **milestoneTarget:** 5–10 Priority A (zero acceptable)
- **milestoneResult:** BELOW_FLOOR (4/5) — standards not weakened

## B. Opportunity dossiers — Priority A

### Gideon Putnam Hotel
- Location: Saratoga Springs, NY, US
- Type: MCT · Status: ACTIVE_SELECTION
- Trigger: ACTIVE_OPERATOR_CONCESSION_RFP
- Thesis: New York State Parks RFP X001531 seeks a new concessionaire to operate and preserve the Gideon Putnam Hotel; proposals due 14 Jan 2027 (extended 17 Nov 2026). Incumbent Delaware North concession expires 5 Jan 2028. Active open operator selection with verified physical hotel identity.
- Window: 2027-01-14 (EVIDENCE_INFERRED_WINDOW)
- Owner/contracting party: NYS Office of Parks, Recreation and Historic Preservation
- Source: https://parks.ny.gov/concessions/x001531-gideon-putnam-hotel-concession-proposal
- Uncertainties: Lease/concession structure differs from private HMA; brand affiliation optional; Capital investment minimum $5M required
- Next action: Monitor shortlist/award; prepare MCT engagement materials for state-concession operators

### Marriott Marquis Chicago
- Location: Chicago, IL, US
- Type: MCT · Status: DECISION_PENDING
- Trigger: OPERATOR_REBID_QMA_EXPIRY
- Thesis: MPEA RFP 2026-15-M sought a hotel operator with a major national lodging brand for the 1,205-key McCormick Place headquarter hotel. Current QMA expires 20 Sep 2027. Proposal due 1 Jun 2026; as of 19 Oct 2026 market intelligence still reports no public winner, with Board vote historically scheduled April 2027. Decision remains pending — not awarded.
- Window: 2027-09-20 (CONFIRMED_DECISION_WINDOW)
- Owner/contracting party: Metropolitan Pier and Exposition Authority (MPEA)
- Source: https://www.mpea.com/wp-content/uploads/2026/03/Hotel_Operator_RFP_03.11_FINALversion.pdf
- Uncertainties: Shortlist/negotiation may be confidential; award could occur before QMA expiry; Incumbent Marriott may be reselected
- Next action: Track MPEA Board agenda for April 2027; confirm shortlist status before outreach

### Downtown Pueblo Hotel (planned — Alan Hamel Ave & Central Main St)
- Location: Pueblo, CO, US
- Type: BCT · Status: ACTIVE_SELECTION
- Trigger: ACTIVE_BRAND_AND_OPERATOR_SELECTION_NEW_HOTEL
- Thesis: Pueblo Urban Renewal Authority open RFP for a new downtown hotel on ~0.95 acre at Alan Hamel Ave & Central Main St. Proposals due 11 Jan 2027 must identify hotel operator and brand/franchise partner. Active brand + operator selection for a verified physical development site.
- Window: 2027-01-11 (EVIDENCE_INFERRED_WINDOW)
- Owner/contracting party: Pueblo Urban Renewal Authority
- Source: https://puebloura.org/rfps/
- Uncertainties: New-build development site (not conversion of an existing hotel); Selection does not guarantee conveyance or public assistance
- Next action: Engage brand/operator teams for Pueblo RFP response; dual BCT+MCT relevance

### Kenricia Hotel
- Location: Kenora, ON, CA
- Type: MCT · Status: ACTIVE_SELECTION
- Trigger: HISTORIC_HOTEL_REVITALIZATION_RFP_UNDER_REVIEW
- Thesis: City of Kenora RFP for purchase/preservation/rehabilitation/adaptive reuse of the historic Kenricia Hotel closed 15 Oct 2026. As of 17 Nov 2026, three proposals (Smitward Adventures, Embark Hotels, BMI Group) remain under review with council selection pending. City expects continued hotel use. Open developer/operator decision not yet awarded.
- Window: 2026-12-31 (EVIDENCE_INFERRED_WINDOW)
- Owner/contracting party: City of Kenora
- Source: https://www.kenora.ca/news-and-notices/posts/city-seeks-proposals-to-revitalize-historic-kenricia-hotel/
- Uncertainties: Adaptive reuse may include mixed uses beyond pure hotel operation; Council vote timing uncertain
- Next action: Monitor Kenora council for proponent selection; assess Embark Hotels path vs other bidders

### Strongest Priority B
- **Jericho Beach Hostel** (CA): City of Vancouver RFP PS20261745 seeks an operator to manage/operate Jericho Beach Hostel under a lease (5 years + options). Proposals due 25 Nov 2026. Active open process with ver…
  Missing for A: V7_PRIORITY_A_CURRENT_BACKWARD, HOSTEL_PRODUCT_NOT_CORE_HOTEL_ICP
- **Westin Denver International Airport** (US): DEN issued a 2026 hospitality consulting RFP (closed 29 Jul 2026) whose scope includes assisting the City with an RFP for a new hotel management agreement and transition to a new D…
  Missing for A: NOISY_HOTEL_IDENTITY

## C. Country × timing
```json
{
  "US": {
    "NOW_ACTIVE_PROCESS": 2,
    "6_12_MONTHS": 1,
    "UNKNOWN": 1,
    "0_6_MONTHS": 2
  },
  "CA": {
    "0_6_MONTHS": 1,
    "NOW_ACTIVE_PROCESS": 1,
    "UNKNOWN": 1
  },
  "GB": {
    "HISTORICAL_SUPERSEDED": 1
  }
}
```

### Coverage gaps (priority markets)
- MX: no open hotel operator/brand RFP corroborated this pass — Fibra Inn Guadalajara conversion already completed (Dec 2025)
- BR: conversion activity observed (e.g. Intercity Campinas) but completed/awarded — no open Priority A process found
- GB: Nottingham Belfry sale completed to Hunton — distress→sale pattern, not open MCT
- CA: Kenricia under review + Jericho hostel RFP open; YQB EOI past deadline without verified next stage
- US: strongest open yield via public procurement (Gideon Putnam, Pueblo, Chicago Marquis pending)
- National hotel-universe coverage % not computed (no credible denominator)

## D. Research direction comparison
```json
{
  "trackA_currentBackward": {
    "effortShareGuide": "50%",
    "hotelsInvestigated": 10,
    "priorityA": 4,
    "priorityB": 2,
    "rejectedCompleted": 3,
    "commercialYieldNote": "Highest verified commercial yield in this pilot — public RFPs and pending awards"
  },
  "trackB_contractWindow": {
    "effortShareGuide": "30%",
    "cohortReviewed": 16,
    "priorityAPromotions": 0,
    "commercialYieldNote": "16 remaining Priority B stay research — expiration-only CMBS without current-status corroboration"
  },
  "trackC_historicalForward": {
    "effortShareGuide": "20%",
    "note": "SEC name-free harvest continues under #528/#531 stack; not re-run this pilot to avoid duplicate inventory writes",
    "commercialYieldNote": "High raw contractual yield; low Priority A yield (prior audit 0/100)"
  }
}
```

## E. Source effectiveness
- Productive: state/municipal hotel operator & development RFPs (NYS Parks, PURA, MPEA, Kenora)
- Productive: recent WARN/regulator filings for operator changes (detect awards to avoid false opens)
- False positive pattern: sale/foreclosure/receivership without brand/operator decision language
- False positive pattern: completed soft-brand announcements already on brand.com
- False positive pattern: aged CMBS expiration-only rows (Track B)

## F. Next research allocation
- Increase CURRENT_BACKWARD share toward public procurement + trade press for open RFPs
- Maintain Track B only when fresh EX-10/8-K/current brand evidence appears for the 16
- Keep SEC historical-forward for inventory growth but do not count toward Priority A without current-backward corroboration
- Prioritize MX/BR native sources next cycle — coverage gap, not proof of absence
- Watch DEN for issuance of actual hotel management RFP after consulting award

## G. Engineering
- First-100 after V7 A promotions: 4/100
- Modules: `hif-v7-opportunity-status-v7.js`
- Script: `hif-v7-current-backward-commercial-pilot.mjs`
- Tests: `test:hif-v7-current-backward-commercial-v7`