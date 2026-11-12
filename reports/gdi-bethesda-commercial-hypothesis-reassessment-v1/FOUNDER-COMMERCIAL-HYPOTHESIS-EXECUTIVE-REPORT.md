# DEALALITY — GDI Full E2E Commercial Hypothesis Development

**Founder deliverable (1–14)** · Offline · $0 paid · No customer publication

---

## 1. Existing modules modified

| Path | Change |
|---|---|
| `lib/group-demand-intelligence/gdi-commercial-hypothesis-development-v1.js` | **New** CHD stage |
| `lib/group-demand-intelligence/gdi-proactive-commercial-intelligence-standard-v1.js` | Practical geography in five-part fit; CHD pub-review gates |
| `lib/group-demand-intelligence/sales-ready-hotel-fit-gate-v1.js` | `CONDITIONAL_STRETCH` honesty blocks SR/AD |
| `lib/group-demand-intelligence/workflow/gdi-watchlist-finalization-handoff-v1.js` | CHD before proactive CI |
| `lib/group-demand-intelligence/workflow/original-full-e2e-research-chain-v1.js` | `COMMERCIAL_HYPOTHESIS_DEVELOPMENT` stage |
| Fixtures + tests + Bethesda reassessment scripts | Permanent regressions |

## 2. Commercial Hypothesis Development

Chain stage after demand recovery:

**Discovery → Trigger → Demand mechanism → CHD → Demand type → Proactive CI → Qualification → Content → Pub-review**

CHD builds primary/secondary revenue hypotheses with mechanism codes, evidence refs, hotel compete reason, confidence, uncertainties, and qualification questions — without inventing bookings.

## 3. Targeted research for unknowns

When geography or requirement is unresolved, CHD emits `targetedHypothesisResearchQuestions` (where travelers stay, Bethesda-side footprint, competitor hotels, travel owner). Weak candidates stay research continuation; no endless fabrication.

## 4. Primary / secondary revenue logic

- Primary from evidence-backed guestroom or catering mechanisms
- Secondary only when independently supported on evidence (not segment defaults)
- `BOTH` only when both are independently credible
- Secondary uncertainty does **not** block a supported primary

## 5. Demand confidence vs revenue confidence

| Field | Role |
|---|---|
| `demandConfidence` | CONFIRMED / RELIABLE_INFERRED / SPECULATIVE |
| `proposedRevenueType` | GUEST_ROOMS / LOCAL_CATERING / BOTH |
| `confirmedCustomerRequirement` | YES / NO / UNKNOWN |
| `commercialDemandType` | Compatibility field; cleared from UNDETERMINED when proposed thesis exists |

## 6. AD + publication-review behavior

AD / pub-review now require practical hotel geography. Broad “same commercial market” / DC-metro labels are insufficient for Bethesda Marriott. Generic or truncated customer copy blocks publication-review.

## 7–8. KnowBe4 / Ameresco before → after

| | KnowBe4 | Ameresco |
|---|---|---|
| Trigger | Arlington office opening | NRL infrastructure project |
| Primary hypothesis | **GUEST_ROOMS** (visiting customers/partners, recruitment, office expansion travel) | **GUEST_ROOMS** (project / infrastructure travel) |
| Secondary | none (catering not independently evidenced) | none |
| Confirmed requirement | UNKNOWN | UNKNOWN |
| Geography | **CONTESTED_METRO** | **CONTESTED_METRO** |
| AD / Sales Ready / Pub-review | false / false / false | false / false / false |
| Prior PR #206 staging | Was pub-review-ready on metro label | Was pub-review-ready on metro label |

Honest correction: both keep evidence-backed guestroom hypotheses; neither is publication-review-ready until Bethesda-side practical fit is researched.

## 9. Revised customer-facing bodies

Not staged for founder publication review. CHD generates improved outreach drafts for research continuation (qualify geography before pitching Bethesda Marriott).

## 10. Other newly eligible candidates

None under the corrected geography bar in this offline pass.

## 11. Unresolved research gaps

- Where will KnowBe4 Arlington visitors actually stay?
- Does KnowBe4 have Montgomery County / Bethesda operating footprint?
- Where do Ameresco NRL project personnel lodge relative to the D.C. campus vs Maryland offices?
- Why would either choose Bethesda Marriott over nearer hotels?

## 12. Tests

- `test:gdi-commercial-hypothesis-development-v1` → **12/12 PASS**
- `test:gdi-proactive-commercial-intelligence-standard-v1` → **16/16 PASS**
- `test:gdi-watchlist-finalization-handoff-v1` → **8/8 PASS**
- `test:gdi-plp-pep-lcm-targeting-dedupe-v1` → **10/10 PASS**

## 13. GitHub PR / main

_Filled after merge._

## 14. Safety

Paid calls: **0** · New discovery: **none** · `$0.09` unused · 88 IDs / 59 canonical preserved · No Airtable / customer publish · Confirmed-demand discovery remains first-class (housing-call fixtures still CONFIRMED)

### GO / NO-GO

**NO-GO** customer publication of KnowBe4 / Ameresco.  
**GO** continue zero-paid / separately authorized targeted geography research before any AD promotion.  
**GO** keep CHD in weekly Full E2E so inferred-demand does not crowd out confirmed-demand open-sourcing opportunities.
