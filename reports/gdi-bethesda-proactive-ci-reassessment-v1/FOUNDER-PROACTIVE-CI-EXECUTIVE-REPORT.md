# DEALALITY — GDI Full E2E Proactive Commercial Intelligence Standard

**Executive deliverable (A–L)**  
Offline implementation · zero paid · no customer publication  
Base: `main` @ pre-PR tip · reassessment artifacts under this folder

---

## A. Commercial qualification standard implemented

Permanent policy in the **existing** GDI Full E2E architecture:

1. **Verified demand-generating trigger** (not necessarily a hotel booking)
2. **Credible hotel-demand mechanism** (GUEST_ROOMS / LOCAL_CATERING / BOTH)
3. **Hotel-specific commercial fit**
4. **Verified named buyer or relevant commercial entry point**
5. **Actionable outreach thesis** (“why reach out now”)

**Demand confidence (unchanged vocabulary):**
- LEVEL 1 `CONFIRMED_DEMAND`
- LEVEL 2 `RELIABLE_INFERRED_DEMAND` — verified activity → reasonable hotel inference; **confirmed booking not required**
- LEVEL 3 `SPECULATIVE_DEMAND` → research continuation

Sales Ready remains the higher bar. Account Development requires CONFIRMED or RELIABLE_INFERRED + five-part thesis. `publicationReviewReady` ≠ `customerPublicationReady`.

---

## B. Existing modules updated

| Module | Change |
|---|---|
| `lib/group-demand-intelligence/gdi-proactive-commercial-intelligence-standard-v1.js` | **New** — five-part thesis, outreach, proactive AD, pub-review |
| `lib/group-demand-intelligence/demand-mechanism-research-v1.js` | Class B via verified commercial triggers; read evidence `statement`; negate false local “no Maryland” |
| `lib/group-demand-intelligence/commercial-demand-thesis-standard-v1.js` | Policy docs + expanded `DEMAND_THESIS_NOT_REQUIRED` |
| `lib/group-demand-intelligence/account-development-eligibility-v1.js` | Require CONFIRMED / RELIABLE_INFERRED |
| `lib/group-demand-intelligence/workflow/gdi-watchlist-finalization-handoff-v1.js` | Proactive CI + AD-capable publication-review readiness |
| `lib/group-demand-intelligence/workflow/original-full-e2e-research-chain-v1.js` | Wire `PROACTIVE_COMMERCIAL_INTELLIGENCE` stage |
| `lib/group-demand-intelligence/research/commercial-pursuit-strategy-v1.js` | Score verified triggers; missing hotel contract not a penalty |
| `fixtures/gdi/proactive-commercial-intelligence-standard-v1.json` | Permanent regression fixtures |
| `scripts/test-gdi-proactive-commercial-intelligence-standard-v1.mjs` | 16 offline checks |
| `scripts/gdi-bethesda-proactive-ci-reassessment-v1.mjs` | Zero-paid Bethesda reassessment |

---

## C. Previous qualification barriers removed

1. **Independent evidence ignored discovery `statement` fields** → Class B never saw office/facility triggers.
2. **Class B required conference-style activity + timing + location + scale** → blocked office openings / facility investments without attendance counts.
3. **Account Development did not require demand confidence** (too loose) while demand classifier was too strict — both corrected.
4. **Publication-review readiness Sales-Ready-only** → now also stages complete AD bodies for founder review.
5. **False belief that confirmed hotel / RFP / room nights were required** — already documented as not required; now enforced in classifier behavior.

---

## D. Evidence requirements preserved

Still rejected: generic corporate profiles, unsupported assumptions, unrelated contacts, out-of-market expansions, invented bookings/dates, fabricated buyer authority.  
Still required: source-backed trigger, realistic mechanism, hotel fit, named commercially relevant entry point, non-generic outreach.

---

## E. Demand confidence vs maturity vs publication readiness

| Axis | Values | Notes |
|---|---|---|
| Demand confidence | CONFIRMED / RELIABLE_INFERRED / SPECULATIVE | Independent of maturity |
| Commercial maturity | Sales Ready / Account Development / Research continuation | AD ≠ Sales Ready |
| Publication-review readiness | complete body + SR or complete AD | Founder staging only |
| Customer publication | always false here | Separate authorization |

---

## F. Bethesda recovery reassessment

| Org | Demand | Classification | Blockers |
|---|---|---|---|
| **KnowBe4** | RELIABLE_INFERRED | **PUBLICATION_REVIEW_READY** | — |
| **Ameresco** | RELIABLE_INFERRED | **PUBLICATION_REVIEW_READY** | — |
| AstraZeneca | RELIABLE_INFERRED | Research continuation (credible trigger) | Usable contact route |
| Rock Creek Mansion | RELIABLE_INFERRED | Research / partnership enrichment | Named entry point + outreach |
| Hidden Level | RELIABLE_INFERRED | Research continuation | Contact / route gaps |
| Workday | RELIABLE_INFERRED | Research continuation | Contact / route gaps |
| True Anomaly (×2 paths) | RELIABLE_INFERRED | Research / duplicate path | Contact / route; cross-segment collapse |
| Energy Bar Association | SPECULATIVE | Research continuation | Invalid “contact” (journal title) |
| X-energy (×2) | SPECULATIVE | Research continuation | Trigger/mechanism incomplete in independent text |
| Menlo Digital | SPECULATIVE | Research continuation | Insufficient trigger |

---

## G. Genuine Account Development newly eligible

- **KnowBe4** — Arlington office opening; Facilities / Corporate Events entry points; DC-metro fit.
- **Ameresco** — NRL $197M infrastructure project; Travel Coordinator entry point; extended-stay / project lodging thesis.

---

## H. Publication-review-ready customer bodies

**2** staged for founder review (KnowBe4, Ameresco).  
`customerPublicationReady = false`. No Airtable / customer publish.

---

## I. Remaining blockers (incomplete prospects)

- AstraZeneca / Workday / Hidden Level / True Anomaly: usable contact route or complete AD path
- Rock Creek Mansion: named venue decision-maker
- Energy Bar / X-energy / Menlo: demand confidence or valid commercial entry point

---

## J. Regression / Full E2E tests

- `test:gdi-proactive-commercial-intelligence-standard-v1` → **16/16 PASS**
- `test:gdi-watchlist-finalization-handoff-v1` → **8/8 PASS** (KnowBe4 now correctly qualifies)
- `test:gdi-plp-pep-lcm-targeting-dedupe-v1` → **10/10 PASS**

---

## K. GitHub PR / main SHA

_Filled after merge._

---

## L. Spend and inventory safety

- Paid calls: **0**
- New 14-segment discovery: **none**
- `$0.09` remainder: **not used**
- Canonical inventory: **59** opportunities preserved
- Research IDs / 67 cards / hotel feedback / publication state: **not destructively modified**
- Customer publication: **none**

---

## GO / NO-GO

**GO** — founder review of KnowBe4 + Ameresco publication-review-ready bodies under the proactive standard.  
**NO-GO** — broad paid discovery / unauthorized live spend.  
Narrow live validation only with separate founder authorization after review of these staged bodies.
