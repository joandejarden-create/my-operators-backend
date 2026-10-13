# Bethesda Marriott GDI — Rad / GM Feedback Iteration

**Date:** 2026-09-13  
**Hotel:** Bethesda Marriott (`recLuxvwwxID7U2B8`)  
**Verdict:** **PARTIAL GO**

> Does the product now give a hotel sales team useful external demand intelligence that can be validated quickly against its own internal systems?

**Yes, with one critical caveat:** GDI can now explain fit, confidence, sources, territory, and next actions — and it explicitly refuses to pretend it knows whether Marriott already sourced a lead. Incremental-value % cannot be measured until Rad / Sales validate opportunities in Hotel Validation mode.

ADP was not modified. Webhound hard cap remains **$15**. No additional Webhound spend in this pass (prior pilot already at/near the ceiling).

---

## Changes Made (mapped to GM feedback)

| GM ask | Change |
|--------|--------|
| STR competitive set | Hotel config now stores the 5 GM-provided STR hotels as `STR_COMP_SET` |
| Nearby Marriott alternatives | Separate `RELEVANT_GROUP_DEMAND_ALTERNATIVE` list (HQ / North / AC) — not treated as STR |
| Too Montgomery-centric | Demand territory expanded to **DMV** with classification: Core / Medical Corridor / DMV Competitive / Stretch / Outside |
| “Has this already been sourced?” | `Sourcing Status` defaults to **UNKNOWN — requires hotel validation**; never inferred as hotel-sourced from public web alone |
| Incremental value | `Incremental Value Status` feedback-driven only; KPI scaffolded but not claimed |
| “What are the numbers for Hotel Fit?” | Component breakdown with friendly labels + plain-English explanation; weights documented as already matching 25/20/15/15/10/10/5 (**unchanged**) |
| “What does Evidence mean?” | Renamed UI to **Evidence Confidence** + tooltip + explanation |
| “Where are these programs pulled from?” | **Sources** list on detail (name, type, URL, fact supported) — no Webhound/provider jargon for hotel users |
| Fact vs estimate | `knownVsEstimated` + claim labels VERIFIED / ESTIMATED / INFERRED |
| Why this matters / Why now / What should sales do? | Sharpened narratives + Recommended Action; booking window display uses QUALIFY NOW |
| Detail structure | Drawer reorganized into the 11 commercial sections Rad asked for |
| Weekly brief | Shows territory, sourcing, Hotel Fit, Evidence Confidence, Why Now, Recommended Action, contact |
| Hotel validation | Familiarity / Commercial Status / Value / Incremental feedback — stored separately from canonical facts |
| Pilot metrics | Internal `pilotMetrics` on summary API (admin) |
| Product boundaries | No CRM, no ADP merge, no proprietary hotel-data prerequisite |

---

## Competitive Set

### A. STR Comp Set (hotel-provided)

1. The Bethesdan Hotel, Tapestry Collection by Hilton  
2. Rockville Hotel, a Ramada by Wyndham  
3. Hilton Washington DC/Rockville Hotel & Executive Meeting Center  
4. Hyatt Regency Bethesda  
5. Sheraton Hotel Rockville  

### B. Relevant Group-Demand Alternatives (not STR)

6. Bethesda Marriott at Marriott HQ  
7. Bethesda North Marriott Hotel & Conference Center  
8. AC Hotel Bethesda Downtown  

Each opportunity now surfaces a likely STR competitor, a likely broader group alternative, and rationale. Canonical Dealality IDs are preferred when resolvable; no duplicate hotel records were invented.

---

## DMV Expansion

Discovery framing moved from “events in Montgomery County” to:

> Could Bethesda Marriott realistically compete for this event?

Territory classifications applied to the existing Bethesda opportunity set (enrichment pass, **$0 Webhound**):

| Territory | Approx. count (incl. disqualified) |
|-----------|-------------------------------------|
| Bethesda / Montgomery Core + North DC Medical Corridor | 21 |
| DMV Competitive / Stretch | 3 |
| Outside / disqualified host patterns | retained as DISQUALIFIED |

**Important:** This iteration prioritized product quality over new lead volume. A fresh Webhound DMV crawl was **not** run because the $15 hard cap was already consumed in the prior deepen/discovery wave. Quality enrichment reclassified and explained the existing 18 qualified opportunities under DMV rules.

Target of 15–25 high-quality qualified DMV opportunities remains the standing research goal for the next funded research wave (still ≤ $15/run unless explicitly raised).

---

## Current Opportunities (after enrichment)

| Bucket | Count |
|--------|------:|
| High Priority | 5 |
| Medium Priority | 8 |
| Watchlist | 5 |
| Qualified | **18** |
| Disqualified | 9 |

Qualified count unchanged vs prior deepen pass — intentional. Success is clarity and actionability, not more rows.

---

## Sourcing Uncertainty

| Status | Count (approx.) |
|--------|----------------:|
| UNKNOWN (default) | 16 |
| Public RFP / housing evidence | remainder where text matched housing/RFP patterns |
| Hotel confirmed sourced / not sourced / pursued | **0** (no hotel validation yet) |

UI copy for hotel users:

**Already Sourced to Hotel?** `Unknown — requires hotel validation`

---

## Hotel Fit

**Methodology unchanged after review against GM framework.**

| Component | Weight | UI label |
|-----------|-------:|----------|
| Physical Fit | 25% | Physical Fit |
| Geography Fit | 20% | **Demand Territory Fit** |
| Timing | 15% | Timing / Winnability |
| Commercial Potential | 15% | Commercial Potential |
| Historical Fit | 10% | Historical Hotel / Brand Fit |
| Competitive Accessibility | 10% | Competitive Accessibility |
| Contactability | 5% | Contactability |

Documented in `config/group-demand-intelligence/scoring-weights.json` (`reviewedAgainstGmFrameworkAt: 2026-09-13`). Detail view shows the 0–100 total, each component, and a plain-English explanation.

---

## Evidence Confidence

Independent of Hotel Fit. Explains source quality, recency, corroboration, and verified vs inferred share. Tooltip:

> How confident Dealality is in the facts underlying the opportunity based on source quality, recency, corroboration and how much is verified versus inferred.

---

## Source Mix

Hotel-facing Sources prefer:

- official organization / government pages (e.g. NIST)
- association / event sites
- housing providers when publicly evidenced
- secondary corroboration

Provider internals (Webhound, research ladder L1–L5) stay in Research Audit only.

---

## Incremental Value

**Cannot yet determine.**

No hotel validation feedback has been collected from Rad / DOS in production pilot use. Internal metric scaffold exists (`% validated`, `% new to hotel`, `% already sourced`, `% worth pursuing`, etc.) but must not be over-claimed until feedback exists.

---

## Questions for Tuesday (hotel validation)

1. Which opportunities were already known?  
2. Which had already been sourced to Bethesda?  
3. Which would sales actually pursue?  
4. Is DMV the right demand territory framing?  
5. Does STR vs group-alternative competitor framing match how Sales thinks?  
6. Are Hotel Fit and Evidence Confidence understandable?  
7. Is Why Now useful for prioritization?  
8. Are Sources sufficient to trust the opportunity enough to check CI/TY / Delphi / Cvent?

---

## Success criteria check

1. Can Rad understand why each opportunity fits? → **Improved (PARTIAL → stronger)** via breakdown + explanation  
2. Confidence understandable? → **Yes** (Evidence Confidence + tooltip)  
3. Sources visible? → **Yes**  
4. Incremental feedback path? → **Yes** (Hotel Validation); measurement pending hotel use  
5. DMV credible? → **Framing yes**; new DMV discovery volume deferred (cap)  
6. Decide what to do next? → **Yes** (Recommended Action)  
7. Avoid pretending Marriott already sourced? → **Yes** (UNKNOWN default)  
8. Scalable without proprietary hotel data? → **Yes**  
9. Complements CI/TY / Cvent / Delphi? → **Yes** (no CRM)  
10. Future ADP composition possible? → **Yes** (APIs/components remain separate)

---

## Final verdict

# PARTIAL GO

The product is now materially more useful as **external demand intelligence a sales team can validate quickly against its own systems**. It is not yet a GO for “proven incremental to the hotel” until Tuesday validation feedback exists, and the next research wave (still ≤ $15) should prioritize net-new DMV-quality opportunities rather than more Montgomery-only volume.

---

## How to review locally

```bash
GROUP_DEMAND_INTELLIGENCE_V1=1 GROUP_DEMAND_INTELLIGENCE_PILOT_READ=1 npm start
# → http://localhost:8080/group-demand-intelligence
# or shell: http://localhost:8080/#/group-demand-intelligence
```

Enrichment (already applied):

```bash
npm run gdi:apply-rad-feedback
npm run test:gdi-foundation
```
