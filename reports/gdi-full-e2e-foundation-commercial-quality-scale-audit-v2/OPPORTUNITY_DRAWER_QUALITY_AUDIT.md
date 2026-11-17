# Opportunity Drawer Quality Audit

## Architecture (CONFIRMED)

- Customer-facing fields on publication packages include: `whatIsHappening`, `potentialHotelBusiness`, `whyThisHotel`, `whyActNow`, `recommendedNextStep`, `sharedTenSectionDrawer`, contacts, sources, CMI fields, sales action pills.  
- UI: `public/js/group-demand-intelligence/dealality-gdi-ui.js`  
- Shared ten-section drawer contract under demand-segments modules.  
- Hotel-specific remediation scripts exist historically (Radisson truncation) — prefer canonical fixes.

## Sample scored opportunities (YOTEL CURRENT)

### A. `gdi_opp_yotel_fe2e_icpic_20270914` — ICPIC 2027

| Criterion (1–5) | Score | Note |
|-----------------|------:|------|
| Demand credibility | 4 | Named congress; venue CICG noted in research path |
| Hotel specificity | 3 | Overflow/La Côte angle present; still generic vs YOTEL product |
| Evidence traceability | 3 | Sources field present; lodging list unverified called out |
| Contact usefulness | 2–3 | Contact present; title edition lag called out |
| Actionability | 4 | Clear qualify questions + timing window |
| Timing relevance | 4 | 2027 dated |
| Forward-looking value | 4 | Future edition |
| Writing clarity | 4 | Substantive next step |
| Completeness | 3 | Uncertainties explicit |

### B. `rx_yotel_rec_chateau nyon` — preferred lodging

Local partnership / photographer org — weaker hotel demand thesis risk; needs rubric scrutiny for partner vs room-night demand (LIKELY weaker commercial density).

### C. `rx_d_630d02a59107` — HIV Science Geneva 2027

Strong dated scientific congress pattern similar to A.

## Six commercial questions — systemic gaps

| Question | Typical strength | Gap |
|----------|------------------|-----|
| What is the opportunity? | Strong for named events | Weaker for non-event corporate |
| Why this hotel? | Mixed | Generic fit prose risk |
| Evidence? | Mixed | Need clearer verified vs inferred |
| Who to contact? | Present | Role currency / buyer vs entry-point |
| How to pursue? | Often good | Keep specific asks |
| What unknown? | Often good on YOTEL | Preserve; don’t overclaim |

## Proposed canonical drawer specification (customer-facing)

1. **Opportunity** — org/event, what/when/where, demand type  
2. **Hotel relevance** — property-specific (market corridor, product fit, realistic limits)  
3. **Evidence** — bullets tagged Verified / Inferred / Unknown  
4. **Contact** — name, role, org, buyer|entry-point, why relevant  
5. **Next action** — one primary action + 2–3 qualifying questions + timing  
6. **Unknowns** — room nights, RFP, authority, competitive status  

Internal research blobs remain in package/sources, not dumped into customer prose.

## QA rubric (use going forward)

Demand credibility · Hotel specificity · Evidence · Contact · Actionability · Timing · Forward-looking · Clarity · Completeness — score 1–5 on ≥10 opps × ≥3 hotels before each publication wave.

## Ranked improvements

1. Hotel-specific why/fit rewriter constrained by evidence  
2. Contact role currency / edition checks  
3. Enforce unknowns section non-empty when lodging/RFP unverified  
4. Suppress partner/local cards that fail room-night thesis gate  
5. Card/drawer field parity regression (Radisson truncation lessons)
