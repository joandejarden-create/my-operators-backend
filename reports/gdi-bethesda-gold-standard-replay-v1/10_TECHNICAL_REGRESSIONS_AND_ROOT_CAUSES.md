# 10. Exact technical regressions and root causes

## R1 — Evidence URL gate vs presentation package shape
- **Module:** `gdi-finalized-qualified-opportunity-contract-v1.js` → `collectEvidenceUrls` / gate 1
- **Current:** Requires structured URL arrays; ignores narrative-only provenance
- **Expected:** Accept evidence-supported demand including historical presentation provenance; do not fail solely because `sourceUrls[]` was omitted from customer package serialization
- **Affected:** All 67 gold IDs on raw FQO; 43 still fail even after blob URL scrape
- **Minimal fix:** Broaden evidence collectors to include known presentation provenance fields + historical-compatibility bypass for previously approved CURRENT/STAGED IDs when demand thesis + fit remain intact
- **Test:** `fqo_fails_gold_presentation_without_toplevel_sourceurls` + new rehydration/compatibility cases
- **Portfolio impact:** High — without this, every historical master package fails hotel-wide PASS

## R2 — Person-specific channel over-strict vs Rule 5
- **Module:** `evaluatePersonSpecificOutreachChannel`
- **Current:** Rejects agency/org-labeled phones and functional routes; requires person email/phone/LinkedIn
- **Expected:** Named person mandatory; direct personal email **not** mandatory when credible labeled outreach pathway exists
- **Affected:** 17 gold rows
- **Minimal fix:** Allow `organizationContactRouteLabel` / verified agency lines as `LABELED_ORG_ROUTE` when primary is a verified named person
- **Test:** Agency-line pathway case from AHIMA/Spargo pattern
- **Portfolio impact:** Medium-High

## R3 — Length-based fit and action gates
- **Module:** `hotelFitOk`, `commercialActionOk`
- **Current:** Arbitrary string length thresholds
- **Expected:** Semantic presence of hotel rationale + next action (structured fields)
- **Affected:** Latent — gold currently passes lengths; blocks short but valid future rows
- **Minimal fix:** Require non-empty canonical fields (`hotelOpportunityThesis` / `recommendedNextStep`) rather than length
- **Test:** Short valid thesis/action fixtures
- **Portfolio impact:** Medium

## R4 — Competitive incumbent hardcoding (NYC)
- **Module:** `evaluateFinalizedQualifiedOpportunity` competitive block
- **Current:** Regex matches Westin New York / Marquis / Sheraton NY
- **Expected:** Incumbent from opportunity competitive context for **that hotel’s market**
- **Affected:** 10 Bethesda competitive-related fails
- **Minimal fix:** Remove NYC hardcode; require `competitiveContext.incumbentHotel` or market-local evidence fields
- **Test:** Bethesda competitive fixture without NYC strings
- **Portfolio impact:** Medium (wrong-market false fails)

## R5 — Presentation projection / candidate shrinkage
- **Module:** hotel-wide candidate write + quality projection
- **Current:** Hotel-wide PASS 28 → written candidate **20**
- **Expected:** All FINALIZATION_PASS rows survive serialization with contacts/types/pills
- **Affected:** 8 PASS rows dropped between disposition and package
- **Minimal fix:** Trace quality-gate / projection filter; preserve gold classification fields
- **Test:** Assert passCount == package.opportunityCount
- **Portfolio impact:** High for parity

## R6 — Historical approved inventory forced to PREVIOUSLY_APPROVED_REVIEW
- **Module:** `assignHotelWideDisposition`
- **Current:** 38 gold IDs marked review-required because FQO fails on package shape
- **Expected:** Historical compatibility — preserve approved records; flag only genuine commercial issues
- **Affected:** 38 IDs
- **Minimal fix:** Compatibility path when ID ∈ active STAGED/CURRENT and regression is evidence-serialization only
- **Test:** Bethesda gold ID remains PASS or RETAINED under compatibility mode
- **Portfolio impact:** Critical for Bethesda 67 preservation
