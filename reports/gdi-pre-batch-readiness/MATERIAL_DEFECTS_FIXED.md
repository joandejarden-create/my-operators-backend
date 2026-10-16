# Material defects fixed in this readiness review

Baseline before fixes: `origin/main` @ `11379922` (PR #107), production deploy SUCCESS same SHA.

## 1. Weekly refresh `opts is not defined` (research/publication blocker for weekly contact-depth)

**File:** `lib/group-demand-intelligence/workflow/weekly-refresh-orchestrator-v1.js`

`invokeWeeklyDownstreamWorkflow` referenced `opts.authorizeSurfe` / `opts.executeBuyerContactEnrichment` without accepting `opts`, so any weekly cycle that queued buyer-contact work threw and failed the isolated weekly proof.

**Fix:** Destructure `authorizeSurfe` (default `false`), `executeBuyerContactEnrichment` (default `true`), `workflowRunId`, `buyerEnrichmentOutDir` on the function signature and use those bindings. Surfe remains opt-in.

**Proof:** `scripts/test-gdi-weekly-refresh-v1.mjs` PASS (two isolated cycles + future clock).

## 2. Reviewed-prospect import revived unexplained `hotelFitScore` 55 (drawer correctness)

**File:** `lib/group-demand-intelligence/reviewed-prospect-import-v1.js`

Object-literal later keys overwrote the careful unexplained-score clear with `base.hotelFitScore` (import default 55) when no structured scored assessment existed.

**Fix:** Later `hotelFitScore` / basis / note assignments only accept structured scored values or canonical basis + non-generic rationale; otherwise null.

**Proof:** `scripts/test-gdi-drawer-content-contract-v2.mjs` PASS (`unexplained fit 55 must be cleared`).

## 3. Reviewed-prospect visibility fixture lacked travel-need evidence (test drift, not production rewrite)

**File:** `scripts/test-gdi-reviewed-prospect-publication-v1.mjs`

After use-case qualification integrity, dated lodging requires `travel_need_evidence`. The unit fixture only had presentation-adjacent fields, so visibility asserted false.

**Fix:** Add research travel evidence fields to the fixture (`travelEvidenceNote`, `roomDemandStatus: REQUIRED`, lodging thesis). No production record rewrite.

**Proof:** `scripts/test-gdi-reviewed-prospect-publication-v1.mjs` PASS.

## 4. Buyer-enrichment source guard updated for weekly Surfe wiring

**File:** `scripts/test-gdi-buyer-contact-enrichment-v1.mjs`

Source assertion expected the broken `opts.authorizeSurfe` pattern; updated to require destructured `authorizeSurfe === true` with default `false`.

Not previously completed as “pending” — these are new closes found while reusing inventory proof.
