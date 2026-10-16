# Full GDI E2E — Lesson register (Renaissance → standard)

**Contract:** `gdi-full-e2e-contract-v1.2`  
**Machine source:** `FULL_E2E_LESSON_REGISTER` in `lib/group-demand-intelligence/workflow/full-e2e-contract-v1.js`  
**Only `IMPLEMENTED_AND_TESTED` counts as incorporated into the proven standard process.**

Hotel-specific repair scripts, overlays, or prompts are **not** the standard entry point. Next hotel: `scripts/gdi-research-to-page-workflow-v1.mjs --mode full-e2e …`.

| ID | Lesson | Defect | Implementation | Test | Release | Status |
|----|--------|--------|----------------|------|---------|--------|
| applicable_segment_declared_depth | Research every applicable segment to a declared depth | Plan-only / label-only “researched” | `assessApplicableSegmentCompletion` + depth rules | plan_only / researched label cases | #119 / v1.1+ | IMPLEMENTED_AND_TESTED |
| missing_and_plan_only_block_complete | Missing segments and plan-only stages prevent COMPLETE | Empty/omitted axes still COMPLETE | `validateSegmentCoverageAxes` | empty/omitted adversarial | #119 / v1.1+ | IMPLEMENTED_AND_TESTED |
| assoc_contacts_not_planner | Association contacts ≠ planner discovery | Assoc contacts counted as planner | planner axis gate | planner via assoc → PARTIAL | #119 / v1.1+ | IMPLEMENTED_AND_TESTED |
| sports_edu_independent | Sports & educational need independent assessment | Proxy from exhibitor/medical/assoc | `evaluateIndependentSegmentAxes` | sports/edu proxy blocked | v1.2 | IMPLEMENTED_AND_TESTED |
| contact_depth_with_org_fallback | Contact depth mandatory with org fallback | Skip because org route exists | org-route skip policy + dispositions | skip-for-org blocked; org fallback OK | #119 + v1.2 | IMPLEMENTED_AND_TESTED |
| hotel_fit_drawers_before_publish | Fit assessments + complete drawers before publication | Lean publish without fit/drawers | `evaluatePublicationQuality` | fit/drawer false blocked | v1.2 | IMPLEMENTED_AND_TESTED |
| canonical_writers_no_post_import_repair | Canonical writers without post-import eligibility repair | Upsert omitted eligibility fields | `assertCanonicalWriteEligibilityFields` in corporate persist | assert + repair-required blocked | v1.2 | IMPLEMENTED_AND_TESTED |
| dedupe_preserve_history | New records ≠ new opportunities; reconcile duplicates | Creates counted as net-new | identity dedupe gate | unreconciled identity blocked | v1.2 | IMPLEMENTED_AND_TESTED |
| delivery_checks_required | Reports/PDFs/links/local/prod are delivery | gap / not_run still COMPLETE | deliverables + verify adapters | gap + not_run adversarial | #119 / v1.1+ | IMPLEMENTED_AND_TESTED |
| completion_from_evidence | Completion from evidence, not manual status | Operator passed overrode machine fail | override blockers | manual vs failed → FAILED | #119 / v1.1+ | IMPLEMENTED_AND_TESTED |
| resume_without_paid_repeat | Resume revisits stale without re-paying research | Skipped stale verify / re-paid | fingerprints + `verificationNeedsRerun` | resume invalidation tests | #119 / v1.1+ | IMPLEMENTED_AND_TESTED |
| hotel_product_budget_isolation | Hotel/product budgets remain separate | Combined/copied budgets | authoritative reconcile + isolation | cross-scope blocked | #119 + v1.2 | IMPLEMENTED_AND_TESTED |

## Still not fully automated (documented gaps — not lesson status demotions)

These remain operator/manual or product gaps; they do **not** weaken the lessons above:

1. Authenticated Memberstack browser drawer walkthrough (API surfaces automated).  
2. Cross-run join of continuation scorecards into an older ledger.  
3. Share-token mint UX (tokens never in git).  
4. PDF visual clipping QA.

## Regression command

```bash
npm run test:gdi-full-e2e-contract
```
