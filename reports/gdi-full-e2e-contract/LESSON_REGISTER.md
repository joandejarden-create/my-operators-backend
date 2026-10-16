# Full GDI E2E — Lesson register (Mayflower / San Diego / Renaissance → standard)

**Contract:** `gdi-full-e2e-contract-v1.4`  
**Machine source:** `FULL_E2E_LESSON_REGISTER` in `lib/group-demand-intelligence/workflow/full-e2e-contract-v1.js`  

**Acceptance criterion:** a working, evidenced hotel research-to-delivery process — **not** a register in which every row has the same status.

Do **not** mark `IMPLEMENTED_AND_TESTED` solely because evaluator fixtures reject explicit failure flags. Each lesson records proof axes separately:

| Axis | Meaning |
|------|---------|
| Rule implemented | Shared rule/module exists |
| Execution path wired | Coordinator / persist / verify path invokes it |
| Evidence generated automatically | Run artifacts populate the gate without handwritten overlays |
| Regression/integration verified | `npm run test:gdi-full-e2e-contract` (or named gate) covers it |
| Production verified | Live hotel delivery observed under the rule (or `n/a`) |

Hotel-specific repair scripts, overlays, or prompts are **not** the standard entry point. Next hotel: `scripts/gdi-research-to-page-workflow-v1.mjs --mode full-e2e …`.

## Status summary (honest)

Run `summarizeLessonRegister()` / see `lessonRegisterSummary` on the contract document. Proven ⊂ total.

| Status | Meaning |
|--------|---------|
| `IMPLEMENTED_AND_TESTED` | All applicable proof axes true |
| `IMPLEMENTED_NOT_VERIFIED` | Rule + path exist; auto-evidence and/or production proof incomplete |
| `PARTIALLY_WIRED` | Rule exists; execution path incomplete |
| `DOCUMENTED_ONLY` | Lesson recorded; not implemented |
| `BLOCKED` | Cannot proceed |

## Register

| ID | Hotels | Rule | Wired | Auto evidence | Regression | Prod | Status |
|----|--------|------|-------|---------------|------------|------|--------|
| applicable_segment_declared_depth | MF/SD/Ren | yes | yes | yes | yes | yes | IMPLEMENTED_AND_TESTED |
| missing_and_plan_only_block_complete | MF/SD/Ren | yes | yes | yes | yes | yes | IMPLEMENTED_AND_TESTED |
| assoc_contacts_not_planner | MF/Ren | yes | yes | yes | yes | n/a | IMPLEMENTED_AND_TESTED |
| sports_edu_independent | Ren | yes | yes | yes | yes | **no** | IMPLEMENTED_NOT_VERIFIED |
| contact_depth_with_org_fallback | MF/SD/Ren | yes | yes | yes | yes | **no** | IMPLEMENTED_NOT_VERIFIED |
| hotel_fit_drawers_before_publish | MF/SD/Ren | yes | yes | **no** | yes | **no** | IMPLEMENTED_NOT_VERIFIED |
| canonical_writers_no_post_import_repair | MF/SD/Ren | yes | yes | **no** | yes | **no** | IMPLEMENTED_NOT_VERIFIED |
| dedupe_preserve_history | MF/SD/Ren | yes | yes | **no** | yes | **no** | IMPLEMENTED_NOT_VERIFIED |
| delivery_checks_required | MF/SD/Ren | yes | yes | yes | yes | **no** | IMPLEMENTED_NOT_VERIFIED |
| completion_from_evidence | MF/SD/Ren | yes | yes | yes | yes | n/a | IMPLEMENTED_AND_TESTED |
| resume_without_paid_repeat | Ren | yes | yes | yes | yes | yes | IMPLEMENTED_AND_TESTED |
| hotel_product_budget_isolation | Ren | yes | yes | yes | yes | yes | IMPLEMENTED_AND_TESTED |
| delivery_verify_fail_closed | MF/SD/Ren | yes | yes | yes | yes | **no** | IMPLEMENTED_NOT_VERIFIED |
| auth_reconcile_ledger_identity_first | Ren | yes | yes | yes | yes | yes | IMPLEMENTED_AND_TESTED |
| geo_local_connection_preserved | MF/SD/Ren | yes | yes | yes | yes | yes | IMPLEMENTED_AND_TESTED |
| pe_schema_link_persistence | MF/SD | yes | yes | yes | yes | yes | IMPLEMENTED_AND_TESTED |
| publication_metadata_preservation | MF/SD/Ren | yes | yes | yes | yes | yes | IMPLEMENTED_AND_TESTED |
| contact_ownership_hotel_scoped | MF/SD/Ren | yes | yes | yes | yes | **no** | IMPLEMENTED_NOT_VERIFIED |
| local_production_parity | MF/SD/Ren | yes | yes | **no** | yes | **no** | IMPLEMENTED_NOT_VERIFIED |

### Notes on demotions (v1.4 honesty correction)

- **sports_edu_independent** — previously overstated: anti-proxy flag absence ≠ independent assessment. Now requires supported independent evidence; completion-input stamps from dedicated provider runs; Ren production COMPLETE not claimed.
- **hotel_fit_drawers_before_publish** — evaluator fixture rejects `false`; write-path `assertPublicationWriteReadiness` now wired on corporate persist + `apply_publish`. Auto evidence from every persist path into completion input is partial; Ren fit tiles still thin.
- **canonical_writers_no_post_import_repair** — prose `whyThisMatters`/`proposedHotelUse` shortcut removed; use-case suitability contract required. Not all writers emit `eligibilityWriteComplete` automatically.
- **dedupe_preserve_history** — derivation prefers change-ledger / import persist outputs; not every run writes a reconciled ledger.
- **delivery_verify_fail_closed / delivery_checks_required / local_production_parity** — adapters + fixtures land; clean-main production deploy of this harden still pending → productionVerified=false.
- **contact_depth_with_org_fallback** — named-buyer depth still open for most Ren accounts.

## Still not fully automated (gaps — not status inflation)

1. Authenticated Memberstack browser drawer walkthrough (API surfaces automated).  
2. Clean-main lean-stage deploy of v1.4 verify/eligibility harden (pending merge/deploy ownership).  
3. Named-buyer depth for majority of accounts.  
4. Structured Overall Hotel Fit tiles remaining thin on some lean packages.  
5. Share-token mint UX (tokens never in git).  
6. PDF visual clipping QA.  
7. Automatic `independentAssessment` stamping from every paid-MS scorecard.

## Regression command

```bash
npm run test:gdi-full-e2e-contract
```
