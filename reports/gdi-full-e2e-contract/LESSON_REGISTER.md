# Full GDI E2E â€” Lesson register (Mayflower / San Diego / Renaissance â†’ standard)

**Contract:** `gdi-full-e2e-contract-v1.5`  
**Machine source:** `FULL_E2E_LESSON_REGISTER` in `lib/group-demand-intelligence/workflow/full-e2e-contract-v1.js`  
**Capture module:** `full-e2e-lessons-capture-v1.js` (required stage `P` / `capture_lessons`)

**Acceptance criterion:** a working, evidenced hotel research-to-delivery process â€” **not** a register in which every row has the same status.

Do **not** claim a lesson is fully incorporated from a status label alone. Each lesson must reference **code, test, execution evidence, and release**, and track five axes separately:

| # | Axis | Meaning |
|---|------|---------|
| 1 | Documented | Lesson recorded in this register + contract |
| 2 | Implemented (shared path) | Shared rule/module prevents or detects recurrence |
| 3 | Tested | Regression/integration gate covers it |
| 4 | Executed successfully | Run path produced evidence under the rule |
| 5 | Production-verified | Live hotel delivery observed (or `n/a`) |

**Defect-fix checklist (same task â€” do not defer):**

1. Shared implementation (where applicable)  
2. Regression/integration test  
3. This lesson register  
4. Full E2E runbook + reusable Cursor prompt  

A **hotel-specific repair is not a completed process improvement** until the shared path prevents recurrence or explicitly detects and routes it. Next hotel inherits when axes 1â€“4 are true on the shared path (productionVerified may still be open for the current hotel).

Hotel-specific repair scripts, overlays, or prompts are **not** the standard entry point. Next hotel: `scripts/gdi-research-to-page-workflow-v1.mjs --mode full-e2e â€¦`.

## Status summary (honest)

Run `summarizeLessonRegister()` / see `lessonRegisterSummary` on the contract document. Proven âŠ‚ total. Closeout must list: lessons incorporated this run Â· remaining gaps Â· next-hotel inheritance.

| Status | Meaning |
|--------|---------|
| `IMPLEMENTED_AND_TESTED` | Applicable proof axes true (inspect five-axis; do not trust label alone) |
| `IMPLEMENTED_NOT_VERIFIED` | Shared path + test exist; auto-evidence and/or production proof incomplete |
| `PARTIALLY_WIRED` | Rule exists; execution path incomplete |
| `DOCUMENTED_ONLY` | Lesson recorded; not implemented |
| `BLOCKED` | Cannot proceed |

## Register

| ID | Hotels | Doc | Shared | Tested | Executed | Prod | Status | Next hotel inherits? |
|----|--------|-----|--------|--------|----------|------|--------|----------------------|
| applicable_segment_declared_depth | MF/SD/Ren | yes | yes | yes | yes | yes | IMPLEMENTED_AND_TESTED | yes |
| missing_and_plan_only_block_complete | MF/SD/Ren | yes | yes | yes | yes | yes | IMPLEMENTED_AND_TESTED | yes |
| assoc_contacts_not_planner | MF/Ren | yes | yes | yes | yes | n/a | IMPLEMENTED_AND_TESTED | yes |
| sports_edu_independent | Ren | yes | yes | yes | yes | **no** | IMPLEMENTED_NOT_VERIFIED | yes* |
| contact_depth_with_org_fallback | MF/SD/Ren | yes | yes | yes | yes | **no** | IMPLEMENTED_NOT_VERIFIED | yes* |
| hotel_fit_drawers_before_publish | MF/SD/Ren | yes | yes | yes | **no** | **no** | IMPLEMENTED_NOT_VERIFIED | yes* |
| canonical_writers_no_post_import_repair | MF/SD/Ren | yes | yes | yes | **no** | **no** | IMPLEMENTED_NOT_VERIFIED | yes* |
| dedupe_preserve_history | MF/SD/Ren | yes | yes | yes | **no** | **no** | IMPLEMENTED_NOT_VERIFIED | yes* |
| delivery_checks_required | MF/SD/Ren | yes | yes | yes | yes | **no** | IMPLEMENTED_NOT_VERIFIED | yes* |
| completion_from_evidence | MF/SD/Ren | yes | yes | yes | yes | n/a | IMPLEMENTED_AND_TESTED | yes |
| resume_without_paid_repeat | Ren | yes | yes | yes | yes | yes | IMPLEMENTED_AND_TESTED | yes |
| hotel_product_budget_isolation | Ren | yes | yes | yes | yes | yes | IMPLEMENTED_AND_TESTED | yes |
| delivery_verify_fail_closed | MF/SD/Ren | yes | yes | yes | yes | **no** | IMPLEMENTED_NOT_VERIFIED | yes* |
| auth_reconcile_ledger_identity_first | Ren | yes | yes | yes | yes | yes | IMPLEMENTED_AND_TESTED | yes |
| geo_local_connection_preserved | MF/SD/Ren | yes | yes | yes | yes | yes | IMPLEMENTED_AND_TESTED | yes |
| pe_schema_link_persistence | MF/SD | yes | yes | yes | yes | yes | IMPLEMENTED_AND_TESTED | yes |
| publication_metadata_preservation | MF/SD/Ren | yes | yes | yes | yes | yes | IMPLEMENTED_AND_TESTED | yes |
| contact_ownership_hotel_scoped | MF/SD/Ren | yes | yes | yes | yes | **no** | IMPLEMENTED_NOT_VERIFIED | yes* |
| local_production_parity | MF/SD/Ren | yes | yes | yes | **no** | **no** | IMPLEMENTED_NOT_VERIFIED | yes* |
| provider_leak_customer_sanitize | MF/SD/Ren | yes | yes | yes | yes | **yes** | IMPLEMENTED_AND_TESTED | yes |
| false_contact_exhaustion_forbidden | MF/SD/Ren | yes | yes | yes | yes | **no** | IMPLEMENTED_NOT_VERIFIED | yes* |
| inventory_facing_vs_unique_count | MF/SD/Ren | yes | yes | yes | **no** | **no** | IMPLEMENTED_NOT_VERIFIED | yes* |
| assessment_unknown_vs_unfinished | MF/SD/Ren | yes | yes | yes | yes | **no** | IMPLEMENTED_NOT_VERIFIED | yes* |
| lessons_capture_required | MF/SD/Ren | yes | yes | yes | yes | n/a | IMPLEMENTED_AND_TESTED | yes |

\*Shared path + test exist â†’ next hotel inherits the correction automatically; current-hotel **productionVerified** may still be open until clean-main deploy / live verify.

### Code / test / evidence refs (latest inventory)

| ID | Implementation | Test | Execution evidence |
|----|----------------|------|--------------------|
| provider_leak_customer_sanitize | `customer-presentation-sanitize-v3` + `detectProviderMetadataLeaks` | `test:gdi-customer-provider-leak-sanitize-v3` | PR #123 / c8278607 / Railway 34cf42a7; postdeploy LEAKS=0 |
| false_contact_exhaustion_forbidden | `buyer-contact-enrichment-v1` researchAttempted gate | false-exhaust revert + Full E2E contract | `â€¦/04b-false-exhaust-revert.json` |
| inventory_facing_vs_unique_count | inventory freeze + identity count blockers | identity_count_inconsistent | Ren `CLOSEOUT_A_K.md` Â§F; freeze JSON |
| assessment_unknown_vs_unfinished | `assertPublicationWriteReadiness` never_executed vs provisional | readiness fixtures | Ren fit batch + readiness unit |
| lessons_capture_required | `full-e2e-lessons-capture-v1` + stage P | contract cases 46â€“49 | coordinator `capture_lessons` |
| dedupe_preserve_history | supersession + publication quality identity | unreconciled identity blocked | Ren SSSP `02-sssp-duplicate-resolution.json` |
| contact_depth_with_org_fallback | queue dispositions + enrichment | org-fallback / false EXHAUSTED | Ren contact-depth continuation |

### Notes (do not duplicate research â€” reconcile only)

- **sports_edu_independent** â€” requires supported independent evidence; Ren production COMPLETE not claimed.  
- **hotel_fit_drawers_before_publish** â€” readiness wired; auto evidence from every persist path still partial.  
- **canonical_writers_no_post_import_repair** â€” use-case suitability required; not all writers auto-stamp eligibility.  
- **dedupe_preserve_history** â€” facing count â‰  unique sales opportunity count; report both.  
- **delivery_verify_fail_closed / delivery_checks_required / local_production_parity / provider_leak** â€” clean-main production deploy of verify-harden still pending â†’ productionVerified=false.  
- **contact_depth_with_org_fallback / false_contact_exhaustion** â€” named-buyer depth still open for many Ren accounts; false EXHAUSTED without sources forbidden on shared path.  
- **assessment_unknown_vs_unfinished** â€” provisional/unknown after assessment â‰  unfinished never_executed work.

## Still not fully automated (gaps â€” not status inflation)

1. Authenticated Memberstack browser drawer walkthrough (API surfaces automated).  
2. Clean-main lean-stage deploy of v1.5 verify/eligibility/sanitize harden.  
3. Named-buyer depth for majority of accounts.  
4. Structured Overall Hotel Fit tiles remaining thin on some lean packages.  
5. Share-token mint UX (tokens never in git).  
6. PDF visual clipping QA.  
7. Automatic `independentAssessment` stamping from every paid-MS scorecard.  
8. Continuous auto inventory freeze evidence on every run (discipline required).

## Regression command

```bash
npm run test:gdi-full-e2e-contract
npm run test:gdi-customer-provider-leak-sanitize-v3
```

