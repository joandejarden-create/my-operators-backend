# Renaissance NYTS — Full GDI E2E continuation closeout (A–K)

**Hotel:** Renaissance New York Times Square (`recG66DQJKP2c0UNh`)  
**Authorization:** `auth_gdi_renaissance_ts_12usd_v1` (same $12 — not additional)  
**Execution owner:** `cursor-agent-renaissance-gdi-exec`  
**Contract:** `gdi-full-e2e-contract-v1.5` + lessons-capture execution proof  
**Release:** PR **#123** merge `c8278607` · Railway **`34cf42a7` SUCCESS** · evidence PR **#124** `cfc32a7b`  
**As of:** 2026-10-16T19:25Z

---

## A–J (execution summary)

| Area | Result |
|------|--------|
| Provider leaks (7 detail IDs) | **Cleared** after Railway `34cf42a7` (local + production re-scan LEAKS=0) |
| Inventory | Prior list **50** → current **47** (not 48). Removals: 76th SSSP 404; Cyera + EliseAI eligibility `no_usable_route` (detail 200) |
| Counts (separate) | List 47 · reviewed 40 · facing-true on list 40 · unique org\|date\|type 42 · Airtable stored 72 / facing 42 |
| Contact register | Primary per facing opp in `07b-contact-disposition-register-airtable-live.json` — not attempted **0**; named-buyer majority still open with documented fallbacks |
| Drawers | Provisional withheld repair applied; 12 empty theses promoted from existing prose |
| Local :8080 | List 47 · leaks 0 · CSV/brief/summary 200 |
| Production | List 47 · leaks 0 · CSV/brief/summary 200 |
| Share | HTML 200; resolve **403**; browser **Access link unavailable** — capability crypto not satisfied by Ren registry token alone |
| PDF | Dedicated PDF routes **404**; CSV export is the available export surface |
| Machine completion | **PARTIAL** — `canClaimComplete=false` |
| Budget | Cap $12 · ~$4.06 incurred · ~$7.94 available · this pass paid **$0** |

---

## K. Next-hotel inheritance (corrected — four modes)

Do **not** read “inherits automatically” as “no operator work.” Distinguish:

| Mode | Meaning |
|------|---------|
| **automatically_performs** | Shared path runs the correction on every hotel (e.g. sanitize-v3, false-exhaustion gate, ledger-first auth) |
| **automatically_validates** | Shared evaluator/adapters fail-closed if the work is missing or wrong |
| **detects_gap_requires_operator** | Shared path detects incompleteness; an operator must still assemble evidence or complete a product step |
| **not_yet_released** | Shared impl/test incomplete — next hotel does **not** inherit |

### Automatically performs

provider_leak_customer_sanitize · false_contact_exhaustion_forbidden · assessment_unknown_vs_unfinished · geo_local_connection_preserved · resume_without_paid_repeat · hotel_product_budget_isolation · auth_reconcile_ledger_identity_first · publication_metadata_preservation · pe_schema_link_persistence · applicable_segment_declared_depth · contact_depth_with_org_fallback · canonical_writers_no_post_import_repair

### Automatically validates

missing_and_plan_only_block_complete · completion_from_evidence · delivery_verify_fail_closed · lessons_capture_required · assoc_contacts_not_planner · sports_edu_independent · dedupe_preserve_history · hotel_fit_drawers_before_publish

### Detects gap — requires operator step (precise)

| Lesson | Operator intervention still required |
|--------|--------------------------------------|
| inventory_facing_vs_unique_count | Run inventory freeze + write stored/facing/unique reconcile into the run ledger (blockers alone do not assemble the freeze artifact) |
| delivery_checks_required | Mint hotel-scoped share capability with live crypto secret; obtain PDF/report when product routes exist — list/CSV ≠ full delivery proof |
| local_production_parity | Run local↔production parity diagnose for this hotel’s declared local process + production URL |
| contact_ownership_hotel_scoped | Confirm hotel-scoped candidate dirs for the run; cross-hotel bleed audit not fully automated |

### Not yet released

None of the shared MF/SD/Ren register rows above are “not yet released” after PR #123 on clean main. Hotel-only Ren repair scripts remain **not** process improvements.

**Do not claim “no bespoke work remains.”** Evidence assembly for inventory freeze, share capability mint, and PDF/report surfaces still requires the operator steps above even when the next hotel uses `--mode full-e2e`.

---

## Lessons capture

Coordinator stage `capture_lessons` must write `full_e2e/lessons-capture-v1.json` with `coordinatorExecutionProof` (runId, hotelId, artifactPath, gitSha/releaseRef). Empty incorporated list is valid only with `noNewLessonsThisRun=true` + that proof — do not invent lessons.
