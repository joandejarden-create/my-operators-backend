# Renaissance NYTS — Full GDI E2E continuation closeout (A–K)

**Hotel:** Renaissance New York Times Square (`recG66DQJKP2c0UNh`)  
**Authorization:** `auth_gdi_renaissance_ts_12usd_v1` (same $12 — not additional)  
**Execution owner:** `cursor-agent-renaissance-gdi-exec`  
**Worktree:** `C:\Dev\dealality\gdi-renaissance-exec` · branch `exec/gdi-renaissance-ts-12usd`  
**Contract coordination:** MOB `fix/gdi-full-e2e-verify-harden` (`gdi-full-e2e-contract-v1.5` + required lessons capture)  
**As of:** 2026-10-16T18:45Z

---

## A. Which v1.4 changes were verified versus still pending?

| Change | Verified | Pending |
|--------|----------|---------|
| Ledger-first auth / fail-closed delivery adapters | Regression PASS | Clean-main prod deploy |
| Pre-publication readiness (corporate + apply_publish + PE promote) | Unit/integration PASS | Prod path after deploy |
| Use-case suitability (no prose shortcut) | Regression PASS | — |
| Publication quality from persist outputs | Partial wiring + tests | Every persist path auto-stamp |
| Independent segment assessment | Rule + tests | Ren COMPLETE not claimed |
| Honest lesson register (per-axis) | Docs + `summarizeLessonRegister` | ProductionVerified axes |
| Customer provider-leak sanitize v3 | Local clears live leaking detail | **Needs clean-main deploy** |
| Mayflower / SD / Ren lesson coverage | Register retained | — |

## B. Was the provider leak fixed across customer paths?

**Shared fix: yes. Production runtime: not yet.**

| Path | Result |
|------|--------|
| Root cause | `evidence[].researchProvider=webhound`, `researchLevel=L5_WEBHOUND`, `webhoundUsed`, `phoneSource=WEBHOUND_*` surviving `sanitizeOpportunityForCustomerPresentation` spread |
| Shared fix | `customer-presentation-sanitize-v3` strips provider keys/enums; preserves source URLs |
| Authenticated list (prod) | 0 `webhound` hits on list payload (evidence often omitted) |
| Authenticated detail (prod, pre-deploy) | **7 facing rows still leak** until deploy |
| Local sanitize on live leaking detail | **Clears** (`WEBHOUND` → clean; source URL preserved) |
| Share DTO | Already omitted `researchProvider` in evidence map; v3 also cleans authenticated path |
| Regression | `npm run test:gdi-customer-provider-leak-sanitize-v3` PASS |
| Legacy 76th | Detail **404** (withdrawn); not special-cased |

## C. Do all publication writers enforce the correct readiness contract?

| Writer | Gate |
|--------|------|
| Corporate persist | Eligibility + readiness on live hotel-facing writes |
| `apply_publish` / coordinator | Readiness via publication-quality module |
| PE canonical promote | **New** facing promotions blocked unless readiness OK; enrichment of already-facing preserves metadata |
| Unknown vs unfinished | Missing / never-executed assessment → **incomplete**; provisional / insufficient_evidence after assessment → **not** auto-fail |

## D. Contact-depth work finished, and what fallback outcomes remain?

Live hotel-facing **42** after unpaid remaining pass + boundary cleanup:

| Named research status | Count |
|----------------------|------:|
| NAMED_RESEARCH_NOT_ATTEMPTED | **0** (was 21) |
| NAMED_BUYER_NOT_YET_ESTABLISHED | ~21 |
| NAMED_RESEARCH_EXHAUSTED_ORG_FALLBACK | ~8 |
| NAMED_PERSON_CHANNEL_PENDING | ~4 |
| NAMED_CONTACT_WEAK_RELEVANCE | ~8 |
| Named person + person-channel | **8** |

False-positive non-person labels rejected (venue titles, “Organization contact desk”, org-name scrapes). Org inboxes not attributed as person-specific. Surfe this pass: **$0**. Diane Dukes restored after credential-suffix false reject.

## E. Which assessments were completed or remain legitimately uncertain?

- Structured `hotelFitAssessment` on **42/42** facing (prior pass; still present).  
- Overall mostly provisional / QUALIFY_FURTHER / insufficient_evidence where thin — **honest**, not invented scores.  
- Some lean drawer thesis/why-now prose still thin.

## F. Was the SSSP duplicate resolved, and what are the unique/facing counts?

| Item | Value |
|------|-------|
| Classification | SAME_SALES_OPPORTUNITY |
| Canonical | `gdi_opp_prospect_mvavz7tu_4fc9f013` (supersedes legacy) |
| Legacy `gdi_opp_76th_annual_meeting_3` | `prospectHotelFacing=false`, `customerVisible=false`, `supersededBy` → canon; history preserved |
| Stored | 72 |
| Hotel-facing | **42** |
| Unique facing org+date opportunities | **42** |
| Production list API | **48** (broader visibility surface; 76th absent) |

## G. Which segment scopes meet substantive declared depth?

| Scope | Depth |
|-------|-------|
| Corporate / scaling / LMC / PE / entertainment | Complete for declared paid-MS tasks (prior) |
| Sports dedicated | Complete (prior gap-fill; independent assessment evidence present) |
| Educational/tours dedicated | Complete (prior; Marathon Tours primary educational with sports secondary tags only where justified) |
| Association freshness | Complete (prior) |
| Contact depth | **Attempted across facing set**; named buyer still majority open |
| No broad rediscovery | Honored |

## H. Did clean-main deployment and local/production delivery pass?

| Check | Result |
|-------|--------|
| Branch pushed | `origin/fix/gdi-full-e2e-verify-harden` @ `be7bea1b` |
| PR / merge | **Blocked** — `gh` not authenticated in this environment (open https://github.com/joandejarden-create/my-operators-backend/pull/new/fix/gdi-full-e2e-verify-harden ) |
| Clean-main `deploy:production` | **not_run** (requires merge) |
| Production list | HTTP 200 · count **48** · hotel match |
| Production detail leaks | **Still fail** until deploy |
| Local :8080 | **not_run** |
| Browser / share / PDF | **not_run** (Memberstack / token mint) |

## I. What was spent, reserved, held, and remains available?

| Field | Value |
|------:|
| Cap | $12.00 |
| Estimated incurred | **$4.06** |
| Reserved | $0.00 |
| Accounting hold | $0.00 |
| Available | **$7.94** |
| Actual billed | UNKNOWN |
| This pass Parallel/Surfe | **$0.00** |

## J. What does the machine evaluator conclude?

**PARTIAL** — publication of current facing set remains successful; Full E2E COMPLETE is not claimed.

Primary blockers: clean-main deploy of v1.4 sanitize/readiness; production detail leak clear; authenticated browser/share/PDF; named-buyer depth majority still open (org routes usable).

## K. What specific bespoke work, if any, remains for the next hotel?

None required if next hotel uses `scripts/gdi-research-to-page-workflow-v1.mjs --mode full-e2e` after v1.5 is on clean main. Do **not** copy Renaissance repair scripts. Remaining bespoke risk is only if a hotel reintroduces post-import eligibility repair or skips readiness — those paths are now shared-gated.

---

## Lessons capture (required Full E2E closeout)

**Canonical register:** `reports/gdi-full-e2e-contract/LESSON_REGISTER.md` · machine `FULL_E2E_LESSON_REGISTER`  
**Module:** `full-e2e-lessons-capture-v1` · coordinator stage `capture_lessons`  
**Rule:** Hotel-specific repair ≠ process improvement until shared path prevents/detects recurrence. Do not claim full incorporation from a status label alone.

### Lessons incorporated during this run

| Lesson ID | Shared impl | Test | Evidence | Release | Doc | Shared | Tested | Exec | Prod | Next hotel inherits? |
|-----------|-------------|------|----------|---------|-----|--------|--------|------|------|----------------------|
| provider_leak_customer_sanitize | sanitize-v3 + detectProviderMetadataLeaks | test:gdi-customer-provider-leak-sanitize-v3 | Local clears Ren leak | v1.5 | yes | yes | yes | yes | **no** (deploy) | **yes** |
| false_contact_exhaustion_forbidden | buyer-contact-enrichment researchAttempted | contract + revert artifact | 04b-false-exhaust-revert.json | v1.4–v1.5 | yes | yes | yes | yes | **no** | **yes** |
| inventory_facing_vs_unique_count | freeze + identity blockers | identity_count_inconsistent | §F freeze / 42 facing | v1.5 | yes | yes | yes | partial | **no** | **yes** |
| assessment_unknown_vs_unfinished | assertPublicationWriteReadiness | readiness fixtures | fit batch + readiness | v1.5 | yes | yes | yes | yes | **no** | **yes** |
| dedupe_preserve_history | supersession + publication quality | unreconciled identity blocked | 02-sssp-duplicate-resolution.json | v1.4–v1.5 | yes | yes | yes | partial | **no** | **yes** |
| contact_depth_with_org_fallback | queue dispositions + enrichment | org-fallback / false EXHAUSTED | contact depth continuation | v1.2+ | yes | yes | yes | yes | **no** | **yes** |
| lessons_capture_required | full-e2e-lessons-capture-v1 + stage P | contract cases 46–49 | this closeout + capture artifact | v1.5 | yes | yes | yes | yes | n/a | **yes** |

MF/SD/Ren baseline lessons already on the shared path (segment depth, plan-only block, assoc≠planner, delivery fail-closed, ledger-first auth, geo localConnection, PE persist, publication metadata, budget isolation, resume) remain in the register — **not duplicated** here.

### Remaining implementation or verification gaps

| Gap | Axes still open |
|-----|-----------------|
| Clean-main deploy of sanitize/readiness/verify-harden | productionVerified for provider_leak, delivery_verify, local_production_parity |
| Named-buyer depth majority still open | productionVerified for contact_depth / false_exhaustion |
| Auto evidence from every persist/freeze path | executedSuccessfully for inventory, dedupe, canonical writers, hotel_fit |
| Browser / share / PDF delivery | delivery_checks productionVerified |
| Independent sports/edu COMPLETE claim | productionVerified for sports_edu_independent |

### Next-hotel inheritance

| Correction class | Inherits automatically? |
|------------------|-------------------------|
| Provider-leak sanitize v3 | **Yes** (shared path + test) — prod clear after deploy |
| False EXHAUSTED gate | **Yes** |
| Readiness unknown vs unfinished | **Yes** |
| SSSP / identity dedupe rules | **Yes** — still need run to emit reconciled persist outputs |
| Inventory facing≠unique discipline | **Yes** for blockers; freeze evidence still operator-disciplined |
| Lessons capture stage P | **Yes** — required on every Full E2E |
| Ren-only repair scripts / overlays | **No** — not process improvements |

**Evaluator posture:** PARTIAL until clean-main deploy + production leak clear + delivery surfaces. Lessons capture itself is on the shared completion path and blocks COMPLETE when missing.
