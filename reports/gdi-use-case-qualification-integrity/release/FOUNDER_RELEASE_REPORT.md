# Use-case qualification integrity — authorized release

Generated: 2026-10-15T22:42:00.000Z  
Closed: after merge + production deploy

## Answers

### A. Does authorized promotion now write publication metadata correctly?

**Yes.** `authorizeProspectPublicationPromotion({ authorizePublication: true })` re-evaluates eligibility, writes `prospectPublicationBasis=REVIEWED_PACKAGE_V1`, sets facing/visible, and appends a durable `publicationEvents` / `activityHistory` entry. Facing flags alone cannot authorize. `preservePublicationMetadata` preserves existing basis on enrichment and keeps explicit withdrawals withdrawn. Lifecycle covered in `test-gdi-use-case-qualification-integrity-v1.mjs` (section J). Visibility-order regression updated so sanitize-filled `summaryWhat` cannot widen travel evidence (`test-gdi-list-visibility-before-sanitize-v1.mjs`).

### B. Which of the nine records are actually restored and visible?

All **9** — fresh Airtable + local `:8080` + production list/detail:

| ID | Org | Hotel | Disposition | Basis | List visible |
|----|-----|-------|-------------|-------|--------------|
| gdi_pe_295cfef5822e63c3 | Sparks Gallery | SD Bayside | RESTORED | REVIEWED_PACKAGE_V1 | yes |
| gdi_pe_879047c3211d2e82 | City Cruises San Diego | SD Bayside | RESTORED | REVIEWED_PACKAGE_V1 | yes |
| gdi_pe_5ce20a54b903e243 | University Club San Diego | SD Bayside | RESTORED | REVIEWED_PACKAGE_V1 | yes |
| gdi_pe_ca47fef6aa622566 | deck655 | SD Bayside | RESTORED | REVIEWED_PACKAGE_V1 | yes |
| gdi_pe_5f72b9d7665314b5 | Flagship Cruises & Events | SD Bayside | RESTORED | REVIEWED_PACKAGE_V1 | yes |
| gdi_pe_4b9b49bae4136428 | Nolita House | SD Bayside | RESTORED | REVIEWED_PACKAGE_V1 | yes |
| gdi_pe_a7a2fda2a474aa0b | The House Next Door | SD Bayside | RESTORED | REVIEWED_PACKAGE_V1 | yes |
| gdi_sc_0fe98055e8df9f75 | Wisedocs | Mayflower | BASIS_REPAIRED | REVIEWED_PACKAGE_V1 | yes |
| gdi_sc_188e25b6b3b56637 | The Shipyard | SD Bayside | BASIS_REPAIRED | REVIEWED_PACKAGE_V1 | yes |

PE seven already carried basis+facing; prior exclusion was the old travel/prose gate. Restore = contract eligibility + durable authorization. Wisedocs/Shipyard required explicit basis repair. No recreations; contacts/feedback/actions/outcomes preserved.

### C. Were any other hotels/records affected?

**No additional visibility writes.** All-hotel contract replay (`all-hotel-contract-replay-diff.json`): `additionalHeldUnappliedCount: 0`. Only the nine authorized IDs were applied. Six-hotel batch remains not started.

### D. Is queued-hotel execution history accurate?

**Corrected.** See `execution-history-corrected.json` and `data/group-demand-intelligence/execution-history/gdi_qualification_integrity_release_*.json`. Queued six-hotel batch remains **not_started**. Qualification repair / contract testing / publication repair are classified separately from actual Mayflower/SD research and excluded from customer research history. Prior `three-hotel-execution-history.json` annotated as superseded (no fabricated timestamps).

### E. Local and production verified on the released implementation?

**Yes.**

| Check | Result |
|-------|--------|
| Merge | PR [#101](https://github.com/joandejarden-create/my-operators-backend/pull/101) + follow-up [#102](https://github.com/joandejarden-create/my-operators-backend/pull/102) |
| Deploy SHA | `b571d945ed5ae8df58c65d53a3fcffd91a48950b` |
| Deploy record | `deploy-production-b571d945.json` — status **SUCCESS**, dirty=false, no hotfix override |
| Railway | deployment `02a316c1-8e5c-4e51-81a5-1d0dd223a40c` **SUCCESS** (2026-10-15T22:33:30Z); `/health` → `{"ok":true}` |
| Served UI asset | production `dealality-gdi-ui.js` SHA256 matches local release tree |
| Local `:8080` | Mayflower facing **38** (Wisedocs present); SD facing **51** (7 PE + Shipyard present); detail 200 for Sparks/Wisedocs/Shipyard |
| Production API | Same nine IDs present on hotel lists; detail 200 with `REVIEWED_PACKAGE_V1` |
| Postdeploy Airtable | `postdeploy-verify.json` — allNineFound/filter/reviewed/basis **true**, exceptions `[]` |

## Reconciled counts (fresh baseline → post-apply → live)

| Hotel | Baseline freeze | Post-apply / live facing (filter) | Reviewed visible |
|-------|-----------------|-----------------------------------|------------------|
| Mayflower `rec0eTEnCqlsGJJt9` | `baseline-freeze.json` | **38** | **33** |
| SD Bayside `recnPB3Q02NO8Ma1o` | `baseline-freeze.json` | **51** | **48** |

- IDs added by this release as **restorations/repairs** (not discoveries): the nine above.
- Concurrent LMC/contact-depth IDs remain separate (not counted as this release’s discoveries).
- Exclusions: none of the nine retained internal after fresh recheck.
- Do not add nine to an older historical total.

## Commit / deployment references

| Item | Value |
|------|-------|
| Integrity code | `205d731e` / merge `7b075e23` (PR #101) |
| Visibility-order test align | `958f7a31` / merge `b571d945` (PR #102) |
| Production deploy | `b571d945` via `railway run npm run deploy:production` |
| Railway deployment id | `02a316c1-8e5c-4e51-81a5-1d0dd223a40c` |

## Artifacts

| File | Purpose |
|------|---------|
| `baseline-freeze.json` | All configured hotels pre-apply visibility |
| `all-hotel-contract-replay-diff.json` | Record-level contract replay |
| `authorized-change-manifest.json` | Exact authorized change set |
| `nine-record-before-after.json` | Nine-record dispositions |
| `production-canonical-verify.json` | Post-apply Airtable verify |
| `post-apply-active-hotels-freeze.json` | May+SD post counts |
| `postdeploy-verify.json` | Fresh post-deploy Airtable+contract |
| `postdeploy-detail-spotcheck.json` | Local + production detail 200s |
| `deploy-production-b571d945.json` | Deploy SUCCESS record |
| `../execution-history-corrected.json` | History classification fix |

## Non-claims

- Restorations ≠ new discoveries.
- Completing this task ≠ complete market coverage.
- No paid calls, outreach, or broad discovery.
- Six-hotel batch not started.
- PR #96 filter-before-sanitize preserved; presentation release unchanged.
- Contact-depth / AI Admin remain separate concurrent workstreams.
