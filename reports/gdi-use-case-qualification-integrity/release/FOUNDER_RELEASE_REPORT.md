# Use-case qualification integrity — authorized release

Generated: 2026-10-15T22:15:00.000Z

## Answers

### A. Does authorized promotion now write publication metadata correctly?

**Yes.** `authorizeProspectPublicationPromotion({ authorizePublication: true })` re-evaluates eligibility, writes `prospectPublicationBasis=REVIEWED_PACKAGE_V1`, sets facing/visible, and appends a durable `publicationEvents` / `activityHistory` entry. Facing flags alone cannot authorize. `preservePublicationMetadata` preserves existing basis on enrichment and keeps explicit withdrawals withdrawn. Lifecycle covered in `test-gdi-use-case-qualification-integrity-v1.mjs` (section J).

### B. Which of the nine records are actually restored and visible?

All **9** — canonical Airtable read-back (`production-canonical-verify.json`):

| ID | Org | Disposition | Basis | Reviewed visible | Filter visible |
|----|-----|-------------|-------|------------------|----------------|
| gdi_pe_295cfef5822e63c3 | Sparks Gallery | RESTORED | REVIEWED_PACKAGE_V1 | true | true |
| gdi_pe_879047c3211d2e82 | City Cruises San Diego | RESTORED | REVIEWED_PACKAGE_V1 | true | true |
| gdi_pe_5ce20a54b903e243 | University Club San Diego | RESTORED | REVIEWED_PACKAGE_V1 | true | true |
| gdi_pe_ca47fef6aa622566 | deck655 | RESTORED | REVIEWED_PACKAGE_V1 | true | true |
| gdi_pe_5f72b9d7665314b5 | Flagship Cruises & Events | RESTORED | REVIEWED_PACKAGE_V1 | true | true |
| gdi_pe_4b9b49bae4136428 | Nolita House | RESTORED | REVIEWED_PACKAGE_V1 | true | true |
| gdi_pe_a7a2fda2a474aa0b | The House Next Door | RESTORED | REVIEWED_PACKAGE_V1 | true | true |
| gdi_sc_0fe98055e8df9f75 | Wisedocs | BASIS_REPAIRED | REVIEWED_PACKAGE_V1 | true | true |
| gdi_sc_188e25b6b3b56637 | The Shipyard | BASIS_REPAIRED | REVIEWED_PACKAGE_V1 | true | true |

PE seven already carried basis+facing; prior exclusion was the old travel/prose gate. Restore = contract eligibility + durable authorization event. Wisedocs/Shipyard required explicit basis repair.

### C. Were any other hotels/records affected?

**No additional visibility writes.** All-hotel contract replay (`all-hotel-contract-replay-diff.json`): `additionalHeldUnappliedCount: 0`. Only the nine authorized IDs were applied.

### D. Is queued-hotel execution history accurate?

**Corrected.** See `execution-history-corrected.json` and `data/group-demand-intelligence/execution-history/gdi_qualification_integrity_release_*.json`. Queued six-hotel batch remains **not_started**. Qualification repair / contract testing / publication repair are classified separately from actual Mayflower/SD research and excluded from customer research history. Prior `three-hotel-execution-history.json` annotated as superseded (no fabricated timestamps).

### E. Local and production verified on the released implementation?

- **Data path:** Airtable canonical verified for all nine (reviewed + filter visible).
- **Code path:** Requires merge/deploy of eligibility + `authorizeProspectPublicationPromotion` (this PR). PR #96 filter-before-sanitize ancestry preserved. UI Proper Case / contact-depth keep coordinating separately.
- Browser spot-check after deploy: SD PE tiles + Mayflower Wisedocs + SD Shipyard.

## Reconciled counts (fresh baseline)

| Hotel | Baseline freeze | Post-apply facing (filter) | Post-apply reviewed |
|-------|-----------------|----------------------------|---------------------|
| Mayflower `rec0eTEnCqlsGJJt9` | see `baseline-freeze.json` | **38** | **33** |
| SD Bayside `recnPB3Q02NO8Ma1o` | see `baseline-freeze.json` | **51** | **48** |

- IDs added by this release as **restorations/repairs** (not new discoveries): the nine above.
- Concurrent LMC/contact-depth IDs remain separate (not counted as this release’s discoveries).
- Exclusions: none of the nine retained internal after fresh recheck.

## Artifacts

| File | Purpose |
|------|---------|
| `baseline-freeze.json` | All configured hotels pre-apply visibility |
| `all-hotel-contract-replay-diff.json` | Record-level contract replay |
| `authorized-change-manifest.json` | Exact authorized change set |
| `nine-record-before-after.json` | Nine-record dispositions |
| `production-canonical-verify.json` | Post-apply Airtable verify |
| `post-apply-active-hotels-freeze.json` | May+SD post counts |
| `backups/` | Pre-apply opportunity docs |
| `../execution-history-corrected.json` | History classification fix |

## Non-claims

- Restorations ≠ new discoveries.
- Completing this task ≠ complete market coverage.
- No paid calls, outreach, or broad discovery.
- Six-hotel batch not started.
