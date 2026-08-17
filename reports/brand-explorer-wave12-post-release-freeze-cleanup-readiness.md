# Wave 12 Post-Release Freeze Cleanup — Freeze Readiness

Generated: 2026-07-25

## Verdict

**not_ready_to_freeze_39_active_public_full_baseline**

Copy/allowlist cleanup for the three target brands is complete within the allowed write surface. The 39-brand freeze remains blocked by **image uniqueness** (Scene7 aspect-ratio variants counted as near-duplicates) and by **out-of-scope** remediations on non-target Active brands.

## Target-brand outcomes

| Brand | Copy / allowlist | Live display | Quality (latest full audit) | Remaining freeze blocker |
| --- | --- | --- | --- | --- |
| bunkhouse-hotels | Cleared — World of Hyatt scrubbed from Presentation + Basics Brand Positioning; tags thickened | `active_profile_ready` / full=true | `approve_for_baseline_freeze` | None (target scope) |
| moxy-hotels | Cleared — Marriott Bonvoy treated as parent-platform context via allowlist (`moxy` + parent company) | `active_profile_ready` / full=true | `approve_for_baseline_freeze` | None (target scope) |
| voco-hotels | Cleared — scenario titles diversified; tags thickened | `draft_applied_with_defects` / full=false | `remediation_required` | Gallery Scene7 aspect-variant duplicates (4/6 distinct) — **image rematerialization required** |

### Voco gallery identity (live)

| Slot | Filename (attachment) | Scene7 asset after aspect strip |
| --- | --- | --- |
| materials.gallery.1 | `…-11133264170-3x2` | distinct |
| materials.gallery.2 | `…-11133264276-3x2` | distinct |
| materials.gallery.3 | `…-10958437436-2x1` | **same as .4** |
| materials.gallery.4 | `…-10958437436-4x3` | **same as .3** |
| materials.gallery.5 | `…-11141397083-2x1` | **same as .6** |
| materials.gallery.6 | `…-11141397083-4x3` | **same as .5** |

Caption-only edits cannot fix this. Image field rematerialization is outside the freeze-cleanup write flags (`--confirm-no-image-writes-except-caption-only-if-flagged`).

## Latest full quality audit (2026-07-25T00:49Z)

- Recommendation counts: `approve_for_baseline_freeze` **31**, `remediation_required` **8**
- Baseline freeze decision: `do_not_freeze_remediation_required`

### Remediations outside target scope (also block 39/39)

| Brand | Class | Notes |
| --- | --- | --- |
| avid-hotels | Image uniqueness | Scene7/filename near-dup (gallery 5/6; scenario dup blocker) |
| holiday-inn-express | Image uniqueness | Gallery near-dup (5/6) |
| vignette-collection | Image uniqueness | Gallery near-dup (5/6) |
| small-luxury-hotels-of-the-world | Forbidden `adr` | Owner-facing scenario copy |
| suburban-studios | Forbidden `adr` | Owner-facing scenario copy |
| trademark-collection-by-wyndham | Forbidden `adr` | Owner-facing scenario copy |
| woodspring-suites | Forbidden `adr` | Owner-facing scenario copy |

These were **not** written by freeze cleanup (targets-only). Stage 9–10 post-release validation had reported 36/39 freeze-ready; live uniqueness + forbidden scans now fail additional brands.

## Gates run after cleanup

| Gate | Result |
| --- | --- |
| Evidence quality (`test:brand-explorer-recent-momentum-evidence-quality`) | **PASS** |
| Mandatory release gates | **PASS** |
| Full 24-tab quality audit | **31/39** freeze (see above) |
| PVQL `--public-full-only` | Not re-confirmed end-to-end (prior suite killed after Airtable rate-limit thrash). Live spot-check: bunkhouse/moxy public-full; voco **not** public-full |
| OS release-readiness | Started; aborted mid-run due to Airtable fan-out / rate limits |
| Active-universe SoT | Not re-run after abort |

## Protected surfaces

No writes to Company Validated, Company Validation Date, Source Library, Registry, Brand Status, release fields, public restore registry, Radisson Collection, or the other 36 Active brands’ content (except validator allowlist code for Moxy parent-platform context).

## Recommended next stage (requires explicit approval)

1. **Wave 12 gallery rematerialization** for uniqueness failures: at minimum `voco-hotels`; for 39/39 also `avid-hotels`, `holiday-inn-express`, `vignette-collection` — with filename/Scene7-aware `pickDistinctImageAssets`.
2. **ADR scrub** on protected brands flagged above (or confirm intentional exemption) — separate from Wave 12 freeze cleanup.
3. Re-run quiet sequential validation: quality audit → PVQL public-full-only → evidence → OS → mandatory gates → SoT.
4. Only then state `ready_to_freeze_39_active_public_full_baseline` and run baseline-39 freeze.

## Artifacts

- `reports/brand-explorer-wave12-post-release-freeze-cleanup-failures.{json,md}`
- `reports/brand-explorer-moxy-parent-platform-allowlist-fix.md`
- `reports/brand-explorer-wave12-post-release-freeze-cleanup.{json,md}`
- `reports/brand-explorer-wave12-post-release-freeze-cleanup-{bunkhouse,moxy,voco}.md`
- `docs/data-intelligence/brand-explorer-wave12-post-release-freeze-cleanup.md`
- This file: `reports/brand-explorer-wave12-post-release-freeze-cleanup-readiness.md`
