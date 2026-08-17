# Brand Explorer — Public Visibility Quality Lock

**Version:** `public-visibility-quality-lock-v1`  
**Type:** Read-only test / reporting / gate  
**npm:** `npm run test:brand-explorer-public-visibility-quality-lock`

## Purpose

The default External Quality Lock still focuses on the primary release cohort. After Profile in Preparation visibility restore, several legacy-approved brands can render full tabs publicly without being in that lock.

This Public Visibility Quality Lock covers **every** Brand Explorer profile that can render full tabs externally, regardless of cohort:

- primary release
- restored legacy public
- future migrated public full profiles

It does **not** write Airtable, change Company Validated, Source Library, Registry, release fields, or owner-facing content.

## Cohorts

| Cohort | Meaning |
| --- | --- |
| `primary_release` | In `PRIMARY_RELEASE_SLUGS` and `shouldRenderFullProfile` |
| `restored_legacy_public` | Transitional public full (e.g. Ascend, Comfort Inn & Suites, Curio, Tribute) or other public full outside primary |
| `founder_preview_only` | Internal / founder preview path; not public full |
| `remediation_locked` | Draft / prep / remediation; must stay locked externally |
| `no_profile_or_not_ready` | No usable profile surface |

## Scope rule

Any brand with `shouldRenderFullProfile === true` is **in lock scope** and must be evaluated.

## Gates (owner-facing rows only)

- Rendered field completeness
- No empty rendered components
- Tab factory audit
- Source provenance by tab
- Image uniqueness (gallery / scenario / property)
- Image role-match
- Scenario + property distinctiveness
- Forbidden owner-facing language
- Generic copy scan
- Raw URL scan
- Wrong-brand / wrong-property role-match hard fails
- Display consistency (full vs Profile in Preparation)
- Company Validated unchanged (before/after read)

**Do Not Display / Internal Only** rows are excluded from public scans and listed in the hidden-row hygiene appendix.

## Hard fails (process exit 1)

- Externally visible full profile not covered
- Primary release cohort gate failure
- Locked / remediation profile leaks full external tabs
- Company Validated changed during the run

Legacy public failures are **explicitly flagged** and do not alone fail the process (acceptance: pass or flagged).

**Note:** Public full rendering (`shouldRenderFullProfile`) can diverge from OS `active_profile_ready`. The lock evaluates live owner-facing quality gates, so a primary brand can be publicly full yet fail completeness — that is a hard fail, not a silent pass.

## Reports

- `reports/brand-explorer-public-visibility-quality-lock.json`
- `reports/brand-explorer-public-visibility-quality-lock.md`
- `reports/brand-explorer-public-visibility-quality-lock-primary.md`
- `reports/brand-explorer-public-visibility-quality-lock-legacy.md`
- `reports/brand-explorer-public-visibility-quality-lock-hidden-row-hygiene.md`

## Related

- `test:brand-explorer-external-quality-lock` — primary cohort DOM lock (unchanged)
- `brand-explorer-post-visibility-reconciliation-sanity-audit` — cohort reconciliation
- OS release-readiness — primary OS state machine
