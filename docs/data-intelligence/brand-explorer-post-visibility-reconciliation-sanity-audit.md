# Brand Explorer — Post-Visibility Reconciliation Sanity Audit

## Purpose

After Profile in Preparation visibility restore (Ascend, Comfort Inn & Suites, Curio, Tribute Portfolio), confirm live state of every Brand Explorer profile and remove ambiguity **before** global image role-match remediation.

This pass is **audit-only**. It does not write Company Validated, Source Library, Registry, owner-facing content, images, or release fields.

## Commands

```bash
npm run brand-explorer-post-visibility-reconciliation-sanity-audit -- --dry-run
```

Reports:

- `reports/brand-explorer-post-visibility-reconciliation-sanity-audit.json`
- `reports/brand-explorer-post-visibility-reconciliation-sanity-audit.md`

## Cohort map (post-restore)

| Cohort | Membership | Public full profile? | In default external quality lock (7)? |
| --- | --- | --- | --- |
| Primary active release | `PRIMARY_RELEASE_SLUGS` | Only when live gates say so | **Yes** |
| Restored legacy approved | `VISIBILITY_RESTORED_RELEASE_SLUGS` (Ascend, Comfort, Curio, Tribute) | Yes (transitional unlock) | **No** (by design until expanded) |
| Remediation | Locked primary or legacy with content/image debt | No | Only if also primary |
| Founder-preview-only | Externally locked + presentation rows | No (use `?beInternalPreview=1`) | No |

### Why external quality lock is still 7/7

`test:brand-explorer-external-quality-lock` defaults to `PRIMARY_RELEASE_SLUGS` only. Restoring four legacy brands does **not** auto-expand that cohort. Those four are a **transitional public** cohort: visible to founders/owners via display unlock, but not yet covered by the main EQL suite.

Recommended follow-up (not done here): dedicated legacy-restored quality lock **or** explicit cohort expansion after image role-match acceptance.

## Radisson Individuals

Canonical slug: `radisson-individuals-by-choice` → discovery config record `recRyvM8OmLlDj9G7` (“Radisson Individuals by Choice”).

Sibling Brand Basics rows (Radisson Blu by Choice, Radisson by Choice, etc.) are **other brands**, not alternate Individuals profiles. Do not merge automatically.

If the visibility/OS audit lists Radisson under `image_remediation`, that refers to **live image uniqueness / OS debt on the canonical Individuals record**, not a wrong legacy-seed pointer. Individuals is **not** in `LEGACY_SEED_BRANDS`.

## Internal-preview-copy (Everhome / Kimpton / Radisson)

OS release-readiness regression runs:

```bash
npm run test:brand-explorer-internal-preview-owner-copy -- --brands everhome-suites,kimpton,radisson-individuals-by-choice
```

This is stricter than external quality lock. The sanity audit records live vs projected vs public-path hits.

### Current findings (post-visibility)

| Brand | Live internal hits | Public path hits | Blocks public baseline? |
| --- | --- | --- | --- |
| Everhome | clean | clean | No |
| Kimpton | clean | clean | No |
| Radisson Individuals | `internal_review` only | clean | No |

Radisson’s sole hit matches the Founder Preview banner phrase (“…visible for **internal review**…”) when the brand is externally locked and rendered with `?beInternalPreview=1`. That is a **scan false positive / hygiene interaction**, not owner-facing content debt. Public locked shell has no forbidden copy. Do not patch brand copy for this without an explicit decision to change the banner wording or exclude the banner from the scan.

## Related

- `docs/data-intelligence/brand-explorer-profile-preparation-visibility.md`
- `docs/data-intelligence/brand-explorer-legacy-approved-profile-reconciliation.md`
