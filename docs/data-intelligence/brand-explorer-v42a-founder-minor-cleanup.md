# v42A — Founder Minor Cleanup (Everhome + Kimpton)

Resolves `approve_after_minor_cleanup` from v42 for Everhome Suites and Kimpton Hotels only. Radisson Individuals remains on `approve_for_active_release` and is **not** modified.

## Commands

```bash
npm run brand-explorer-v42a-founder-minor-cleanup -- --brands everhome-suites,kimpton --dry-run

npm run brand-explorer-v42a-founder-minor-cleanup -- --brands everhome-suites,kimpton --apply \
  --approve-brand-explorer-v42A-founder-minor-cleanup \
  --confirm-no-company-validation-claim \
  --confirm-no-active-profile-approval \
  --confirm-no-source-library-changes \
  --confirm-no-registry-changes \
  --confirm-no-incomplete-brand-unlock \
  --confirm-brand-only
```

## What it does

| Brand | Cleanup |
|-------|---------|
| Everhome | Audit 4 property examples; **keep all 4** when distinct + imaged (or hide weakest via `External Display Status = Do Not Display`) |
| Kimpton | Rewrite residual Item-19 / franchise-performance / scrub boilerplate into lifestyle owner copy; extras decision keep-or-hide |

## Allowed Airtable writes (apply only)

- Presentation: Title, Body, Case Summary fields, External Display Status
- Hide path sets `Do Not Display` only — rows are not deleted
- No Image / Registry / Source Library / Company Validated / active approval

## Projection target

Both brands → recommendation **`approve_for_active_release`** (subject to explicit founder OK). OS state target remains `founder_review_ready`. **No active release applied.**

## Outputs

- `reports/brand-explorer-v42a-founder-minor-cleanup.json`
- `reports/brand-explorer-v42a-founder-minor-cleanup.md`
- `reports/brand-explorer-v42a-everhome-cleanup.md`
- `reports/brand-explorer-v42a-kimpton-cleanup.md`

## Guardrails

- Brand-only: `everhome-suites`, `kimpton`
- Refuses Radisson + incomplete-control brands
- No unlock / no active release / no Company Validated
