# v42 — Brand Explorer Founder Visual Review Packet

Founder-facing visual review after v40C remediation. Produces tab readiness, visual asset confirmation, copy quality, brand lenses, and a **release recommendation** — without applying active release.

## Command

```bash
npm run brand-explorer-v42-founder-visual-review -- --brands everhome-suites,kimpton,radisson-individuals-by-choice --dry-run
```

## What it reviews

| Area | Source of truth |
|------|-----------------|
| Tab readiness (10 atelier tabs) | Live internal preview DOM (`?beInternalPreview=1`) + Presentation rows |
| Gallery / property examples | Brand Library API blocks + Asset Registry classification |
| Copy quality | Forbidden + mechanical patterns (v40B) on internal preview + Presentation |
| Brand lenses | Everhome extended-stay · Kimpton lifestyle · Radisson soft-brand |
| Incomplete lock | Hotel Indigo, MGallery, Design Hotels, SLH external Profile in Preparation |
| Prior remediation | `reports/brand-explorer-v40c-economics-chrome-remediation.json` |

## Release recommendations (human gate only)

- `approve_for_active_release` — automated gates clean; founder may still reject on taste
- `approve_after_minor_cleanup` — soft issues (e.g. extra property examples, minor tab concerns)
- `remediation_required` — tab fail, high mechanical, or brand-lens avoid failures
- `not_owner_ready` — forbidden language, visual minimums, or external lock failure

**v42 never applies** active release, active approval, Company Validated, unlock, Source Library, Registry, or image-field writes.

## Outputs

- `reports/brand-explorer-v42-founder-visual-review.json`
- `reports/brand-explorer-v42-founder-visual-review.md`
- `reports/brand-explorer-v42-founder-review-<slug>.md`

## Modules

- `lib/partner-intelligence/brand-explorer-founder-visual-review.js`
- `scripts/brand-explorer-v42-founder-visual-review.mjs`

## OS wiring

When Brand Explorer OS routes `founder_visual_review`, the exact next command points at this v42 packet (dry-run).

## Guardrails

- Read-only / `--dry-run` only (`--apply` refused)
- Incomplete control brands must remain locked
- External quality lock PASS ≠ owner-ready
