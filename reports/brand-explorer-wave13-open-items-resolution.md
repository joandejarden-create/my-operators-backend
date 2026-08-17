# Wave 13 — Open items resolution

Version: `brand-explorer-wave13-open-items-resolution-v1` · Generated: 2026-07-26T22:22:11.151Z
Dry-run: **false** · Airtable writes: **true**
Ready: `wave13_open_items_resolution_applied_stage4_may_proceed_seven_excluding_house`

## 1. SO/ Brand Basics

| Check | Result |
| --- | --- |
| Record ID | `recPCWbTmBPe5SMm0` |
| Brand Name | `SO/` |
| Brand Status | `Under Review` |
| Parent Company | `AccorHotels` |
| Created this run | **true** |
| Active/Live | **false** |
| Display alias (notes / factory identity) | `SO/ Hotels & Resorts` |
| Code-side slug | `so-hotels-and-resorts` |

Details: [`brand-explorer-wave13-so-brand-basics-creation.md`](brand-explorer-wave13-so-brand-basics-creation.md)

## 2. Fairmont

- Brand Basics name remains **Fairmont** (expected Fairmont)
- Consumer/display context allowed: **Fairmont Hotels & Resorts**
- Rename performed: **false** · Slug changed: **false** · Airtable writes: **false**

## 3. The House of Originals

- Recommendation: **C**
- Official sources no longer support House of Originals as an active Accor/Ennismore brand; Morgans Originals appears to be the current collection. Replacing (B) requires a separate approved Basics/slug task. Keeping (A) risks publishing under an obsolete name. Recommend excluding House from Stage 4 and proceeding with the other seven Wave 13 brands after SO/ Basics exists.
- Details: [`brand-explorer-wave13-house-of-originals-founder-review.md`](brand-explorer-wave13-house-of-originals-founder-review.md)

## Stage 4 posture

| Path | Allowed |
| --- | --- |
| All eight brands | **false** |
| Seven brands excluding The House of Originals | **true** |
| Replacement brand pending approval | **false** |

Stage 4 may proceed with seven brands excluding The House of Originals (after SO/ Basics exists). Replacement with Morgans Originals remains a separate approved task if founder chooses B later.

## Post-validation

- Manifest SO Basics exists: **true** (`recPCWbTmBPe5SMm0`, status Under Review)
- Active universe remains 39: **true** (count=39)
- SO in Active universe: **false** (must be false)
- Protected 39 baseline: **true**

## Commands

```bash
npm run brand-explorer-wave13-factory -- --stage open-items-resolution --dry-run
npm run brand-explorer-wave13-factory -- --stage open-items-resolution --apply \
  --approve-so-brand-basics-creation \
  --confirm-so-under-review-only \
  --confirm-no-active-live-status \
  --confirm-no-release-field-writes \
  --confirm-no-company-validation-changes \
  --confirm-no-source-library-status-changes \
  --confirm-no-registry-approval-changes \
  --confirm-no-presentation-writes \
  --confirm-no-image-writes \
  --confirm-no-protected-39-brand-changes \
  --confirm-no-morgans-originals-record-changes
```
