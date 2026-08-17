# Brand Asset Review Decision Writer v5.1

Generated: 2026-07-08T08:36:08.246Z
Mode: **dry-run** · Airtable modified: **no**
Brand: Tribute Portfolio `recCvV0PuZOi8c3hC`
Text/governance Platform Ready: **yes**
Brand Setup media untouched: **yes**
Company Validated untouched: **yes**

## 1. Summary

- Total asset records scanned: **27**
- Formal approved records: **9**
- Approval-state conflicts: **0**
- Records proposed for correction: **0**
- Records untouched: **27**
- Primary candidates: **11**
- Selected for approval: **0**
- Selected for rejection: **0**
- Kept as candidate: **0**
- Blocked: **0**
- Decision records updated on apply: **0**
- Correction records updated on apply: **0**

## 2. Formal approved records

- `rec0tC9UF3R9peC9D` — Ermita, Cartagena, a Tribute Portfolio Hotel — Hero Image (exterior) (Approved For Explorer Use / Approved For Explorer / Usage Review Complete)
- `rec610hZiW38N0A3e` — Casa Nizuc, a Tribute Portfolio Resort — Image Gallery 1 (gallery) (Approved For Explorer Use / Approved For Explorer / Usage Review Complete)
- `recF3DPsX5f10pbIY` — Crystal Cove, Barbados, a Tribute Portfolio All-Inclusive Resort — Where This Brand Creates the Most Value (property) (Approved For Explorer Use / Approved For Explorer / Usage Review Complete)
- `recVgqx54u3cNbccv` — Hotel Rumbao, a Tribute Portfolio Hotel — Image Gallery 4 (gallery) (Approved For Explorer Use / Approved For Explorer / Usage Review Complete)
- `recXf8J8AXnY10XvN` — Humano, Lima, a Tribute Portfolio Hotel — Image Gallery 5 (gallery) (Approved For Explorer Use / Approved For Explorer / Usage Review Complete)
- `recZKHvtCZWy1eOED` — Humano, Lima, a Tribute Portfolio Hotel — Where This Brand Creates the Most Value (exterior) (Approved For Explorer Use / Approved For Explorer / Usage Review Complete)
- `recZqBSBezJYt8ZOC` — Crystal Cove, Barbados, a Tribute Portfolio All-Inclusive Resort — Image Gallery 2 (gallery) (Approved For Explorer Use / Approved For Explorer / Usage Review Complete)
- `recfuXCqBA6x9gfFD` — Loma, Medellin, a Tribute Portfolio Hotel — Image Gallery 6 (gallery) (Approved For Explorer Use / Approved For Explorer / Usage Review Complete)
- `reczTkwignWPydWJp` — Tribute Portfolio logo — tribute-black.svg (preferred confirmation candidate) (Approved For Explorer Use / Approved For Explorer / Usage Review Complete)

## 3. Approval-state conflicts

None.

## 4. Records proposed for correction

None.

## 5. Primary candidates (record IDs)

| Record ID | Slot | Asset | Approval-eligible |
|-----------|------|-------|-------------------|
| `rec0tC9UF3R9peC9D` | Hero Image → Brand Setup — Explorer Hero | Ermita, Cartagena, a Tribute Portfolio Hotel — Hero Image (exterior) | yes |
| `rec610hZiW38N0A3e` | Image Gallery → materials.gallery.1 | Casa Nizuc, a Tribute Portfolio Resort — Image Gallery 1 (gallery) | yes |
| `recF3DPsX5f10pbIY` | Value Driver: Resort | Crystal Cove, Barbados, a Tribute Portfolio All-Inclusive Resort — Where This Brand Creates the Most Value (property) | yes |
| `recVgqx54u3cNbccv` | Image Gallery → materials.gallery.4 | Hotel Rumbao, a Tribute Portfolio Hotel — Image Gallery 4 (gallery) | yes |
| `recXf8J8AXnY10XvN` | Image Gallery → materials.gallery.5 | Humano, Lima, a Tribute Portfolio Hotel — Image Gallery 5 (gallery) | yes |
| `recZKHvtCZWy1eOED` | Value Driver: Urban | Humano, Lima, a Tribute Portfolio Hotel — Where This Brand Creates the Most Value (exterior) | yes |
| `recZqBSBezJYt8ZOC` | Image Gallery → materials.gallery.2 | Crystal Cove, Barbados, a Tribute Portfolio All-Inclusive Resort — Image Gallery 2 (gallery) | yes |
| `recfuXCqBA6x9gfFD` | Image Gallery → materials.gallery.6 | Loma, Medellin, a Tribute Portfolio Hotel — Image Gallery 6 (gallery) | yes |
| `recgq6wOvz5yOWPmY` | Value Driver: Conversion / Adaptive Reuse | Ermita, Cartagena, a Tribute Portfolio Hotel — Where This Brand Creates the Most Value (exterior) | yes |
| `recxVPbTlsrP9v4bQ` | Image Gallery → materials.gallery.3 | Ermita, Cartagena, a Tribute Portfolio Hotel — Image Gallery 3 (gallery) | yes |
| `reczTkwignWPydWJp` | Logo → Brand Setup — Logo | Tribute Portfolio logo — tribute-black.svg (preferred confirmation candidate) | yes |

### Excluded from visual review (not approval-eligible)

- `reckFtzYCGq9fZRo4` — 2026 Tribute Portfolio FDD (reference) (Internal/source reference — not a visual Explorer review candidate)

## 6. Selected for approval

None.

## 7. Selected for rejection

None.

## 8. Kept as candidate

None.

## 9. Blocked records

None.

## 10. Records updated on apply

None (dry-run or no eligible selections).

## 11. Approved asset coverage by slot

- **Hero Image**: Ermita, Cartagena, a Tribute Portfolio Hotel — Hero Image (exterior)
- **Image Gallery**: Casa Nizuc, a Tribute Portfolio Resort — Image Gallery 1 (gallery); Hotel Rumbao, a Tribute Portfolio Hotel — Image Gallery 4 (gallery); Humano, Lima, a Tribute Portfolio Hotel — Image Gallery 5 (gallery); Crystal Cove, Barbados, a Tribute Portfolio All-Inclusive Resort — Image Gallery 2 (gallery); Loma, Medellin, a Tribute Portfolio Hotel — Image Gallery 6 (gallery)
- **Value Driver: Resort**: Crystal Cove, Barbados, a Tribute Portfolio All-Inclusive Resort — Where This Brand Creates the Most Value (property)
- **Value Driver: Urban**: Humano, Lima, a Tribute Portfolio Hotel — Where This Brand Creates the Most Value (exterior)
- **Logo**: Tribute Portfolio logo — tribute-black.svg (preferred confirmation candidate)

## 12. Slots still missing

- **Recent Openings** — No property + opening/PR/date candidate
- **Value Driver (Boutique / Lifestyle)** — No primary candidate
- **Value Driver (Mixed-Use)** — No primary candidate
- **PR / Opening Link** — Provenance only until Rendered Source Capture v1

## 13. Readiness

- Ready for asset download/attachment: **yes (approved assets exist)**
- Ready for Explorer promotion: **no**

## 14. Correction apply command

```bash
npm run brand-asset-review-decision-writer -- --brand tribute-portfolio --apply --approve-brand-asset-approval-state-corrections
```

## 15. Next approval command (after correction)

```bash
npm run brand-asset-review-decision-writer -- --brand tribute-portfolio --apply --approve-brand-asset-review-decisions --approve-records recgq6wOvz5yOWPmY,recxVPbTlsrP9v4bQ
```

## 16. Remaining work before download/attachment

- Human approves selected records via --approve-records (explicit IDs only).
- Confirm rights/usage for each approved Marriott-controlled source URL.
- Build asset download + attachment writer (separate module — not this task).
- Do not attach files until download + rights registry exists.

## 17. Remaining work before Explorer promotion

- Approve a coherent slot set (logo + hero + gallery + value drivers) after human review.
- Capture Recent Openings with property + PR/opening/date (currently Missing).
- Fill Boutique/Lifestyle and Mixed-Use value-driver imagery gaps.
- Build governed Explorer hero/logo promotion writer (separate module).
- Do not replace Mock/Demo hero until governed CALA hero is approved and promoted.
- Rendered Source Capture v1 for Marriott newsroom PR before PR link promotion.
