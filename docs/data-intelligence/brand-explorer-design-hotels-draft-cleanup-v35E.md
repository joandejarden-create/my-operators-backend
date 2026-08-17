# Design Hotels Draft Cleanup v35E

Generated: 2026-07-15T07:18:17.975Z
Mode: **apply**

## 1. Opening rows before/after
- Before visible: **3** / total 6
- After (projected) visible: **3**

## 2. Duplicate rows hidden
- (none proposed)

## 3. Visible CALA property examples
- **Wake BioHotel** (`rec5sNCVcRGZfTwbV`) — Wake BioHotel — CALA Property Example
- **Condesa DF** (`rec59aTn7CDtoZN7O`) — Condesa DF — CALA Property Example
- **Carlota** (`recLtxEB4hSVkLuWl`) — Carlota — CALA Property Example

## 4. Section label before/after
- Before: (mixed / U.S. labels)
- After: **Curated CALA examples · Not a full directory**

## 5. Registry candidates promoted
- Stage 1 candidates found: 0
- Scenario registry backfill creates: 0
- Promotions planned: **12**
- `recHW8siJSwWRqUfp` → footprint.openings (presentation `rec59aTn7CDtoZN7O`)
- `rec2Jsa5zg2Sd2vUl` → materials.gallery.3 (presentation `rec5MEkMW60boWJ9A`)
- `recitMaT3si0qbgEl` → footprint.openings (presentation `rec5sNCVcRGZfTwbV`)
- `recgOHjboU2eeOJrz` → overview.scenario.3 (presentation `rec9PlBl4Segs37M0`)
- `recYaoopybgyeBQ0d` → materials.gallery.4 (presentation `recHukOd9wOlD7LBE`)
- `recP7rJmhs8Pvmd02` → footprint.openings (presentation `recLtxEB4hSVkLuWl`)
- `rec0nDF4ZAFthyTsw` → materials.gallery.2 (presentation `recedtKvclhbUxlKU`)
- `recqut4yu6WTHhwbR` → overview.scenario.2 (presentation `recfxEGrnDvEMxxm4`)
- `recbRai4AxNO7R4MV` → materials.gallery.1 (presentation `rechyS0U9kYw8Ki6z`)
- `recrFgp42TNRTnyhB` → overview.scenario.1 (presentation `reckiLJXDBEQEpspo`)
- `recnCXKqL754936Pr` → materials.gallery.5 (presentation `reclKuGcUZ6loU1TV`)
- `recFCkHu6xKjC51yk` → materials.gallery.6 (presentation `recr58vn0AG3cQnnm`)

## 6. Presentation–registry link audit
- footprint.openings `rec59aTn7CDtoZN7O` ↔ `recHW8siJSwWRqUfp` — registry_only_traceability
- materials.gallery.3 `rec5MEkMW60boWJ9A` ↔ `rec2Jsa5zg2Sd2vUl` — registry_only_traceability
- footprint.openings `rec5sNCVcRGZfTwbV` ↔ `recitMaT3si0qbgEl` — registry_only_traceability
- overview.scenario.3 `rec9PlBl4Segs37M0` ↔ `recgOHjboU2eeOJrz` — registry_only_traceability
- materials.gallery.4 `recHukOd9wOlD7LBE` ↔ `recYaoopybgyeBQ0d` — registry_only_traceability
- footprint.openings `recLtxEB4hSVkLuWl` ↔ `recP7rJmhs8Pvmd02` — registry_only_traceability
- materials.gallery.2 `recedtKvclhbUxlKU` ↔ `rec0nDF4ZAFthyTsw` — registry_only_traceability
- overview.scenario.2 `recfxEGrnDvEMxxm4` ↔ `recqut4yu6WTHhwbR` — registry_only_traceability
- materials.gallery.1 `rechyS0U9kYw8Ki6z` ↔ `recbRai4AxNO7R4MV` — registry_only_traceability
- overview.scenario.1 `reckiLJXDBEQEpspo` ↔ `recrFgp42TNRTnyhB` — registry_only_traceability
- materials.gallery.5 `reclKuGcUZ6loU1TV` ↔ `recnCXKqL754936Pr` — registry_only_traceability
- materials.gallery.6 `recr58vn0AG3cQnnm` ↔ `recFCkHu6xKjC51yk` — registry_only_traceability

## 7. Company Validated untouched
- Before: false
- After (projected): false
- Untouched: **yes**

## 8. Active-profile approval untouched
- Active profile approved: **false** (must remain false)

## 9. Expected founder visual review
- Pass (projected): **yes**
- PASS — 6 gallery images visible with imageUrl (6/6)
- PASS — 3 property examples visible with hotel images (3 cards)
- PASS — No logo/lifestyle/generic property images (gallery + property scan)
- PASS — No IMAGE placeholders on scenario cards (ok)
- PASS — No FDD / Item 19 / ADR / RevPAR / net contribution language (0 high findings)
- PASS — Standard detail governance visible and safe (unknown)
- PASS — Company Validated untouched (before=false after=false)
- PASS — Registry traceability for visual slots (0 gaps)
- PASS — No stale UI fallback titles (0 risks)

## 10. Expected Final QA
- Readiness: **blocked**
- Defects: 18

## 11. Apply command
```bash
npm run brand-explorer-design-hotels-draft-cleanup -- --brand design-hotels --apply --approve-brand-explorer-v35E-design-hotels-draft-cleanup --founder-approved-design-hotels-draft-assets --confirm-cala-property-examples-first --confirm-no-us-fallback-when-three-cala-examples-exist --confirm-no-company-validation-claim --confirm-no-active-profile-approval --confirm-no-summary-url-field --confirm-design-hotels-only
```
