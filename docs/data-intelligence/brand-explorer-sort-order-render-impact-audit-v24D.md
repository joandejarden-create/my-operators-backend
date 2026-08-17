# Brand Explorer Sort Order Render Impact Audit v24D

- Generated: 2026-07-09T10:20:35.875Z
- Mode: **dry-run** · Airtable modified: **no**
- Brand: **Tribute Portfolio** (`recCvV0PuZOi8c3hC`)
- Tribute rows reviewed: **105**
- Writer-default Sort Order rows: **85**

## Section Impact
### Geographic Footprint
- Classification: **sort_order_only_affects_ordering**
- Visible issue: no explicit defect in v24 audit
- Rows exist: yes · count: 6
- Frontend uses Sort Order: yes
- Sort likely causes issue: yes
- Recommended action: Optional ordering normalization only
### Openings / Examples / Properties
- Classification: **not_sort_order_related_missing_row**
- Visible issue: no explicit defect in v24 audit
- Rows exist: no · count: 0
- Frontend uses Sort Order: yes
- Sort likely causes issue: no
- Recommended action: Address row/content/image/mapping first; do not apply global sort changes
### Recent Momentum
- Classification: **not_sort_order_related_missing_row**
- Visible issue: no explicit defect in v24 audit
- Rows exist: no · count: 0
- Frontend uses Sort Order: yes
- Sort likely causes issue: no
- Recommended action: Address row/content/image/mapping first; do not apply global sort changes
### Portfolio Mix
- Classification: **not_sort_order_related_source_evidence**
- Visible issue: no explicit defect in v24 audit
- Rows exist: yes · count: 1
- Frontend uses Sort Order: yes
- Sort likely causes issue: no
- Recommended action: Address row/content/image/mapping first; do not apply global sort changes
### Portfolio Context
- Classification: **not_sort_order_related_missing_row**
- Visible issue: missing_peer_portfolio_context, generic_placeholder_copy
- Rows exist: no · count: 0
- Frontend uses Sort Order: no
- Sort likely causes issue: no
- Recommended action: Address row/content/image/mapping first; do not apply global sort changes
### Standard Detail / Where Available
- Classification: **not_sort_order_related_missing_row**
- Visible issue: no explicit defect in v24 audit
- Rows exist: no · count: 0
- Frontend uses Sort Order: yes
- Sort likely causes issue: no
- Recommended action: Address row/content/image/mapping first; do not apply global sort changes
### Demand Scenario View
- Classification: **not_sort_order_related_missing_image**
- Visible issue: no explicit defect in v24 audit
- Rows exist: yes · count: 1
- Frontend uses Sort Order: yes
- Sort likely causes issue: no
- Recommended action: Address row/content/image/mapping first; do not apply global sort changes
### Loyalty Program
- Classification: **not_sort_order_related_missing_row**
- Visible issue: no explicit defect in v24 audit
- Rows exist: no · count: 0
- Frontend uses Sort Order: yes
- Sort likely causes issue: no
- Recommended action: Address row/content/image/mapping first; do not apply global sort changes
### Where This Brand Creates the Most Value
- Classification: **sort_order_unlikely_related**
- Visible issue: missing_card_image, blank_image_placeholder
- Rows exist: yes · count: 3
- Frontend uses Sort Order: yes
- Sort likely causes issue: no
- Recommended action: No sort-order action
### Image Gallery
- Classification: **not_sort_order_related_missing_image**
- Visible issue: no explicit defect in v24 audit
- Rows exist: yes · count: 6
- Frontend uses Sort Order: no
- Sort likely causes issue: no
- Recommended action: Address row/content/image/mapping first; do not apply global sort changes
### Value Creation Scenarios
- Classification: **not_sort_order_related_missing_row**
- Visible issue: title_only_card, title_only_card, title_only_card, title_only_card
- Rows exist: no · count: 0
- Frontend uses Sort Order: yes
- Sort likely causes issue: no
- Recommended action: Address row/content/image/mapping first; do not apply global sort changes
### Key Watchouts
- Classification: **sort_order_unlikely_related**
- Visible issue: no explicit defect in v24 audit
- Rows exist: yes · count: 1
- Frontend uses Sort Order: no
- Sort likely causes issue: no
- Recommended action: No sort-order action
### Why Value Is Strongest
- Classification: **sort_order_unlikely_related**
- Visible issue: no explicit defect in v24 audit
- Rows exist: yes · count: 1
- Frontend uses Sort Order: no
- Sort likely causes issue: no
- Recommended action: No sort-order action
### Differentiators
- Classification: **sort_order_unlikely_related**
- Visible issue: no explicit defect in v24 audit
- Rows exist: yes · count: 2
- Frontend uses Sort Order: no
- Sort likely causes issue: no
- Recommended action: No sort-order action

## High-confidence Sort Order corrections
| Record | Slot | Current | Proposed | Section |
|--------|------|---------|----------|---------|
| `recah9ILLxcXHwmOa` | footprint.region.am | 260 | 11 | Geographic Footprint |
| `recUMmwVFf3V3or7j` | footprint.region.apac | 270 | 15 | Geographic Footprint |
| `recMxUaZjR2Q2c1fP` | footprint.region.cala | 280 | 12 | Geographic Footprint |
| `recnQuk93grWDJl38` | footprint.region.eu | 290 | 13 | Geographic Footprint |
| `recLyiJs6OapVhrLU` | footprint.region.mea | 300 | 14 | Geographic Footprint |

## Exact next command
```bash
npm run brand-explorer-sort-order-render-impact-audit -- --brand tribute-portfolio --dry-run
```