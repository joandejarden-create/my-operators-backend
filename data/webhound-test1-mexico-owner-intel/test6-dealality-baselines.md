# Test 6 — Dealality SoT baselines (read-only, pre-reconciliation)

Extracted while Webhound runs. **No SoT writes.**

## BE Active/Live presence

| Brand | In BE 54? | Dealality name | Slug | Record ID |
|-------|-----------|----------------|------|-----------|
| Hotel Indigo | Yes | Hotel Indigo | hotel-indigo | recegXrqaPiSLGCIe |
| Kimpton | Yes | Kimpton Hotels | kimpton | recCKuXCmGvxHPfb3 |
| Tribute Portfolio | Yes | Tribute Portfolio | tribute-portfolio | recCvV0PuZOi8c3hC |
| Avani | **No** | — | — | — (Minor planned; census only) |
| Radisson Individuals Americas | Yes | Radisson Individuals by Choice | radisson-individuals-by-choice | recRyvM8OmLlDj9G7 |

## Census affiliation scale (audit)

| Brand | Census exact | CALA |
|-------|-------------:|-----:|
| Hotel Indigo | 16 | 15 |
| Kimpton Hotels | 12 | 11 |
| Tribute Portfolio | 11 | 10 |
| Radisson Individuals by Choice | 14 | 14 |
| Avani | not in Brand Setup inventory | — |

## Mexico approx (amenity-blank subset)

Indigo ~9 · Kimpton ~7 · Tribute ≥2–4 · Avani 1 (Cancún Airport) · Radisson Individuals 0 Mexico in that artifact (CALA = Faranda CO/PA)

## Key paths

- docs/data-intelligence/brand-explorer-54-active-public-full-baseline.md
- reports/census-affiliation-vs-brand-setup-inventory.csv
- reports/census-amenities-blank-rows.csv
- fixtures/brand-explorer-presentation-kimpton-*.json
- fixtures/brand-explorer-presentation-radisson-individuals-*.json
