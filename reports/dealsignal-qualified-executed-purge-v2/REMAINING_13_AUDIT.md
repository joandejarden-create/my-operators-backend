# Remaining Qualified audit (after PR #563 tighten)

| Metric | Count |
|--------|------:|
| Before any correction | 45 |
| After initial #563 gate | 13 |
| After remaining-13 audit + Track A alignment | **7** |

## Final Qualified (7)

| ID | Hotel | Why Qualified |
|----|-------|---------------|
| find_cb1_lakeville_bct_brand_search_2026 | Lakeville Inn | Owner goal to secure new brand by YE2026 |
| find_cb4_ahip_aimbridge_mct_2026 | AHIP Aimbridge MCT | Must retain replacement manager |
| find_cb2b_shorebreak_huntington_bct_2026 | Kimpton Shorebreak | Month-to-month; evaluating retain vs rebrand |
| find_cb2b_courtyard_denver_bct_2026 | Courtyard Denver | Evaluating renew vs higher brand vs independent |
| find_cfa_hilton_trinidad_mct_hma_sale_2026 | Hilton Trinidad | EOI + operator talks after buyer process |
| find_cb3_embassy_one_bengaluru_mct_2026 | Embassy ONE Bengaluru | Soliciting/evaluating new operators |
| find_ds_cm_v2_voir_beach_brand_open_2025 | Voir Beach | No hotel partner; vigorously engage brands |

## Demoted from the prior 13

| ID | Destination | Reason |
|----|-------------|--------|
| Bidwell Portland | watch_contractual | Successive franchise extensions / expiry milestone |
| Holtsville RI | watch_contractual | Franchise expiration only |
| Margaritaville Key West | watch_contractual | Operator contract expiration only |
| Margaritaville Hollywood | watch_contractual | Terminable-at-will only |
| Hilton Spark Brazil | watch_emerging | Program-level early engagement |
| Justo Real Inn | watch_emerging | Early purchase / brands-to-change without open process |

## Policy alignment

Reader uses `hif-deal-opportunities-commercial-eligibility-v1.js`, which calls shared
`dealsignal-qualified-opportunity-classification-v1.js` (Track A persist guard).
PRIMARY claim narrative (before `|` tags) decides expiry vs open — pathway tags alone cannot Qualify.
