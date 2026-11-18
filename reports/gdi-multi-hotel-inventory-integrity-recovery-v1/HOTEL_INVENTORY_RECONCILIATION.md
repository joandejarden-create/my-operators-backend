# Hotel inventory reconciliation

| Hotel | Hotel ID | Research store | Broken Research Preview (customer-facing) | Corrected Research Preview | Suppressed IDs | STAGED | CURRENT | Actually deleted? | Store preserved? |
|---|---|---:|---:|---:|---:|---:|---:|---|---|
| Sheraton Guadalajara Expo | recCAGqkERSXgiku4 | 43 | 6 | 43 | 37 | 38 | — | No | Yes |
| Wyndham San Diego Bayside | recnPB3Q02NO8Ma1o | 82 | 47 | 82 | 35 | — | — | No | Yes |
| Castillo Hotel Son Vida | rec82D9zpB8fede1I | 20 | 0 | 20 | 20 | — | — | No | Yes |
| Bethesda Marriott | recLuxvwwxID7U2B8 | 86 | 67 | 74* | 19 | 67 | 67 | No | Yes |
| YOTEL Geneva Lake | recrPQcZg7SFARRb2 | 42 | 17 | 42 | 25 | 42 | 42 | No | Yes |
| Radisson Blu Toronto Downtown | recH9h3McgMNFQEla | 66 | 17 | 66 | 49 | 66 | 66 | No | Yes |

\* Bethesda corrected Research Preview uses salesperson filter on store (74); with the API fix, Bethesda `?live=1` uses FULL internal union surface (research-complete, not published-only). Published inventory remains CURRENT **67**.

## Notes

- Broken column = what founders saw when customer-facing filters ran on Research Preview.
- Corrected column = research inventory after removing that inappropriate filter (salesperson view for non-Bethesda).
- Machine-readable matrix: `RECONCILIATION_MATRIX.json`.
