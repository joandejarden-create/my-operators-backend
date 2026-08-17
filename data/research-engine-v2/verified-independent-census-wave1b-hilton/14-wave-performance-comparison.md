# Wave Performance — IHG 1A vs Hilton 1B

| Metric | IHG Wave 1A | Hilton Wave 1B |
|--------|-------------|----------------|
| Independent hotels | 195 | 102 |
| Discovery source | IHG CALA directory extract | Live Hilton Mexico locations pages |
| Core-field completion | 100% | 100% |
| Material-field completion | 56% | 71% |
| Runtime | ~119s | ~62s |
| External research cost | $0 | $0 |
| Legacy reference size | denser IHG MX | sparse Hilton MX parent=26 |
| Legacy match (exact) | 151 | 0 |
| Probable match | (see 1A) | 2 |
| Independent-only | 29 | 100 |
| Legacy-only | 63 | 24 |
| Data eligibility % | (see 1A) | 100% |
| Research effort / hotel | ~611ms | ~603ms |

## Generalization

Architecture **does generalize**. Hilton structured amenityIds/coords/openDate lifted material completion to **71%** (above the ≥65% target) without fabricating rooms. Legacy Hilton Parent=Hilton Mexico is sparse — high independent-only is a **product signal**, not a matcher failure (matcher hardened against brand-token false positives).
