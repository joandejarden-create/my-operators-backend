# Market Presence Readiness Impact

Phase 1: **20 creates**, **0 updates** to existing rows.

| Operator | Air MP rows | Distinct countries | Phase1 mp≥2 gate | Dry countries≥1 gate |
| -------- | ---------: | -----------------: | ---------------- | -------------------- |
| Arbor Lodging (CALA) | 5 | 4 | pass | pass |
| Hotel Equities (CALA) | 10 | 10 | pass | pass |
| GHL Hoteles (GHL Holding) | 7 | 7 | pass | pass |
| Aimbridge Hospitality (LATAM) | 1 | 1 | FAIL | pass |
| Playa Hotels & Resorts | 3 | 3 | pass | pass |
| Grupo Hotelero Santa Fe | 1 | 1 | FAIL | pass |
| Highgate | 5 | 5 | pass | pass |
| Driftwood Hospitality Management | 4 | 4 | pass | pass |
| Atlantica Hotels International (AHI) | 2 | 2 | pass | pass |
| Cenote Azul Operadores | 1 | 1 | FAIL | pass |
| Grupo Iberostar | 0 | 0 | FAIL | FAIL |
| Álvarez Argüelles Hoteles | 1 | 1 | FAIL | pass |
| Marriott International (Managed) | 0 | 0 | FAIL | FAIL |
| Hilton (Managed) | 0 | 0 | FAIL | FAIL |
| Accor (Managed) | 0 | 0 | FAIL | FAIL |
| IHG Hotels & Resorts (Managed) | 0 | 0 | FAIL | FAIL |
| Hyatt (Managed) | 3 | 3 | pass | pass |
| Minor Hotels (Managed) | 0 | 0 | FAIL | FAIL |
| Sonesta International | 1 | 1 | FAIL | pass |
| Four Seasons Hotels and Resorts | 3 | 3 | pass | pass |
| Rosewood Hotel Group | 2 | 2 | pass | pass |
| Mandarin Oriental Hotel Group | 2 | 2 | pass | pass |
| Radisson Hotel Group | 0 | 0 | FAIL | FAIL |
| Meliá Hotels International | 3 | 3 | pass | pass |
| Auberge Resorts Collection | 2 | 2 | pass | pass |
| Shangri-La Group | 2 | 2 | pass | pass |
| Barceló Hotel Group | 2 | 2 | pass | pass |

## Findings

- Existing unchanged rows are still consumed by the payload builder (no taxonomy block observed).
- City / Assignment Count often empty — **not** used by either readiness function today.
- Aimbridge / Cenote / Santa Fe show **mp row count = 1** → fail Phase1 Useful; dry-run still passes if that one row yields a country and asg≥2.
- Strategic Interest countries counted toward dry-run geography (permissive).
