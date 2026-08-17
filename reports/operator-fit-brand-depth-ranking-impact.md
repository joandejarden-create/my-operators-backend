# Brand Depth — Ranking Impact

Engine: operator-fit-v2.1.0 · Scoring weights **frozen**

| Deal | Operator | Brands | Compat category | Numeric | Project Approval | Rank change expected |
| ---- | -------- | ------ | --------------- | ------: | ---------------- | -------------------- |
| Deal A | Highgate | Sonesta, Marriott, Courtyard | Unsupported | 20 | Both Parties Must Confirm | Usually none — validation wording/honesty improved; weights frozen |
| Deal A | GHL Hoteles (GHL Holding) | Sonesta, Marriott, Courtyard | Partially Supported | 55 | Both Parties Must Confirm | Usually none — validation wording/honesty improved; weights frozen |
| Deal B | Álvarez Argüelles Hoteles | Accor, Hilton, Independent | Unknown | 0 | Both Parties Must Confirm | Usually none — validation wording/honesty improved; weights frozen |
| Deal B | AADESA | Accor, Hilton, Independent | Unknown | 0 | Both Parties Must Confirm | Usually none — validation wording/honesty improved; weights frozen |
| Deal C | Grupo Hotelero Santa Fe | Hilton, Marriott, Krystal | Partially Supported | 55 | Both Parties Must Confirm | Usually none — validation wording/honesty improved; weights frozen |
| Deal C | Highgate | Hilton, Marriott, Krystal | Unsupported | 20 | Both Parties Must Confirm | Usually none — validation wording/honesty improved; weights frozen |

## Defect addressed (not a weight retune)

Prior `/approv/i` matching treated phrases like “not global approval” as Approved. Explicit approval statuses only. Project approval validation items always emitted when preferred brands exist.
