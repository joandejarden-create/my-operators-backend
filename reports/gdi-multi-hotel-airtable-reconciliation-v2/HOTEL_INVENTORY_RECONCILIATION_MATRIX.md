# Hotel inventory reconciliation matrix

| Hotel | Cohort | FS | AT | Overlap | FS-only | AT-only | CREATE | UPDATE | UNCHANGED | DEFERRED | CONFLICT | REJECTED |
|-------|--------|----|----|---------|---------|---------|--------|--------|-----------|----------|----------|----------|
| YOTEL Geneva Lake | PRIMARY | 42 | 108 | 0 | 42 | 108 | 0 | 0 | 0 | 150 | 0 | 0 |
| Radisson Blu Toronto Downtown | PRIMARY | 66 | 36 | 30 | 36 | 6 | 0 | 0 | 29 | 42 | 1 | 0 |
| Sheraton Guadalajara Expo | PRIMARY | 43 | 0 | 0 | 43 | 0 | 0 | 0 | 0 | 43 | 0 | 0 |
| Bethesda Marriott | REFERENCE | 59 | 86 | 59 | 0 | 27 | 0 | 1 | 57 | 27 | 1 | 0 |
| Wyndham San Diego Bayside | REFERENCE | 82 | 82 | 82 | 0 | 0 | 0 | 0 | 82 | 0 | 0 | 0 |
| Castillo Hotel Son Vida | REFERENCE | 20 | 20 | 20 | 0 | 0 | 0 | 0 | 20 | 0 | 0 | 0 |
| Nobu Hotel London Portman Square | REFERENCE | 0 | 65 | 0 | 0 | 65 | 0 | 0 | 0 | 65 | 0 | 0 |
| Waterstone Resort & Marina | REFERENCE | 29 | 36 | 29 | 0 | 7 | 0 | 0 | 29 | 7 | 0 | 0 |

Counts are opportunity-ID joins. Stores are not forced to equal each other.