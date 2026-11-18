# Idempotency test results

| Pass | CREATED | UPDATED | UNCHANGED | WOULD_CREATE | WOULD_UPDATE |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 (first apply sim) | 2 | 0 | 1 | 0 | 0 |
| 2 (repeat unchanged evidence) | 0 | 0 | 3 | 0 | 0 |

Repeat creates zero: **true**
