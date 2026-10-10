# Property dropdown — visual QA

| Check | Result |
|---|---|
| Canonical format identified | YES — `Name — City, State|Country` |
| W Rome label after | `W Rome — Rome, Italy` |
| W Rome matches pattern of peers | YES |
| Shared component (`#gdiHotel.filter-select`) | YES |
| Font / weight / padding parity | YES (shell + GDI select rules) |
| Selected height 3rem | YES |
| Chevron padding-right 2rem | YES |
| Ellipsis on long labels | YES (`text-overflow: ellipsis`) |
| W-Rome-only CSS | NO |

## Long-label cases covered by shared rules
short · long · hotel+city · brand-heavy · accented (A Coruña / Galicia)
