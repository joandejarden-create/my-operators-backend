# Filter layout before/after

## After (this PR)

Measured on :8080 Wyndham via CDP while switching All → Hotel → Competitive:

| Control | Width (px) | Left (px) | Stable across scopes |
|---|---:|---:|---|
| Weekly Change | 168 | — | yes |
| Opportunity Type | 220 | 927 | yes |
| Market Scope | 336 | 1159 | yes |

Opportunity Type left edge and width unchanged when Market Scope changes.
Competitive empty state correctly shows: no CMI records for this hotel.

## Before

Opportunity Type used content-flexible lex: 0 1 170px / max-width: 240px, so option-label length shifts moved adjacent controls.
