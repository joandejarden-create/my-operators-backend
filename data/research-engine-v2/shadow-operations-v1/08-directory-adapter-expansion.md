# Directory Adapter Expansion

## Hilton
- Reuses `lib/hilton-hotel-status-fetch.js` GraphQL via `adapters/hilton.js`
- Supports: operating/bookable, official URL pattern, ctyhocn identity
- **Gap:** Mexico/CALA directory gap scan needs property codes; missing ctyhocn → Source Empty + identity escalation (not "closed")
- Existing write path: `scripts/audit-hilton-census-status.mjs` (--dry-run / --apply)

## Choice / Radisson Individuals Americas
- Sitemap extract + `adapters/choice.js`
- 403/429 → **Blocked** — never infer reflag
- `computeChoiceIndividualsGaps` for Faranda/Individuals mapping reviews

## IHG / Marriott
- Hardened Exact/High + geo gates retained from V1.1
