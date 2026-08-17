# Existing System Audit — Census Autopilot V2

## REUSE
| Component | Path | Role |
|-----------|------|------|
| Golden Priority Schema | `census-autopilot-v1/golden/golden-schema.js` | Authoritative field contract (census-autopilot-v1.2-golden-schema) |
| Golden completeness | `golden/golden-completeness.js` | Priority % scoring |
| Golden geography | `golden/golden-geography.js` | Mexico markets (extend for LATAM) |
| Cvent challenge adapter | `challenge-adapters.js` | URL→name hint; never production evidence |
| Cvent LATAM harvest | `census-cvent-latam-harvest.js` + `reports/cvent-venue-cache/country-results/harvest-*.json` | 13,369 hotel URLs |
| VIC index | `data/.../verified-independent-census-mexico-combined-4family/` | Independent seeds (Mexico 4-family) |
| SerpApi provider | `providers/serpapi-google-hotels/` | Limited fields; Exact/High |
| StayingAPI provider | `providers/staying-api/` | Deferred — do not spend |
| Brand adapters | `adapters/{ihg,hilton,choice,marriott}.js` | Native strong/partial |
| Resume/checkpoint | `census-autopilot-v1/resume-state.js` | Pattern extended |
| Source rights | `data/.../verified-independent-census-v1/04-source-rights-registry.json` | Rights gate |
| Token match utils | `adapters/adapter-utils.js` | Identity similarity |

## EXTEND
- Full-universe master candidate layer + property_identity_id
- LATAM geography lite (country→continent/sub-continent); Market/Submarket still Mexico-strong
- SerpApi demand forecast + Phase B ceiling governance
- Rooms gap map at universe scale
- Airtable dry-run migration design (no writes)
- Brand Explorer readiness hooks

## DEPRECATE
- Nothing deleted. Do not use Cvent/legacy as production evidence (already quarantined).
- StayingAPI: deferred secondary — keep artifacts, no V2 spend.

## MISSING (acknowledged; routed around)
- Full Market/Submarket taxonomy for all LATAM countries
- Accor/Wyndham/Hyatt/Minor native adapters
- Temporal affiliation store (designed, not production-backed)
- First-party pack delivery channel
- Operator Explorer V2 (design hook only)
