# Exa Mission Gap Analysis — Bethesda Live Results

Exa Agent discovery uses shared contract prompts (`buildExaAgentQuery` + segment request titles) with hotel context blocks. Different titles alone are insufficient — observed Bethesda results show missions still converging on national calendars.

## Cross-cutting weaknesses

| Weakness | Evidence | Affected segments |
|---|---|---|
| Too event/calendar focused | AEA, AHA, NASPA, SIOP, SMFM dominate processed set | associations, corporate meetings, education |
| Too broad geographically | National orgs without Bethesda-reachable demand | sports (NFL), meetings, associations |
| Weak early corporate indicators | Only Amentum survived as corporate NEW_UNIQUE | corporate_accounts_scaling |
| Weak project/workforce framing | AlphaStruxure / Heven AeroTech EXCLUDE | projects, relocation |
| Rediscovery cost | Rock Creek Mansion duplicate | preferred_lodging |
| Thin contacts | 2 sales-ready / 20 processed | all |
| Low per-cell diversity after fair queue | Many segments contributed 1–2 processed orgs | all |

## Mission-specific notes

1. **associations_conferences_events** — Needs rotation/overflow + Montgomery County / DC-metro secondary hotel logic; suppress primary-city national conventions unless overflow evidence.
2. **corporate_accounts_scaling** — Best relative performance; strengthen HQ moves, office expansions, contract wins in Bethesda/Rockville/NoVA with lodging implications.
3. **corporate_meetings_incentives** — Over-indexes on annual association meetings; prefer company kickoffs/training near Lockheed/NIH/federal campuses.
4. **government_public_sector** — APSA watchlist useful; add federal program offices, NIST, DOE, HHS meeting patterns.
5. **education_scientific** — Baseline already rich in NIH/PDA/biomaterials; mission should hunt **net-new** lab/consortium demand not already published.
6. **sports_entertainment** — NFL/Big Time Hoops unfit; prefer local tournament overflow already partially in baseline, or skip if rediscovery risk high.
7. **local_catering_day_meetings** — Needs chamber, corporate campus, hospital day-meeting sources — AFCEA alone insufficient.
8. **preferred_lodging_partnerships** — Must prefilter against published PLP inventory before paid deep.

## What not to do now

Do not rewrite all missions in one PR. Prefer a focused catchment + pre-deep dedupe + corporate-expansion mission PR next.
