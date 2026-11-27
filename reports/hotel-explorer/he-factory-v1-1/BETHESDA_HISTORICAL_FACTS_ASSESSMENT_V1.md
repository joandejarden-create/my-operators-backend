# Bethesda Marriott — Year Built & Last Renovated assessment

**HPC:** `recLuxvwwxID7U2B8`  
**Date:** 2026-11-27

## Year Built — 1979

| Layer | Finding |
|---|---|
| Research | SEC CMBS filing + CMBS news: built **1979** |
| Airtable | No `Year Built` column on HPC. Value stored as `Opening Date = 1979-01-01` |
| Prior bug | Radar `HPC_SELECT_FIELDS` omitted Opening Date; DTO omitted `yearBuilt` → UI "Not yet verified" |
| Fix | Select Opening Date; map → `hotel.yearBuilt` with provenance `VERIFIED_DERIVED_FROM_OPENING_DATE` |
| UI expectation | Overview **Year built = 1979** |

SEC: `https://www.sec.gov/Archives/edgar/data/1013611/000153949724000828/n4133-x6ts.htm`

## Last Renovated — dual milestones

| Event | Year | Scope | Role |
|---|---|---|---|
| Major renovation | **2015** | Property-wide (SEC/CMBS / Travel Weekly / press) | Primary Last Renovated display |
| Guestroom unveil | **2017** | Guestroom / public-space unveil; Cvent “Renovated 2017” | Secondary milestone — not a replacement |

| Layer | Finding |
|---|---|
| Airtable | `Renovation / Conversion Date = 2015-01-01` (applied via factory standing auth) |
| Status text | Notes 2015 major + 2017 milestone retained |
| Source URL | SEC CMBS filing |
| UI | `2015 (major); 2017 milestone` when events present |

**Do not** overwrite 2015 with 2017 solely because Cvent lists a later year.  
**Do not** treat Opening Date as a renovation date.

## Factory gates (after Apply)

See `FACTORY_APPLY_bethesda_marriott.json` — historical facts + persistence + API hydration YES; browser gates still outstanding until Radar 8080 11-tab pass.
