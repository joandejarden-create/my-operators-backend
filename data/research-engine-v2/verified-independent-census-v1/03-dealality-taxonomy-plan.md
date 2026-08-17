# Dealality-Owned Taxonomy Plan

Reuse existing schemas first:

| Taxonomy | Existing home | Action |
|----------|---------------|--------|
| Dealality Market | Radar / census geography docs | Prefer over STR Market |
| Dealality Submarket (corridors) | `lib/radar-buildout/country-configs.js` + census corridor backfill | Assign via rules + steward |
| Segment / positioning | Brand Explorer / Brand Setup | Do not invent parallel Chain Scale unless needed |
| Property Type / Hotel Service Model | Census fields already | Steward + brand evidence |
| Location type (Urban/Resort) | Census `Location` | Dealality rules; not STR import as SoT |

## Versioning

Each derived assignment should store: rule_version, inputs, provenance=dealality_derived, steward_override flag.

## VIC v1 applied

Mexico discoveries receive Dealality Market = `Mexico` (country grain). Corridor Submarket remains Unknown until city→corridor rules run without legacy seed.
