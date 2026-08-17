# Operator Fit — Differentiation Root Causes

## Ranked issues

| Rank | Issue | Class | Impact | Deal C? | Multi-scenario? | Owner decision risk? | Reopen frozen methodology? | Fix without weight change? | Needs data? | Needs Airtable? | Needs UI? |
| ---: | ----- | ----- | ------ | ------- | --------------- | -------------------- | -------------------------- | -------------------------- | ----------- | ---------------- | --------- |
| 1 | Narrative implies #1≠#2 while scores tie | Presentation + comparable underuse | **Critical** | Yes | Seen on C; risk elsewhere | Yes | Not required for presentation fix | Yes (presentation) | Helps | Optional | **Yes** |
| 2 | Comparable relevance collapses to same asset bucket | Scoring granularity / comparable underuse | **Critical** | Yes | Yes | Yes | Targeted refinement | Yes (logic, same top weights) | Better comps help | Schema for strength/geo | No |
| 3 | Unknown structure + regional → risk −10 **and** zero factors | Double penalty / risk treatment | **High** | Yes | Common when SI incomplete | Yes (band Limited vs Potential) | Possibly risk-policy only | Yes | Capture structure/regional | Fields exist | No |
| 4 | Geography factor ignores presence depth | Scoring granularity | **High** | Yes | Yes | Medium | Targeted | Yes | Presence rows | Presence table | No |
| 5 | Brand/structure N/A on Deal C pilot path | Data insufficiency | **High** | Yes | Path-specific | Medium | No | Data wiring | Yes | Deal SI fields | No |
| 6 | Ranking Ready ≠ alignment quality for owner set | Presentation / semantics | **High** | Yes (all Limited RR) | Yes | Yes | Policy | Policy only | No | No | **Yes** |
| 7 | Stable ID ordinal as owner #1/#2 | Presentation | **High** | Yes | Exact ties exist | Yes | No | Yes | No | No | **Yes** |
| 8 | 70/15/15 unknown structure compresses | Layer compression | **Medium** | Yes | When structure unknown | Medium | Maybe | Careful | Structure prefs | Deal fields | No |
| 9 | Regional/commercial factors sparse (binary/unknown) | Data + granularity | **Medium** | Yes | Yes | Low–med | Later | Partial | Yes | Enrichment | No |
| 10 | Governance / brand factors often N/A | Data insufficiency | **Medium** | Yes | Yes | Low on C | No | Data | Yes | Yes | No |
| 11 | Exact/near ties frequent in distributions | Expected + granularity | **Medium** | Yes | Yes | Medium | Threshold policy | Presentation | No | No | Yes |
| 12 | No performance evidence layer | Expected gap | **Low** now | No | Future | Future | Separate track | N/A | Major | Major | Later |

## Deal C synthesis

Not a single root cause. Primary: **comparable underuse + coarse geography** produce score equality while narratives diverge; **unknown double-penalty risk** pulls both into Limited; **ordinal tie-break** manufactures a false #1.
