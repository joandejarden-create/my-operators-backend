# Operator Fit — Geography Differentiation Audit

**Code:** `scoreGeographyFactor` (`alignment-factors.js`) · Market Presence eligibility (`market-presence.js`)

## Factor score scale (weight 22)

| Input state | Score |
| ----------- | ----: |
| City/market hit | 100 |
| Country hit (no city hit) | 78 |
| Documented no overlap | 12 |
| Unknown | 0 (in denom) |

**Distinct effective levels:** 4 (plus unknown).

## Market Presence types vs factor score

| Presence type | Eligibility | Geography **factor** score |
| ------------- | ----------- | --------------------------: |
| Current Managed Property | Strong match | Not read by factor — uses countries/markets lists |
| Current Operating Portfolio | Strong match | Same |
| Regional Office or Team | Strong match | Same |
| Active Development | Conditional eligibility | Not scored as depth |
| Historical / Strategic / Claimed / Unknown | Non-eligible alone | Not scored as depth |

**Market Presence Type primarily drives eligibility, not the 22-weight geography factor.**

## Depth questions

| Question | Current behavior |
| -------- | ---------------- |
| Multiple current properties vs one? | **No** difference in factor |
| Country depth? | Binary country hit → 78 |
| Region-only? | May miss unless in markets/countries strings |
| Local team separate? | **No** in geography factor (regional resources is separate factor, often unknown) |
| Recency? | **No** |
| Same-city? | Yes → 100 if city on project **and** in markets (Deal C city null → unreachable) |
| Santa Fe vs Highgate Deal C? | **Both 78** despite different Mexico operating depth |

## Verdict

Geography is **important but coarse**. Deal C cannot express “deep Mexico third-party operator” vs “international operator with Mexico country flag” inside the factor score. Eligibility may both pass; scores tie.
