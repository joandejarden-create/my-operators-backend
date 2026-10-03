# ADP Attribute Audit — YOTEL Geneva Lake

Generated: 2026-10-03T14:58:48.764Z
HPC Hotel ID: `recrPQcZg7SFARRb2`
ADP Property ID: `adp_yotel_geneva_lake`
Mode: DRY-RUN

## Summary

| Metric | Value |
|---|---|
| Active attributes proposed | 14 |
| Used in ADP | 12 |
| Not used in ADP | 2 |
| Categories | Identity, Positioning, Location, Commercial, Meeting / Group, Need Period |
| Missing critical | Total Meeting Space Sq Ft, Meeting Room Count |
| Profile completeness | 2/7 |
| Airtable creates | 0 |
| Airtable updates | 14 |
| Airtable deactivates | 0 |

## Attributes used by ADP

| Attribute | Value | Category | ADP Use Type | Source Type | Confidence |
|---|---|---|---|---|---|
| Hotel Name | YOTEL Geneva Lake | Identity | Prompt Context; Query Generation; Exclusion Logic | HPC | HIGH |
| Brand | YOTEL | Positioning | Prompt Context; Query Generation; Recommendation Context | HPC | HIGH |
| Property Identity Key | yotel_ch_geneva_lake_founex | Identity | Exclusion Logic; Reporting Only | HPC | HIGH |
| Address | Chemin Ballessert 1 | Location | Query Generation; Prompt Context; Exclusion Logic | HPC | HIGH |
| City | Founex | Location | Query Generation; Prompt Context; Exclusion Logic | HPC | HIGH |
| State | Vaud | Location | Query Generation; Prompt Context; Exclusion Logic | HPC | HIGH |
| Postal Code | 1297 | Location | Query Generation; Prompt Context; Exclusion Logic | HPC | HIGH |
| Market | Lake Geneva / La Côte | Location | Query Generation; Prompt Context; Exclusion Logic | HPC | HIGH |
| Submarket | Founex / Nyon corridor | Location | Query Generation; Prompt Context; Exclusion Logic | HPC | HIGH |
| Rooms | 237 | Commercial | Prompt Context; Recommendation Context | Hotel Commercial Profile | MEDIUM |
| Official Events URL | https://www.yotel.com/en/hotels/yotel-geneva-lake/meetings/ | Meeting / Group | Reporting Only; Prompt Context | Hotel Commercial Profile | HIGH |
| Official Property URL | https://www.yotel.com/en/hotels/yotel-geneva-lake | Identity | Reporting Only; Prompt Context | HPC | HIGH |

## Attributes present but not used by ADP

| Attribute | Value | Notes |
|---|---|---|
| Country | Switzerland | Available in Hotel Intelligence; not currently wired into ADP consumption paths. |
| Need Period | NOT_PROVIDED | Present in Hotel Intelligence; ADP does not yet consume seasonality/need periods in live prompts. Hotel-supplied need periods not provided. Public/market seasonality is separate. Not an ADP production blocker; ADP does not consume need periods today. |

## How ADP uses key attributes

- **Hotel Name** = `YOTEL Geneva Lake` → Prompt Context / Query Generation / Exclusion Logic
- **Brand** = `YOTEL` → Prompt Context / Query Generation / Recommendation Context
- **Property Identity Key** = `yotel_ch_geneva_lake_founex` → Exclusion Logic / Reporting Only
- **Address** = `Chemin Ballessert 1` → Query Generation / Prompt Context / Exclusion Logic
- **City** = `Founex` → Query Generation / Prompt Context / Exclusion Logic
- **State** = `Vaud` → Query Generation / Prompt Context / Exclusion Logic
- **Postal Code** = `1297` → Query Generation / Prompt Context / Exclusion Logic
- **Market** = `Lake Geneva / La Côte` → Query Generation / Prompt Context / Exclusion Logic
- **Submarket** = `Founex / Nyon corridor` → Query Generation / Prompt Context / Exclusion Logic
- **Rooms** = `237` → Prompt Context / Recommendation Context
- **Official Events URL** = `https://www.yotel.com/en/hotels/yotel-geneva-lake/meetings/` → Reporting Only / Prompt Context
- **Official Property URL** = `https://www.yotel.com/en/hotels/yotel-geneva-lake` → Reporting Only / Prompt Context
