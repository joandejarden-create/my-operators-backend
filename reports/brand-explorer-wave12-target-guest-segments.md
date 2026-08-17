# Wave 12 — Target Guest Segments

Generated: 2026-07-24T14:49:43.677Z
Validated (no generic_audience_prose risk): **true**

| Slug | Recommended | Write eligible | Will write |
| --- | --- | --- | --- |
| `even-hotels` | Experience-Oriented, Bleisure | true | true |
| `voco-hotels` | Experience-Oriented, Leisure, International Inbound | true | true |
| `avid-hotels` | Bleisure, Leisure | true | true |
| `holiday-inn-express` | Bleisure, Leisure | true | true |
| `courtyard-by-marriott` | Bleisure, Leisure | true | true |
| `ac-hotels-by-marriott` | Experience-Oriented, Bleisure | true | true |
| `city-express-by-marriott` | Bleisure, Leisure, International Inbound | true | true |
| `moxy-hotels` | Experience-Oriented, Leisure | true | true |
| `canopy-by-hilton` | Experience-Oriented, Leisure, International Inbound | true | true |
| `motto-by-hilton` | Experience-Oriented, Leisure | true | true |
| `tempo-by-hilton` | Experience-Oriented, Bleisure | true | true |
| `bunkhouse-hotels` | Experience-Oriented, Leisure | true | true |

## Rule

- Avoid Luxury / Discerning + Leisure (or Experience-Oriented) adjacency.
- Pattern: `/Luxury\s*\/\s*Discerning[,\s]+(?:Experience-Oriented|Leisure)|Leisure Discerning travelers/i`
- Do not write Target Guest Segments when risk is detected.
