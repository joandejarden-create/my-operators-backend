# Choice Mexico — Source Extraction

## Primary discovery

Live Choice Mexico regional page (JSON-LD Hotel nodes + embedded hotel cards):
`https://www.choicehotels.com/en-uk/mexico/regional-hotels?placeId=ChIJU1NoiDs6BIQREZgJa760ZO0`

| Signal | Coverage |
|--------|----------|
| Regional LD hotels | 50 |
| Rich hotel cards | 50 |
| Sitemap MX* union added | 18 |
| Final independent universe | 68 |

## Structured fields from regional HTML

| Field | Hotels with value |
|-------|-------------------|
| Latitude | 50 |
| Longitude | 50 |
| Address 1 | 50 |
| Amenities (amenityGroups) | 50 |
| Restaurant (Y/N) | 50 |
| Conference (Y/N) | 35 |
| Spa (Y/N) | 44 |
| Rooms | 0 |
| Open Date | 0 |
| Management Company | 0 |

## Property pages

Default: **not fetched** (403 risk). Set `RE_V2_CHOICE_PROPERTY_PAGES=1` to attempt.
Blocked ≠ closed / reflagged / missing.

## Core / material

- Core: **97%**
- Material: **56%**
