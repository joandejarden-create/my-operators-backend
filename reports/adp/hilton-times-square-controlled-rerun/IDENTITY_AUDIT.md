# Hilton New York Times Square — Identity Audit

## Verdict
- **HILTON IDENTITY VERIFIED:** YES
- **HILTON ALIAS ISSUE FOUND:** YES (proven; fixed in profile entity_v2)

## Canonical identity
| Field | Value |
|---|---|
| Canonical name | Hilton New York Times Square |
| ADP subject | adp_hilton_times_square |
| HPC / Census | rec35fExUxCClpOP6 |
| Brand | Hilton Hotels & Resorts |
| Parent / loyalty | Hilton / Hilton Honors |
| Address / market | Times Square / Midtown West, New York, New York |
| Lat/lng | 40.755343, -73.986047 |
| Official domain | hilton.com |
| Official property URL | https://www.hilton.com/en/hotels/nyctshh-hilton-times-square/ |
| Property code | NYCTSHH |

## Aliases (entity_v2)
- Hilton Times Square
- Hilton New York Times Square Hotel
- Hilton NY Times Square
- Hilton New York Times Square
- NYCTSHH

## Confusable exclusions (do not credit as subject)
- Tempo by Hilton Times Square
- DoubleTree by Hilton Times Square
- Doubletree by Hilton Times Square
- Hilton Garden Inn Times Square
- Hilton Garden Inn New York Times Square Central
- Embassy Suites by Hilton New York Manhattan Times Square
- New York Hilton Midtown
- Hilton Midtown
- Hilton Garden Inn Midtown

## Alias collision check
- **Hilton Midtown** — excluded via confusable list; distinct HPC peer.
- **Tempo / DoubleTree / Garden Inn / Embassy TS** — excluded.
- **No alias collision with Renaissance** — different brand family.

## Bug found (Sept 27 forensic)
Profile previously had **no** `identityAliases`. AI commonly writes **"Hilton Times Square"** while canonical name is **"Hilton New York Times Square"**.
`detectPropertyMention` therefore scored true subject appearances as absent.
Sept 27 reparse with aliases: **7 → 17** mentioned observations (+10 recovered; ChatGPT 0→8 on that period).
