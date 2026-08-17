# Operator Intelligence — Airtable Backup Manifest

Timestamp: 2026-08-03T22-17-55-690Z
Path: `C:/Dev/deal-capture-proxy/backups/operator-intelligence/2026-08-03T22-17-55-690Z`

| Operator | Master ID | Platform ID | Commercial ID | Checksum |
| -------- | --------- | ----------- | ------------- | -------- |
| Arbor Lodging (CALA) | recF5Z87OAqFgndoq | rec5xtfsucKI4oNTT | reculd8i9bSPy5qud | 7007114a625bb75c |
| Cenote Azul Operadores | recQ6Cf8O2z0tiqBz | rec2SuuTjB265DwW8 | rec0UQHrreczSpmjT | 63644b15addcc0ad |
| Hotel Equities (CALA) | recWPKu5laVZxsvpn | recUZmUXShe8fxMNB | recamEubuHaoFEaeW | 9a89b7ce41f876e9 |
| GHL Hoteles (GHL Holding) | reciI2tYQBfMoMK9G | recbY3IGCh2LZQ3Mi | recG13bhcYsA4ttiS | 0f4d8af316bff609 |
| Playa Hotels & Resorts | rec3TUHT9Z4AnFp5P | recFC6Oc5RFKVk6Un | recN3i0xlZMV8e0np | 29b820257c0310cf |
| Aimbridge Hospitality (LATAM) | recGWxIJqnYHkJZFD | recxlOa0pz187Br9H | rechQXIYfq4WGxsLd | 089b956e8bcd30b0 |

## Restoration

PATCH Platform/Commercial records with `fields` from each `{operatorId}.json` snapshot for Active Countries / Management Structures Supported.

## Limitations

Meta schema snapshot is names-only; newly created Claims rows require separate delete if rolling back creates.
