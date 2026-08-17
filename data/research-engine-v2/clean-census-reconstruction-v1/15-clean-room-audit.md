# Clean-Room Audit — Wave Pilot Indigo+Kimpton Mexico

| Item | Value |
|------|-------|
| Cohort | Hotel Indigo + Kimpton — Mexico |
| Discovery sources | IHG destination directory (Mexico) |
| Research start | 2026-08-04T22:12:13.164Z |
| Independent hotels discovered | 9 |
| Freeze hash | `ffd264fad567c730cefb2f5461ee4afe6b822a51b41065e58ec4341eb3c8128b` |
| Freeze timestamp | 2026-08-04T22:12:18.611Z |
| Legacy comparison timestamp | 2026-08-04T22:12:18.763Z |
| Legacy reference rows | 16 |
| Matches | 6 |
| Independent-only | 1 |
| Legacy-only | 8 |
| Firewall blocked legacy pre-freeze | true |
| legacy_used_as_source | **false** (all independent claims) |
| Airtable writes | none |
| Webhound | not used |

## Sequencing proof

1. Discovery from official IHG directory only
2. Independent record build + property page enrichment
3. Freeze (`freeze_hash_sha256`)
4. Legacy CSV loaded only after freeze via firewall
5. Legacy-only challenges without adopting legacy field values
