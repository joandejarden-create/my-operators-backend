# Brand Explorer Wave 12 — Factory Preview Cohort

Generated: 2026-07-24
Dry-run plan applied as **code-only** update to `lib/partner-intelligence/brand-explorer-factory-preview-candidates.js`
Airtable writes: **false**
Brand Status writes: **false**
Release field writes: **false**
Protected 27 baseline writes: **false**

Display state: `factory_preview_internal`  
Banner: Factory Preview — Not Public / Not Active Baseline

## Candidates (12)

| Slug | Name | Record ID |
| --- | --- | --- |
| `even-hotels` | Even Hotels | `recvvmiyReHhiKdoK` |
| `voco-hotels` | Voco Hotels | `recwONQTqGU1jHCsM` |
| `avid-hotels` | avid hotels | `recoEarnE8T6sDjZq` |
| `holiday-inn-express` | Holiday Inn Express | `recmGmiIqDtAsm01f` |
| `courtyard-by-marriott` | Courtyard by Marriott | `rec6hye5H8zJmAGv3` |
| `ac-hotels-by-marriott` | AC Hotels by Marriott | `rec9aZp7GHtzUEg0c` |
| `city-express-by-marriott` | City Express by Marriott | `recucEzAS6724tOYA` |
| `moxy-hotels` | Moxy Hotels | `recahVIW4aCx0Ao84` |
| `canopy-by-hilton` | Canopy by Hilton | `recsggfbKlJbjeRP9` |
| `motto-by-hilton` | Motto by Hilton | `reclt44apoi8co0e6` |
| `tempo-by-hilton` | Tempo by Hilton | `recqiHq3GHKMj8Meo` |
| `bunkhouse-hotels` | Bunkhouse Hotels | `recGv268Wda31PlSZ` |

## Notes

- Replaces prior factory preview cohort (Tapestry / Dazzler / Trademark), which are now in the protected 27 Active/Live public-full baseline.
- All 12 are Brand Status **Under Review** — not Active/Live.
- Presentation row counts are currently **0** — Stage 4 tab-factory-build required before useful visual preview.
- Preview URLs: `/brand-explorer-combined.html?brandId={recordId}&beInternalPreview=1&factoryPreview=1`

## Commands

```bash
npm run brand-explorer-wave12-factory -- --stage factory-preview-cohort --dry-run
npm run test:brand-explorer-factory-preview-mode
npm run test:brand-explorer-27-active-public-full-baseline
```
