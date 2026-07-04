# Radisson Blu (Choice) — Brand Explorer fixtures

All fixtures target **Brand Basics** name `Radisson Blu (Choice)` (`recWPEvxBQxVVzSq3`). CALA hotel examples use:

- Radisson Blu Bariloche  
- Radisson Blu Aruba  
- Radisson Blu Belo Horizonte, Savassi  
- Radisson Blu São Paulo  
- Radisson Blu Plaza El Bosque Santiago  

## Fixture files

| File | Tabs / slots |
|------|----------------|
| `brand-explorer-presentation-radisson-blu.example.json` | Hero, Overview, Operations, Value to Owners, Loyalty, Insight |
| `brand-explorer-presentation-standards-radisson-blu.json` | Standards |
| `brand-explorer-presentation-radisson-blu-materials.json` | Official materials (`materials.file`) |
| `brand-explorer-presentation-economics-radisson-blu.json` | Economics (v2 + legacy KPI slots) |
| `brand-explorer-presentation-radisson-blu-case-studies.json` | Case studies (5 CALA Blu hotels) |
| `brand-explorer-presentation-radisson-blu-gallery.json` | Image gallery shells (`materials.gallery.1`–`6`) |
| `brand-explorer-presentation-radisson-blu-footprint-openings.json` | Openings / Examples / Properties |
| `brand-explorer-presentation-radisson-blu-footprint-momentum.json` | Recent momentum timeline |
| `brand-explorer-presentation-radisson-blu-footprint-geo-growth.json` | Footprint geo, regions, growth, editorial |
| `brand-explorer-presentation-radisson-blu-footprint.json` | `footprint.geo.summary`, `footprint.growth.narrative` |
| `brand-explorer-presentation-radisson-blu-portfolio-compliance-similar.json` | Portfolio mix, compliance, similar brands |

Generated economics/portfolio/geo/momentum/gallery: `npm run build-radisson-blu-fixtures`

## Push to Airtable

**Do not** use `npm run apply-brand-explorer-presentation -- --brand-name "Radisson Blu (Choice)"` on Windows—the shell may resolve the name to `Radisson` only. Use:

```bash
npm run build-radisson-blu-fixtures
npm run apply-radisson-blu-choice-fixtures
```

(`apply-radisson-blu-choice-all-fixtures.mjs` calls Node with `--brand-record-id recWPEvxBQxVVzSq3`.)

Single prefix example:

```bash
node scripts/apply-brand-explorer-presentation-fixture.mjs --brand-record-id recWPEvxBQxVVzSq3 --fixture fixtures/brand-explorer-presentation-radisson-blu-case-studies.json --replace-slot-prefix materials.caseStudy
```

Attach **Image** on gallery and opening rows in Airtable when photos are available.

If Blu fixtures were ever applied to core **Radisson** by mistake (Windows/npm brand-name parsing), remove leaked rows:

```bash
npm run purge-radisson-blu-leak -- --dry-run
npm run purge-radisson-blu-leak
```
