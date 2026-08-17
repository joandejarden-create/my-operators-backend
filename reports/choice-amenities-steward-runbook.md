# Choice amenities steward runbook

Human path to fill blank Hotel Census **Amenities** for Choice CALA properties when automated fetch returns **403 / Akamai**.

## Hard rules

- Do **not** invent amenity lists.
- Do **not** claim live automated Choice property fetches work while they still 403.
- Apply writes **fill-blank Amenities only** (skips rows that already have Amenities).
- Labels must come from saved property HTML with amenity markers (`amenityFeature` / amenit* DOM).

## Field map

| Airtable (Hotel Census) | Source |
| --- | --- |
| `Amenities` | Parsed HTML labels (`MAP_CHOICE_AMENITIES_HTML_APPLY.amenities`) |
| Match via `Property ID` / `Website` | Filename `{propertyId}.html` |

## Pilot (~10–15 rows)

1. Refresh worklist + opener:

```bash
node scripts/export-choice-amenities-pilot-worklist.mjs
node scripts/generate-choice-amenity-pilot-opener.mjs
```

2. Open `reports/choice-amenity-pilot-opener.html` in a browser.
3. For each row: open the property page → wait for amenities → **Ctrl+S → Webpage, Complete** → save as:

`reports/choice-amenity-html/{propertyId}.html`

(example: `reports/choice-amenity-html/mx077.html`)

4. Dry-run, then apply:

```bash
node scripts/apply-choice-amenities-from-html.mjs
node scripts/apply-choice-amenities-from-html.mjs --apply
```

Single file:

```bash
node scripts/apply-choice-amenities-from-html.mjs --file reports/choice-amenity-html/mx077.html
node scripts/apply-choice-amenities-from-html.mjs --apply --file reports/choice-amenity-html/mx077.html
```

## Full steward pool

```bash
node scripts/export-choice-amenities-steward-worklist.mjs
```

→ `reports/choice-amenities-steward-worklist.csv` (~178 blank-Amenities Choice rows with Website).

## CLI states (apply script)

| State | Meaning |
| --- | --- |
| `loading` | Reading HTML / census |
| `empty` | No HTML files in `reports/choice-amenity-html/` |
| `error` | Blocked shell, missing amenity markers, no census match, API failure |
| `success` | Valid parse + blank Amenities (dry-run preview or write) |

## Failure modes

- **403 / Access Denied HTML** — re-save from a normal browser; do not invent.
- **Missing amenity markers** — page incomplete or wrong save type; wait for amenities, save Webpage Complete.
- **Amenities already populated** — skipped (fill-blank policy).

## Optional: Wayback harvest (pilot)

When live Choice pages 403, official `web.archive.org` snapshots of the same property URLs often still embed the property `amenities` JSON. Parser reads those labels (never invents).

```bash
node scripts/harvest-choice-amenity-wayback-pilot.mjs
node scripts/retry-choice-amenity-wayback-misses.mjs
node scripts/apply-choice-amenities-from-html.mjs
node scripts/apply-choice-amenities-from-html.mjs --apply
```

If a snapshot is an empty SPA shell (no property amenities payload), fall back to manual save above.

## Notes

- Affiliation normalize is a separate path — this script only writes `Amenities`.
- Apply log: `reports/choice-amenities-html-applies.csv` (created on successful `--apply`).
- Ignore `_*.html` debug files in `reports/choice-amenity-html/`.
- Pilot + full steward (2026-07-23): Wayback harvest applied **~107** Choice Amenities fills cumulative (`reports/choice-amenities-html-applies.csv`). Remaining blanks: refresh `reports/choice-amenities-steward-worklist.csv` (~71). Misses are mostly no usable archive amenity payload.
