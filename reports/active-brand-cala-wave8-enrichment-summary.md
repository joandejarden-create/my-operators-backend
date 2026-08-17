# Active Brand CALA — Wave 8 enrichment summary

**Generated:** 2026-07-24  
**Prior:** Waves 1–7  
**Scope:** Execute steward queue — Choice Wayback pass + multi-brand opener HTML packs  
**Rule:** Official sources only · fill-blank · Affiliation = Brand Setup `Brand Name`

## Verdict

Wave 8 is an **execution / ops pack**, not a coverage lift. Wayback cannot replace browser saves for the remaining Choice amenity gaps (**0/40** usable archives). Human steward openers are ready.

Coverage unchanged: `reports/active-brand-cala-enrichment-coverage-wave8-after.csv`

## What shipped

### Choice steward pack
| Artifact | Notes |
|----------|-------|
| `reports/choice-wave8-steward-opener.html` | Click-to-open table for **61** Choice browser-save residuals |
| `reports/choice-wave8-steward-worklist.csv` | Same list + `hasUsableHtml` + save path |
| `scripts/run-wave8-choice-steward-pass.mjs` | Build opener → optional Wayback → apply |

**Choice browser-save:** 61 rows · **56** missing usable HTML · **5** already have HTML (already applied in prior waves)

### Wayback harvest (Wave 8)
- Attempted **40** missing Choice PIDs
- **Saved/reused with amenity markers: 0**
- Typical outcomes: no CDX hit, or archive HTML with `markers=false` (Akamai/shell pages)
- Apply: **0**

### Non-Choice opener packs
| Group | Rows | Opener |
|-------|-----:|--------|
| BWH Premier/Signature | 10 | `reports/wave8-bwh-steward-opener.html` |
| Wyndham Dazzler/Trademark | 17 | `reports/wave8-wyndham-steward-opener.html` |
| Marriott Autograph/Tribute/Design | 10 | `reports/wave8-marriott-steward-opener.html` |
| IHG Kimpton/Indigo/Vignette | 4 | `reports/wave8-ihg-steward-opener.html` |
| Hilton Curio/Tapestry | 2 | `reports/wave8-hilton-steward-opener.html` |
| Other (SLH residual) | 1 | `reports/wave8-other-steward-opener.html` |

Script: `scripts/generate-wave8-nonchoice-steward-openers.mjs`

### Fix carried in this wave
- Corrected `writeCsv(file, rows)` argument order in steward exporters (Wave 7 CSV had been corrupted as `[object Object]` headers). Re-exported `active-brand-cala-steward-worklist-wave7.csv`.

## How to execute next (human)

1. Open `reports/choice-wave8-steward-opener.html`
2. For each row with HTML?=no: Open → Save as Webpage Complete → path in last column
3. `node scripts/backfill-choice-wave4-from-html.mjs` (dry-run) then `--apply`
4. Repeat with BWH / Wyndham / Marriott openers as capacity allows

## Change impact

**Low** for Airtable (0 writes). **Medium** for steward ops tooling.

**Rollback:** N/A

## Manual QA

- [ ] Choice opener opens valid choicehotels.com URLs
- [ ] After saving 3–5 HTML files, dry-run Choice apply shows Ready > 0
- [ ] Wave 7 CSV opens cleanly in Excel (headers not `[object Object]`)
