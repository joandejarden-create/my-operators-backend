# Wave 13 — SO/ Section Pattern Cleanup

SO/ Presentation-only micro cleanup after public release so PVQL / 24-tab can pass across the **46** Active/Live universe.

## Scope

- Brand: `so-hotels-and-resorts` (`recTJdPlr4mDs9app`)
- Presentation Brand Name: **SO/ Hotels & Resorts**
- Basics display name: **SO/**

## Fixes applied

1. **Recent Momentum** — replaced undated `Directory` / `Collection` cards with dated cards:
   - `Mar 2026` — Accor Brandbook fashion-rooted luxury lifestyle collection
   - `2025` — SO/ Paris fashion/art lifestyle flagship (International Reference)
2. **MEA region** — empty panel suppressed (`Active=false` + `Do Not Display`); no invented MEA inventory
3. **Growth priorities** — brand-specific themes + editorial/fit for selective luxury lifestyle hotels and resorts (distinct from Mama Shelter / Fairmont / MGallery / generic Accor)
4. **Geo intro + regions** — refreshed brand-specific copy; EU / APAC / AM / CALA retained

## Post-apply validation

| Gate | Result |
| --- | --- |
| Tab-factory (SO/) | PASS |
| Completeness / no-empty / golden / evidence / images | PASS |
| PVQL public-full-only | **46/46 PASS** |
| 24-tab quality | **46/46 `approve_for_baseline_freeze`** |
| Mandatory release gates | PASS |
| Active universe SoT | **46**, SO/ `public_full_clean` |

## Guardrails

- No Brand Status / release field / CV / Source Library / Registry / restore registry / image writes
- Active 45 brands untouched
- House of Originals / Morgans Originals / Radisson Collection / Wave 14 untouched

## Ready statement

`wave13_so_section_pattern_clean_ready_for_46_baseline_freeze`

## Next

Freeze protected **46** Active/Live public-full baseline (separate task).

## Commands

```bash
npm run brand-explorer-wave13-factory -- --stage so-section-pattern-cleanup --dry-run
npm run brand-explorer-wave13-factory -- --stage so-section-pattern-cleanup --apply ...confirm flags...
```
