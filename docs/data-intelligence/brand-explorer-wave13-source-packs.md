# Brand Explorer — Wave 13 Official Source Packs

Version: `wave13-source-packs-v1`

## Purpose

Stage 3 of the Wave 13 factory builds **official source packs** for 8 Accor / Accor-adjacent brands before any tab-factory content generation or SO/ Brand Basics creation.

## Source hierarchy

1. Brand-specific official brand page
2. Brand-specific development page where available
3. Official property pages (match by **property name**, not array index)
4. Official parent-company pages — **parent/platform context only**
5. Credible announcement / opening / development sources
6. Image sources tied to official brand or property pages

## Geography labels

- **CALA** — Caribbean & Latin America examples preferred when available
- **International Reference** — non-CALA examples explicitly labeled

## Gate

Source-packs runs when:

- Wave 13 preflight = PASS / `protected_39_live_clean_wave13_may_resume`
- Wave 13 manifest = ready / `mayProceedToFactoryPreviewCohort = true`
- Factory-preview-cohort = applied or dry-run ready (soft warning if not)

Accept same-session reports, or `--reuse-fresh-reports` when Wave 13 reports still pass **and** live PVQL + quality audits are clean and current. Never proceed on stale cached PVQL alone.

## Open items retained

- **SO/ Hotels & Resorts** — missing Brand Basics; creation recommendation documented; not created in Stage 3
- **Fairmont** — Brand Basics name `Fairmont` vs consumer `Fairmont Hotels & Resorts`; documented; not renamed
- **The House of Originals** — likely superseded by **Morgans Originals**; founder/manual review before Stage 4

## Target Guest Segments

Do not combine Luxury / Discerning with Leisure (or Experience-Oriented adjacency that renders as generic audience prose). Prefer brand-specific Bleisure / Experience-Oriented / Leisure / International Inbound only when source-supported.

## Command

```bash
npm run brand-explorer-wave13-factory -- --stage source-packs --dry-run
npm run brand-explorer-wave13-factory -- --stage source-packs --dry-run --reuse-fresh-reports
```

## Outputs

- `reports/brand-explorer-wave13-source-pack-<slug>.md` (8 files)
- `reports/brand-explorer-wave13-source-pack-summary.{json,md}`
- This doc

## Next stage

1. Separate SO/ Brand Basics creation (recommended values in SO pack)
2. Founder review for House of Originals vs Morgans Originals
3. Stage 4 — `tab-factory-build` for brands that clear open items

