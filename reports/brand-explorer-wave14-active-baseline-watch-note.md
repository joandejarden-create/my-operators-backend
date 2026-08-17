# Wave 14 — Active baseline watch note (read-only)

**Created:** 2026-07-28  
**Scope:** Observation only — **do not patch Accor Wave 13 Active brands in Wave 14 Stage 5**.

## Observation

A fresh `brand-explorer-24-tab-section-quality-audit --dry-run` exited non-zero on selected **Accor Wave 13 Active** brands:

- `mama-shelter`
- `mercure`
- `novotel`
- `pullman`
- `so` / `so-hotels-and-resorts`
- `fairmont` / `fairmont-hotels-and-resorts`

## Post–Wave 14 Stage 5 update (2026-07-28)

`npm run test:brand-explorer-46-active-public-full-baseline` now **FAILS** on the same Accor set (plus aggregate `quality_freeze_count:40_expected_46` / `pvql_public_full_count:40_expected_46`).

This is **not** caused by Wave 14 Stage 5 image writes (nine Marriott Under Review brands only; no Accor / protected-46 Presentation writes). Treat as the same open Accor active-baseline reconciliation item.

PVQL `--public-full-only` still reports `overallPass=true` for the public-full cohort it scans (40).

## Guardrails already confirmed

- Protected **46** Active/Live public-full baseline regression still **PASS** after Wave 14 Stage 4.
- Wave 14 Stage 4 Presentation writes did **not** touch these Accor brands.
- Wave 14 Stage 5 image materialization must **not** write Accor Wave 13 Active brands, House of Originals, Morgans Originals, Radisson Collection, or protected-46 brands.

## Recommended later action

If the Accor 24-tab remediation signal persists after Wave 14 Stage 5 / post-image cleanup:

1. Run a **separate** active-baseline quality reconciliation (outside Wave 14 factory apply).
2. Reconcile slug aliases (`so` ↔ `so-hotels-and-resorts`, `fairmont` ↔ `fairmont-hotels-and-resorts`) in quality audit indexing if needed.
3. Re-freeze or refresh quality artifacts only after Accor findings are classified (true quality gap vs report/alias noise).

## Explicit non-actions for Wave 14 Stage 5

- No Accor Presentation / Image / Brand Status / release / CV / Source / Registry writes.
- No protected-46 brand writes.
- Do not block Wave 14 Marriott image materialization on this Accor watch item.

## Accor protected-46 reconciliation (2026-07-28)

Root cause: Wave 13 Accor identity fell out of factory-preview maps (Wave 14-only) →
`resolveActiveUniverseRecordId` null → slug-as-name `brand_not_found` → quality freeze 40/46.

Secondary (Mercure only, unmasked after resolve): Airtable CDN path hashes falsely matched Accor DAM `wd*` → role-match fail → `draft_applied_with_defects`. Fixed in `brand-explorer-image-role-match.js` (code-only).

- Fix: durable `brand-explorer-wave13-active-identity-anchors.js` + active-universe alias resolve + Accor CDN detector guard
- Airtable / Presentation / Wave 14 writes: **none**
- Ready state: **protected_46_accor_baseline_reconciled_wave14_may_resume**
- Wave 14 post-image cleanup may resume: **true**
- Validated: protected-46 baseline PASS · PVQL public-full 46/46 · quality approve 46/46 · footnote enriched no `brand_not_found` · evidence PASS · mandatory gates PASS

Artifacts: `reports/brand-explorer-46-accor-baseline-reconciliation.json`, `reports/brand-explorer-46-accor-baseline-reconciliation-failures.json`, `docs/data-intelligence/brand-explorer-46-accor-baseline-reconciliation.md`.

