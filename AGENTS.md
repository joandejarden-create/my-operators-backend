# Dealality — project memory for agents

> **Last updated:** 2026-07-05  
> **Keep this file under ~150 lines.** Process rules live in `.cursor/rules/deal-capture-implementation-partner.mdc`.  
> **Schema authority:** `docs/*-airtable-fields.md` and `.env.example` — never invent field names or select options.

## What this repo is

- **Product:** Dealality — confidential hotel deal-flow platform (Webflow front-end + Node proxy + Airtable).
- **Stack:** `server.js` API routes in `api/`, static UI in `public/`, scripts in `scripts/`, schema docs in `docs/`.
- **Two Airtable bases:** main product base (`AIRTABLE_BASE_ID`) and platform/GTM base (`AIRTABLE_BASE_ID_ALT`, GTM `AIRTABLE_GTM_BASE_ID=appKZuK006BWIVjNW`).

## Non-negotiables

1. **Never assume Airtable schema** — read the matching `docs/*-airtable-fields.md` before reads/writes.
2. **Validate before every write** — required fields, types, allowed select options, linked-record IDs.
3. **Dry-run first** — any script with `--apply` or live upsert: run `--dry-run` (or `DRY_RUN=true`) and review output before apply.
4. **Central field maps** — use existing mapping objects (`map_*`, `api_*`); do not scatter raw Airtable field names in UI code.
5. **UI states** — loading, empty, error, success for all data-driven UI; no blank components.
6. **No silent catches** — log or rethrow; never empty `catch () {}`.

## Repo layout (where things live)

| Area | Path | Notes |
|------|------|--------|
| API routes | `api/*.js` | Auth via Memberstack; check `test:batch*-route-auth` when touching routes |
| Operator Setup UI | `public/third-party-operator-setup-new-two.html` | 13-tab intake; 417 form fields |
| Operator Setup writer | `api/lib/operator-setup-new-base-writer.js` | New-base writer; bindings in `api/lib/third-party-operator-new-two-field-bindings.json` |
| Operator Explorer | `public/js/operator-explorer*.js` | Reads new-base; gold profiles keyed by Master `rec…` |
| Brand Explorer | `public/js/brand-explorer*.js`, `api/brand-library.js` | Presentation rows in Airtable + `fixtures/brand-explorer-presentation-*` |
| GTM / pilot | `api/target-list.js`, `api/outreach-setup.js`, scripts `*gtm*`, `*pilot-target*` | GTM base only; never expose CoStar data product-facing |
| Master To-Do | `lib/dealality-master-todo/`, `docs/dealality-master-todo.md` | Upserts to Founder Project Plan table |
| Radar / CALA | `lib/travel-infrastructure/`, `lib/radar-buildout/` | TI audits → `data/*-travel-infrastructure-audit.json` |
| Automations | `airtable/automations/` | Paste into Airtable; not executed from Node |

## Operator Setup — writer flags (`.env`)

- `OPERATOR_SETUP_USE_NEW_BASE_WRITER=0` — **legacy writer is primary** by default.
- `OPERATOR_SETUP_NEW_BASE_SHADOW_WRITE=0` — parallel shadow writes off unless explicitly enabled.
- Field-mapping audit: `docs/operator-setup-to-explorer-field-mapping-audit.md` (477 rows; 266 still static-form-only).
- Before operator save-path changes: run `npm run test:operator-setup-new-base-save-coverage` if available.

## Brand Explorer — fixtures and audits

- Presentation patches: `fixtures/brand-explorer-presentation-*.json`
- Apply: `npm run apply-brand-explorer-presentation` (and Choice batch scripts)
- **Before PR touching fixtures or brand presentation:** `npm run audit-choice-explorer-presentation-gaps`
- Format audit: `npm run audit-brand-explorer-presentation-formats`
- Gap report output: `docs/choice-explorer-presentation-gap-audit.md`

## CALA / Travel Infrastructure backfills

- Audit (read-only): `npm run audit:<country>-ti` or `node scripts/audit-market-travel-infrastructure.mjs --country … --market … --output data/…`
- Backfills: always `--dry-run` first; outputs under `data/`
- Submarket options: `npm run ensure:radar-submarket-options` with `--dry-run` before apply

## GTM / Master To-Do

- Structure audit: `node scripts/audit-dealality-master-todo-structure.mjs --dry-run`
- Upsert: `node scripts/upsert-dealality-master-todo.mjs --dry-run` before apply
- Seed test: `npm run test:dealality-master-todo`
- Daily views setup: `reports/founder-project-plan-daily-views-manual.md`

## Hotel Census geography (product)

- **Dealality-defined geography is authoritative** for product UI (Scout, property profiles, Radar). Do **not** recommend STR Market/Submarket as the enrichment target.
- **`Market`** = Dealality commercial market (e.g. `Puerto Rico`, `Greater Santo Domingo`) from `lib/radar-buildout/country-configs.js` / steward assignment.
- **`Submarket`** = Dealality corridor label (e.g. `East Coast / Island Access`) via `lib/hotel-census/census-dealality-submarket.js` — replaces legacy STR `*Regional` buckets.
- STR Excel import paths for Market/Submarket are **legacy reference only**; corridor backfill: `scripts/plan-hotel-census-dealality-submarket.mjs`.

## Common mistakes (append when you fix one)

- Treating STR Market/Submarket as product geography — use Dealality Market + corridor Submarket instead.
- Assuming new-base operator writer is primary — it is **not** unless `OPERATOR_SETUP_USE_NEW_BASE_WRITER=1`.
- Inventing Airtable select options — options must exist in schema doc or be created via `ensure-*` scripts first.
- Running GTM CoStar import/apply without dry-run — always preview JSON/CSV first.
- Patching brand explorer fixtures without running gap audit afterward.
- Using `Done` for master todo status — Airtable uses **`Completed`** (see `docs/dealality-master-todo.md`).
- TI audits fall back to Cancún keywords only for **Mexico** — other countries need config in `lib/radar-buildout/market-travel-infrastructure-audit-configs.js`.

## PR checklist

See **`docs/dealality-pr-validation-matrix.md`** — run the scripts for your changed paths and note risk tier before merge.

```bash
npm run dealality:pr-check-suggest
```

## Parallel / overnight work

See **`docs/dealality-parallel-dry-run-experiment.md`** — start with read-only audits; no live Airtable writes in parallel until validation pipeline is trusted.

## Planning multi-option UX

Use Canvas or HTML mockups (see `engagements/*/`) before coding Webflow/JS when there are 3+ viable UX options. Schema/data tasks use markdown + dry-run reports, not mockups.
