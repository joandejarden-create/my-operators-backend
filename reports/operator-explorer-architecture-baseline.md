# Operator Explorer — Architecture Baseline

**Mode:** Audit only (no Airtable writes, no scoring changes)  
**Date:** 2026-08-09  
**Branch:** `app-shell-left-nav`  
**Commit:** `3c88c0b4e22a35052e450d00c5e2f1b9e417c040` (`3c88c0b`)  
**Live schema dump:** `reports/operator-explorer-architecture-live-schema-dump.json`

---

## Airtable bases used by operator/company functionality

| Base | Env | Role |
| ---- | --- | ---- |
| Deal Capture Platform (`appvtnDurnMSjINP6`) | `AIRTABLE_BASE_ID` | **Primary** — Operator Setup, Operator Intelligence, Operator Fit Shortlist, PI Source Library, Deals, Company Profile |
| Platform / GTM alt | `AIRTABLE_BASE_ID_ALT` / `AIRTABLE_GTM_BASE_ID` | GTM/outreach — not Operator Explorer SoT |

---

## Operator-related environment variables (from `.env.example`)

| Variable | Default / note |
| -------- | -------------- |
| `AIRTABLE_OPERATOR_SETUP_MASTER_TABLE` | `Operator Setup - Master` |
| `AIRTABLE_OPERATOR_COMPANY_NAME_FIELD` | `company_name` |
| `AIRTABLE_OPERATOR_SETUP_SUBMISSION_STATUS_FIELD` | `submission_status` |
| `AIRTABLE_OPERATOR_SETUP_ACTIVE_STATUS_VALUES` | `Active` |
| `AIRTABLE_TABLE_OPERATOR_DEAL_REQUESTS` | `Operator Deal Requests` |
| `AIRTABLE_ME_USERS_OPERATOR_SETUP_LINK` | `Operator Setup - Master` |
| `OPERATOR_FIT_ENGINE_V2` | `0` (OFF) |
| `OPERATOR_FIT_ENGINE_V2_SHADOW` | off |
| `OPERATOR_FIT_DIFFERENTIATION_V21` | `0` |
| `OPERATOR_FIT_INTERNAL_PILOT` | `0` (OFF) |
| `OPERATOR_FIT_INTERNAL_PILOT_ALLOW_PRODUCTION` | off |
| `OPERATOR_SETUP_USE_NEW_BASE_WRITER` | `0` (legacy writer primary) |
| `OPERATOR_SETUP_NEW_BASE_SHADOW_WRITE` | `0` |
| `OPERATOR_EXPLORER_HIDE_TEST_RECORDS` | optional hide |
| `ENABLE_OWNER_OPERATOR_WRITES` | `0` |
| `PARTNER_INTELLIGENCE_*_TABLE_ID` | Source / Facts / Published / Helena |

---

## Current Operator Fit version

- **Engine:** Operator Fit Engine **v2** (+ optional **v2.1 differentiation** flag)
- **Legacy:** OAS (Operator Alignment Scoring) remains parallel; Fit v2 default **OFF**
- **Owner pilot:** **Disabled** (`OPERATOR_FIT_ENGINE_V2=0`, `OPERATOR_FIT_INTERNAL_PILOT=0`)
- **Shortlist table:** `Operator Fit - Shortlist` (`tbl4D5DCK7oPFhi98`) — internal pilot store

---

## Current Brand Explorer research architecture (summary)

Wave/factory orchestration with dry-run → apply → post-write gates:

- OS state machine: `lib/partner-intelligence/brand-explorer-os-*.js`
- Tab Factory + PVQL / protected baseline freezes
- PI Source Library + Extracted Facts + Published Explorer Fields
- Per-brand source packs / content writers (brand-specific)
- Image uniqueness / Scene7 / gallery selection (brand-specific)
- `npm run brand-explorer-os`, wave factories, baseline freeze tests

Full reuse analysis: `reports/operator-explorer-brand-research-reuse-audit.md`

---

## Current research scripts (operator intelligence / explorer)

| Area | Scripts / npm |
| ---- | ------------- |
| Operator Intelligence calibration | `operator-intelligence-calibration-*`, `operator-intelligence-airtable-apply-calibration` |
| Wave 2/3 | `operator-intelligence-wave-2-evaluate`, `seed-operator-intelligence-wave-*` |
| Market Presence | `lib/operator-intelligence/market-presence.js` + apply scripts |
| Operator Explorer OS / gates | `operator-explorer-os`, `test:operator-explorer-*` |
| Tab Factory | `operator-explorer-factory-init`, `operator-explorer-factory-content-materialize`, tab-factory audits |
| Fit readiness | `operator-fit-data-readiness`, `test:operator-fit-v2`, shortlist migrate |
| Readonly schema audit | `scripts/audit-operator-fit-airtable-readonly.mjs` |

---

## Operator-related internal routes / UI

| Surface | Path / module |
| ------- | ------------- |
| Explorer list | `public/operator-explorer.html`, `public/js/operator-explorer.js` |
| Detail / gold | `public/operator-explorer-detail.html`, `operator-explorer-gold-mock.html`, `operator-explorer-new-base-profile.js` |
| APIs | Operator setup writer/read, company profile, match-score / Fit (flagged), brand-library parallel patterns |
| Lib | `lib/partner-intelligence/operator-explorer-*`, `lib/operator-fit/*`, `lib/operator-intelligence/*` |

---

## Known operator-related tables (live)

26 inventoried tables — see `reports/operator-explorer-airtable-table-inventory.md`.

Core:

- `Operator Setup - Master` (`tbl4YPJ3XhnYLHLsD`) — **canonical Operator Master**
- Child Operator Setup tables (Profile, Platform, Commercial, Governance, Case Studies, presentation children)
- `Operator Intelligence - Claims` (`tblZE18CKPISe1Dcs`)
- `Operator Intelligence - Market Presence` (`tblrFqjMNGzxzbZnu`)
- `Operator Fit - Shortlist` (`tbl4D5DCK7oPFhi98`)
- PI Source Library / Facts / Published
- `Operator Deal Requests`
- `Company Profile` / `Companies` (platform/outreach — not Explorer SoT)

---

## Known source / evidence tables

- `Partner Intelligence - Source Library`
- `Partner Intelligence - Extracted Facts`
- `Partner Intelligence - Published Explorer Fields`
- `Operator Intelligence - Claims` (claim spine; Source URLs field; PI links on Master)
- Local calibration JSON under `data/operator-intelligence/calibration-cohort/` (historical)

---

## Known beta / test fixtures

- Code fixtures: `fixtures/operator-*-arbor-cala.json`, `fixtures/operator-*-he-cala.json`, factory slug fixtures
- Airtable dummy/demo Masters (In Review): Antillano Norte + 8 Spanish synthetic names — see universe audit
- Gold mock UI: `public/operator-explorer-gold-mock.html`

---

## Existing backups (relevant)

- `reports/operator-intelligence-airtable-backup-manifest.md`
- Calibration apply / post-write validation JSON under `reports/operator-intelligence-*`
- Shortlist file fallback: `data/operator-fit/shortlist-store.json`

---

## Production-protected modules

| Module | Protection |
| ------ | ---------- |
| Operator Explorer quality baseline | Arbor `recF5Z87OAqFgndoq` + Hotel Equities `recWPKu5laVZxsvpn` — `lib/partner-intelligence/operator-explorer-quality-baseline.js` |
| Brand Explorer Active/Live public-full baseline | Separate brand universe; do not import Brand Status freeze into operators |
| Company Validated / do-not-overwrite | PI governance — never auto-overwrite validated company data |
| Operator Fit scoring | Flags OFF; do not change scoring in this audit phase |
| Owner writes | `ENABLE_OWNER_OPERATOR_WRITES=0` |

---

## Live volume snapshot (2026-08-09)

| Object | Count |
| ------ | ----: |
| Operator Setup Masters | 36 |
| Claims | 28 |
| Market Presence rows | 42 |
| Case Studies | 58 |
| Brand Relationships (presentation rows) | 73 |
| Shortlist rows | 10 |
| Operator-related fields audited | 886 |
| Operator-related tables | 26 |
