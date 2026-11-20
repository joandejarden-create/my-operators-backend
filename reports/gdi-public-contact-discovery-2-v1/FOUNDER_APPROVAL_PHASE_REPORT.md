# Public Contact Discovery 2.0 — Founder approval phase report

**Status:** STOP for founder review before full-database backfill  
**PR:** [#475](https://github.com/joandejarden-create/my-operators-backend/pull/475)  
**Date:** 2026-11-20

## 1. Two-case Airtable write / read-back

| Contact | Record | Email | Phone | Certified |
|---------|--------|-------|-------|-----------|
| Maritza Jeanette Medina García | `recOCOXEyeOBJ629G` | maritza.medina@jalisco.gob.mx | +52 33 3819 2333 (office + ext) | YES |
| Alejandra Vázquez | `recJwAz4jhuAjA5cY` | alejandra.vazquez@igeco.mx | none (not invented) | YES |
| Paulina Sánchez (secondary on Alejandra row) | same | paulina.sanchez@igeco.mx | none | YES |

Evidence: `persist/APPLY_READBACK.json` · `allCertified: true` · Surfe: **0**

## 2. Publication refresh

- Restaged Sheraton STAGED package only (eligibility unchanged; CURRENT not promoted — none existed).
- Version: `gdi_pub_recCAGqkERSXgiku4_20261120_public_contact_channels_v1`
- Fingerprint: `33620bef36ec23914cf2ac56f5b2b3ae5f9fa628aed081f98891aba3145e62c9`
- Patched: Maritza (email+phone, Surfe CTA none) · Alejandra (email, Find Phone) · Paulina secondary
- Evidence: `publication-refresh/APPLY.json`

## 3. 100-contact EXA_ONLY pilot

| Metric | Value |
|--------|-------|
| Provider mode | EXA_ONLY (Serper missing) |
| Contacts researched | 100 |
| Eligible pool scanned | 247 named missing ≥1 channel |
| Previously verified reused | 1 |
| New public emails | 14 |
| New public phones | 14 |
| Organizational routes | 31 |
| Incorrect matches rejected | 0 |
| Both channels | 11 |
| One channel | 32 |
| No recoverable channels | 57 |
| Source verification success | 42 |
| Exa queries | 178 |
| Pages fetched | 297 |
| Actual spend | **$1.25** / $10 ceiling |
| Cost per verified new channel | ~$0.045 |
| Airtable write/read-back success | 37 (0 fail) |
| Surfe calls | **0** |
| Surfe lookups avoided | 53 |
| Budget stop | false |

Evidence: `pilot-100/PILOT_RESULTS.json` · `pilot-100/PILOT_REPORT.md` · `pilot-100/COHORT.json`

Live Exa searches are distinct from offline seeded Maritza/Alejandra regressions (regression seeds still available for cert scripts).

## 4. Local / external browser validation

**Local `:8080` STAGED preview** (after syncing restaged package into canonical checkout):

| Opportunity | Package version | Email | Phone | Surfe channel |
|-------------|-----------------|-------|-------|---------------|
| Maritza | `…_20261120_public_contact_channels_v1` | maritza.medina@jalisco.gob.mx | +52 33 3819 2333 | none |
| Alejandra | same | alejandra.vazquez@igeco.mx | — | phone (Find Phone) |
| Paulina (secondary) | same | paulina.sanchez@igeco.mx | — | — |

Evidence JSON: `LOCAL_MARITZA_DETAIL.json` · `LOCAL_ALEJANDRA_DETAIL.json`

**Railway production:** still on `ffc15db0…` (PR #475 not merged/deployed). External cards will not show new channels until IC merge + deploy.

## 5. Integration

- Phase commit: `dd181dd96` on `feat/gdi-public-contact-discovery-2-v1` (push to update #475).
- GDI does **not** merge — Integration Controller owns merge + deploy.
- Live Surfe auth (PR #474) kept separate.

## 6. Remaining limitations

- Default customer CURRENT for Sheraton still absent; STAGED refreshed only.
- Railway deploy of #475 not yet merged.
- 57/100 contacts still unresolved publicly (expected; Surfe remains optional fallback later).
- Serper not configured — hybrid not evaluated.
- Full DB backfill **not started**.

## STOP

Await founder review before entire-database backfill. No further paid provider spend in this phase beyond the completed $1.25 pilot.
