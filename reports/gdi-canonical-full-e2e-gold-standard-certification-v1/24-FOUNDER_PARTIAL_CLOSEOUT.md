# GDI Full E2E Gold Standard — PARTIAL Closeout (Founder GO continuation)

**Status:** `PARTIAL`  
**Not certified:** `GDI_CANONICAL_FULL_E2E_GOLD_STANDARD_CERTIFIED`  
**STAGED tip:** `gdi_hotel_wide_fqo_recnPB3Q02NO8Ma1o_20261127_17efc1bf`  
**Baseline tip at start:** `3330c02db` → merged `origin/main` `e1ec7a0df` (HE #655) on feature branch  
**Hotel:** Wyndham San Diego Bayside (`recnPB3Q02NO8Ma1o`)

## Completed this cycle

| Requirement | Result |
|---|---|
| Airtable contact persist (Findymail + staged emails) | **Applied** — 17 person-matched emails; read-back parity `STAGED_EMAILS_PRESENT_IN_AIRTABLE` |
| Full STAGED inventory upsert to GDO | **Applied** — 7 missing IDs created; **26/26** staged IDs present in Airtable |
| Airtable ID/email reconstruction parity | **PASS** (`22-AIRTABLE_RECONSTRUCTION_PARITY.json`) — STAGED not used as source |
| Alternative entry points | **14/14** secondary contacts mapped into `alternativeEntryPoints[]` and staged (`43deafbe` → carried into `17efc1bf`) |
| Drawer visibility of secondary/alts | Already supported via `secondaryContact`; array field now also populated |
| Findymail remaining live | **10 found / 3 not found / ~10 credits** — all attribution-safe; hydrated onto **SECONDARY** contacts (primaries still ineligible) |
| Surfe | Not used; never persisted |
| UI changes | None |

## Scorecard (measured on STAGED `17efc1bf`)

| Metric | Count |
|---|---:|
| Qualified opportunities | 26 |
| Primary email | **19 / 26** |
| Primary phone | **7 / 26** |
| Primary LinkedIn | **14 / 26** |
| Secondary contacts | 14 |
| `alternativeEntryPoints[]` populated | 14 |
| Direct buyers (no alt required by buyerVsEntry) | 6 |
| Unspecified without alt/secondary | 6 |
| Findymail spend this cycle | ~10 credits (secondaries) |
| Airtable staged-ID coverage | **26 / 26** |

## Outstanding (blocks CERTIFIED)

1. **Primary email gaps 7/26** — Findymail eligible unique people now **0**. Remaining primaries need identity/domain completion before Findymail (e.g. `Suzanne K.`, org domain resolution for ESET / EDAN / Athletic Brewing / Firestorm / Flatiron / Hensel Phelps / Saronic).
2. **Primary phone gaps 19/26** — Findymail is email-only; requires authorized public/Exa professional telephone research (person-specific vs org/switchboard classification).
3. **Primary LinkedIn gaps 12/26** — no promotable `sourceUrl` → `/in/` profiles remaining; needs public identity research (no company-page acceptance).
4. **WHO/HOW unspecified without alt: 6** — recover settled WHO/HOW or complete mandatory two-attempt alt-entry methodology where required (do not invent contacts).
5. **Full package regeneration from durable stores** — GDO ID/email parity PASS, but isolated rebuild of complete publication package (ES / receipt / drawers / outlook) via canonical production builder from Airtable + research inventory is **not yet executed**.
6. **Secondary emails in typed Airtable columns** — secondary emails live in STAGED/payload; GDO typed fields remain primary-focused (by design). Confirm payload read-back for secondaries if required for gold standard.

## Exact commands

```powershell
node scripts/gdi-gold-standard-cert-completion-v1.mjs --airtable-apply --auth FOUNDER_GO_GDI_FINDYMAIL_STAGE4_PERMANENT_E2E_20261127 --hydrate-alt-entry --apply-staged
node scripts/gdi-canonical-full-e2e-gold-standard-certification-v1.mjs --findymail-live --credit-hard-cap 15
node scripts/gdi-findymail-hydrate-remaining-v1.mjs --apply --persist-airtable --auth FOUNDER_GO_GDI_FINDYMAIL_STAGE4_PERMANENT_E2E_20261127
node scripts/gdi-wyndham-airtable-full-inventory-upsert-v1.mjs --apply --auth FOUNDER_GO_GDI_FINDYMAIL_STAGE4_PERMANENT_E2E_20261127
node scripts/gdi-airtable-reconstruction-parity-v1.mjs
pwsh -File scripts/gdi/Invoke-GdiValidateFullE2E.ps1 -HotelId recnPB3Q02NO8Ma1o
```

## Authorization still required for remaining channel research

- Targeted Findymail after domain/identity remediation for the 7 primary gaps (smallest justified manifest once domains resolve).
- Exa/public professional telephone research authorization + budget for person-specific phones.
- Public LinkedIn identity research for 12 primaries (and alt contacts missing LinkedIn).
- WHO/HOW live alt-entry for the 6 unspecified-without-alt cases (or evidence that settled research already completed two attempts).
