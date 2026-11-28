# Shared Canonical Contact Intelligence V1 — Founder Report

**Date:** 2026-11-28  
**Branch:** `feat/shared-canonical-contact-intelligence-v1`  
**Worktree:** `C:\Dev\dealality\wt-pr677-clean-main` (clean from `origin/main`)  
**Base:** `af4ba8aba458e4a25fa191c7ec80921e738c7ce2`

## Acceptance criteria

**One canonical identity → One eligible verified channel record in Airtable → One shared backend resolver → Consistent display in GDI and Hotel Explorer.**

## 1. Current canonical Airtable contact schema

Reuse only (no new parallel DB, no unauthorized schema create):

| Table | Role |
|---|---|
| HI Org Contacts | Canonical Person ID + affiliation |
| HI Contact Channels | Email / Telephone / LinkedIn + provenance / retention |
| HI Organizations | Employer |
| HI Hotel–Org Relationships | Hotel ↔ org |

Audit: `docs/data-intelligence/shared-canonical-contact-intelligence-v1-architecture-audit.md`

## 2. GDI ↔ HE Person ID mapping

Reversible map: `data/shared-contact-intelligence/gdi-hi-person-id-map-v1.json`

| Person | HI Person ID | GDI personIdentityKey | HI Airtable |
|---|---|---|---|
| Jeff Mullen | `dle_06GZE1QEZWGPH6C9QSHJ2CTH1B` | `52be62e032b2d6a6` | `recCmiDOrJoI6kYrM` |
| Katherine Button | `dle_06GZE1QGHR0D2RRKEG84GNM7TE` | `96363eba0c922295` | `recNzfommzJEvCUEP` |
| Patrick Reece | `dle_06GZE1QJ2WMBR4NC4RDJ1ANAV1` | `3e7309d347d585de` | `rechprAGnkCt7VbmP` |
| Meinrad Lang III | `dle_06H1TN755CY1NZS0Q7G026PKSC` | `0e49d3dfa1c3af68` | `recfx0E3oYVBdXjkJ` |
| Jenny Pyne | `dle_06H1TN7738KV6TQ9BE8Q4D6F4B` | `9798049956665ea1` | `recktMaD8KuMDPkDM` |

## 3. Duplicate identities

Bethesda: 8 HI Org Contact rows → 5 unique people (Validated preferred over Evidence Collected). Collapsed by shared resolver; no destructive merge.

## 4. Existing verified channels recovered (live Airtable read-back)

| Person | Email | Phone | LinkedIn |
|---|---|---|---|
| Jeff Mullen | jeff.mullen@mullencap.com | — | recovered |
| Katherine Button | kbutton@mullencap.com | — | recovered |
| Patrick Reece | preece@mullencap.com | — | recovered |
| Meinrad Lang III | unresolved | — | recovered |
| Jenny Pyne | unresolved | — | recovered |

## 5. New channels persisted this PR

**None.** This PR establishes the shared SoT resolver/API/gates. Findymail emails were already persisted under HE50 Live authorization; golden run is read-only (spend $0).

## 6. Airtable read-back proof

Live golden + validator: `airtableLive: true`, gates **PASS**.  
Reports under `reports/shared-contact-intelligence/`.

## 7. Shared API parity

- `GET /api/contact-intelligence/canonical/hotels/:hotelId`
- `GET /api/contact-intelligence/canonical/people/:personId`
- HE hotel GET always overlays HI graph when contacts exist (not only when sparse)
- GDI hydrate helper: `hydrateGdiPrimaryContact` (same channel values)
- `CONTACT_GDI_HE_CHANNEL_PARITY` = PASS (same resolver)

## 8–9. Product rendering

- GDI: Who to Contact / Contact Details via `toGdiContactPresentation`
- HE: five-column directory via `toHeContactDirectoryRow`
- Surfe remains session-only (`CONTACT_SURFE_TRANSIENT_ONLY` PASS)

## 10. Contactability before/after

| Metric | After (live) |
|---|---|
| Verified Named Contacts | 5 |
| Verified Direct Emails | 3 |
| Verified Direct Phones | 0 |
| Verified LinkedIn | 5 |
| Directly Contactable | 3 |
| Direct Contactability Rate | **60%** |
| Unresolved Direct Channels | 2 (Meinrad, Jenny) |

## 11. Findymail usage this PR

**$0 / 0 credits.** Reuse policy blocks duplicate paid lookups when canonical email or ledger hit exists.

## 12. Unresolved gaps

- Meinrad Lang III / Jenny Pyne: Findymail previously NOT_FOUND — no fabricated emails
- Duplicate HI Org Contact rows remain in Airtable (collapsed at read time; destructive merge deferred)
- Optional typed GDO `canonicalPersonId` column — founder-gated schema proposal only
- No production deploy

## 13. Integration status

- Dirty HE / GDI / HE50 / Integration Controller worktrees **preserved**
- Focused PR from clean worktree
- PowerShell: both `Invoke-HotelExplorerFactory.ps1` and `Invoke-GdiValidateFullE2E.ps1` call the **same** shared validator

### Module entry

`lib/shared-contact-intelligence/`

### Permanent gates

`CONTACT_CANONICAL_IDENTITY_RESOLVED` · `CONTACT_AIRTABLE_SOURCE_OF_TRUTH` · `CONTACT_CHANNEL_PROVENANCE_VALIDATED` · `CONTACT_AIRTABLE_PERSISTENCE_READBACK` · `CONTACT_GDI_HE_CHANNEL_PARITY` · `CONTACT_NO_DUPLICATE_PAID_ENRICHMENT` · `CONTACT_DIRECT_CONTACTABILITY_VALIDATED` · `CONTACT_SURFE_TRANSIENT_ONLY`
