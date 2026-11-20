# HE 5.0 — Founder Pre-Apply Review Package

**Status:** STOP — AWAITING SEPARATE FOUNDER AUTHORIZATION FOR PRODUCTION AIRTABLE APPLY.  
**Paid spend:** $0.00 confirmed · $0.00 reserved · **$50.00 remaining**  
**Airtable writes:** 0 · Persistence: `PERSISTENCE_DESIGNED` (not FULL_E2E_VALIDATED)

## 1. Conflict resolutions

| Conflict | Disposition |
| --- | --- |
| AC rooms 140 vs 146 | **RESOLVED → 140 HIGH** (opening press 86+50+4; discard OTA 146) |
| Secrets Cap Cana vs Dreams | **VERIFIED distinct** — HPC `recd7XAM6tLjzQhaI` Secrets @ 18.4844,-68.3943 vs Dreams `reclS16CMUS6eJgpC` |
| Secrets Papagayo | **HPC CREATE** rooms 202 — legacy census `recyX1wLsHBJYRPw7` provenance only (not runtime SoT) |
| Bethesda 2015 vs 2017 | **Dual events** — 2015 major reno near-complete; 2017 guestroom unveil press |
| Mullen portfolio | **Proposed listings ≠ verified holdings** (legacy Owner Website signal only) |

## 2. Mutation matrix (10 READY / 1 HOLD / 1 REJECT)

| # | Op id | Hotel | Rec | Conflict |
| --- | --- | --- | --- | --- |
| 1 | M-BETH-001 | Bethesda Marriott | READY | NONE |
| 2 | M-BETH-002 | Bethesda Marriott | READY | RESOLVED_AS_DUAL_EVENTS |
| 3 | M-BETH-003 | Bethesda Marriott | READY | NONE — evidence only, no lender edge |
| 4 | M-BETH-REF-STAKEHOLDER | Bethesda Marriott | HOLD | NOT_A_STANDALONE_MUTATION |
| 5 | M-NEW-ac_hotel_newtown_square | AC Hotel Newtown Square | READY | ROOMS_RESOLVED_140 |
| 6 | M-NEW-carton_house_fairmont | Carton House, a Fairmont Hotel | READY | NONE |
| 7 | M-NEW-hgi_newtown_square_radnor | Hilton Garden Inn Newtown Square Radnor | READY | ROOMS_RESOLVED_138_HIGH |
| 8 | M-UPD-jw_marriott_costa_elena | JW Marriott Costa Elena Resort All-Inclusive | READY | MULLEN_OPERATOR_CLAIM_HELD |
| 9 | M-UPD-secrets_cap_cana-identity | Secrets Cap Cana Resort & Spa | REJECT | IDENTITY_VERIFIED_NO_WRITE_NEEDED |
| 9.1 | M-UPD-secrets_cap_cana-rooms | Secrets Cap Cana Resort & Spa | READY | ROOMS_RESOLVED_AUTONOMOUS |
| 10 | M-NEW-secrets_papagayo | Secrets Papagayo Costa Rica | READY | LEGACY_CENSUS_INDEPENDENCE — CREATE not promote |
| 11 | M-NEW-secrets_st_lucia | Secrets St. Lucia | READY | NONE |

**REJECT:** `M-UPD-secrets_cap_cana` — identity already correct; no field UPDATE.  
**HOLD:** `M-BETH-REF-STAKEHOLDER` — prior package reference, not a standalone write.

## 3. Completeness (Min HPC ≠ Full HE)

See `HE50-COMPLETENESS-MATRIX-V1.json`. Only Bethesda approaches Full HE (still staged). Seven others are min-identity / HPC CREATE-or-UPDATE candidates. Legacy Hotel Census is not a runtime dependency (HE-QL-041).

## 4. Expected HE improvement after approved apply

- Bethesda: Year Built 1979 + renovation/CMBS evidence in Sources; prior stakeholder package still separate.
- 5× NEW_HPC (incl. Papagayo CREATE): Founder Preview deep-links with min Overview.
- Cap Cana / JW: identity already usable; JW rooms/alias after UPDATE.

## 5. Remaining Full HE work

Ownership verification, contacts, demand/TI population, Market Alerts discovery, reputation licensing, and browser validation after apply.

Machine packages: `HE50-MUTATION-RECONCILIATION-V1.json`, `HE50-AIRTABLE-DRY-RUN-PACKAGE.json`, `HE50-IDENTITY-CONFLICT-PROBE-V1.json`.
