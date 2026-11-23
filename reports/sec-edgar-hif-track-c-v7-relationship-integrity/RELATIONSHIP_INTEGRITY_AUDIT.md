# Track C V7 — Relationship Integrity Audit (READ-ONLY)

**As of:** 2026-11-23T09:47:38.058Z  
**Airtable writes:** 0 · **CUSTOMER_BLOCKED** · PR #557 MERGED (not re-run)

## Finding-ID mismatch causes

### Holiday Inn Monterrey Valle
- **Cause:** `TRACK_C_SYNTHETIC_FINDING_ID_NEVER_IN_TRACK_A_CORPUS`
- Track C invented findingId + discoveryPropertyId for identity linking. Track A CALA Discovery V1 RESEARCH_CANDIDATES never emitted hotel-level findings for Monterrey Valle or Puebla La Noria — only portfolio-parent FINN internalization + named Courtyard/Yucatán/Cabo/Santander. Identity inventory exists; DealSignal finding row does not.
- Inventory exists: true · Finding live: false
- **Repair:** Map hotel cards to parent finding find_ds_cala_v1_finn_internalization_franchise_2026 (evidence of HI conversions/franchise renewals) OR create Track A–authorized hotel-level findings later — do not invent duplicate navigation findings. Preserve Track C discoveryPropertyId inventory as identity SoT with HPC link.

### Holiday Inn Puebla La Noria
- **Cause:** `TRACK_C_SYNTHETIC_FINDING_ID_NEVER_IN_TRACK_A_CORPUS`
- Track C invented findingId + discoveryPropertyId for identity linking. Track A CALA Discovery V1 RESEARCH_CANDIDATES never emitted hotel-level findings for Monterrey Valle or Puebla La Noria — only portfolio-parent FINN internalization + named Courtyard/Yucatán/Cabo/Santander. Identity inventory exists; DealSignal finding row does not.
- Inventory exists: true · Finding live: false
- **Repair:** Map hotel cards to parent finding find_ds_cala_v1_finn_internalization_franchise_2026 (evidence of HI conversions/franchise renewals) OR create Track A–authorized hotel-level findings later — do not invent duplicate navigation findings. Preserve Track C discoveryPropertyId inventory as identity SoT with HPC link.


## Hotel link matrix

| Hotel | Disc ID in Track A? | Finding ID in Track A? | Finding live? | HPC link OK? |
|-------|---------------------|------------------------|---------------|--------------|
| Courtyard Playa del Carmen | true | true | true | true |
| The Yucatán Resort Playa del Carmen | true | true | true | true |
| Holiday Inn Monterrey Valle | false | false | false | true |
| Holiday Inn Puebla La Noria | false | false | false | true |

## STOP

No destructive identity changes. No expanded live writes. Founder approval required for any Airtable mutations listed in JSON.
