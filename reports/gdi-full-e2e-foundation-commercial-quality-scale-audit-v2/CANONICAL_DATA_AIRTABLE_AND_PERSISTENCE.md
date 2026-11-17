# Canonical Data, Airtable, and Persistence

## Source-of-truth map (CONFIRMED / LIKELY)

| Data type | Authoritative store | Derivative / notes |
|-----------|---------------------|--------------------|
| Customer-visible published inventory | `bundled-publication-packages/<hotelId>/CURRENT.json` + immutable package dir | Share resolve uses approved package resolver |
| Staged pre-approval | `STAGED.json` + staged package | Fingerprint gates |
| Execution observability | `data/group-demand-intelligence/execution-observability/` | Historical UNKNOWN allowed |
| Discovery ledgers / cache | `data/group-demand-intelligence/discovery-ledgers` (and related) | Zero-cost replay |
| Research run history | Research run stores + reports/ | Campaign manifests |
| Airtable | Presentation / research / ops tables (multiple scripts) | **Not sole customer SoT** for published GDI packs |
| Share registry | `config/client-share/gdi-share-registry/` | tokenId metadata; secrets private |

## Airtable empty-view phenomenon

**LIKELY causes (do not assume data loss):**

1. Customer inventory lives in bundled packages, not mirrored 1:1 to Airtable opportunity rows.  
2. Wrong base/table/view filters.  
3. Write path not invoked on campaign scripts that only stage packages.  
4. Silent write failures without read-back (investigate with existing `gdi:research-storage-reconciliation-v1` / airtable audit scripts — dry-run only).

This audit did **not** mutate Airtable. Recommend a dedicated read-only reconciliation PR later.

## Lifecycle stages (as operated)

Raw discovery → candidate → evidence → qualified → hotel-specific presentation → staged package → founder approval → CUSTOMER_PUBLISHED CURRENT → share resolve → feedback (optional grant).

## Persistence / recovery

- Immutable package versions + fingerprints: **CONFIRMED** (YOTEL 35343009…).  
- Read-back certification: present in parity/activation gates.  
- Resume after failure: partial (orchestrator-dependent).  
- Prefer extending package + ledger architecture over migration.

## Minimal recommendations

1. Document SoT explicitly in operator runbooks (packages win for customer UI).  
2. Add read-back assertions whenever Airtable writes are claimed.  
3. Do not “fix empty Airtable” by rewriting CURRENT.
