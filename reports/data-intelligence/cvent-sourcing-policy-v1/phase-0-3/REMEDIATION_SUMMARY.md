# Remediation Inventory Summary (Read-Only)

**Generated:** 2026-10-03T15:59:02.502Z  
**Policy:** `source-policy-v1`  
**Destructive writes:** 0 · **Deletes:** 0 · **Mass overwrites:** 0

## Prior shell audit (DR / CR / PA)

| Slice | Count |
|-------|------:|
| Cvent-only shells | 256 |
| Cvent + HBX | 144 |

These remain labeled discovery / multi-source candidates (Not Field Source) per prior audit — Class D / B for steward purposes.

## Inventory file counts (this pass)

| Class | Meaning | Count |
|-------|---------|------:|
| A | Independently verified elsewhere | 0 (not asserted without live Airtable sweep) |
| B | Mixed / needs review | 35 (+ 144 prior mixed shells in aggregate metric) |
| C | Cvent-only canonical exposure risk | 2 (Choice rooms-fill targets) |
| D | Historical / non-customer / cache | 5 |

| Metric | Count |
|--------|------:|
| Local venue cache files | 290 |
| ADP fixture Cvent mentions | 11 |
| HI/GDI report artifacts with venue URLs | 26 |
| **Cvent-only canonical exposure (flagged)** | **2** |
| **Mixed-provenance (inventory + prior mixed)** | **179** |
| **Customer-visible exposure rows** | **6** |
| **Scoring-impact exposure rows** | **11** |

## Next (out of Phase 0–3)

1. Live Airtable HI Commercial / Event Space `sourceUrl` host sweep (read-only).
2. Independent verification queue for Class C Choice rooms rows.
3. Optional Class D cache retention policy (keep vs archive) — no delete without approval.

See `REMEDIATION_INVENTORY.csv` for row-level detail.
