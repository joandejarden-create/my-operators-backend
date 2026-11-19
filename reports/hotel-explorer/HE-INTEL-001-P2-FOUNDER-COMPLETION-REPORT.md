# HE-INTEL-001 P2 — Founder completion report (HPC linkage)

**PR:** [#422](https://github.com/joandejarden-create/my-operators-backend/pull/422) — **OPEN**  
**HEAD:** `cfd8d19d762323a92824bef312acaa82ed774e5c` (+ report/docs follow-up)  
**Status:** AWAITING_INTEGRATION_CONTROLLER — **do not merge until CI green + founder review**

## Canonical placement
| Item | Value |
| --- | --- |
| Graph base | `appCCUsuGsE1ifoLk` |
| HPC table | `tbl9aY5ijiuIzzWam` |
| Forbidden base | `appa2cE7FTRmIbB32` (refused by gates) |

## Live HI tables (HPC base)
| Table | ID |
| --- | --- |
| HI Organizations | `tbliXEfUOWk32AN9y` |
| HI Hotel–Org Relationships | `tblGiwN0foXXDESvZ` |
| HI Org Contacts | `tbldJAj0nK1l39JZL` |
| HI Contact Channels | `tblqC7C4ypy5Zojyw` |
| HI Ownership Research Runs | `tblGwk1YefumCf2eu` |
| HI Graph Evidence | `tblc9JhKT1edr4grE` |

## Native HPC linkage (validated live)
| Field | ID | Notes |
| --- | --- | --- |
| **HPC Hotel** | `fldcpmADxtKNKDTDN` | `multipleRecordLinks` → `tbl9aY5ijiuIzzWam` |
| **HPC Hotel ID** | (text) | Retained singleLineText twin |
| **Organization** | `fldkupFaxgSyemlhI` | → HI Organizations `tbliXEfUOWk32AN9y` |
| **HI Organization Relationships** (HPC reciprocal) | `fldIrXDEBhEnhDFDg` | → `tblGiwN0foXXDESvZ`; rename-only, no duplicate on rerun |
| Cardinality | app-enforced | Exactly one HPC hotel per relationship |

## Referential integrity
Implemented in `lib/hotel-intelligence/research-engine-v1/airtable-graph-referential-integrity.js`: resolve HPC → verify base → resolve org → dual native links → read-after-write → HPC Hotel ID match → reject orphan/ambiguous/mismatch.

## Synthetic demo (not verified research)
One synthetic HEI org → three distinct pilot HPC hotels. Second `--apply` = UPDATE only; `orgCount=1`, each `relCount=1`.

| Hotel | HPC ID | Rel record | RAW OK |
| --- | --- | --- | --- |
| Wyndham San Diego Bayside | `recnPB3Q02NO8Ma1o` | `recev4kNe5kpWzEcn` | yes |
| Renaissance New York Times Square | `recG66DQJKP2c0UNh` | `recurh6asTT3S6j3Z` | yes |
| Westin Tampa Waterside | `recdmo0CnhIZAwHGC` | `recfxgZAQVEzuzSUy` | yes |

Org: `recxtysEZn9PELHha` · `dle_06GZ6DMCDW1KMJR5GCZA6EGVR6` · INTERNAL_ONLY / synthetic.

Evidence: `reports/hotel-explorer/HE-INTEL-001-P2-hpc-link-synthetic-demo-v1.json`

## Evidence architecture
- **HI Graph Evidence** on HPC base (`tblc9JhKT1edr4grE`)
- **ADP Evidence** `tblHxS8x0niDEGVsh` on `appa2cE7FTRmIbB32` — preserved

## Misplaced-table cleanup (still open)
Five empty wrong-base tables on ADP base: Meta API `DELETE` returns `404`. **Manual Airtable UI deletion** still required. Do not delete ADP Evidence.

## Local gates
- `npm run test:he-intel-001-p2-airtable-graph-v1` — 15/15
- `npm run test:he-intel-001-research-engine-v1` — 24/24
- `npm run dealality:ci-precheck` — PASS

## Merge posture
**STOP.** Do not merge until GitHub required checks pass and founder accepts this corrected report.
