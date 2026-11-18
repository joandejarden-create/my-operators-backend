# Complete GDI-related table inventory

**Base:** `appa2cE7FTRmIbB32` · **Live read:** 2026-11-18 · **Mutation:** none  
**Evidence:** `EVIDENCE_REGISTER.json`, `_live_table_inventory.json`

ADP-only measurement tables (Published Reports, Monitoring Periods, Observation Ledger, etc.) exist in the same base but are **out of Full E2E write scope**. They are listed as adjacent context only.

## GDI / shared tables audited

| Table | Table ID | Identity field (contract / practice) | Live count | Fields (meta) | Last created observed |
|-------|----------|--------------------------------------|----------:|--------------:|-----------------------|
| Group Demand Opportunities | `tblRuReslJMwsfRQj` | `opportunityId` | 998 | 67 | see evidence |
| Demand Generators | `tblykUVOjGVkawvdD` | `Demand Generator ID` | 95 | 27 | |
| Demand Programs | `tblxm7wqfFNG37Z2m` | `Program ID` | 94 | 22 | |
| Demand Generator Signals | `tblZ0cOM78uYMCTXx` | `Demand Generator Signal ID` | 23 | 28 | |
| Hotel Demand Generator Fit | `tblqamXbIk9n6gXfV` | `Hotel Generator Fit ID` | 276 | 20 | |
| Private Event Venues | `tblE5p4HjVHfcMDna` | `Venue ID` | 38 | 48 | |
| Private Event Signals | `tbl5rSZxKv218GuOP` | `Event Signal ID` | 26 | 35 | |
| Hotel Venue Fit | `tbluXqX7ecocCrdE1` | `Hotel Venue Fit ID` | 44 | 27 | |
| GDI Research Targets | `tblVyuEf5vjooWDKX` | `Target ID` | 480 | 41 | |
| GDI Research Runs | `tblzNMUIo2T6onaKH` | `Run ID` | 40 | 27 | |
| GDI Research Target Runs | `tblSA0cFVplNfWMjp` | `Target Run ID` | 383 | 25 | |
| GDI Hotel Commercial Profiles | `tbl8KSNUCRbIzEv15` | `Profile ID` | 2 | 19 | |
| GDI Commercial Profile Change History | `tblrFnRPOoUz6oNCn` | `Change ID` | 8 | 12 | |
| GDI Hotel Need Periods | `tbl50kj5XkGs22C6j` | `Need Period ID` | 22 | 37 | |
| GDI Need Period Change History | `tblMTSB0bkzFO2zzh` | `Change ID` | 42 | 14 | |
| GDI Need-Date Campaign Runs | `tblghbxp4reaQWx2b` | `Campaign ID` | 1 | 19 | |
| GDI Need Period Opportunity Matches | `tblOw5r7r95eQ1dPq` | **`Match Key` (practice)** / Need Period ID (legacy hint) | 139 | 12 | |
| Decisions | `tblulPWvEmd3iiDhJ` | `decisionId` | 93 | 26 | |
| Decision Events | `tblLL1CuKpvI43DtB` | `eventId` | 160 | 26 | |
| Hotel Commercial Profiles (facility) | `tblBYJtKj6oi4J0Ow` | `Profile Key` / HPC Hotel ID | 23 | 41 | |
| Hotel Event Spaces | `tblDwW2jpbKBBtpko` | `Space Key` | 32 | 25 | |
| Hotel Demand Nodes | `tbl5xEOk7Hnr5Iebq` | `Node Key` | 81 | 24 | |
| Hotel Seasonality & Need Periods | `tbllCSH31bxsXM3C0` | `Period Key` | 27 | 17 | |
| Hotel Intelligence Evidence | `tblHxS8x0niDEGVsh` | `Evidence ID` | 722 | 18 | |

## Adjacent ADP tables (not Full E2E destinations)

Documented in prior inventory CSV rows for ADP Published Reports, Monitoring Periods, Snapshots, Metrics, Evidence Index, Observation Prompt Ledger, Report Corrections, Hotel ADP Attributes. Full E2E must **not** write these as a substitute for GDI opportunity persistence.

## Count deltas vs 2026-11-13 complete-model audit

| Table | Prior | Live | Delta |
|-------|------:|-----:|------:|
| Group Demand Opportunities | 962 | 998 | +36 |
| GDI Hotel Need Periods | 13 | 22 | +9 (incl. Sheraton provisional scenarios) |
| GDI Need Period Opportunity Matches | 119 | 139 | +20 |
| GDI Need Period Change History | 33 | 42 | +9 |
| Demand Generators / Programs / Signals | 95/94/23 | same | 0 |
| Research Targets / Runs / Target Runs | 480/40/383 | same | 0 |

## Notes

- All listed GDI table IDs still resolve in live meta API.
- Inventory preserves records; no deletions performed.
- Detailed per-table purpose classifications live in `TABLE_PURPOSE_AND_OWNERSHIP_MATRIX.md`.
