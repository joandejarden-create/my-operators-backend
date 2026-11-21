# HIF Track C — P0 Commercial Opportunity Canonical Population

**Achieved: 5/5 verified canonical hotel links**  
**Publication: CUSTOMER_BLOCKED**  
**Exa: $0.137 cumulative / $10 hard cap (this run $0; no additional allocation)**  
**As of: 2026-11-21**

IC note: four principals were already `MATCHED` in live Discovery Inventory at apply time; only Lakeville was still `DISCOVERED_NOT_IN_HPC`. Lakeville HPC was created under Independent Official Property URL gate (`https://lakevilleinnmn.com/`, address-corroborated). Brand-directory gate unchanged for branded hotels.

## Hotel-by-hotel table

| P0 | Commercial opp | Canonical HPC | Match / create | Discovery linkage | HE handoff | Exa req / $ | Source verification | Read-back / idempotency | Remaining gaps |
|----|----------------|---------------|----------------|-------------------|------------|-------------|---------------------|-------------------------|----------------|
| P0-1 Lakeville Inn | BCT | `recUTXDsmwVI3M1LQ` | **CREATED** then linked | Inventory `recPXlLWkd63mLK5h` → MATCHED; Finding `recjL2vx3ExNlReF1` / `find_cb1_lakeville_bct_brand_search_2026` preserved | READY | 0 / $0 (prior wave spend only) | Independent property official `lakevilleinnmn.com` + DMO/Chamber/Star Tribune corroboration; no Hilton directory (flag dropped) | Inventory+HPC read-back OK; re-run ALREADY_LINKED | HE: owner/operator validation, decision-maker, full profile; historical brand Hampton Inn |
| P0-2 Shorebreak Huntington | BCT | `reckTzVQUT2H8hbug` | MATCH existing | `recTnywid3vtCErTu` MATCHED; Finding `recrgvSQeupLLKe0B` | READY | 0 / $0 | IHG Kimpton brand directory | OK / idempotent | HE enrichment; operator not set (not inferred) |
| P0-3 Courtyard Denver Downtown | BCT | `recvn447rdN53NgZ7` | MATCH existing | `recmomZidWqHS7vTv` MATCHED; Finding `recBL4VUbarrWCUDS` | READY | 0 / $0 | Marriott brand directory DENCD | OK / idempotent | HE enrichment |
| P0-4 Hilton Trinidad | MCT | `recFNfTvcph0fYQnm` | MATCH existing | `rec0f9E8genYc3tI2` MATCHED; Finding `recDRyNUriKMAl58C` | READY | 0 / $0 | Hilton brand directory POSHIHH | OK / idempotent; Address filled non-destructively `1B Lady Young Road, Belmont` | HE enrichment; MCT/HMA sale diligence |
| P0-5 Embassy ONE Bengaluru | MCT | `recD8mIAMUyQ9ilka` | MATCH existing | `recoF4KfmJj2LYRET` MATCHED; Finding `recQmiSbQOyMmzalO` | READY | 0 / $0 | Four Seasons brand directory | OK / idempotent | HE enrichment; MCT ownership/contract |

## Lakeville CREATE gate (exact evidence)

- Physical identity: **Lakeville Inn**, 20851 Keokuk Avenue, Lakeville, MN 55044
- Historical name: Hampton Inn Lakeville (Hilton terminated franchise ~Jan 2026)
- Current: Independent; Elevate Hospitality LLC / JAMP Hotels (Jaymin Patel); brand-search goal end-2026
- Official Property URL: `https://lakevilleinnmn.com/` (property-owned; address on page)
- Supporting: Visit Lakeville DMO; Lakeville Chamber 2026-04-13; Star Tribune 2026-04-09
- Live HPC collision: `HPC_NEW_RECORD_PROPOSED_VERIFIED_ABSENT` (no Lakeville/Keokuk/20851 hit)
- Gate kind: `INDEPENDENT_PROPERTY_OFFICIAL` — **not** a brand-directory weaken for branded hotels

## Hotel Explorer handoff

See `HOTEL_EXPLORER_HANDOFF_P0.json`. Track C does not run HE research engine. All five: recommended priority **IMMEDIATE**. Operator left null (brand ≠ operator).

## Secondary priorities (not in this create batch)

- **Legacy Green Bay** — brand-conversion intelligence; below these five open first-100 principals
- **Aena Nordeste** — project-level airport hotel development (4 sites); do **not** force into existing-hotel HPC

## Persistence

- Script: `scripts/hif-track-c-p0-canonical-populate.mjs`
- Ledgers: `PERSISTENCE_LEDGER_APPLY.json`, dry-run + table JSON in this folder
- No duplicate commercial opportunities created; qualification unchanged
- CUSTOMER_BLOCKED maintained
