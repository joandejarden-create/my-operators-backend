# AHIP Portfolio — Opportunity-First Canonical Identity

**Verified remaining hotels affected by Aimbridge/ONE Lodging exit: 19**  
**Rooms (AIF-supported): 2,018**  
**HPC: 19/19 linked** (16 initial CREATE + 3 Residence Inn CREATE after geo false-positive remediations)  
**Genuine pre-existing US HPC matches: 0** · **Unresolved: 0**  
**Exa: $0.137 / $10 cumulative** (this wave $0; free brand-directory sources)  
**CUSTOMER_BLOCKED** · Parent MCT `find_cb4_ahip_aimbridge_mct_2026` / inventory `recWQQKIVI89cJnSV`  
**P0 five commercial hotels remain higher priority** than this portfolio queue.

## Methodology (current portfolio, not historical)

| Source | Count |
|--------|------:|
| AHIP AIF YE2025 portfolio table (~2026-03-30) | 30 |
| Completed 2026 dispositions through MD&A 2026-11-13 | −12 named (incl. TPS Pittsburgh sold before AIF date) |
| Remaining owned (website “19 Hotels”) | **19** |
| Room sum check | 2,303 (Q3) − 285 (Q4 closes) = **2,018** |

Aimbridge/ONE Lodging MHMA extended to **2027-04-30**. Nine PSAs ($137.3M / 947 keys) unnamed in MD&A summary remain in the affected owned set until close.

Excluded sold: TPS Pittsburgh Airport; Embassy Cleveland/Dublin/Tempe; SpringHill Arundel Mills; HGI Baltimore White Marsh; Homewood Dover/Allentown; HIE Fort Myers; RI+TPS Chattanooga; Fairfield Jacksonville West.

## Hotel-by-hotel (live verify)

| Hotel | Rooms | Brand | HPC | Action |
|-------|------:|-------|-----|--------|
| Hampton Inn Asheboro | 111 | Hampton Inn | `recuvfBAPbIFVD3c7` | CREATE |
| Courtyard Bismarck North | 89 | Courtyard | `recjaW6oj5xsx1BDt` | CREATE |
| Embassy Suites Cincinnati RiverCenter (Covington) | 227 | Embassy Suites | `recXz2j0s8KIXbEQF` | CREATE |
| Hampton Inn Emporia | 85 | Hampton Inn | `recb5YyTpd3jR7kF7` | CREATE |
| Hampton Inn & Suites Arundel Mills/Baltimore | 130 | Hampton Inn & Suites | `recT3hWFsKCfb0d7M` | CREATE |
| Hampton Inn Harrisonburg - South | 90 | Hampton Inn | `rechAxjRKG6kS2QwQ` | CREATE |
| Staybridge Suites Midland | 98 | Staybridge Suites | `rec6qWRy1anavTsZR` | CREATE |
| Home2 Suites Midland | 93 | Home2 Suites | `recqHextrUg2qCZBJ` | CREATE |
| Hilton Garden Inn Milford | 120 | Hilton Garden Inn | `recvJQmVjBRCC1bo0` | CREATE |
| Residence Inn Mt. Laurel Bishop's Gate | 144 | Residence Inn | `recm9TOomonh7OL9n` | CREATE (remediated) |
| Residence Inn Neptune Gateway Centre | 105 | Residence Inn | `recrbjynDhwdsdWT3` | CREATE (remediated) |
| Fairfield Inn & Suites Orlando Ocoee | 80 | Fairfield Inn & Suites | `recJzQoEA0SirtA1d` | CREATE |
| Fairfield Inn & Suites Pittsburgh Airport/Robinson | 103 | Fairfield Inn & Suites | `recGoF6Kpp1gmBE1t` | CREATE |
| Home2 Suites San Angelo | 93 | Home2 Suites | `recu0nYC267cYyqbH` | CREATE |
| Residence Inn San Angelo | 92 | Residence Inn | `rec0MRvFfbKHp6xSQ` | CREATE (remediated) |
| Fairfield Inn & Suites South Hill I-85 | 68 | Fairfield Inn & Suites | `recnVGM0FNPqRAfZh` | CREATE |
| Courtyard Tampa North/I-75 Fletcher | 81 | Courtyard | `recp0bOx39pO8zqw0` | CREATE |
| Fairfield Inn & Suites Titusville KSC | 96 | Fairfield Inn & Suites | `rec7lT7p49zFAUSCE` | CREATE |
| Courtyard Wall at Monmouth Shores | 113 | Courtyard | `recBQAmo6WePymMJn` | CREATE |

## Portfolio linkage

- Parent inventory `recWQQKIVI89cJnSV` + finding `recxh3z8kPr3I8n1r` retained as portfolio-level MCT principal.
- Each child Discovery Inventory notes parent IDs; **no** per-hotel commercial findings / opportunities.
- Operator evidence: ONE Lodging / Aimbridge until 2027-04-30 — **not** written as HPC Operator field (brand≠operator).
- Parent notes updated with child/HPC counts.

## Remediation note

Initial collision search falsely matched three US Residence Inns to Suriname HPC `rec2v3Ixn3bbt8PCo`. Rejected via country/city/address gate; three correct US HPCs created and inventories re-linked. Script now rejects geo-mismatched collisions.

## Hotel Explorer handoff

`HOTEL_EXPLORER_HANDOFF_AHIP.json` · priority **HIGH_AFTER_P0_FIVE** · HE owns enrichment.

## Persistence

`PERSISTENCE_VALIDATED` for identity create/link · **not** `FULL_E2E_VALIDATED` · `CUSTOMER_BLOCKED`
