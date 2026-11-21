# FOUNDER REPORT — Commercial Inventory Reconciliation (No Research Horizon)

As of: 2026-11-21
PR: #503 · researchHorizonFilter: **NONE** · geographicRestriction: **NONE**

## Why Brand Conversion (136) ≠ Tier B (76) + Tier C (71)
projectDualProspectLists includes (a) all FRANCHISE Tier A/B rows and (b) Tier C UNKNOWN rows only when brand/conversionSignals.brand are present. Of 71 Tier C, only those with brand signals joined Brand Conversion — yielding 76 Franchise Tier B + ~60 branded Tier C ≈ 136. Management list was 0 because OPPORTUNITIES had 0 MANAGEMENT agreementType rows (all CMBS franchise-like).

Tier B (76) = verified hotel-specific contractual expirations (all FRANCHISE in prior inventory). Brand Conversion list (136) = Tier B franchise rows + Tier C brand-signal research candidates. Not the same grain.

## Management Contract list was 0
Scale pilot source family was CMBS franchise expiration schedules; EX-10 named MANAGEMENT agreements were not projected into MANAGEMENT_CONTRACT_TARGETS.json (count 0).
After EX-10 named MANAGEMENT merge: **4** distinct management-contract prospect hotels.

## Tier B (76) — all remain in research scope
- Future-dated: **17**
- Historically expired (status follow-up): **60**
- No 36-month eligibility cutoff. `within36Months` / near-term flags are **application filter metadata only**.

## Dual lists after reconcile
- Brand Conversion Targets (deduped hotels): **136**
- Management Contract Targets (deduped hotels): **4**
- Research-eligible opportunities: **152**
- Verified expirations: **79** (historical 60 / future 19)
- HOLD/quarantine: **299** (false opening/loan dates remain excluded from prospect lists)

## App filter examples (do not delete inventory)
- allBrand: 136
- historicalBrand: 60
- futureBrand: 17
- next36Brand: 6
- nearTermBrand: 6
- allMgmt: 4

## Sample management-contract additions
- Embassy Suites Las Vegas · MANAGEMENT · end=undisclosed · notice=90 · tier=TIER_C_CONVERSION_RESEARCH_CANDIDATE
- SpringHill Suites Denver Airport · MANAGEMENT · end=2035-12-30 · notice=n/a · tier=TIER_B_VERIFIED_CONTRACT_EXPIRATION
- Hilton San Francisco Financial District · MANAGEMENT · end=undisclosed · notice=30 · tier=TIER_C_CONVERSION_RESEARCH_CANDIDATE
- Marriott Executive Apartments · FRANCHISE · end=2027-01-30 · notice=60 · tier=TIER_A_VERIFIED_DECISION_WINDOW
- Hilton Philadelphia Airport · MANAGEMENT · end=2035-03-30 · notice=180 · tier=TIER_A_VERIFIED_DECISION_WINDOW

## Governance
- CUSTOMER_BLOCKED · no HPC · no unauthorized Meta schema · IC owns merge