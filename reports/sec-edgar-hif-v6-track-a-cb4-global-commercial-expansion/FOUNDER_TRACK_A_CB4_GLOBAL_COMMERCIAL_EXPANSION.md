# HIF Track A — CB4 Global Commercial Opportunity Expansion

**As of:** 2026-11-21  
**Publication state:** `CUSTOMER_BLOCKED`  
**Branch:** `feat/hif-v6-track-a-cb4-global-commercial-expansion`  
**Prior PR:** #544 **MERGED** (`aca2b09e`)  
**Research cost:** $0 (free sources only)

## Research volume

| Metric | Value |
|--------|------:|
| New distinct hotels investigated | **285** |
| Cumulative deduplicated hotels investigated | **418** |
| Volume target (new) | 250 |
| Stop reason | `RESEARCH_VOLUME_TARGET_MET` |

## First-100 (incremental vs cumulative)

| Metric | Value |
|--------|------:|
| Incremental qualified BCT | 0 |
| Incremental qualified MCT | **1** (AHIP Aimbridge replacement portfolio) |
| Cumulative first-100 after CB4 | **6 / 100** |
| Incremental qualified yield / 100 new hotels | 0.35 |
| Cumulative qualified yield / 100 cumulative hotels | 1.44 |

### Cumulative first-100 IDs

1. Lakeville Inn (BCT)  
2. Hilton Trinidad (MCT)  
3. Shorebreak Huntington (BCT)  
4. Courtyard Denver Downtown (BCT)  
5. Embassy ONE Bengaluru (MCT)  
6. **AHIP remaining portfolio Aimbridge MCT** *(CB4 incremental)*

## Expanded opportunities by deal type

- **MCT portfolio (principal):** AHIP remaining ~19 hotels — Aimbridge exit **2027-04-30**  
- **Development / operator selection (adjacent):** Thimarafushi + Fonadhoo plots A/B/C (MV) — bids **2027-01-07**  
- **Concession (adjacent):** Cedar Lake Outdoor Center (MI) — bids **2027-02-03**  
- **Acquisition-related (adjacent):** Radisson La Crosse — Choice management ends **2026-12-31** + Hilton PIP  
- **Award pending (not open):** Grand Teton CC-GRTE001-27 (submissions closed Jan 2026)  
- **Committed / not open:** ÓLEO Cancún → Distinctive Concept; RLJ Boston → Tapestry; RLJ Key West → Margaritaville  

## Source-family yield (CB4)

| Family | Hotels | First-100 | Note |
|--------|-------:|----------:|------|
| OWNER_EARNINGS_AHIP_AIMBRIDGE_MCT | 32 | 1 | **Increase allocation** |
| PUBLIC_DEV_LEASE_MALDIVES | 4 | 0 (4 adjacent) | Productive |
| PUBLIC_RFP_CONCESSION_MI | 1 | 0 (1 adjacent) | Productive |
| PUBLIC_RFP_CONCESSION_NPS | 12 | 0 | Award-pending — **reduce** new NPS closed-solicit searches |
| OWNER_EARNINGS_APPLE_PORTFOLIO_SCAN | 216 | 0 | Completed transitions — **reduce** repeat |
| RLJ_CONVERSION / MIDWAY_SALE | 9 | 0 | Committed / sold |

## Cross-track

- **Track C:** opportunity-first AHIP portfolio handoff (`TRACK_C_HANDOFF.json` + `IC_AHIP_PORTFOLIO_TRACK_C_HANDOFF.json`) — one parent MCT (`disc_cb4_ahip_remaining_portfolio_mct_2026` / `recWQQKIVI89cJnSV`); **19** remaining named property identities; **0** existing HPC links; **no fictional portfolio HPC**  
- **Track B:** no competing active opportunity writes  
- **Hotel Explorer priorities:** AHIP remaining assets, La Crosse, Cedar Lake, carry opens (Embassy ONE, Shorebreak, Denver)

## IC commercial qualification (PR #547)

| Check | Result |
|-------|--------|
| Aimbridge / ONE Lodging exit | **2027-04-30** (extension exercised 2026-10-21; settlement originally Jan 31 2027) |
| Replacement management required | **Yes** (FLI assumes retain a replacement hotel manager) |
| Selection open | **Yes** |
| Replacement agreements awarded | **No** (not disclosed) |
| One vs multiple managers | Singular FLI phrasing only — **not an explicit multi-manager decision** |
| Affected portfolio | **Not all historical AHIP hotels** — Sept 30 2026: **22** / 2,303 rooms; remaining named affected: **19** after 2026 dispositions |
| Decision-maker | **AHIP REIT LP** (GP: American Hotel Income Properties REIT (GP) Inc.) |
| First-100 counting | **One** portfolio MCT → cumulative **6/100** |

## Airtable

- Destination: HI Discovery Inventory + Findings (`appCCUsuGsE1ifoLk`)  
- Mode: dry-run → apply → idempotent NOOP → read-back  
- No HPC creates; no schema mutation; prior CB1–CB3 records preserved  

## Checkpoint

- **Resume from:** this CB4 report + `INVESTIGATED_HOTELS.json` + Airtable read-back IDs  
- **Next:** continue high-yield OWNER_EARNINGS private MCT/BCT + open public concessions/dev leases; avoid completed Apple/RLJ conversion news  
