# Track C — CB3 First-100 Priority Queue (HPC linkage)

**Publication:** CUSTOMER_BLOCKED  
**IC source:** PR #544 **MERGED** (CB3 first-100 = 5 open BCT/MCT; Legacy not open competitive)  
**Branch / PR:** `feat/hif-v6-track-c-wave3b-opportunity-exa` → **#551**  
**Exa budget:** cumulative pilot — **no new $10**; spend now **$0.133 / $10**

## Success criterion

Proportion of commercially relevant first-100 hotels connected to verified HPC:

### **4 / 5 = 80%**

| # | Hotel | Opportunity | HPC record ID | Status |
|---|-------|-------------|---------------|--------|
| 1 | Lakeville Inn, MN | BCT first-100 | — | **UNRESOLVED** — independent; address `20851 Keokuk Avenue` verified; Exa Lite found **no** brand-directory URL; DMO URL present; CREATE blocked by existing Official URL gate |
| 2 | Kimpton Shorebreak Huntington Beach, CA | BCT first-100 | `reckTzVQUT2H8hbug` | Linked inventory `recTnywid3vtCErTu` |
| 3 | Courtyard Denver Downtown, CO | BCT first-100 | `recvn447rdN53NgZ7` | Linked inventory `recmomZidWqHS7vTv` |
| 4 | Hilton Trinidad & Conference Centre, TT | MCT first-100 | `recFNfTvcph0fYQnm` | Matched existing; linked inventory `rec0f9E8genYc3tI2` |
| 5 | Four Seasons Bengaluru at Embassy ONE, IN | MCT first-100 | `recD8mIAMUyQ9ilka` | Linked inventory `recoF4KfmJj2LYRET` |

## Expanded pipeline (not first-100)

| Hotel | Tier | HPC ID |
|-------|------|--------|
| Legacy Hotel, Green Bay | Preferred-brand / conversion intelligence (IC) | `recRNGJk5lpaHeeo6` |
| Marriott Marquis Chicago | Award-pending MCT | `rec43YKxOBdvxnpd9` |
| Gideon Putnam | Adjacent concession | Unresolved (independent official site gate) |
| Honey Creek Resort | Award-pending | Unresolved (independent) |
| Hotel do Mel | Development project | **No HPC** — project-level, correctly excluded |

## Persistence

- Inventory↔HPC non-destructive links applied for 6 properties (see `CB3_FIRST100_PERSISTENCE_APPLY.json`).
- No new HPC creates this run (4/5 already created/matched in Wave 3b).
- Idempotent: re-run links MATCHED status; no schema changes.
- Qualification **not** changed by Track C.

## Exa

| Metric | Value |
|--------|------:|
| Hard cap | $10.00 |
| Cumulative spend | **$0.133** |
| Remaining | $9.867 |
| This instruction new allocation | **$0** (cumulative only) |
| Lakeville Exa Lite | 3 queries; no brand-directory unlock |

## Hotel Explorer handoff

`HOTEL_EXPLORER_HANDOFF_CB3_FIRST100.json` — HE worktree not modified; CUSTOMER_BLOCKED.

## Founder ask (Lakeville)

Authorize independent Official Property URL (Visit Lakeville / Chamber) for HPC CREATE_READY when brand directory is absent — then Lakeville reaches **5/5**.
