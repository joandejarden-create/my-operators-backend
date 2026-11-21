# HIF V6 Track B — Licensed CCOD + OCOD Activation

**As of:** 2026-11-21  
**Publication:** CUSTOMER_BLOCKED  
**Research cost:** $0  
**PR #513:** MERGED (`58b9382ad`) — licences accepted; this run completes FULL baseline download + matching.

## 1–2) Authenticated downloads

| Dataset | Result | File | Bytes (on disk) | SHA-256 (prefix) | Rows scanned |
|---------|--------|------|----------------:|------------------|-------------:|
| CCOD FULL | **OK** | `CCOD_FULL_2026_11.zip` | 320,996,653 | `efdf0691…` | 4,485,124 |
| OCOD FULL | **OK** | `OCOD_FULL_2026_11.zip` | 7,479,151 | `79cfe2cb…` | 90,950 |

- `HM_LAND_REGISTRY_KEY`: PRESENT (length 36; value not logged)
- Auth: **AUTHENTICATED_OK** — FULL tickets authorized (no licence restriction)
- Raw files: `data/hmlr-ulpd/` (**gitignored**); signed URLs never persisted
- NPS / Business Gateway / paid titles: not used

## 3) Baseline ingestion

Reporting period **2026-11**. FULL = baseline; monthly COU for deltas only. Streaming CSV match.

## 4) Eight-hotel pilot

| Hotel | Match class |
|-------|-------------|
| The Savoy | REVIEW_REQUIRED (Savoy Court offices — no primary hotel title) |
| Claridge's | **VERIFIED** |
| The Langham London | **VERIFIED** |
| Hilton London Bankside | REVIEW_REQUIRED |
| Park Plaza Westminster Bridge | REVIEW_REQUIRED (room leaseholds excluded) |
| Manchester Airport Marriott | REVIEW_REQUIRED (restaurant units) |
| Hilton Manchester Deansgate | REVIEW_REQUIRED (tower apartments) |
| The Grand Hotel Birmingham | **VERIFIED** |

## 5) Independently verified legal proprietors

1. Claridge's — **CLARIDGE'S HOTEL LIMITED** — Freehold NGL633367 — CCOD — co. 29022
2. The Langham London — **GREAT EAGLE HOTELS (UK) LIMITED** — Freehold NGL593764 — OCOD
3. The Grand Hotel Birmingham — **THE GRAND HOTEL BIRMINGHAM LIMITED** — Leasehold MM94259 — CCOD — co. 10521069
4. Hilton London Metropole (expansion) — **HPREF METS LONDON LTD** — Freehold NGL201807 — CCOD — co. 10951979

Proprietor ≠ operator/franchisor/franchisee/UBO. CH API key absent — matching not blocked; public CH links prepared where company numbers exist.

## 6–7) Expansion / ambiguous

- Evaluated: 16 · VERIFIED: **4** · REVIEW_REQUIRED: **11** · UNRESOLVED: **1**
- Title/ownership rows are **not** opportunity counts

## 8–10) Cost, licence, monthly

- Cost $0 · CUSTOMER_BLOCKED · no raw redistribution
- Monthly: `CCOD_COU_*` / `OCOD_COU_*` after FULL baseline

## 11) Integration

Track B workspace · branch `feat/hif-v6-track-b-ccod-ocod-activation` · IC merges · dirty product trees preserved
