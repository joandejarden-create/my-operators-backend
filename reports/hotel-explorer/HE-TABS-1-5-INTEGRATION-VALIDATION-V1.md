# HE Tabs 1–5 — Integration validation (post Gate 1 approval)

**Status:** INTEGRATION VALIDATED (Airtable-backed Bethesda on `:8091`)  
**Paid research:** still **BLOCKED** — awaiting founder approval of recommended scope below  
**Runtime:** `http://127.0.0.1:8091` (`C:\Dev\dealality\ws-hotel-explorer`)  
**Audited:** 2026-11-19  
**Branch:** `feat/he-tabs-1-5-airtable-first-e2e-v1`

## Architecture (confirmed)

| Layer | Role |
|---|---|
| KGPV golden demo | UX gold reference only — not Airtable proof |
| Hotel Property Census | Canonical property facts for Bethesda (`recLuxvwwxID7U2B8`) |
| HI Organizations + HI Hotel–Org Relationships | Canonical org / operator / brand edges |
| Fixtures / local showcase | Not verified production intelligence |

## P0 / P1 delivery

| # | Item | Result |
|---|---|---|
| 1 | HPC vs Census ownership via `RECORD_ID()` | **PASS** — Bethesda `owningTableKind=HPC` |
| 2 | Brand-presence formatter + source label | **PASS** — `censusSourceTable=Hotel Property Census`; Parent Company stays `null` when HPC empty |
| 3 | Organization / Relationships ← HI graph | **PASS** — Marriott International; 2 Validated edges (Operator + Brand) |
| 4 | No unverified fixture fallback on HPC | **PASS** — Mexico fixture overlay scoped to Hotel Census–owned demos only |
| 5 | Portfolio from existing graph | **PASS** — Known Hotels=1 (focus hotel); rooms/markets blank until broader graph |
| 6 | Contacts HPC fallback | **PASS** — Contact tab shows property phone + website with `HPC_PROPERTY_FALLBACK` provenance |
| 7 | KGPV Census↔HPC identity strategy | **Documented** — no blind migrate (`HE-TABS-1-5-KGPV-CENSUS-HPC-IDENTITY-STRATEGY-V1.md`) |
| 8 | Provenance / evidence status | **PASS** — graph + HPC fallback labeled; economic owner remains unverified |

## Browser validation (golden demo HE chrome on `:8091`)

URL: `http://127.0.0.1:8091/hotel-intelligence-golden-demo.html?hotelExplorer=recLuxvwwxID7U2B8`

| Tab | Observed |
|---|---|
| 1 Hotel Overview | Bethesda Marriott · 407 rooms · Upper Upscale · Marriott Hotels · Operator Marriott International · Open · live address/coords |
| 2 Ownership | HI graph Ownership & Control; economic owner not verified; HPC property contact fallback phone/website |
| 3 Contact Intelligence | **Property contacts (HPC fallback)** — `+1 301-897-9400` + Marriott property URL; explicit no CI write-on-GET note |
| 4 Organization & Portfolio | Marriott International org; Known Hotels **1**; Owned **0**; Managed/Operated **1**; ownership not implied |
| 5 Relationships | All (2) · Operated (1) · Branded (1); economic owner/PropCo held back when unverified |

Authenticated `/app#/opportunity-radar` remains Memberstack-gated in this session; Airtable-backed payloads and golden-demo HE chrome validated on `:8091`.

## Recalculated Gate 1 metrics

| Metric | Value |
|---|---:|
| KGPV golden-demo field coverage | 77.6% |
| Bethesda existing Airtable coverage | 57.1% |
| Bethesda Airtable not yet connected | 15.9% |
| Bethesda **live Airtable→UI** | **38.1%** |
| Genuine research gaps | 25.4% |
| UI without verified HPC field | 6.3% |
| Integration actions remaining | 17 |
| Research gap count | 16 |

Artifacts: `HE-TABS-1-5-GATE1-KGPV-GOLD-BETH-AIRTABLE-METRICS-V1.json`, `HE-TABS-1-5-GATE1-FOUNDER-REVIEW-V1.md`

## Persistence / mutation / isolation

- No competing property store; HI holds relationship edges only.
- Fixtures not migrated into Airtable.
- Contact Intelligence GET paths: affinity to HE worktree; no write-on-read for hotelGet (tests PASS).
- HE runtime isolation: port **8091** + `DEALALITY_ALLOW_WORKTREE_SERVER=1`.
- ADP / HPC product paths and tabs 6–9 left untouched in this cycle.

## Tests run

- `npm run test:he-property-census-identity-v1` — PASS (live)
- `npm run test:brand-presence-census-field-aliases-v1` — PASS
- `npm run test:he-contact-intelligence-data-root-affinity-v1` — PASS
- `npm run test:contact-intelligence-v1` — PASS
- `npm run gate1:he-tabs-1-5-kgpv-gold-beth-metrics` — PASS

## Remaining genuine research gaps (still blocked)

Economic owner / PropCo / UBO, decision-maker people + emails, evidence bibliography depth, broader Marriott portfolio beyond focus hotel edge, submarket census map load for Bethesda corridor.

### Recommended targeted research (request founder approval)

1. **Free first:** Maryland SDAT / public property records for current economic owner of 5151 Pooks Hill Rd (no paid spend).
2. **If blocked:** one paid ownership enrichment package **≤ $25**, scoped to Bethesda Marriott ultimate/economic owner + PropCo only.
3. **Do not authorize** broad Surfe / multi-hotel person enrichment until ownership identity is resolved.

## Git note

Dirty feature branch with integration code + Gate 1 reports. Behind `origin/main` by 3 / ahead 2 vs remote feature tip. Ready to commit focused HE files and open/update PR on founder request — Integration Controller owns merge.

## Success criterion

**Met for Airtable reuse objective:** Bethesda Marriott tabs 1–5 retrieve verified HPC + HI graph information on `:8091`, with KGPV remaining the UX depth reference and paid research still blocked.
