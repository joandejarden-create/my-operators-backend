# Track B — Historical Intelligence V3

Publication state: `CUSTOMER_BLOCKED`. Research cost: `$0`. No paid providers. No HPC creates. No schema changes. No Graph Evidence writes. No weekly scheduler.

E2E classification: `PERSISTENCE_VALIDATED`. Not `FULL_E2E_VALIDATED`. Not `CUSTOMER_PUBLISHED`.

## Safe resume

- Workspace: `C:\Dev\dealality\ws-hif-v6-track-b`
- PR #546: **OPEN**, CI green, awaiting Integration Controller. Not assumed merged.
- Successor branch: `feat/hif-v6-track-b-historical-intelligence-v3` created from the V2 HEAD to preserve #546 commits.
- `tmp/ic-fold-519-audit.mjs` preserved. Local `main` not reset.
- `origin/main` tip at resume included Track A merges after #543; this branch remains based on #546 work and does not merge main.

## 1. Documents investigated

Approximately **82** contractual source documents and listings reviewed under the document-research objective (capacity-bound, not a ceiling):

| Family | Docs | Yield |
| --- | ---: | --- |
| SEC management discovery pass (EX-10 / 8-K / 10-K) | 40 | 331 raw extractions; **0** persist-eligible after QA (noise/duplicates) |
| Curated exhibits and issuer/rating reports | 12 | Allegro, Sotherly, FibraHotel/HR Ratings, Choice, Hyatt Investor Day |
| IHG UK corporate filings | 3 | 0 named UK hotels |
| Park / Ashford / CMCT loan-or-portfolio instruments | 3 | staged only |
| EFTS hit listings reviewed | 24 | led to curated exhibit selection |

## 2–3. Hotels and contractual events

18 distinct hotels / 18 events persisted this cycle.

| Set | Hotels | Events |
| --- | ---: | ---: |
| Allegro Royal Sonesta recovery | 1 | 1 |
| Sotherly / Our Town named HMAs | 10 | 10 |
| Mexico lease / status recovery | 6 | 6 |
| Brazil Oscar Freire confirmation | 1 | 1 |

## 4. Country and agreement-type distribution

| Country | Hotels | Agreement types |
| --- | ---: | --- |
| US | 11 | MANAGEMENT 11 |
| Mexico | 6 | LEASE 2, unresolved status rows 4 |
| Brazil | 1 | Brand current-status only |
| United Kingdom | 0 new named | Portfolio-only staged |

## 5. Expiration and decision timing

- Allegro / Sonesta amended expiration: **2043-06-30** (beyond 60 months). Not commercially actionable.
- Sotherly Current Hotel HMAs: **2035-03-31**, with two mutual five-year renewals and a **90-day** manager renewal notice / **30-day** lessee response.
- Villahermosa InHouse lease commencement: **2026-07-01**.
- Ciudad del Carmen InHouse lease commencement: **2026-04-01**.
- Yucatán: suspension **2026-04-30** with owner intent to move into the lease portfolio; lessee not named.

## 6. Current-status and freshness

| Class | Count |
| --- | ---: |
| AGREEMENT_EXTENDED_OR_AMENDED | 11 |
| BRAND_CHANGED | 3 |
| HISTORICALLY_VERIFIED_CURRENT_UNKNOWN | 3 |
| CURRENTLY_CORROBORATED | 1 |

Sources dated 2024–2026. Absence of a newer filing was not treated as proof an old agreement remains in force.

## 7. Portfolio versus hotel-level

- **Hotel-level persisted:** Allegro; ten Sotherly wholly-owned hotels; Villahermosa and Ciudad del Carmen leases.
- **Portfolio staged:** Ashford master HMA fee amendment (no hotel schedule); Park Hotels 5–30 year management / 10–20 year franchise ranges; IHG UK unnamed managed hotels; Sotherly condo rental programs (Hyde Beach House / Lyfe); Sheraton Grand Sacramento HMA referenced only inside a loan agreement (terms not inferred from the loan).

## 8. Track A research candidates

Qualified opportunities: **0**.

Research candidates: **1**

- The Yucatán Resort Playa del Carmen — FibraHotel suspended operations on 2026-04-30 with the objective of including the hotel in its lease portfolio. No named lessee. Classification: `RESEARCH_CANDIDATE`. See `TRACK_A_HANDOFF.json`.

Distant Sonesta / Sotherly expirations were not handed as opportunities.

## 9. Track C identity handoffs

- Hyatt Zilara Riviera Maya: `AMBIGUOUS_HPC_IDENTITY`.
- Hyatt Ziva Riviera Cancún: no unique HPC match; not in the Tortuga 13-of-14 owned set.
- Several Sotherly hotels: not in HPC; no HPC create requested.

## 10. Source-family productivity

Highest quality yield: **Sotherly EX-10.22** (10 named HMAs + verified notice periods) and **SVC EX-10.3 Allegro recovery**. Strong current-status yield: **HR Ratings FibraHotel** (two lease commencements + Yucatán lease intent). Lowest persist yield after QA: automated 8-K name extraction (255 duplicates / artifact names).

## 11–12. Airtable and idempotency

Base `appCCUsuGsE1ifoLk`.

| | First apply | Reapply |
| --- | ---: | ---: |
| Creates | 56 | 0 |
| Inventory | 12 | 0 |
| Findings | 18 | 0 |
| Contracts | 13 | 0 |
| Milestones | 13 | 0 |
| Organizations / relationships / evidence | 0 / 0 / 0 | 0 |
| No-ops | 6 | 62 |
| Conflicts / failures | 0 / 0 | 0 / 0 |

Read-back publication state: `CUSTOMER_BLOCKED`. Graph Evidence not written. Org/relationship rows require unique HPC matches; none applied this cycle for the new counterparties.

## 13. Spending

`$0`.

## 14. Tests, branch, PR

- `npm run test:hif-track-b-global-historical-waves` — PASS
- Branch: `feat/hif-v6-track-b-historical-intelligence-v3`
- This PR supersedes open PR #546 by including its commit plus V3 research.
- Integration Controller owns the merge.

## 15. Stop reason and checkpoint

Stopped after curated high-yield exhibits were persisted and the noisy automated 8-K extractor was quarantined. Approximate document count **82** of the ~100 research objective.

Resumable checkpoint:

- Do not rerun exhausted 424H or the zero-yield 424B2 page window.
- Do not bulk-persist `reports/sec-edgar-hif-v6-management-discovery/INCREMENTAL.json` without hotel-name QA.
- Next high-yield candidates: retrieve the actual Sheraton Grand Sacramento management exhibit (not the loan); sample Ashford hotel schedules if a named addendum exists; continue Mexico issuer Q3 primary PDF when BMV serves the file; UK named hotel exhibits outside IHG portfolio totals.

## 16. Recommended next waves

1. Named EX-10 schedules with hotel-level parties and dates (Ashford / other REIT masters that publish Exhibit A lists).
2. FibraHotel / Fibra Inn Q3 primary issuer PDFs for Mexicali reopen day and any new lease counterparties.
3. Brazil CVM fatos relevantes for named management/franchise changes.
4. UK hospitality REIT or Companies House filings that name individual managed hotels.
