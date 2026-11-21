# Track B — Historical Intelligence V2

Publication state: `CUSTOMER_BLOCKED`. Research cost: `$0`. No paid providers. No HPC creates. No schema changes. No Graph Evidence writes. No weekly scheduler. Integration Controller owns the merge.

E2E classification: `PERSISTENCE_VALIDATED`. This is not `FULL_E2E_VALIDATED` and not `CUSTOMER_PUBLISHED`.

## 1. Hotels researched this cycle

57 distinct hotels, 58 contractual or current-status events.

| Set | Hotels | Events |
| --- | ---: | ---: |
| Sonesta EX-10.3 schedule | 51 | 51 |
| Mexico named events | 6 | 7 |
| Total | 57 | 58 |

The Sonesta schedule is 48 United States hotels, 2 Canada hotels already in inventory, and Royal Sonesta San Juan. One schedule sentence, The Allegro Royal Sonesta Hotel, was split across lines in the exhibit HTML and was not persisted.

## 2. New contractual intelligence by agreement type

| Agreement type | What was stored |
| --- | --- |
| MANAGEMENT | 51 amended hotel management agreements expiring 2043-06-30, plus Hilton Garden Inn Tijuana Aeropuerto managed by Grupo Posadas as of 2026-06-30, with no expiration stated |
| FRANCHISE | Fairfield Inn & Suites Villahermosa franchise terminated 2026-07-01 |
| OTHER | Same Villahermosa sentence's `contrato de operación`, stored as OTHER because the schema has no OPERATING type. It was not classified as MANAGEMENT |
| LEASE | Not stored. The InHouse Villahermosa lease is prospective (`iniciará`) |
| LICENSE / CONCESSION | None new |

The Villahermosa sentence names franchise and operating contracts together. They are two rows. The lease in the same sentence was not treated as executed.

## 3. Hotel counts versus event counts

57 hotels, 58 events. The extra event is Villahermosa's second instrument. Prior findings on the same hotels were not overwritten.

## 4. Mexico and Brazil

Mexico, from the FibraHotel Q2 2026 report dated 15 July 2026:

- Hilton Garden Inn Tijuana Aeropuerto is in the managed section with operador Grupo Posadas, 140 rooms. Reopening on 2026-03-05 is not stored as a contract commencement. HPC matched `Hilton Garden Inn Tijuana Airport`. The prior Gamma finding is unchanged.
- Fairfield Inn & Suites Villahermosa is the only limited-service Villahermosa hotel in that portfolio. Franchise and operating contracts ended 2026-07-01. InHouse lease not commenced. Not in HPC.
- Real Inn Mexicali closed 2026-05-31 and is scheduled to reopen in Q3 2026 as Holiday Inn operated by Aimbridge. The quarter is not stored as a day. Fiesta Inn Mexicali stays a separate hotel. No contract row.
- The Yucatán Resort Playa del Carmen suspended operations on 2026-04-30. The report's heading is temporary closure. No successor manager. This hotel is not Hyatt Vivid Playa del Carmen. HPC matched the existing Tapestry record. The February 2025 termination finding is unchanged. A trade article's 2025-02-05 date was not stored as an exact milestone.

Brazil:

- Quality Suites Oscar Freire was not re-fetched. The operator page timed out. No new row. The 2021 conversion finding was not overwritten, and no contradiction was established this cycle.
- Grand Mercure Rebouças had no newer instrument than the 18 May 2026 CVPAR fato relevante. That timeline was left as stored.

## 5. United Kingdom, Canada, and other countries

Canada: The Yorkville Royal Sonesta and Sonesta ES Suites Toronto Thornhill are on the same 25 September 2026 SEC exhibit. Their amended expiration is 2043-06-30. No newer amendment was found. These are not near-term opportunities. Existing inventory rows were no-ops. New findings, contracts, and milestones were added.

United Kingdom: no new named hotel agreement this cycle.

Puerto Rico: Royal Sonesta San Juan, operator Sonesta San Juan LLC, owner SVC San Juan TRS LLC, same amended expiration. HPC matched `The Royal Sonesta San Juan`.

United States: 48 additional named Sonesta management agreements from the same exhibit, each with address, operator, owner, commencement 2025-08-01, and expiration 2043-06-30.

The SVC 10-Q also says eight Sonesta hotels (1,509 keys) were sold in October 2026 and one more (300 keys) is being marketed. The hotels are not named, so they were not given hotel rows. A seven-hotel Radisson portfolio agreement expiring 2031-07-31 is likewise unnamed and was not expanded into hotels.

## 6. Source vintage and current status

| Source | Vintage | Use |
| --- | --- | --- |
| SEC EX-10.3, filing date 2026-11-04, amendment dated 2026-09-25 | 2026 | Named HMA schedule and 2043-06-30 expiration |
| SVC 10-Q quarter ended 2026-09-30 | 2026 | Extension was in lieu of a termination fee stated as 11,035 in a section denominated in thousands of dollars. August 2025 agreements had expired 2040-07-31 with two 10-year options. Option exercise is not recorded |
| FibraHotel Q2 report, 2026-07-15 | 2026 | Mexico events as of 2026-06-30, plus the 2026-07-01 Villahermosa terminations and the 2026-04-30 Yucatán suspension |
| Hotel Investment Today, 2025-06-30 | 2025 | Brand mention only for the two Riviera Hyatt hotels. Not a management agreement |

Current-status classes on the new rows: `AGREEMENT_EXTENDED_OR_AMENDED` for the 51 Sonesta hotels, `CURRENTLY_CORROBORATED` for Tijuana's managed operator, `SUPERSEDED` for the two Villahermosa terminations, `BRAND_CHANGED` for Mexicali, and `HISTORICALLY_VERIFIED_CURRENT_UNKNOWN` for Yucatán and both Riviera Hyatt successor-manager questions. Absence of a newer filing was not treated as proof that an old agreement is still in force.

## 7. Relationship timelines

- Tijuana: Gamma brand finding retained. As of 2026-06-30 the managed operator is Grupo Posadas under Hilton Garden Inn. December 2025 had listed the operator as Pendiente while closed.
- Villahermosa: Marriott International was the portfolio operador at 2026-06-30. Franchise and operating contracts ended the next day. InHouse lease announced, not stored.
- Mexicali: Real Inn closed 2026-05-31. Planned brand Holiday Inn. Planned operator Aimbridge. Still closed at the report date.
- Yucatán: Playa management termination in February 2025 stands. Operations suspended 2026-04-30. Successor brand and operator unknown.
- Hyatt Ziva Riviera Cancún and Hyatt Zilara Riviera Maya: management termination 2023-12-15 stands. They are not part of the 13-of-15 fifty-year Hyatt–Tortuga agreements. Successor manager is not in a primary instrument.
- Toronto Sonesta hotels: prior secondary citation of the 2043 extension is now backed by the SEC exhibit. The expiration was not duplicated as a second term.
- Rebouças and Oscar Freire: prior timelines retained.

## 8. Track A handoff

Qualified commercial signals: **0**.

Villahermosa, Mexicali, and Tijuana have named successor or current operators. The Sonesta expirations are 2043. The Yucatán suspension and the Riviera Hyatt terminations do not show a prospective decision a third party could pursue. None were created as Track A opportunities. See `TRACK_A_HANDOFF.json`.

## 9. Country, timing, agreement type, freshness

| Country | Hotels | Events | Agreement types | Timing | Freshness |
| --- | ---: | ---: | --- | --- | --- |
| US | 48 | 48 | MANAGEMENT 48 | Beyond 60 months | Exhibit filed 2026-11-04 |
| CA | 2 | 2 | MANAGEMENT 2 | Beyond 60 months | Same exhibit |
| PR | 1 | 1 | MANAGEMENT 1 | Beyond 60 months | Same exhibit |
| MX | 6 | 7 | FRANCHISE 1, OTHER 1, MANAGEMENT 1, unresolved 4 | Two historical dates; five undated or imprecise | Issuer report 2026-07-15 |
| GB | 0 | 0 | — | — | No new hotel-level source |
| BR | 0 new | 0 new | — | — | Prior rows kept; live page not retrieved |

Unknown coverage denominators remain. A thin local result is not an absence of agreements. HPC matches this cycle: 3 unique (San Juan, Tijuana Airport, Yucatán Tapestry), 1 ambiguous (Hyatt Zilara Riviera Maya), the rest not in HPC. Existing inventory rows were not rewritten to add HPC links.

## 10. Airtable

Base `appCCUsuGsE1ifoLk`. First apply, then a reapply.

| | First apply | Reapply |
| --- | ---: | ---: |
| Creates | 220 | 0 |
| Inventory | 51 | 0 |
| Findings | 58 | 0 |
| Contracts | 54 | 0 |
| Milestones | 53 | 0 |
| Organizations | 2 | 0 |
| Relationships | 2 | 0 |
| Graph Evidence | 0 | 0 |
| No-ops | 7 | 227 |
| Conflicts | 0 | 0 |
| Failures | 0 | 0 |

Organizations: Sonesta San Juan LLC and Grupo Posadas, type Operator, `CUSTOMER_BLOCKED`. Relationships link those organizations to the matched HPC hotels. Current? was left unset. Read-back publication state on created rows is `CUSTOMER_BLOCKED`.

Staged and not written as contracts: Mexicali, Yucatán suspension, and the two Riviera Hyatt successor notes. Graph Evidence was not written because the live Entity Type choices still do not include a contract or finding type.

## 11. Yield and spending

| Source family | Investigated | Net new |
| --- | --- | --- |
| SEC 424B2 operating query, pages starting at 2 | 10 documents, 4 extractable | 0 hotels. Trailing yield 0. That page window is exhausted |
| SEC 424H | Not rerun | Prior wave already had about 316 duplicate skips |
| SEC EX-10.3 Sonesta schedule | 1 exhibit, 51 parsed hotels | 51 management amendments |
| FibraHotel Q2 2026 | 1 issuer report | 6 hotels, 7 events |
| Atlantica Oscar Freire page | 1 attempt | Timeout. No row |
| Spend | — | $0 |

## 12. Tests, branch, and handoff

`npm run test:hif-track-b-global-historical-waves` — PASS.

Branch: `feat/hif-v6-track-b-historical-intelligence-v2`, created from `origin/main` at `81425a067` after PR #543 merged. Local `main` was not switched or reset. `tmp/ic-fold-519-audit.mjs` was not staged.

Integration Controller merges. This cycle is `AWAITING_INTEGRATION_CONTROLLER` until that merge.

## 13. Next research wave

Recover The Allegro Royal Sonesta from the split exhibit line. Retry Quality Suites Oscar Freire when the operator site responds. Look for a named United Kingdom hotel agreement. Keep searching for a successor manager at The Yucatán after the 30 April 2026 suspension. Do not rerun the exhausted 424H query or the zero-yield 424B2 page window. Do not turn the 2043 Sonesta expirations into commercial opportunities.
