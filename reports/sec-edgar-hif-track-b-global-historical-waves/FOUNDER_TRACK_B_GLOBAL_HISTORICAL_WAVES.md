# HIF Track B — Global historical contractual waves (B1–B4)

**As of:** 2026-11-21  
**Stream:** Track B (`ws-hif-v6-track-b` · `feat/hif-v6-track-b-global-historical-waves`)  
**Publication:** **CUSTOMER_BLOCKED**  
**Research cost:** **$0** (SEC EDGAR, FibraHotel annual report, CVPAR fato relevante, Atlantica pages; no metered research)  
**E2E classification:** **PERSISTENCE_VALIDATED** (creates, read-back, idempotent re-apply) — not FULL_E2E and not CUSTOMER_PUBLISHED  
**Merge:** Integration Controller only

Prior hotel-level foundation (Yorkville Royal Sonesta, Sonesta ES Thornhill, Hilton Garden Inn São Paulo Rebouças) remains from merged PR **#539**. This wave adds evidence and does not overwrite those rows.

---

## 1. Unique hotels researched

| Set | Count |
|-----|------:|
| Hotel-level identities in this wave | **31** |
| Of which new versus PR #539 | **30** (Rebouças is an additional finding on the existing discovery id) |
| SEC schedule rows quarantined as artifacts | 3 (portfolio/mortgage glue, bare “Hotel Indigo”) |
| Portfolio findings staged, not written as hotels | 5 |
| Cumulative Track B hotel identities | **33** (3 from #539 + 30 new) |
| HPC unique name+country matches | 7 |
| HPC ambiguous | 4 |
| Not in HPC | 20 |
| HPC records created | **0** |

## 2. New and cumulative contractual events

| Wave | What ran | Hotel-level rows |
|------|----------|-----------------:|
| B1 | Name-free 424H (14 docs) then operating/management 424B2 (8 docs). Dedup kept. Combined columns stayed unresolved. | 18 |
| B2 | Mexico leases and management terminations; Brazil Oscar Freire brand change; Rebouças official confirmation | 6 |
| B3 | Playa 10-K named management agreements in Mexico, Dominican Republic | 4 |
| B4 | Brand/operator succession (Punta Cana, Tijuana, Rebouças fato relevante) | 3 |

Raw SEC net-new before quarantine: 5 (424H) + 16 (424B2). 424H trailing yield fell to 0 and duplicate skips were 316, so that query is largely exhausted. The operating/management 424B2 query was still productive and still unresolved as to franchise versus management.

## 3. Agreement type breakdown (31 hotel rows)

| Type | Hotels | How typed |
|------|-------:|-----------|
| MANAGEMENT | 7 | Source says “management agreement” |
| LEASE | 3 | Source says arrendamiento / lease |
| UNRESOLVED | 21 | Combined CMBS column, or a brand change with no instrument label |

No row was typed franchise from a combined license/franchise/operating column. Loan maturities in the FibraHotel report were not stored as hotel expirations.

## 4. Expiration and decision timing

Timing is the observed date class. It is not commercial availability.

| Country | Type | Timing | Hotels |
|---------|------|--------|-------:|
| MX | MANAGEMENT | Historical exact day (2023-12-15) | 2 |
| MX | MANAGEMENT | Month or quarter only | 3 |
| MX | LEASE | Expiration not disclosed | 3 |
| MX | UNRESOLVED | Brand change, no term | 1 |
| DO | MANAGEMENT | Quarter/month only | 2 |
| BR | UNRESOLVED | Brand change, no term | 2 |
| US | UNRESOLVED | Historical schedule date | 4 |
| US | UNRESOLVED | Within 24 months | 1 |
| US | UNRESOLVED | 24–60 months | 2 |
| US | UNRESOLVED | Beyond 60 months | 11 |

The single US date inside 24 months is Courtyard Tulsa, 2027-05-09, taken from a **2017** 424H. It is not an active opportunity.

## 5. Historical versus current-status

| Class | Hotels |
|-------|-------:|
| HISTORICALLY_VERIFIED_CURRENT_UNKNOWN | 25 |
| BRAND_CHANGED | 4 |
| CONFLICTED | 2 (InHouse León and Ciudad Obregón lease execution dates disagree inside the same FibraHotel report) |
| Lack of a newer filing treated as proof of currency | **0** |

Current-status corroboration that was actually retrieved:

- Quality Suites Oscar Freire still listed on Atlantica’s own hotel page (historical Comfort Suites conversion announced 2021-07-05).
- Hilton Garden Inn Tijuana Aeropuerto: Gamma Tijuana closed August 2025 and the 2025 report says it opened as Hilton Garden Inn in March 2026. Franchise versus management was not stated, so no contract row.
- Rebouças: CVPAR fato relevante dated 2026-05-18 confirms Átrio / Grand Mercure replaced Atlantica / Hilton Garden Inn. New finding only. No expiration invented.

## 6. Mexico and Brazil depth

Mexico used the FibraHotel 2025 integrated report (Spanish) and the Playa 10-K.

Hotel-level Mexico:

- Hyatt Ziva Riviera Cancún and Hyatt Zilara Riviera Maya — management agreements terminated effective **2023-12-15**. The $6.5 million fee is stated for both resorts together.
- The Yucatán Playa del Carmen (Tapestry) — management agreement terminated **February 2025** (month precision only). HPC match: “The Yucatan Playa del Carmen, Tapestry Collection by Hilton”.
- Kimpton Tres Ríos and Paraiso de la Bonita — management agreements with quarter-level commencement. Paraiso matched in HPC.
- InHouse León and InHouse Ciudad Obregón — leases with Operadora Hotelera InHouse. Both matched in HPC. Execution dates conflict, so no commencement was stored and no contract row was written.
- Fiesta Americana Condesa Cancún — lease, matched in HPC, no expiration.
- Hilton Garden Inn Tijuana Aeropuerto — brand change from Gamma, not in HPC under the new name.

Brazil:

- Quality Suites Oscar Freire (from Comfort Suites), São Paulo. HPC name search was ambiguous, so no link.
- Rebouças official CVM/B3 fato relevante on the existing discovery record. HPC match “Hilton Garden Inn Reboucas” was not written onto the existing inventory row (no-op, non-destructive).

FibraHotel’s portfolio operator list and fee terms were staged. They do not name hotel expirations.

## 7. UK, Canada, and other international progress

| Market | This wave |
|--------|-----------|
| Canada | No new named hotel. Prior Sonesta HMAs and the staged Choice Canada MFA stand. |
| United Kingdom | IHG’s 10 March 2005 sale of 73 hotels, with 20-year management on 63 core hotels, is staged. Hotel names were not in the filing. Vintage 2005; later supersession was not retrieved. |
| Dominican Republic | Wyndham Alltra Samaná (HPC match) and Wyndham Alltra Punta Cana (formerly Jewel; HPC ambiguous). |
| Jamaica | No new hotel-level instrument. |
| Europe | No hotel-level instrument. IHG’s 2025 20-F describes the franchise/management system and was not turned into hotel expirations. |
| United States | 18 CMBS schedule hotels, agreement type unresolved. Service Properties Trust’s Hyatt 17-hotel agreement (expires 2031-03-31) and its unnamed single IHG hotel (expires 2026-01-31, one-year option) are staged because the hotels are not named. |

Hyatt’s 30 June 2025 release of 50-year management agreements for 13 of 15 Playa resorts is staged. The release does not assign that term to named hotels.

## 8. Source vintage and supersession gaps

| Vintage | Hotel rows |
|---------|----------:|
| 2017 | 3 |
| 2019 | 10 |
| 2021 | 4 |
| 2024 | 2 |
| 2025 | 11 |
| 2026 | 1 |

Gaps left open on purpose:

- 2017–2021 CMBS dates have no successor filing in this wave.
- Playa management footnotes stop at the 2024 10-K. Hyatt closed the Playa acquisition on 17 June 2025. That transaction was not applied as an automatic supersession of third-party management agreements.
- InHouse lease dates conflict inside one report.
- Tijuana and Oscar Freire have brand evidence and no term, renewal, or termination clause.
- UK 2005 management terms are historical portfolio evidence only.

## 9. Country × timing × agreement type

See section 4. Full machine matrix: `MATRIX_COUNTRY_TIMING_TYPE.json`.

## 10. Airtable persistence ledger

Base `appCCUsuGsE1ifoLk`. No schema changes. No HPC creates. Track A Lakeville ids were not written.

| Phase | Creates | No-ops | Failures | Staged |
|-------|--------:|-------:|---------:|-------:|
| Dry-run | 73 planned | 1 | 0 | 33 |
| First apply | **73** | 3 | 0 | 33 |
| Re-apply | **0** | **76** | 0 | 33 |

First-apply breakdown: Inventory 29 · Findings 31 · Contracts 8 · Milestones 2 · Organizations 1 · Relationships 2 · Graph Evidence 0.

The two milestones are the exact-day Hyatt Ziva / Hyatt Zilara terminations (2023-12-15). Month-only and quarter-only dates were not written into date fields.

Contracts were written only for resolved MANAGEMENT or LEASE. The two conflicted InHouse leases have findings and relationships, not contract rows.

Relationships use the existing **Operator** select for Operadora Hotelera InHouse on the two matched InHouse hotels. `Current?` was left unset. Graph Evidence was not written: live Entity Type choices are Organization, Hotel–Org Relationship, contacts, and Other, with no contract/finding type. Evidence stays on the finding and contract rows.

Read-back: `Publication State = CUSTOMER_BLOCKED`.

Ledgers: `PERSISTENCE_LEDGER_DRY_RUN.json`, `PERSISTENCE_LEDGER_FIRST_APPLY.json`, `PERSISTENCE_LEDGER_REAPPLY.json`.

## 11. Source-level yield and cost

| Source | Docs or reports | Yield | Cost |
|--------|----------------:|-------|-----:|
| SEC 424H related-license continuation | 14 fetched, 11 extractable | 5 net-new, 3 kept as hotels, 316 duplicate skips, trailing yield 0 | $0 |
| SEC 424B2 operating/management column | 8 fetched, 7 extractable | 16 net-new, 15 kept as hotels, type unresolved, trailing yield 3.2 | $0 |
| Playa 10-K and note R11 | 2 | 7 named management events | $0 |
| FibraHotel Informe Anual 2025 | 1 | 4 hotel-level lease/brand events; portfolio staged | $0 |
| CVPAR fato relevante 2026-05-18 | 1 | 1 corroborating finding on Rebouças | $0 |
| Atlantica announcement + current hotel page | 2 | 1 São Paulo brand change | $0 |
| IHG 2005 6-K, Hyatt 2025 release, SVC 2025 note | 3 | staged portfolios only | $0 |

**Total research cost: $0.**

## 12. Tests, branch, and PR

| Item | Value |
|------|-------|
| Test | `npm run test:hif-track-b-global-historical-waves` |
| Branch | `feat/hif-v6-track-b-global-historical-waves` |
| PR | **[#543](https://github.com/joandejarden-create/my-operators-backend/pull/543)** OPEN |
| Workspace | `C:\Dev\dealality\ws-hif-v6-track-b` |
| HEAD at PR open | `56ffd63fc` |
| Base | `origin/main` moved 28 commits during the wave. This branch was not merged with `main`. |
| Untracked file preserved | `tmp/ic-fold-519-audit.mjs` (not in the commit) |
| Merge | Integration Controller |

---

## Final status

**AWAITING_INTEGRATION_CONTROLLER**

CUSTOMER_BLOCKED. No production deployment. No customer publication. Local `main` was not switched or reset.
