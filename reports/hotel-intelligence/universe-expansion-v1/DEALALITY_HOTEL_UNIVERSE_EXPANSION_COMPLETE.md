# DEALALITY_HOTEL_UNIVERSE_EXPANSION_COMPLETE

**Generated:** 2026-08-10T10:23:21.288Z  
**Airtable writes:** **0** (locked)  
**Enrichment:** not run (discovery only)

## Previous Audits Reused

HOTEL_UNIVERSE_PREVIOUS_AUDITS_FOUND: **7**

| Audit | Path | Drove |
| --- | --- | --- |
| full-cala-geography-coverage-registry-audit | `docs/data-intelligence/full-cala-geography-coverage-registry-audit.md` | Zero-record geographies, HOLD concentration, source-gap priority queue |
| full-cala-15k-shell-universe-exhausted | `docs/data-intelligence/full-cala-15k-shell-universe-exhausted.md` | Stop at 5956; next unresolved pool = Brazil; HBX-safe candidates exhausted |
| full-cala-15k-shell-orchestrator-final | `reports/research-engine-v2/full-cala-15k-shell-orchestrator-final.json` | 9,630 weak holds by country (Brazil 4,842); no production inserts remaining |
| full-cala-15k-source-inventory | `reports/research-engine-v2/full-cala-15k-source-inventory.md` | Cvent ~14k + HBX Wave1 3,385 candidate sources |
| full-cala-hbx-geography-discovery-final | `reports/research-engine-v2/full-cala-hbx-geography-discovery-final.md` | 47 non-Wave1 geographies HBX-blocked (HTTP 403) — cannot rely on HBX for Brazil+ |
| full-cala-15k-census-shell-insert-v1 | `docs/data-intelligence/full-cala-15k-census-shell-insert-v1.md` | Eligible shell plan ~10.7k; quality gate held Cvent-only missing-city |
| holds-ledger | `data/research-engine-v2/full-cala-15k-shell-orchestrator/holds-ledger.json` | Reopenable discovery pool (cvent_only_missing_city dominant) |

### Current coverage (live)
- Hotels in Dealality: **5956**
- Known-source expected (Cvent/HBX/holds upper bound): **16168**
- Largest gaps: Brazil (~4842); Argentina (~788); Chile (~311); Peru (~303); Jamaica (~170)
- Countries needing expansion: zero-record + POOR/PARTIAL flags (see scorecard)
- Prior recommended priorities: geography audit top-10 source gaps + orchestrator **Brazil** next pool

### Do not repeat
- Wave1 HBX shell inserts already applied (MX/DO/CO/CR/PA safe HBX pool exhausted)
- Do not re-run HBX geography discovery until credentials/licensing fix (403)

## Coverage Scorecard

Sorted worst → best (priority then gap).

| Country | Hotels in Dealality | Expected approx | Coverage % | Confidence | Priority | Flag |
| --- | ---: | ---: | ---: | --- | ---: | --- |
| Brazil | 494 | 5336 | 9.3% | medium | 2 | POOR |
| Argentina | 129 | 917 | 14.1% | medium | 2 | POOR |
| Chile | 65 | 376 | 17.3% | medium | 2 | POOR |
| Peru | 57 | 360 | 15.8% | medium | 2 | POOR |
| Jamaica | 78 | 248 | 31.5% | medium | 2 | POOR |
| Belize | 12 | 130 | 9.2% | medium | 2 | POOR |
| Saint Barthélemy | 0 | 108 | 0% | medium | 2 | POOR |
| Paraguay | 0 | 102 | 0% | medium | 2 | POOR |
| Saint Martin | 0 | 100 | 0% | medium | 2 | POOR |
| Sint Maarten | 0 | 100 | 0% | medium | 2 | POOR |
| Ecuador | 31 | 120 | 25.8% | medium | 2 | POOR |
| Bahamas | 9 | 94 | 9.6% | medium | 2 | POOR |
| Guatemala | 13 | 87 | 14.9% | medium | 2 | POOR |
| Martinique | 0 | 64 | 0% | medium | 2 | POOR |
| Bolivia | 0 | 58 | 0% | medium | 2 | POOR |
| Turks and Caicos | 0 | 57 | 0% | medium | 2 | POOR |
| Bonaire | 0 | 51 | 0% | medium | 2 | POOR |
| U.S. Virgin Islands | 0 | 51 | 0% | medium | 2 | POOR |
| Montserrat | 0 | 50 | 0% | medium | 2 | POOR |
| Honduras | 12 | 60 | 20% | medium | 2 | POOR |
| Curaçao | 5 | 52 | 9.6% | medium | 2 | POOR |
| Saint Lucia | 3 | 48 | 6.3% | medium | 2 | POOR |
| Nicaragua | 7 | 49 | 14.3% | medium | 2 | POOR |
| Uruguay | 16 | 56 | 28.6% | medium | 2 | POOR |
| Cayman Islands | 7 | 45 | 15.6% | medium | 2 | POOR |
| Venezuela | 0 | 36 | 0% | medium | 2 | POOR |
| Antigua and Barbuda | 5 | 40 | 12.5% | medium | 2 | POOR |
| Aruba | 13 | 45 | 28.9% | medium | 2 | POOR |
| Guadeloupe | 0 | 32 | 0% | medium | 2 | POOR |
| Anguilla | 0 | 25 | 0% | medium | 2 | POOR |
| Saint Vincent and the Grenadines | 0 | 25 | 0% | medium | 2 | POOR |
| Grenada | 4 | 27 | 14.8% | medium | 2 | POOR |
| British Virgin Islands | 3 | 25 | 12% | medium | 2 | POOR |
| El Salvador | 10 | 32 | 31.3% | medium | 2 | POOR |
| Dominica | 2 | 23 | 8.7% | medium | 2 | POOR |
| Haiti | 0 | 19 | 0% | medium | 2 | POOR |
| Guyana | 0 | 16 | 0% | medium | 2 | POOR |
| Saint Kitts and Nevis | 3 | 18 | 16.7% | medium | 2 | POOR |
| Suriname | 0 | 15 | 0% | medium | 2 | POOR |
| Trinidad and Tobago | 7 | 21 | 33.3% | medium | 2 | POOR |
| French Guiana | 0 | 8 | 0% | medium | 2 | POOR |
| Mexico | 2181 | 3614 | 60.3% | high | 3 | PARTIAL |
| Puerto Rico | 49 | 131 | 37.4% | medium | 3 | PARTIAL |
| Barbados | 47 | 102 | 46.1% | medium | 3 | PARTIAL |
| Colombia | 967 | 1266 | 76.4% | high | 4 | GOOD |
| Costa Rica | 748 | 950 | 78.7% | high | 4 | GOOD |
| Dominican Republic | 654 | 654 | 100% | high | 5 | COMPLETE |
| Panama | 325 | 325 | 100% | high | 5 | COMPLETE |
| Bermuda | 0 | — | —% | low | 5 | UNKNOWN |
| Cuba | 0 | — | —% | low | 5 | UNKNOWN |
| Saba | 0 | — | —% | low | 5 | UNKNOWN |
| Sint Eustatius | 0 | — | —% | low | 5 | UNKNOWN |
| Turks and Caicos Islands | 0 | — | —% | low | 5 | UNKNOWN |

## Discovery Queue

| Rank | Tier | Country | Expected gain | Why prioritized |
| ---: | --- | --- | ---: | --- |
| 1 | T2 | Brazil | 4842 | Orchestrator next_unresolved_pool=Brazil; Materially underrepresented (coverage 9.3%, gap ~4842); HBX geography discovery never completed (403 block on non-Wave1); 4842 prior holds — reopen for identity discovery |
| 2 | T1 | Saint Barthélemy | 108 | Country completely missing or zero census records (geography audit); 108 Cvent candidates available to reopen; 6 weak holds staged from prior shell run |
| 3 | T1 | Paraguay | 102 | Country completely missing or zero census records (geography audit); 102 Cvent candidates available to reopen; 61 weak holds staged from prior shell run |
| 4 | T1 | Saint Martin | 100 | Country completely missing or zero census records (geography audit); 100 Cvent candidates available to reopen; 98 weak holds staged from prior shell run |
| 5 | T1 | Sint Maarten | 100 | Country completely missing or zero census records (geography audit); 100 Cvent candidates available to reopen |
| 6 | T1 | Martinique | 64 | Country completely missing or zero census records (geography audit); 64 Cvent candidates available to reopen; 31 weak holds staged from prior shell run |
| 7 | T1 | Bolivia | 58 | Country completely missing or zero census records (geography audit); 58 Cvent candidates available to reopen; 57 weak holds staged from prior shell run |
| 8 | T1 | Turks and Caicos | 57 | Country completely missing or zero census records (geography audit); 57 Cvent candidates available to reopen; 57 weak holds staged from prior shell run |
| 9 | T1 | Bonaire | 51 | Country completely missing or zero census records (geography audit); 51 Cvent candidates available to reopen; 51 weak holds staged from prior shell run |
| 10 | T1 | U.S. Virgin Islands | 51 | Country completely missing or zero census records (geography audit); 51 Cvent candidates available to reopen; 50 weak holds staged from prior shell run |
| 11 | T1 | Montserrat | 50 | Country completely missing or zero census records (geography audit); 50 Cvent candidates available to reopen; 47 weak holds staged from prior shell run |
| 12 | T1 | Venezuela | 36 | Country completely missing or zero census records (geography audit); 36 Cvent candidates available to reopen; 34 weak holds staged from prior shell run |
| 13 | T1 | Guadeloupe | 32 | Country completely missing or zero census records (geography audit); 32 Cvent candidates available to reopen; 32 weak holds staged from prior shell run |
| 14 | T1 | Anguilla | 25 | Country completely missing or zero census records (geography audit); 25 Cvent candidates available to reopen |
| 15 | T1 | Saint Vincent and the Grenadines | 25 | Country completely missing or zero census records (geography audit); 25 Cvent candidates available to reopen; 25 weak holds staged from prior shell run |
| 16 | T1 | Haiti | 19 | Country completely missing or zero census records (geography audit); 19 Cvent candidates available to reopen; 19 weak holds staged from prior shell run |
| 17 | T1 | Guyana | 16 | Country completely missing or zero census records (geography audit); 16 Cvent candidates available to reopen; 16 weak holds staged from prior shell run |
| 18 | T1 | Suriname | 15 | Country completely missing or zero census records (geography audit); 15 Cvent candidates available to reopen; 15 weak holds staged from prior shell run |
| 19 | T1 | French Guiana | 8 | Country completely missing or zero census records (geography audit); 8 Cvent candidates available to reopen; 8 weak holds staged from prior shell run |
| 20 | T1 | Bermuda | 0 | Country completely missing or zero census records (geography audit) |
| 21 | T1 | Cuba | 0 | Country completely missing or zero census records (geography audit) |
| 22 | T1 | Saba | 0 | Country completely missing or zero census records (geography audit) |
| 23 | T1 | Sint Eustatius | 0 | Country completely missing or zero census records (geography audit) |
| 24 | T1 | Turks and Caicos Islands | 0 | Country completely missing or zero census records (geography audit) |
| 25 | T2 | Mexico | 1433 | Materially underrepresented (coverage 60.3%, gap ~1433); 1276 prior holds — reopen for identity discovery |
| 26 | T2 | Argentina | 788 | Materially underrepresented (coverage 14.1%, gap ~788); HBX geography discovery never completed (403 block on non-Wave1); 788 prior holds — reopen for identity discovery |
| 27 | T2 | Chile | 311 | Materially underrepresented (coverage 17.3%, gap ~311); HBX geography discovery never completed (403 block on non-Wave1); 311 prior holds — reopen for identity discovery |
| 28 | T2 | Peru | 303 | Materially underrepresented (coverage 15.8%, gap ~303); HBX geography discovery never completed (403 block on non-Wave1); 303 prior holds — reopen for identity discovery |
| 29 | T2 | Colombia | 299 | Materially underrepresented (coverage 76.4%, gap ~299); 299 prior holds — reopen for identity discovery |
| 30 | T2 | Costa Rica | 202 | Materially underrepresented (coverage 78.7%, gap ~202); 202 prior holds — reopen for identity discovery |

## Batch Results

Country: **Brazil** · Batch: `discovery_brazil_250_2026-08-10T10-20-34-375Z` · Limit: 250

| Metric | Value |
| --- | ---: |
| Hotels before (production) | 5956 |
| Hotels after (production) | 5956 |
| Hotels after (provisional staged) | 6203 |
| NEW_HOTEL (explicit city) | 0 |
| REVIEW_REQUIRED staged shells | 247 |
| Matched existing | 3 |
| Duplicates prevented | 3 |
| Ambiguous | 0 |
| Review queue items | 247 |
| Rejected | 0 |
| City inferred from Cvent URL | 250 |
| Batch validation | PASS  |
| Duplicate rate % | 1.2 |
| Review burden % | 98.8 |

Status counts: `{"DISCOVERED":0,"MATCHED":3,"NEW_HOTEL":0,"AMBIGUOUS":0,"REVIEW_REQUIRED":247,"REJECTED":0}`

## Country Improvements

Production census unchanged (read-only). Provisional Brazil staged shells: **247** (identity discovery pending review — city inferred from Cvent URL).

## Remaining Gaps

- **HBX blocked** for 47 geographies (auth 403) — largest structural blocker for high-confidence city/address shells outside Wave1.
- **Brazil** still the largest reopenable hold pool (~4.8k) after this 250 batch.
- **Zero-record geographies** (22): Bermuda, Sint Eustatius, Saba, Cuba, Turks and Caicos Islands, U.S. Virgin Islands, Anguilla, Bonaire, Guadeloupe, Martinique, Saint Barthélemy, Saint Martin, Sint Maarten, Bolivia, Venezuela, Haiti, Saint Vincent and the Grenadines, French Guiana, Guyana, Paraguay, Suriname, Montserrat.
- Cvent is meetings-venue inventory — not a complete national census; official brand/government directories still needed for true universe completeness.

## Progress Toward Target

| Milestone | Target | Current production | % of target |
| --- | ---: | ---: | ---: |
| First | 10,000 | 5956 | 59.6% |
| Second | 12,500 | 5956 | 47.6% |
| North star | 15,000+ | 5956 | 39.7% |

Provisional staged (not production): +247 review shells this batch.

## Recommended Next Batch

**Do not auto-start.**

**Brazil — next 500** (after founder/review accepts Cvent URL city-inference shells). Why: orchestrator next pool; ~4842 remaining gain; HBX still blocked. Do not production-insert until sample review of inferred cities (Rio de Janeiro, São Paulo, etc.) passes duplicate + geo sanity checks.

## Safety confirmation

```
ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES=0
ENABLE_HBX_CENSUS_WRITES=0
```

Artifacts:
- `reports\hotel-intelligence\universe-expansion-v1\coverage-scorecard.json`
- `reports\hotel-intelligence\universe-expansion-v1\discovery-queue.json`
- `reports\hotel-intelligence\universe-expansion-v1\batch-brazil-250.json`
- `data\hotel-intelligence\universe-expansion\staged-hotels.json`
