# Tripadvisor Room Verification V2

`TRIPADVISOR_ROOM_VERIFICATION_V2_COMPLETE`

**Mode:** READ ONLY / staged — no Airtable or census writes
**Module:** tripadvisor-apify-rooms-enrichment-v2
**Verify:** tripadvisor-verify-v2
**Sample:** 50 missing-room CALA hotels

## 1. Executive summary

Tripadvisor remains the **candidate generator**. Verification v2 adds official-site path crawl, composition totals, source-independence gating, and conflict classification.

- Candidate→verified conversion: **2%**
- Room resolution rate (sample): **2%**
- Primary verified: **1** · Multi: **0** · Single-source: **48**
- Conflicts: **1** · Independence uncertain: **0**
- Total cost (attributed): **$1.1675**

## 2. Architecture changes

- `official-site-verify.js` — capped path crawl + PDF links + composition arithmetic
- `independence.js` — cluster-based independence (blocks OTA↔OTA / TA↔HBX auto-multi)
- `conflicts.js` — conflict cause taxonomy (no averaging)
- `verify.js` → **v2** waterfall

## 3. Verification waterfall

1. Official hotel/brand website paths (`/rooms`, `/about`, `/fact-sheet`, ES paths, …)
2. Hotelbeds Content API when `HBX Hotel Code` present
3. Existing room-count research (SerpApi + ≤4 page fetches)
4. Extra approved observations hook

Skipped as room-count sources: StayingAPI, SerpApi Google Hotels field, GIATA Drive (firewalled).

## 4. Source hierarchy

| Tier | Sources |
| --- | --- |
| 1 | Official hotel / brand / owner pages & fact sheets |
| 2 | Tourism/gov docs, Hotelbeds structured roomsNumber |
| 3 | Credible press / industry pubs via research |
| 4 | OTAs / aggregators (never sufficient alone for MULTI) |

## 5. Source-independence logic

- Tripadvisor is never an independent confirmer of itself
- OTA-only agreement → `SOURCE_INDEPENDENCE_UNCERTAIN`
- Tripadvisor + Hotelbeds alone → uncertain (possible shared upstream)
- Official explicit match → `VERIFIED_PRIMARY_SOURCE`
- Two distinct non-OTA clusters → `VERIFIED_MULTI_SOURCE`

## 6. 50-hotel results

| Metric | Value |
| --- | ---: |
| TOTAL_SAMPLE | 50 |
| TRIPADVISOR_MATCHES | 50 |
| TRIPADVISOR_ROOM_CANDIDATES | 50 |
| VERIFIED_PRIMARY_SOURCE | 1 |
| VERIFIED_MULTI_SOURCE | 0 |
| CANDIDATE_SINGLE_SOURCE | 48 |
| CONFLICT_REVIEW_REQUIRED | 1 |
| SOURCE_INDEPENDENCE_UNCERTAIN | 0 |
| UNRESOLVED | 0 |
| FALSE_MATCH_REJECTED | 0 |
| OFFICIAL_WEBSITE_ROOM_COUNT_FOUND | 2 |
| OFFICIAL_PDF_FACTSHEET_FOUND | 0 |
| SECONDARY_SOURCE_VERIFICATIONS | 0 |
| CANDIDATE_TO_VERIFIED_CONVERSION | 2% |
| ROOM_RESOLUTION_RATE | 2% |

## 7. Hotel-level evidence (verified)

### Awa Resort Hotel

| Field | Value |
| --- | --- |
| Room count | 87 |
| Status | VERIFIED_PRIMARY_SOURCE |
| Source | official_site / Official Hotel |
| Source URL | https://www.awaresort.com.py/ |
| Evidence | mpleto para todo tipo de eventos, y con 87 habitaciones concebidas para su comodidad. |
| Retrieved | 2026-08-12 |

## 8. Conflict analysis

- **Karibea Le Squash Hotel**: TA=105 preferred=— cause=`MATCH_ERROR` — Large gap — possible match or complex-boundary error

## 9. Cost analysis

| Item | USD |
| --- | ---: |
| Tripadvisor (attributed PPE) | 0.1875 |
| Other incremental (SerpApi est.) | 0.98 |
| Total | 1.1675 |
| Per room candidate | 0.0234 |
| Per verified room | 1.1675 |

## 10. Candidate-to-verified conversion

`(PRIMARY + MULTI) / TRIPADVISOR_ROOM_CANDIDATES = 2%`

## 11. GIATA defer/buy assessment

**KEEP_GIATA_OPTION_OPEN — conversion too low on this sample to defer MHG solely on Tripadvisor+waterfall**

Tripadvisor is a strong **candidate generator** (100% match + room field in this 50-hotel matched set), but independent primary verification from crawlable official HTML remains rare (**2%** candidate→verified). Brand SPAs, blocked fetches, and portfolio-page noise (see Karibea conflict) dominate the unresolved tail.

Compare vs GIATA MHG proposal (€200/mo + €150 setup, 24-mo → €4,950): decision is driven by conversion, provenance, and unresolved tail — not price alone.

| Dimension | Tripadvisor + v2 waterfall | GIATA MHG Facts (proposed) |
| --- | --- | --- |
| Coverage | High candidates; low verified resolution | Expected higher structured keys coverage if entitled |
| Accuracy | Strong when official HTML states inventory (Awa Resort = 87) | Curated facts product |
| Provenance | Auditable URL + quote when verified | Commercial feed provenance |
| Freshness | Live crawl; brittle on JS sites | Feed SLA dependent |
| Scalability | Cheap PPE + SerpApi; slow wall-clock | API scale without HTML crawl |
| Complexity / maintenance | High (match + crawl + independence) | Lower ops once integrated |
| Unresolved tail | ~96% still single-source candidates | Likely smaller for keys |

## 12. Recommended next step

1. Human-QA Awa Resort (verified) and Karibea (conflict — portfolio “500 chambres” noise vs property count).  
2. Prototype rendered/official fact-sheet packs for major brands before expanding Apify volume.  
3. Keep Tripadvisor as candidate-only; keep GIATA MHG as an open option for the keys tail.  
4. **No production writes.**

## Phase 1 inspection notes (why v1 stayed single-source)

- v1 fetched only homepage URLs (no `/rooms`, `/fact-sheet`, ES paths)
- No composition arithmetic for rooms+suites
- No independence gating (would have over-called MULTI on weak pairs)
- Sample hotels often lacked HBX codes; StayingAPI/GIATA Drive do not expose room counts
- SerpApi snippets alone rarely yield explicit inventory phrases

## Safety

```text
PRODUCTION_WRITES: 0
Census Rooms / Keys overwrites: 0
Tripadvisor treated as candidate only
```

## STOP

50-hotel staged validation complete. No production enrichment enabled.
