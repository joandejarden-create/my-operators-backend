# DEALALITY — Bethesda GDI Qualification + Local/Published Parity (Zero-Paid)

**Founder deliverable (1–14)** · Offline + local server verify · $0 paid · No customer publication

---

## 1. KnowBe4 after targeted investigation

**Does not qualify** for Bethesda Marriott customer publication review.

| Field | Result |
|---|---|
| Trigger | Arlington (Ballston) office opening — verified |
| Primary thesis | Guest Rooms (`RELIABLE_INFERRED_DEMAND`) retained |
| Confirmed requirement | UNKNOWN |
| Geography | **CONTESTED_METRO** |
| AD / Sales Ready / Pub-review | false path for Bethesda publication (geography + copy gates) |

Evidence: Arlington hub at 4075 Wilson Blvd; public reporting that KnowBe4 **chose Virginia over Maryland**; no Bethesda/Rockville footprint. Do not present as a Bethesda opportunity on metro adjacency alone.

## 2. Ameresco after targeted investigation

**Does not qualify** for Bethesda Marriott customer publication review.

| Field | Result |
|---|---|
| Trigger | $197M NRL ESPC — verified |
| Primary thesis | Guest Rooms (`RELIABLE_INFERRED_DEMAND`) retained |
| Confirmed requirement | UNKNOWN |
| Geography | **CONTESTED_METRO** |
| Sites | NRL Washington D.C. campus + Chesapeake Bay Detachment |
| Offices | DC + Columbia MD — not Bethesda |

No evidence project travelers lodge at Bethesda Marriott rather than nearer DC / Columbia options.

## 3. Remaining blockers (both)

- Practical Bethesda-side lodging / operating footprint not evidenced
- Contested metro activity sites (Arlington / NRL D.C.)
- No forced qualification; research continuation only

Artifacts: `reports/gdi-bethesda-knowbe4-ameresco-geography-investigation-v1/`

## 4. Zero-paid Full E2E continuation

| Run | Result |
|---|---|
| `gdi:bethesda-zero-paid-finalization-recovery` | 11 unique orgs reconciled; **0** historical publication-review-ready; fixture path still proves handoff when evidence is complete; **NO_GO** broad paid |
| Fourteen-segment chain dry-run (`--hotels recLuxvwwxID7U2B8`) | Cached segments reused (`LIVE_PROVIDER_ALREADY_COMPLETED`); **$0** fresh retrieval; discoveryCompleteClaimAllowed=false |

## 5–8. Opportunity counts (this pass)

| Metric | Count |
|---|---|
| Genuinely new commercial opportunities finalized | **0** |
| Newly qualifying Account Development (Bethesda) | **0** |
| Newly qualifying Sales Ready | **0** |
| Ready for founder publication review (new) | **0** |

Prior published customer feed preserved at **19** qualified/approved rows after parity filter (was 67 unsafe commercial-surface rows on local).

## 9. Customer-facing bodies staged

**None** staged for KnowBe4 / Ameresco. Geography investigation deliberately withholds publication-review staging.

## 10. Local / published opportunity-list parity

**Fixed.** Default Bethesda local commercial list now applies the same `filterCustomerFacingOpportunities` + qualified-feed bar as published/share:

- Blocks WATCHLIST / Research Continuation / geography-blocked / shadow previews / unapproved `publicationReviewReady`
- Preserves approved SALES_READY / ACCOUNT_DEVELOPMENT / REVIEWED_PACKAGE / published-registry rows
- Live verify: `localPublishedParity=true`, count **19**, watchlist **0**, `customerVisible=false` **0**

Research inventory remains on `?surface=research|full` / admin tools — not the hotel feed.

## 11. Existing approved opportunities preserved

Facing IDs retained (examples): AHIMA, ACC Legislative, Potomac Memorial, MSYSA State Cup, NICE, BEBPA, Bethesda Premier Cup, Loudoun Showcase, CMSS, AFCEA HITS, ACTS, NAR GAD, Alexandria Soccer Kickoff, plus scaling/private-event published rows. Early-watch WATCHLIST cards removed from the **customer feed only** (not deleted from research store).

## 12–13. Further paid discovery

**Not justified as a broad restart.** Zero-paid continuation recovered no new publication-ready bodies; prior $6 campaign exhaustion still applies.

### Proposed narrowly scoped budget (authorization required — **not executed**)

| Item | Proposal |
|---|---|
| Scope | 3–4 segments only: Association Events (open housing), Sports overflow with future-cycle displacement, Local Catering/Day Meetings, Scaling Companies with Bethesda-side footprint |
| Problem addressed | Cached WATCHLIST lacks Bethesda-practical geography + named lodging path |
| Dedup | Pre-deep inventory + cross-segment org keys (existing) — no re-deep of KnowBe4/Ameresco/NRL without new Bethesda-side evidence |
| Est. discovery | ~$2.50–$4.00 |
| Est. deep | ~$3.00–$5.00 |
| Cap | **$8.00** hard stop |
| Stop | First 3 publication-review-ready **or** spend cap **or** 2 consecutive zero-yield segments |
| Success | ≥1 Sales Ready or ≥2 Account Development with practical Bethesda geography |
| Explicitly excluded | Reusing $0.09 remainder; re-running exhausted Exa queries unchanged |

## 14. GitHub PR / tests / main SHA

See PR from branch `gdi-bethesda-local-published-parity-zero-paid-v1` (this change set).

### Tests run

- `test:gdi-qualified-customer-opportunity-feed-v1` PASS
- `test:gdi-customer-visibility` PASS
- `test:gdi-bethesda-commercial-surface-route-fail-closed-v1` PASS (commercialCount=19)
- `test:gdi-commercial-hypothesis-development-v1` PASS (12/12)
- `test:gdi-proactive-commercial-intelligence-standard-v1` PASS (16/16)
- `test:gdi-watchlist-finalization-handoff-v1` PASS (8/8)

### Local URL (verified)

`http://localhost:8080/group-demand-intelligence.html?hotelId=recLuxvwwxID7U2B8`

Select **Bethesda Marriott** if the hotel picker is used; opportunities load from `/api/group-demand-intelligence/hotels/recLuxvwwxID7U2B8/opportunities` (default commercial surface).

### Safety

Paid calls: **0** · No Airtable opportunity writes · No customer publication · 88 research IDs preserved · Hotel feedback untouched · No parallel GDI engine
