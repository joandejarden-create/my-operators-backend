# Golf Planet Holidays 2027

| Field | Value |
|-------|-------|
| opportunityId | `camp_gph_sheraton_golf_break_2027_sheraton` |
| campaignId | `camp_gph_sheraton_golf_break_2027` |
| hotelId | `recjhQdAUiSyxfCqE` |
| Airtable | `recHoqdOTdSFlajfN` |
| FS | `data/group-demand-intelligence/hotels/recjhQdAUiSyxfCqE/opportunities.json` |
| packet/summaryQuality | `STRONG` |
| maturity/customerFacingState | `WATCH` |
| priority | `WATCHLIST` |
| Watch gate | **FAIL** class=`RESEARCH_BACKLOG_NOT_WATCH` reasons=`template_monitor_rationale_without_confirmed_cycle` |
| Ready gate | **FAIL** |
| Surface | `DOWNGRADE_TO_DEMAND_GENERATOR` reasons=`public_demand_generator_without_hotel_thesis` |
| customerVisible | **false** |
| customerActiveEligible | `false` |
| publication state | NOT_CUSTOMER_PUBLISHED (no separate pub ID) |
| publication ID | n/a |
| API record | ABSENT (facing filter) |
| property-scoped API membership | NO |
| UI payload | empty for hotel |
| UI render | not rendered |

## First stage where it disappears

**Watch gate + surface eligibility + customerVisible:false** — never entered customer publication.
E2E report `dispositionFromFit` falsely labeled `VALID_FUTURE_WATCH` from seed heuristic without calling `isValidFutureWatch`.
