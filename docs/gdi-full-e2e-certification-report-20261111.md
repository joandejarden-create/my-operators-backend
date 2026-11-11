# GDI Full E2E certification report

Measured commit: `58f567391e82bc26fd4c0a6bf2d53f492fe28240`. Contract: `gdi-full-e2e-contract-v1.5`.

This report records a zero-paid, no-publication measurement. A completed script is not commercial certification.

## Scorecard

| Decision | Status |
| --- | --- |
| engineExecution | FAIL |
| eventsOrchestrator | NOT VERIFIED |
| researchQuality | NOT VERIFIED |
| commercialQualification | FAIL |
| customerDelivery | FAIL |
| publicationSafety | PASS |
| fullE2e | FAIL |

Events fixture: MISSING. Spice published set matches the certified seven: true. Bethesda published rows: 0.

## Catalog

Fourteen workflows: association_events, scaling_companies, corporate_meetings_incentives, government_public_sector, sports_entertainment, education_scientific, projects_infrastructure, social_special_events, local_catering_day_meetings, corporate_celebrations_private_events, travel_trade_group_intermediaries, relocation_extended_stay, preferred_lodging, professional_event_planners.

Eleven segments: associations_government, sports, corporate_meetings_training, entertainment_production, medical_academic, tours_educational_specialist, weddings_social_private_events, professional_planners_dmc, scaling_companies, local_meetings_catering, exhibitor_sponsor_teams.

## Hotels

### Bethesda Marriott

Research store 59. Unique ids 59. Duplicate title groups 0. Sales Ready by the existing evaluator 9/59. Source URL present 58/59. Need-date field present 59/59. Customer-ready 24/59. Internal-label leaks 0. Evaluation errors 0.

Thesis: {"RELIABLE_INFERRED_DEMAND":4,"CONFIRMED_DEMAND":51,"SPECULATIVE_DEMAND":4}.

Eligibility: {"RESEARCH_CONTINUATION":50,"SALES_READY":9}.

Contact class: {"DIRECT_BUYER":8,"INELIGIBLE_NOT_A_PERSON":30,"INELIGIBLE_UNSPECIFIED":2,"RELEVANT_COMMERCIAL_ENTRY_POINT":7,"INELIGIBLE_LEADERSHIP_WITHOUT_DEMAND_LINK":4,"INELIGIBLE_IRRELEVANT":5,"RESEARCH_CANDIDATE":1,"INELIGIBLE_AFFILIATION_UNSUPPORTED":2}.

Workflow ids found on stored rows: none.

### Renaissance New York Times Square Hotel

Research store 72. Unique ids 72. Duplicate title groups 0. Sales Ready by the existing evaluator 10/72. Source URL present 72/72. Need-date field present 72/72. Customer-ready 10/72. Internal-label leaks 0. Evaluation errors 0.

Thesis: {"RELIABLE_INFERRED_DEMAND":40,"CONFIRMED_DEMAND":27,"SPECULATIVE_DEMAND":5}.

Eligibility: {"RESEARCH_CONTINUATION":62,"SALES_READY":10}.

Contact class: {"INELIGIBLE_NOT_A_PERSON":31,"DIRECT_BUYER":9,"INELIGIBLE_UNSPECIFIED":20,"INELIGIBLE_LEADERSHIP_WITHOUT_DEMAND_LINK":3,"INELIGIBLE_IRRELEVANT":5,"RELEVANT_COMMERCIAL_ENTRY_POINT":3,"INELIGIBLE_AFFILIATION_UNSUPPORTED":1}.

Workflow ids found on stored rows: none.

### Wyndham San Diego Bayside

Research store 82. Unique ids 82. Duplicate title groups 2. Sales Ready by the existing evaluator 8/82. Source URL present 76/82. Need-date field present 82/82. Customer-ready 16/82. Internal-label leaks 0. Evaluation errors 0.

Thesis: {"RELIABLE_INFERRED_DEMAND":40,"CONFIRMED_DEMAND":36,"SPECULATIVE_DEMAND":6}.

Eligibility: {"RESEARCH_CONTINUATION":74,"SALES_READY":8}.

Contact class: {"INELIGIBLE_IRRELEVANT":8,"INELIGIBLE_NOT_A_PERSON":52,"INELIGIBLE_LEADERSHIP_WITHOUT_DEMAND_LINK":8,"DIRECT_BUYER":9,"UNRELATED_CONTACT":1,"RESEARCH_CANDIDATE":1,"INELIGIBLE_UNSPECIFIED":3}.

Workflow ids found on stored rows: none.

### Spice Island Beach Resort

Research store 77. Unique ids 77. Duplicate title groups 4. Sales Ready by the existing evaluator 0/77. Source URL present 68/77. Need-date field present 77/77. Customer-ready 0/77. Internal-label leaks 0. Evaluation errors 0.

Thesis: {"CONFIRMED_DEMAND":77}.

Eligibility: {"RESEARCH_CONTINUATION":77}.

Contact class: {"INELIGIBLE_NOT_A_PERSON":77}.

Workflow ids found on stored rows: none.

## Publication and authentication

Registry present: true. Isolation leak: false. Promotion without approval blocked: true.

Anonymous route statuses: [{"route":"/api/group-demand-intelligence/hotels/recLuxvwwxID7U2B8/opportunities","status":200},{"route":"/api/group-demand-intelligence/hotels/recLuxvwwxID7U2B8/research-runs","status":403},{"route":"/api/ai-demand-positioning/review/admin-publication-control","status":403},{"route":"/app/admin/ai-demand-admin.html","status":200}]. Signed-in Memberstack delivery was not available.

## Independent source sample

Host and HTTP status only. Reachability does not confirm the demand thesis.

[{"hotelId":"recLuxvwwxID7U2B8","host":"www.nado.org","status":200,"error":null},{"hotelId":"recLuxvwwxID7U2B8","host":"www.showsbee.com","status":503,"error":null},{"hotelId":"recG66DQJKP2c0UNh","host":"plusweb.org","status":403,"error":null},{"hotelId":"recG66DQJKP2c0UNh","host":"www.rockefellercenter.com","status":403,"error":null},{"hotelId":"recnPB3Q02NO8Ma1o","host":"www.midway.org","status":200,"error":null},{"hotelId":"recnPB3Q02NO8Ma1o","host":"sdtechscene.org","status":403,"error":null},{"hotelId":"recKRJjcPnb4tVDDS","host":"www.puregrenada.com","status":200,"error":null},{"hotelId":"recKRJjcPnb4tVDDS","host":"www.travelandtourworld.com","status":403,"error":null}]

## Operational controls

Weekly auto-publish default: {"allowed":false,"reason":"auto_publish_disabled","policy":null}. Existing paid envelope constant: 11 USD. This run did not spend it.

## Workflow runs

{"mode":"full-e2e --dry-run --skip-airtable-reuse","isolatedDataRoot":true,"authoritativeStoresUnchanged":true,"publicationRegistryUnchanged":true,"budgetLedgerMtime":"2026-10-16T13:01:25.948Z","paidUsd":0,"hotels":[{"hotelId":"recLuxvwwxID7U2B8","exitCode":0,"paidCalls":0,"computedOverallStatus":"FAILED","evaluatedCount":59,"hotelFacingCount":34,"readyCount":22,"discoverGaps":"incomplete_coverage","paidMultisegment":"skipped","localDelivery":"failed","productionDelivery":"failed","reconcile":"failed","completionReasons":["SEGMENT_COVERAGE_INCOMPLETE","MISSING_OR_SHALLOW_SEGMENT","INSUFFICIENT_HOTEL_FACING_PROSPECTS","CUSTOMER_DELIVERY_INCOMPLETE"]},{"hotelId":"recG66DQJKP2c0UNh","exitCode":0,"paidCalls":0,"computedOverallStatus":"FAILED","evaluatedCount":60,"hotelFacingCount":41,"readyCount":14,"discoverGaps":"incomplete_coverage","paidMultisegment":"skipped","localDelivery":"failed","productionDelivery":"failed","reconcile":"failed","completionReasons":["SEGMENT_COVERAGE_INCOMPLETE","MISSING_OR_SHALLOW_SEGMENT","INSUFFICIENT_HOTEL_FACING_PROSPECTS","CUSTOMER_DELIVERY_INCOMPLETE"]},{"hotelId":"recnPB3Q02NO8Ma1o","exitCode":0,"paidCalls":0,"computedOverallStatus":"FAILED","evaluatedCount":60,"hotelFacingCount":28,"readyCount":13,"discoverGaps":"incomplete_coverage","paidMultisegment":"skipped","localDelivery":"failed","productionDelivery":"failed","reconcile":"failed","completionReasons":["SEGMENT_COVERAGE_INCOMPLETE","MISSING_OR_SHALLOW_SEGMENT","INSUFFICIENT_HOTEL_FACING_PROSPECTS","CUSTOMER_DELIVERY_INCOMPLETE"]},{"hotelId":"recKRJjcPnb4tVDDS","exitCode":0,"paidCalls":0,"computedOverallStatus":"FAILED","evaluatedCount":60,"hotelFacingCount":2,"readyCount":0,"discoverGaps":"incomplete_coverage","paidMultisegment":"skipped","localDelivery":"failed","productionDelivery":"failed","reconcile":"failed","completionReasons":["SEGMENT_COVERAGE_INCOMPLETE","MISSING_OR_SHALLOW_SEGMENT","INSUFFICIENT_HOTEL_FACING_PROSPECTS","CUSTOMER_DELIVERY_INCOMPLETE"]}]}

## Exceptions

- The events mock fixture `gdi-events-mock-provider-responses-v1.js` is not in the repository or the local Dealality tree. It was not reconstructed.
- Explicit `--dry-run` on the research-to-page coordinator still sets weekly-refresh state persistence. Certification runs must use an isolated `GDI_DATA_ROOT`.
- PR #166 was open at measurement time, so this commit is the stabilization branch head rather than a merged main commit.
- Authenticated hotel and admin sessions were not available. Feedback was not posted.
- The coordinator synthesizes hotel relevance for at most 60 stored opportunities in one run.
- Zero-paid discovery stays plan-only. Local and production delivery verification failed, and budget reconcile reported the ledger missing from the isolated root until the existing ledger was copied. No spend was recorded.
- Thesis class CONFIRMED_DEMAND is assigned from accommodation language in the stored thesis text. It is not an independent confirmation.
- Stored rows use segment labels. They do not carry the fourteen workflow ids, so an empty workflow-id match is not a missing catalog.
- Sales Ready and the raw thesis classifier disagree on 4 stored rows (1 Bethesda, 2 Renaissance, 1 Wyndham). Those rows are Sales Ready while the thesis classifier still says SPECULATIVE_DEMAND.
- A paid-provider validation was not run. It needs a separate authorization. The historical envelope must not be treated as a fresh allowance.

## Regression commands

```bash
npm run test:gdi-full-e2e-certification-readiness-v1
npm run test:gdi-full-e2e-certification-execute-v1
npm run gdi:full-e2e-certification-execute
```
