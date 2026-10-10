# 005 — Failure Types

```json
[
  {
    "failure": "NO_RESEARCH_ATTEMPTED",
    "count": 14979,
    "pct_audited_universe": 99.9,
    "pct_of_unresolved": 99.9,
    "countries_most_affected": [
      "ALL_CALA"
    ],
    "example_hotel_ids": [],
    "likely_next_action": "Select bounded E2E cohort; do not claim coverage until researched",
    "scope": "planning_universe_remainder"
  },
  {
    "failure": "LEGACY_DATA_NOT_EVIDENCED",
    "count": 7,
    "pct_audited_dataset": 33.3,
    "countries_most_affected": [
      "Mexico"
    ],
    "example_hotel_ids": [
      "recGZZCek9vDQGG1L",
      "rec79Xs4mZkuiWnuN",
      "rec19X4tsCUM1A2q6",
      "recL4PrLJpwXxyvV6",
      "rec2ossLX1BBaaZuw"
    ],
    "likely_next_action": "Promote only after evidence adjudication into OCG / HOTEL_TO_OWNER"
  },
  {
    "failure": "OWNER_KNOWN_CONTACTABLE_ORG_UNKNOWN",
    "count": 2,
    "pct_audited_dataset": 9.5,
    "countries_most_affected": [
      "Mexico",
      "Bermuda"
    ],
    "example_hotel_ids": [
      "recIwaP1etgx2g9nA",
      "recTYaiA4S6fR6ixx"
    ],
    "likely_next_action": "Org-domain + IR/contact-page discovery before person enrichment"
  },
  {
    "failure": "CONTACTABLE_ORG_NO_PERSON",
    "count": 16,
    "pct_audited_dataset": 76.2,
    "countries_most_affected": [
      "Mexico"
    ],
    "example_hotel_ids": [
      "recGZZCek9vDQGG1L",
      "rec79Xs4mZkuiWnuN",
      "rec19X4tsCUM1A2q6",
      "recL4PrLJpwXxyvV6",
      "rec2ossLX1BBaaZuw"
    ],
    "likely_next_action": "Person affiliation research gated on confirmed owner org"
  },
  {
    "failure": "PERSON_KNOWN_NO_EMAIL",
    "count": 2,
    "pct_audited_dataset": 9.5,
    "countries_most_affected": [
      "Mexico",
      "Bermuda"
    ],
    "example_hotel_ids": [
      "recIwaP1etgx2g9nA",
      "recTYaiA4S6fR6ixx"
    ],
    "likely_next_action": "Public email / pattern discovery with verification — not Surfe-first"
  },
  {
    "failure": "OPERATOR_NOT_OWNER",
    "count": 7,
    "pct_audited_dataset": 33.3,
    "countries_most_affected": [
      "Mexico"
    ],
    "example_hotel_ids": [
      "recGZZCek9vDQGG1L",
      "rec79Xs4mZkuiWnuN",
      "rec19X4tsCUM1A2q6",
      "recL4PrLJpwXxyvV6",
      "rec2ossLX1BBaaZuw"
    ],
    "likely_next_action": "Enforce PropCo vs operator separation in adjudication"
  },
  {
    "failure": "PROPCO_UNRESOLVED",
    "count": 3,
    "pct_audited_dataset": 14.3,
    "countries_most_affected": [
      "Mexico"
    ],
    "example_hotel_ids": [
      "recIwaP1etgx2g9nA",
      "recsYJb2R1jarPpK3",
      "recTYaiA4S6fR6ixx"
    ],
    "likely_next_action": "Registry / title / corporate filings playbook per country"
  },
  {
    "failure": "GRAPH_REUSE_UNAVAILABLE",
    "count": 7,
    "pct_audited_dataset": 33.3,
    "countries_most_affected": [
      "Mexico"
    ],
    "example_hotel_ids": [
      "recGZZCek9vDQGG1L",
      "rec79Xs4mZkuiWnuN",
      "rec19X4tsCUM1A2q6",
      "recL4PrLJpwXxyvV6",
      "rec2ossLX1BBaaZuw"
    ],
    "likely_next_action": "Materialize sibling hotels into OCG only after hotel-link validation"
  },
  {
    "failure": "NO_FAILURE_DATA_AVAILABLE",
    "count": 0,
    "pct_audited_dataset": 0,
    "countries_most_affected": [],
    "example_hotel_ids": [],
    "likely_next_action": "Instrument research runs to persist earliest_failure_stage"
  },
  {
    "failure": "DEMO_FIXTURE_BYPASS",
    "count": 10,
    "pct_audited_dataset": 47.6,
    "countries_most_affected": [
      "Mexico"
    ],
    "example_hotel_ids": [
      "recUNycnMwOVFX0hc"
    ],
    "likely_next_action": "Route Explorer through staging CI store — stop showcase-only path for production claims"
  }
]
```
