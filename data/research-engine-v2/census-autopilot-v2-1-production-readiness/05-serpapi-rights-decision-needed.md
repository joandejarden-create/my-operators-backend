# SERPAPI_PRODUCTION_RIGHTS_DECISION_NEEDED

Status: **DECISION NEEDED — do not enable production persistence**

Source: `serpapi-benchmark-v1/22-production-rights-questions.md` + VIC source-rights registry patterns.

## Still unknown (require written SerpApi answers)

| Topic | Unknown? |
|-------|----------|
| Persist factual hotel data (name, address, phone, website, coords, amenities, class) | YES |
| Retain after request/session completes | YES |
| Combine with independently researched proprietary Census data | YES |
| Use derived factual fields in proprietary Census product | YES |
| Customer-facing display to SaaS users | YES |
| Historical snapshots / change history | YES |
| Persist Google/property_token identifiers | YES |
| Store image URLs as references (no download) | YES |
| Download/reuse images | YES (assume Not Approved until proven) |
| Google underlying-source obligations | YES |
| R&D vs production plan differences | YES |
| Plan cancellation / deletion obligations | YES |

## Separation (do not collapse)

- `research_allowed` = true for technical benchmark/wave research
- `technical_candidate` = Exact/High fields may stage
- `production_persistence_allowed` = **false until written clarification**
- `customer_display_allowed` = **false until written clarification**
- `image_reuse_allowed` = **false**

Technical research continues. Production Airtable writes remain blocked for SerpApi-derived fields.

## Exact message Joan should send SerpApi

```
Subject: Written clarification — commercial SaaS use of Google Hotels API data

Hello SerpApi team,

We operate a commercial hotel intelligence SaaS. We use your Google Hotels API for research.

Please confirm in writing (not marketing copy) whether our plan allows us to:

1) Persist factual property fields returned by the API (name, address, phone, website, coordinates, amenities, hotel class) in our database;
2) Retain those facts after the API request completes;
3) Combine them with independently researched hotel data;
4) Use derived factual fields inside a proprietary Hotel Census product;
5) Display those factual fields to paying SaaS customers;
6) Keep historical snapshots of those fields;
7) Persist and reuse property_token / Google property identifiers over time;
8) Store image URLs as references without downloading;
9) Download or reuse images (if ever permitted);
10) Any Google-source attribution, prohibited uses, or geo restrictions we must follow;
11) Whether R&D/benchmark use differs from production enrichment under our plan;
12) What we must delete if we cancel the plan.

We will not enable production persistence until we have your written answers.

Thank you,
Joan
```
