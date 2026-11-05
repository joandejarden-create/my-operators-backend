# AVA Three-Hotel Research Comparison

**Baseline:** Digital Authority V2 + Digital Content Readiness (existing certified payloads)
**Incremental executed this run:** Zero-cost HTTP platform coverage only
**Billable providers:** Not executed — awaiting founder authorization

## Success metric

Primary metric = **new evidence-supported commercial actions**, not raw backlink count.

## Hotel baselines (existing AVA pipeline)

| Hotel | Verified BL | Competitor BL | Mentions | Opps | Research coverage | DCR sources | Platforms IDENTIFIED_NOT_ASSESSED | Corrections |
|---|---:|---:|---:|---:|---|---:|---:|---:|
| Waterstone Resort & Marina | 0 | 0 | 1 | 2 | ACCESS_RESTRICTED | 5 | 8 | 1 |
| AC Hotel A Coruña | 2 | 0 | 4 | 2 | RESEARCH_SUFFICIENT_NO_ADDITIONAL_LINKS_VERIFIED | 3 | 8 | 1 |
| Castillo Hotel Son Vida, a Luxury Collection Hotel, Mallorca | 4 | 1 | 3 | 2 | RESEARCH_SUFFICIENT_NO_ADDITIONAL_LINKS_VERIFIED | 4 | 7 | 1 |

## What zero-cost enhancement added

| Hotel | Reachable platforms probed | Still IDENTIFIED_NOT_ASSESSED | New correction candidates | Evidence gate |
|---|---:|---:|---:|---|
| Waterstone Resort & Marina | 4 | 9 | 1 | PASS |
| AC Hotel A Coruña | 3 | 8 | 0 | PASS |
| Castillo Hotel Son Vida, a Luxury Collection Hotel, Mallorca | 3 | 8 | 0 | PASS |

## Gaps that require billable / entitlement-backed providers

| Gap | Why Exa+HTML is insufficient | Best provider class |
|---|---|---|
| Complete referring-domain inventory | Public search ≠ backlink index | Ahrefs (if entitled) or DataForSEO OPTIONAL_DEEP_DIVE |
| GBP / Maps field audit | Listing URL often unknown; Maps APIs paid | DataForSEO Maps / Context scrape after URL known |
| OTA field-level consistency | Need listing URL + JS-heavy pages | Context.dev CONDITIONAL after URL discovery |
| Bot-protected brand HTML facts | HTTP 403 ≠ false page | Context.dev scrape/extract CONDITIONAL |
| Multilingual missed sources | Exa recall uneven | Parallel CONDITIONAL Task |

## Projected incremental value (not yet measured live)

If founder authorizes the pilot below, re-measure:

- New verified backlinks / referring domains (property-specific only)
- New actionable competitor gaps (confirmed absence with evidence)
- New destination/industry references
- Previously inaccessible evidence recovered
- Duplicate/low-quality rate
- **New commercially useful recommendations** (primary)
- Incremental USD per hotel

## Proposed billable pilot (STOP — authorization required)

```json
{
  "status": "AWAITING_FOUNDER_AUTHORIZATION",
  "paidResearchAuthorizedEnv": "AVA_PAID_RESEARCH_AUTHORIZED=1",
  "proposedBudgetUsdCap": 45,
  "hotels": [
    "adp_waterstone_boca_raton",
    "adp_ac_hotel_a_coruna",
    "adp_castillo_hotel_son_vida"
  ],
  "proposedCalls": [
    {
      "provider": "ahrefs",
      "blockedReason": "No Ahrefs API token or client in repo — confirm account entitlement first",
      "ifAvailable": "URL backlinks for brand property page + hotel-owned domain; competitor referring domains sample",
      "estimatedUnits": "TBD after Ahrefs plan confirmation",
      "estimatedUsd": "TBD (Lite plan ~$129/mo class if subscription required)"
    },
    {
      "provider": "dataforseo",
      "calls": "Backlinks summary + live mode for 3 property URLs; optional Google Maps hotel search for listing URL discovery",
      "estimatedUsdMax": 20,
      "note": "Credentials present; still requires AVA paid-research flag"
    },
    {
      "provider": "context_dev",
      "calls": "Per hotel: 1 search (≤10 results) + 2–3 scrape_markdown + optional 1 extract on brand/hotel page",
      "estimatedCredits": "15–40 credits × 3 hotels",
      "estimatedUsd": "Convert via Context.dev dashboard rate; propose cap $15"
    },
    {
      "provider": "parallel",
      "calls": "1–2 Task core runs per hotel for multilingual / missed-source synthesis (not backlink DB)",
      "estimatedUsdMax": 0.15
    }
  ],
  "expectedIncrementalBenefit": "New verified referring domains, GBP/OTA listing URLs for field audit, recovery of ACCESS_RESTRICTED brand-page facts, evidence-supported competitor gaps — measured as new commercially useful actions, not raw backlink count.",
  "stop": "No billable calls executed in this run."
}
```

**Proposed maximum spend before stop:** $45

*No Ahrefs/Parallel/Context.dev/DataForSEO calls were made in this comparison run.*
