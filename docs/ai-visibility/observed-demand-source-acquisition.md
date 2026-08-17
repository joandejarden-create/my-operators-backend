# Observed demand — source acquisition

> **Status:** `OBSERVED_DEMAND_SEED_PARTIAL` · Budget-capped DataForSEO sample complete 2026-08-17  
> **AI provider calls:** 0 · **Census:** none · **Airtable writes:** 0 · **Prompt Mix:** hidden (6 distinct themes < 10)  
> **Live overlay:** empty — signals are file-store only

## Budget guard (binding)

Account funding is not a project budget. A founder top-up does not authorize extra spend.

| Cap | Value |
|-----|-------|
| `MAX_SOURCE_SAMPLE_COST_USD` | **1.00** |
| `MAX_TOTAL_DATAFORSEO_SPEND_THIS_PHASE_USD` | **2.00** |

This sample: **$0.282** actual. Phase spent **$0.282**. Remaining phase budget **$1.718 unused on purpose**. Projected spend above $2.00 must stop with `DATAFORSEO_BUDGET_APPROVAL_REQUIRED`.

Do not run broad keyword expansion, hundreds of SERPs, country-wide discovery, or additional paid calls unless explicitly approved.

## Finding

Dealality already has a DataForSEO account (used for census discovery). After the account was funded, a **budget-capped** Keywords Data + Google Organic SERP sample succeeded:

| Result | Count |
|--------|-------|
| Queries tested | 36 |
| Usable signal rows | 10 |
| Distinct themes | 6 |
| AI provider calls | 0 |

Signals are stored in `fixtures/ai-visibility/demand-signals-v1.json`. They are **not** attached to live monitored prompts. Client Prompt Mix stays hidden until ≥10 validated observed themes exist in the monitored library.

Do not treat public hospitality articles as search volume. They only show that owner-decision **topics exist**.

## Sample themes (do not invent more)

| Theme | Evidence | Notes |
|-------|----------|--------|
| hotel franchise fees | US EN vol **90** HIGH; MX EN vol **10** MEDIUM; PAA | Licensed volume |
| soft brand hotel | US EN vol **50** MEDIUM; MX EN vol **10** HIGH; PAA | Licensed volume |
| franquicia hotelera | MX ES vol **20** HIGH | Licensed volume; no PAA in this sample |
| contrato de gestion hotelera | MX ES vol **10** MEDIUM | Licensed volume |
| hotel franchise vs management agreement | volume **null**; US/MX EN **PAA SUPPORTED** | Not volume; still usable as PAA |
| hotel reflagging | volume **null**; US/MX EN **PAA SUPPORTED** | Some PAA questions are generic/off-intent — treat carefully |

Tiers are **relative within one country+language volume set**, not comparable across US vs Mexico. Geography is **country** (US/Mexico), **not CALA**. Volume is **not** “owner searches.”

Failed seed concepts remain `NEEDS_EVIDENCE`. Do not spend remaining phase budget to chase them without approval.

## Recommended architecture (smallest practical stack)

| Role | Source |
|------|--------|
| **PRIMARY** | DataForSEO Google Ads Search Volume (live) — licensed `SEARCH_VOLUME` by country + language |
| **SECONDARY** | DataForSEO Google Organic SERP advanced — `PAA` + `RELATED_SEARCH` validation |
| **OPTIONAL** | Dealality Search Console later (if owner-intent queries appear); first-party product queries later |
| **DO NOT USE** | Large-scale Google scraping; Semrush/Ahrefs for this phase; Census weighting; SerpApi Google Hotels as a demand source; Dealality-branded GSC queries as market demand |

Estimate for this seed: **$0.282** (3 volume tasks × $0.09 + 6 SERP × $0.002). Sample cap **$1.00**. Phase cap **$2.00**.

## Geography / language

Google Ads locations are **countries**, not Dealality CALA. Sample plan: United States EN (reference), Mexico EN, Mexico ES. Do not label US volume as CALA.

## Demand tier (once volume exists)

Assign HIGH / MEDIUM / LOW **within** one country+language licensed-volume set (`LICENSED_SEARCH_VOLUME` + relative rank). Do not compare US volume to Mexico volume as one scale. Heterogeneous public-article evidence stays `UNKNOWN` for tier. PAA-only rows stay `UNKNOWN` for tier.

## Public question sample (not volume)

Industry pages discuss franchise vs HMA, soft-brand affiliation, and branded residences. Signal type: `PUBLIC_QUESTION`. Strength: `WEAK`–`SUPPORTED` for **topic existence** only. Not usable as OBSERVED search demand.

## Search Console

Brand AI discoverability still uses `SEARCH_CONSOLE_CONNECTION` as a **brand-website** state. No Dealality GSC owner-intent query export was found.

TOTAL_QUERY_THEMES_REVIEWED: 0  
OWNER_INTENT_RELEVANT: 0  
BRAND_DECISION_RELEVANT: 0  
USABLE_OBSERVED_SIGNALS: 0

## First-party behavior

Landing analytics are page/session events. No Explorer / deal-workflow / AI-assistant query log for these themes.

## Storage (not applied to live prompts)

**Airtable (later):** prompt origin, theme, demand tier, review status, date, short source name.  
**File store now:** normalized signals in `fixtures/ai-visibility/demand-signals-v1.json`; raw sample report in `reports/ai-visibility/observed-demand-source-sample-2026-08-17.json`; phase ledger in `reports/ai-visibility/observed-demand-dataforseo-phase-spend.json`. No Airtable dumps. Overlay `fixtures/ai-visibility/prompt-provenance-v1.json` classifications remain empty.

## Refresh

**QUARTERLY** for this seed (search demand moves slower than AI answers). Do not bind to AI monitoring cadence. Do not auto-refresh without a new budget approval.

## Sampling later (not built)

HIGH observed demand + HIGH commercial importance → higher repeat sampling priority. Critical Scenario-only decisions can stay high priority without demand. Demand is one input.

## Next

1. Do **not** spend remaining **$1.718** or the account balance without a new explicit budget.  
2. If more themes are approved and ≥10 validate → `OBSERVED_DEMAND_ACTIVATION` (still no monitoring until approved).  
3. Then `REPEATED_TESTING_AND_STABILITY`.
