# Prioritized Discovery Enhancements — Next Implementation PR

Recommended next focused PR (after this reporting PR merges): **Bethesda/DMV catchment + pre-deep published-inventory dedupe + corporate-expansion mission sharpening**.

## Ranked recommendations

### 1. Pre-deep published inventory dedupe
- **Observed weakness:** Rock Creek Mansion rediscovered and burned deep/discovery budget.
- **Segment:** preferred_lodging (+ general).
- **Evidence:** DUPLICATE ↔ `gdi_plp_live_fa2063dc1195`.
- **Root cause:** Paid deep eligibility does not hard-block exact published org matches early enough for all segments.
- **Module:** `original-full-e2e-research-chain-v1.js` / paid-deep dedupe + discovery handoff.
- **Improvement:** Skip paid deep when org-key matches CUSTOMER_PUBLISHED inventory unless enrichment intent flagged.
- **Impact:** Lower duplicate spend; more budget for novel orgs.
- **Complexity:** Low–medium · **Tests:** deterministic duplicate skip · **Cost impact:** reduces waste.

### 2. Bethesda catchment negative filters in Exa discovery context
- **Weakness:** NFL, AEA, AHA, NASPA dominate then EXCLUDE.
- **Segments:** associations, meetings, sports, education.
- **Module:** Exa context block builder + segment query specs / agent context.
- **Improvement:** Enforce Montgomery County / Bethesda-reachable constraints; require overflow evidence for national DC primaries.
- **Impact:** Higher precision · **Complexity:** Medium · **Cost:** fewer useless deep seeds.

### 3. Corporate expansion / workforce mission upgrade
- **Weakness:** Only one strong corporate NEW_UNIQUE (Amentum).
- **Segment:** corporate_accounts_scaling (+ relocation).
- **Module:** Exa discovery contract context + segment routing.
- **Improvement:** Explicit office expansion, contract award, training cohort, extended-team lodging signals for Rockville/Bethesda/NoVA.
- **Impact:** More actionable accounts · **Complexity:** Medium.

### 4. Contact relevance gate
- **Weakness:** Sales-ready contacts rare; one attached to EXCLUDE opp.
- **Module:** contact eligibility / commercial spine.
- **Improvement:** Require demand disposition ≥ watchlist before counting sales-ready in scorecards; strengthen entry-point vs DIRECT_BUYER labels.
- **Complexity:** Low.

### 5. Association rotation / medical ecosystem net-new hunt
- **Weakness:** Education/medical missions rediscover national calendars while baseline already holds NIH/PDA-style inventory.
- **Improvement:** Novelty-aware prompts referencing known published org denylist (internal).
- **Complexity:** Medium · **Cost:** reduces rediscovery.

## Explicitly deferred

- Customer-facing opportunity drawer UX
- New orchestrator / provider framework
- Apify / Surfe / PDL / SerpAPI
- Second paid Exa campaign
