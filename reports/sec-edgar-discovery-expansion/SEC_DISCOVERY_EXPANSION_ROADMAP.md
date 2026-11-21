# SEC Discovery Coverage Expansion — Universe Audit & Roadmap

**Generated:** 2026-11-21T00:20:26.220Z  
**Packet:** Permanent factory expansion workstream (post Contract Terms & Renewals pilot)  
**Paid research launched:** **No**  
**Exhaustive coverage claimed:** **No**

---

## 1. Current SEC discovery universe (audit)

### Issuer / CIK registries

| Registry | Count | Role |
|---|---:|---|
| Pilot V1 | 4 | Original seed |
| Historical V2 | 13 | Expanded historical research set |
| Universal V24 | 32 | Current expanded seed (19 incremental adds) |

Organization mix (V24): REIT 21 · Brand / Franchisor 5 · Owner / Operator 5 · Brand / Operator 1

**Binding rule:** brand names and the issuer registry are **seeds, not ceilings**. Exhausting the registered list or predefined queries does **not** mean all SEC hotel intelligence has been discovered.

### Filing forms observed (eligible inventory sample)

Companies inventoried in universal wave sample: **5** · Eligible filings: **1940** · Since year: **1994**

Top forms: 8-K (1252), 10-Q (260), 424B5 (86), 10-K (85), DEF 14A (84), 8-K/A (38), ARS (30), S-11/A (21), 424B3 (18), S-3 (17), 10-K/A (8), 424B2 (8)

### Search terms / families

- Taxonomy: `sec-edgar-search-taxonomy-v2.1.0` — **14** families (identity, ownership, management, franchise, renewal, distress, etc.)
- Contract-timing SEC-API families: **11**
- Exa families: **6**
- Period partitions: 2012_2015, 2016_2018, 2019_2020, 2021_2022, 2023_2024, 2025_2026
- CMBS baseline accession: `0001539497-20-000579`

### Contract intelligence baseline (from completed terms pilot)

- VERIFIED agreements: **258** (franchise-only)
- Terms pilot sample: **40** · incremental beyond expiration: **7.5%**
- Management discovery leads: **5** (separate subset)
- Spend: **$0.052** of $25

---

## 2. Important uncovered / partial source categories

- **SEC_REGISTRANT_HOTEL_EXPOSURE** (PARTIAL): SEC registrants with hotel ownership/ops/franchise/management/lease/financing exposure
- **CMBS_ISSUERS_AND_COLLATERAL** (PARTIAL — JPMCC baseline + incremental V2/V3 findings; deal graph incomplete): CMBS issuers and hotel collateral pools (424B schedules)
- **REIT_PUBLIC_DISCLOSURES** (PARTIAL — registry seeds + eligible inventory; processing backlog large): Lodging REIT and public-company 10-K / 10-Q / 8-K / DEF 14A
- **EXHIBITS_AND_AMENDMENTS** (PARTIAL — exhibit ledger present; low exhibitsProcessed vs eligible): Hotel-related exhibits (EX-10*) and amendments
- **INCORPORATED_BY_REFERENCE** (MOSTLY_UNCOVERED): Incorporated-by-reference contractual documents
- **HISTORICAL_BACKFILL** (PARTIAL): Historical filings + subsequent amendments (1994+)
- **UNREGISTERED_RELEVANT_ISSUERS** (DESIGNED — adaptive queue caps exist; not continuously expanded): Relevant companies not previously in issuer registry
- **INTL_PUBLIC_DISCLOSURES** (READINESS_ONLY): International public disclosures beyond SEC

## 3. Blind spots (measurable)

- **ISSUER_REGISTRY_CEILING_RISK** [HIGH]: Universal registry lists 32 companies — seed only; must not be treated as complete SEC hotel universe.
- **ELIGIBLE_VS_PROCESSED_GAP** [HIGH]: Coverage ledger V2 shows large pending eligible inventories (e.g. thousands of filings queued vs dozens processed).
- **CMBS_SCHEDULE_VS_EX10_TERMS** [HIGH]: Terms pilot: CMBS franchise schedules yield expiration baseline but weak lifecycle terms; EX-10 HMA prose yields term/renewal/notice.
- **FRANCHISE_ONLY_VERIFIED_INVENTORY** [HIGH]: 258 VERIFIED agreements are FRANCHISE-only; management agreements require separate discovery.
- **EXECUTED_RENEWAL_GAP** [MEDIUM]: Terms pilot verified 0 executed renewals/amendments — later-document research backlog remains.
- **IBR_AND_EXHIBIT_ENUMERATION** [MEDIUM]: Parent accession ≠ exhibit; IBR targets and EX-10 package enumeration under-processed.
- **PRE_2012_AND_DEAL_DEPOSITORS** [MEDIUM]: Period partitions start 2012 for contract-timing queries; CMBS depositors/issuers outside registry under-covered.
- **INTL_NON_SEC** [MEDIUM]: SEC may surface global hotels; intl adapters exist as readiness only — no production non-SEC harvest authorized here.
- **BRAND_BIASED_EXA_QUERIES** [LOW]: Some Exa families still mention Marriott/Hilton/Hyatt as examples — must remain non-restrictive hints.

## 4. Brand-neutral expandable discovery process

Designed in `lib/hotel-intelligence/sec-edgar/sec-discovery-expansion-design-v1.js` and `BRAND_NEUTRAL_DISCOVERY_PROCESS.json`.

Loop: **Seed/Resume → Content discovery → Entity extraction → Source relationships → Retrieve/Extract → Geo/HPC priority → Coverage report**.

Proposed factory stage: **`DISCOVER_EXPAND`** (Research Mode only; not in default sequence until authorized). Reuses existing PowerShell/Node engine, checkpoints, cost ledgers, and `ENRICH_CONTRACT_TERMS`.

## 5. Durable coverage ledger V3

Module: `sec-discovery-coverage-ledger-v3`  
Status: **schema ready** (empty — awaiting population in expansion workstream).

Tracks per document/exhibit: CIK, accession, filing type/date, document IDs, retrieval/extraction status, hotels discovered, contractual intel counts, last research date, follow-ups, cost.

## 6. Geographic policy

- Persist verified intelligence **globally**
- Prioritize HPC reconcile/enrich: **Americas + Europe**
- Expand beyond SEC via intl adapters (readiness only in this packet)

## 7. Incremental contractual intelligence per research dollar (next workstream order)

1. **EX10_HMA_FRANCHISE_EXHIBIT_HARVEST** — Terms pilot showed EX-10 management/franchise prose >> CMBS schedule rows for lifecycle terms
2. **CMBS_DEAL_GRAPH_EXPANSION** — Existing verified inventory is CMBS-heavy; many deals/issuers still unsearched
3. **UNREGISTERED_ISSUER_ADAPTIVE_QUEUE** — Registry of ~32 companies is a seed, not the universe
4. **LATER_AMENDMENT_AND_RENEWAL_PROBES** — Terms pilot: 0 executed renewals verified — needs systematic later-doc search
5. **REIT_10K_PROPERTY_SCHEDULE_BACKFILL** — Large eligible inventories barely processed (pending >> processed)
6. **IBR_AND_HISTORICAL_PERIOD_PARTITIONS** — Incorporated-by-reference + pre-2019 periods under-covered
7. **INTL_NON_SEC_ADAPTERS** — SEC finds global hotels; durable intl coverage needs local disclosures

## 8. Sequencing & governance

1. Contract Terms & Renewals pilot — **completed on PR #499** (do not relaunch uncontrolled).
2. **This packet** — audit + design + roadmap (no paid expansion run).
3. Next — founder-authorized `DISCOVER_EXPAND` execution under budget ceiling.

Boundaries preserved: Research Mode · Canonical Promotion · publication authorization · CUSTOMER_BLOCKED default · no HPC creates in this packet.
