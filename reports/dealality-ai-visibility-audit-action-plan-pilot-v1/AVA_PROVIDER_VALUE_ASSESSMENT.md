# AVA Provider Value Assessment

**Contract:** `AVA_RESEARCH_PROVIDER_POLICY_V1`
**Generated:** 2026-11-05T17:04:07.003Z
**Paid research authorized:** No
**Billable calls executed:** None

## Decision summary

| Provider | Classification | Key in env | Integrated in AVA | Use when |
|---|---|---|---|---|
| Exa | **DEFAULT** | Yes | Yes | Primary public discovery for Digital Authority V2 candidate pages |
| Parallel | **CONDITIONAL** | Yes | No | Supplementary deep research for hard hotel-specific questions / multilingual gaps Exa missed |
| Context.dev | **CONDITIONAL** | Yes | No | JS-heavy scrape, structured extract, brand retrieve for bot-protected or incomplete HTML |
| Ahrefs | **OPTIONAL_DEEP_DIVE** | No | No | Referring-domain / URL backlink index for verified property backlinks + competitor gaps |
| DataForSEO | **OPTIONAL_DEEP_DIVE** | Yes | No | Optional pay-as-you-go backlinks + maps/SERP for listing URL discovery |

## Provider detail

### Exa (`exa`)

- **Classification:** DEFAULT
- **Incremental value:** Discovers destination/CVB/guide candidates; does not replace a backlink index
- **Cost model:** Usage-based Exa credits/API (existing Dealality Exa account)
- **Per-hotel estimate:** Typically low single-digit credits for ≤10 searches; confirm Exa dashboard
- **Note:** Already the AVA V2 default path. Additional Exa volume still counts as research spend — bound searches per hotel.

### Parallel (`parallel`)

- **Classification:** CONDITIONAL
- **Incremental value:** Broader research synthesis when Exa recall is thin; not a backlink database
- **Cost model:** Task list prices in-repo: lite $0.005, base $0.01, core $0.025, pro $0.10 per task (verify Parallel dashboard)
- **Per-hotel estimate:** Pilot envelope: 2–4 core tasks ≈ $0.05–$0.10 + FindAll preview/base if used
- **Note:** Client exists under hotel-intelligence / GDI. Do not enable as AVA default.

### Context.dev (`context_dev`)

- **Classification:** CONDITIONAL
- **Incremental value:** May recover facts from WAF/JS pages and extract structured hotel attributes for DCR
- **Cost model:** Credit ledger in lib/context-dev/credit-ledger.js
- **Per-hotel estimate:** Pilot envelope: ~15–40 credits/hotel (search+scrape+optional extract) — convert via Context.dev plan rate
- **Note:** Best for ACCESS_RESTRICTED brand pages and schema/fact extraction — not for backlink graphs.

### Ahrefs (`ahrefs`)

- **Classification:** OPTIONAL_DEEP_DIVE
- **Incremental value:** Highest incremental value for complete backlink inventories Ahrefs/Semrush-class indexes provide
- **Cost model:** Public Lite ~$129/mo class + API units (max(50, rows×per-row)); confirm current plan
- **Per-hotel estimate:** Pilot 3 hotels: expect tens–hundreds of API units depending on referring-domain depth — quote before buy
- **Note:** Listed in AVA paid-provider allowlist but **no API key and no client**. Account entitlement must be confirmed outside this repo before any call.

### DataForSEO (`dataforseo`)

- **Classification:** OPTIONAL_DEEP_DIVE
- **Incremental value:** undefined
- **Cost model:** Pay-as-you-go; backlinks ~$0.02+/task class — verify live price list
- **Per-hotel estimate:** Pilot 3 hotels: set hard cap e.g. $15–25 total before run
- **Note:** Credentials exist for census paths; AVA paid research flag still required. Not a substitute for Ahrefs quality without validation.

## Default Full E2E path

- Digital Authority: exa + HTML inspection (V2)
- Digital Content Readiness: identity graph + zero-cost HTTP platform probes
- Paid enrichment: off until founder authorization + budget

## Do not use

- Semrush / Majestic / Moz — no entitlement confirmed for AVA; do not subscribe
- Any provider call that bypasses AVA_PAID_RESEARCH_AUTHORIZED for paid backlink indexes

## Founder decision required

Authorize a **bounded three-hotel paid pilot** (Ahrefs entitlement confirmation **or** DataForSEO backlinks + Context.dev scrapes + optional Parallel) with a hard USD cap before any billable call. See `AVA_THREE_HOTEL_RESEARCH_COMPARISON.md` for the exact proposed call list.
