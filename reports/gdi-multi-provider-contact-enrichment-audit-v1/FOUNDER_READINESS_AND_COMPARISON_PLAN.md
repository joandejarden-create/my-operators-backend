# GDI Multi-Provider Contact Enrichment — Readiness & Comparison Plan

**Generated:** 2026-11-20  
**Stream:** GDI (`ws-gdi`) · branch `feat/gdi-google-style-contact-search-before-surfe-v1`  
**PR #484:** OPEN (Integration Controller owns merge)  
**Authorization:** Zero paid person-enrichment executed in this audit.

---

## Verdict

All four paid enrichment APIs have credentials locally and respond on non-chargeable account endpoints (where available). **Do not buy new subscriptions yet.** Prefer Google/Exa-first public research, then a **selective** paid ladder: PDL (identity) → Prospeo (email) → Surfe on-click → FullEnrich waterfall last. Apollo stays disabled. Apify remains LIMITED USE.

**STOP** — no paid enrichment until founder sets **per-provider credit ceilings**.

---

## 1. Provider credential and connectivity status

| Provider | Env var | Key present | Non-chargeable probe | Result |
|----------|---------|-------------|----------------------|--------|
| **Surfe** | `SURFE_API_KEY` | yes | `GET /v1/credits` | HTTP 200 |
| **PDL** | `PDL_API_KEY` | yes | *(no free balance API in client)* | Key present; enrich **not** called |
| **FullEnrich** | `FULL_ENRICH_API_KEY` | yes | `GET /account/keys/verify` + `/account/credits` | HTTP 200 / 200 |
| **Prospeo** | `PROSPEO_API_KEY` | yes | `GET /account-information` | HTTP 200 |
| Apollo | `APOLLO_IO_API_KEY` | yes | prior pilot | Free plan blocks People Enrichment |
| Apify | `APIFY_TOKEN` | yes | prior pilot | LIMITED USE (~$0.09 spent earlier) |
| Google Gemini | existing GDI Google-first | configured in prior work | — | Primary public research |
| Exa | existing PCD | — | — | Unresolved / conflict cases |

API keys are never logged or written into this report.

---

## 2. Current available credits

| Provider | Balance (live read this audit) | Notes |
|----------|--------------------------------|-------|
| **Surfe** | email **993** · mobile **198** · search **0** | Search uses daily quota (200 results/day without search credits) |
| **FullEnrich** | **36** credits | Low — protect for justified waterfall only |
| **Prospeo** | **100** remaining · **0** used · plan **FREE** | Renews ~2026-12-14 |
| **PDL** | **Unknown without chargeable call** | Credits appear on enrich response headers only |

---

## 3. Exact endpoint permissions (Dealality code + docs)

### Surfe (`lib/surfe/client.js`)
- `GET /v1/credits` — balance (free)
- `POST /v2/people/search` — Search People
- `POST /v2/people/enrich` + poll — email / mobile / LinkedIn / job history via `include.*`
- Mobile independently controllable (`include.mobile=false`; GDI default `SURFE_INCLUDE_MOBILE=false`)
- Landlines returned without mobile credits (Surfe docs)

### PDL (`lib/pdl/client.js`)
- `GET /v5/person/enrich` — 1 credit per HTTP 200 match
- `POST /v5/person/bulk` — ≤100; charged per 200 row
- Inputs: name + company/domain and/or LinkedIn; **email/phone forbidden as inputs**
- Returns identity, LinkedIn, title, employer, work/personal emails, phones (plan-dependent; may mask as booleans on under-entitled plans)

### FullEnrich (`lib/fullenrich/client.js`)
- `GET /account/keys/verify` · `GET /account/credits` — free
- `POST /contact/enrich/bulk` · poll get — waterfall
- Dealality helper defaults to **work emails only** (`contact.phones` / personal emails off)
- Rate limit: **60 req/min**, ≤100 contacts/request

### Prospeo
- `GET https://api.prospeo.io/account-information` — free
- `POST /enrich-person` · bulk enrich — **no Dealality client module yet** (key present; registry says `client_exists_gated` but no `lib/prospeo` found)
- `enrich_mobile=false` for email-only

---

## 4. Per-provider email and phone costs

| Provider | Email / identity | Mobile / phone | No match | Re-query |
|----------|------------------|----------------|----------|----------|
| **Surfe** | 1 email credit / person (enrich L1, worst-case in client) | 1 mobile credit / person when `include.mobile` | Job may complete empty | EnrichmentID reuse; do not blind resubmit |
| **Surfe Search** | — | — | — | Docs: 1 search credit / 10 results when search credits enabled; **account has 0 search credits** → 200 results/day free quota |
| **PDL** | 1 Enrichment credit / successful 200 match (includes contact fields when entitled) | Same credit (phones in response when entitled) | Non-200 typically 0 | Avoid retries except transport |
| **Prospeo** | **1** credit / verified email found | **10** / mobile (email included free) | **0** | **0** within 90 days same record |
| **FullEnrich** | **1** / work email found (deliverable/HP/catchall) | **10** / mobile · **3** / personal email | **0** | **0** duplicate within ~3 months (with caveats) |

Monetary $/credit conversion depends on each dashboard purchase tier — not inferred here. Report exposure in **credits**.

---

## 5. Current API integration coverage

| Capability | Surfe | PDL | FullEnrich | Prospeo | Google/Exa | Apify | Apollo |
|------------|-------|-----|------------|---------|------------|-------|--------|
| GDI on-demand reveal | **yes** | no | no | no | via PCD | LIMITED | blocked |
| HE contact intelligence | **yes** (session) | eval modules | gated submit | stub only | hierarchy | — | — |
| Identity / LinkedIn | enrich + search | **strong** | via LI input | enrich | discovery | weak | — |
| Domain matching | companyDomain | company website param | domain field | company_website | public | — | — |
| Email recovery | yes | yes (plan) | waterfall | yes | public first | weak | blocked |
| Phone recovery | mobile opt-in | yes (plan) | opt-in | opt-in | public | false+ | blocked |
| Disable mobile | **yes** | N/A (single credit) | **yes** (omit phones) | **yes** | — | — | — |
| Account credits API | yes | headers only | yes | yes | — | usage API | — |

Reusable modules: `lib/surfe/client.js`, `lib/pdl/client.js`, `lib/fullenrich/*`, `fullenrich-gated-submit.js`, `gdi-surfe-contact-provider-v1.js`, `gdi-contact-identity-preflight-v1.js`, Google-first / PCD / missing-channel resolver.

---

## 6. Licensing and customer-display feasibility

**Policy baseline (Dealality):** Independently verified **public** facts may persist and publish under existing GDI rules. Provider-derived emails/phones stay **separate**, never silently promoted to public-verified, and **no customer publication** until permitted-use + founder auth.

| Provider | Internal eval | Airtable persist | In-product display | Hotel customer disclosure | Redistribution / sublicense | Retention notes |
|----------|---------------|------------------|--------------------|---------------------------|-----------------------------|-----------------|
| **Surfe** | yes | **Forbidden** for Surfe PII (session-only binding in HE/GDI) | Session reveal only | Do **not** publish as Dealality-verified public | Treat as provider-restricted | Ephemeral; reload clears |
| **PDL** | yes | Internal/eval only until counsel confirms display scope | `.env.example`: written permission for **Dealality embedded product use (2026-09)** — still **not** resale; flow-down terms apply | Possible **only** if matches written PDL license + founder auth; keep labeled provider-derived | **No** resale / competing DB | Min fields; no bulk contact cache |
| **Prospeo** | yes | Internal only under standard ToS | Standard ToS: **internal B2B use**; no third-party disclose without Order Schedule 5 | **Not permitted** under FREE/standard ToS without redistribution Order | Schedule 5 optional; not assumed | Cache limits per plan/order |
| **FullEnrich** | yes | Internal only under standard ToS | Standard ToS does **not** cover resale; reseller/custom TOS required for customer redistribution | **Not permitted** under standard TOS without custom terms | Reseller program exists — not signed here | Provider stores enrich ~3 months |
| **Google/Exa public** | yes | Yes when independently verified | Yes when verified | Yes when verified + publication rules | N/A (public facts) | Provenance required |

**Do not assume API access = right to show or sell contact data to hotels.**

---

## 7. Recommended test cohort (10)

Shared cohort — real GDI contacts, Google/Exa exhausted or channel-incomplete, distinct people, high commercial relevance where possible.

| # | Person | Opportunity ID | Employer (as known) | Gaps | Why selected |
|---|--------|----------------|---------------------|------|--------------|
| 1 | Abbas Zavar | `rx_d_2400ebe4c2bb` | U of Toronto / IHPME | email, phone | Verified LinkedIn; identity READY |
| 2 | Daryl Simon | `wy_pd_fc13e2e08dd5` | The Tie | email, phone, LI verify | Candidate LinkedIn |
| 3 | Ariela Kiradjian | `gdi_opp_15th_annual_boutique_hotel_investment_conference_11` | Boutique & Luxury Lodging Assn | email, phone, LI | Domain `bllaevents.com`; entry-point |
| 4 | Francesco Francavilla | `gdi_opp_wrome_pilot_v0_red_bull_sailgp_rome_2027` | Red Bull | email, phone, LI | Title known; exhausted |
| 5 | Olivia Jallits | `gdi_opp_iaadfs_summit_of_the_americas_2027_6` | IAADFS | email, phone, LI | Event Director |
| 6 | Aimee Welsh | `rx_d_5976330f194d` | ICC / World Chambers | email, phone, LI | Exhausted; Apify candidate email unverified |
| 7 | Agathe Lairy | `rx_d_52859ee0961e` | OENOVITI / hosts | email, phone, LI | Exhausted |
| 8 | Alexandre Catton | `rx_d_d29851414427` | Easyfairs Group | email, phone, LI | Trade organizer |
| 9 | Andrew Bartucci | `rx_d_096138499154` | Insurance Bureau of Canada | email, phone, LI | Association demand |
| 10 | Ben Hawkins | `gdi_opp_alexandria_soccer_kickoff_2027` | Alexandria Soccer Assn | email, phone, LI | Bethesda-area hotel demand |

Excluded: org-label rows (IAFL, City Cruises), generic mailboxes, invented names, Abramo quarantined phone.

---

## 8. Estimated pilot charges (proposal only — **not executed**)

Assumptions: **email-only**, mobile **off**, waterfall **off**, no Surfe Search (0 search credits), one pass per provider max, stop when named email verified.

| Arm | Max credits if all 10 charged | Likely lower if misses / free re-query |
|-----|-------------------------------|----------------------------------------|
| A. PDL identity+contact | ≤ **10** PDL credits | Non-matches free |
| B. Prospeo email-only | ≤ **10** Prospeo credits | 0 on miss; FREE plan 100 avail |
| C. Surfe enrich email-only | ≤ **10** Surfe email credits | On-click / batch-auth only |
| D. FullEnrich work-email-only | ≤ **10** FE credits | Balance only **36** — cap ≤10 |
| **All four sequential worst-case** | ≤ **40** credits across systems | **Not recommended** |

**Proposed first paid gate (founder must authorize separately):**

1. **PDL only** — ceiling **10** credits — identity + LinkedIn + email/phone presence  
2. If still missing email: **Prospeo email-only** — ceiling **10** credits  
3. Surfe / FullEnrich **not** in first paid gate  

Optional later: Surfe Search People for org-internal better contacts (needs search credits or stays within 200/day quota) — separate ceiling.

---

## 9. Recommended provider ordering (hypothesis → validate)

```
0. Contact Identity Preflight (free) → READY/PARTIAL/UNRESOLVED
1. Google Gemini + Exa          → independently verifiable public channels
2. Apify                        → LIMITED USE hard cases only
3. PDL                          → identity / employer / LinkedIn / supplementary contacts
4. Prospeo                      → targeted professional email when public+PDL insufficient
5. Surfe Search People          → better commercially relevant people in verified orgs (quota/credits)
6. Surfe enrich                 → explicit on-click email (mobile off unless approved)
7. FullEnrich                   → selective work-email waterfall when single-provider fails + cost justified
8. Apollo                       → DISABLED (Free plan)
```

---

## 10. Proposed automated routing rules

1. Never run all providers automatically.  
2. Skip paid if named email already verified public **or** identity UNRESOLVED (fix identity first).  
3. Cache permitted outcomes (provider + person key + channel); block repeat chargeable calls within provider dedupe windows.  
4. Mobile / waterfall require **separate** founder flags.  
5. Surfe email/phone remain **on-click** unless batch ceiling approved.  
6. High commercial value + PARTIAL/READY identity → allow PDL then Prospeo under ceilings.  
7. FullEnrich only if Prospeo+PDL miss **and** FE balance ≥ reserved floor (recommend keep ≥20 spare).  
8. Persist: public-verified → canonical; provider-derived → labeled internal / session / eval stores only per license.  
9. Quarantine wrong-person, generic mailboxes, switchboards.  
10. Coordinate shared HE/GDI provider changes via Integration Controller.

---

## 11. Overlap risks

- **Same email billed twice** across Surfe + Prospeo + FullEnrich + PDL if sequential without cache.  
- **Wrong-person LinkedIn** amplified if PDL/Surfe trust weak LI inputs — require identity preflight VERIFIED only for LI.  
- **FullEnrich waterfall** may re-hit providers Dealality already called → double spend + license mix.  
- **Surfe Search vs enrich** — search finds people; enrich reveals channels; both cost.  
- **PDL masked booleans** on under-entitled plans look like “has email” without usable value — do not count as recovery.  
- **Prospeo FREE** contribution license on uploads — minimize unnecessary PII uploads.  
- Licensing conflict if one provider’s email is shown to customers while another forbids disclosure.

---

## 12. Most cost-effective GDI production architecture (recommendation)

1. **Public-first forever:** Google → Exa → (Apify LIMITED) with identity preflight.  
2. **One identity provider:** PDL for unresolved identity/LinkedIn/employer (written embedded-use permission already noted).  
3. **One cheap email specialist:** Prospeo email-only after public+PDL miss (build thin client; stay internal-display until redistribution Order).  
4. **Surfe:** keep as **customer on-click** enrich with identity-preflight inputs (name+company+domain+verified LI); Search People only when need better org contacts and quota allows.  
5. **FullEnrich:** rare escalation (work-email-only), balance-gated — not default.  
6. **No new subscriptions** until cohort comparison shows incremental lift vs public+PDL+Prospeo.  
7. **Customer-facing channels:** prefer independently verified public; provider-derived only under confirmed license + founder auth; Surfe never into Airtable public fields.

---

## Phase 4 test design (for next authorization)

| Dimension | Method | Pass criteria |
|-----------|--------|---------------|
| A Identity | PDL vs baseline preflight | Correct person + employer |
| B LinkedIn | PDL / Surfe vs classifier | VERIFIED or qualified candidate; reject mismatches |
| C Named email | Prospeo → Surfe → FE | Named person mailbox; no generics |
| D Phone | **Deferred** (mobile off) | — |
| E Better commercial contact | Surfe Search (optional later) | Higher commercial relevance than baseline |

Verification checklist: correct person/org/role; LinkedIn qualified; email↔person match; phone classification; extensions retained; commercial relevance; quarantine mismatches.

---

## Integration notes

- Work stays on open GDI contact-enrichment stream (**PR #484**); no competing PR.  
- No STAGED→CURRENT. No Surfe paid reveals in this phase. No full-database backfill.  
- Prospeo: implement client only after founder authorizes first paid Prospeo ceiling.

---

## STOP

Ready for founder review. **No new paid enrichment** without provider-specific credit ceilings.  
Next actionable ask: authorize **PDL ≤10** and/or **Prospeo email-only ≤10** on the cohort above — or revise cohort/order.
