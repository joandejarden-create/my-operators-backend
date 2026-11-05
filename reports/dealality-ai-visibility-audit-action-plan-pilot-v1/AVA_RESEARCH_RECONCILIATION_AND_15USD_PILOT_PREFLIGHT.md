# AVA — Research Reconciliation and $15 Provider Pilot Preflight

**Contract:** `AVA_RESEARCH_RECONCILIATION_AND_15USD_PILOT_PREFLIGHT_V1`  
**Generated:** 2026-11-05  
**Status:** `AWAITING_FOUNDER_SPENDING_AUTHORIZATION`  
**Paid calls executed this turn:** **$0** (DataForSEO `appendix/user_data` balance read only; no backlinks/maps/Context.dev chargeable calls)  
**Publication / deploy / ADP remeasurement / Ahrefs / Parallel paid:** **not authorized / not executed**  
**Portfolio scope:** three hotels only — **no 38-hotel regeneration**

---

## Executive verdict

Count gaps between Digital Authority V2 certification figures and later provider-comparison / PDF presentation numbers are **not data loss**. They come from **layered counts** (research inventory vs customer-facing sources), **PDF evidence-trim demotions**, one **http/https duplicate**, and **quality / access filters**.

Authoritative customer PDF counts remain the quality-filtered `digitalAuthority.sources` rows. Admin `researchInventory` + `discoveryCoverage.verified*` preserve fuller research provenance and must not be silently collapsed into customer counts.

**Spending gate:** exact call manifest below. Combined hard cap **$15** (DataForSEO ≤ **$10**, Context.dev ≤ **$5**).  
**Recommendation: CONDITIONAL GO** — proceed only after founder explicitly approves this manifest. Context.dev scrapes remain cost-bound only via a conservative observed ceiling (see §5); prefer extract + search.

---

## 1. Reconciled research counts

Payload authority: `lib/ai-visibility-audit-action-plan/payloads/{hotel}-v1.json` (current certified AVA V2 payloads).

### Authoritative table

| Hotel | Customer PDF verified backlinks (raw / unique linking pages) | Customer PDF external mentions | Research inventory verified backlinks (raw / unique) | Research inventory external mentions | `discoveryCoverage.verifiedBacklinks` | `discoveryCoverage.verifiedExternalMentions` |
|---|---:|---:|---:|---:|---:|---:|
| **Castillo Hotel Son Vida** | **4 / 3** | **3** | **7 / 6** | **10** | **7** | **10** |
| **Waterstone Resort & Marina** | **0 / 0** | **1** | **0 / 0** | **5** | **0** | **5** |
| **AC Hotel A Coruña** | **2 / 2** | **4** | **2 / 2** | **18** | **2** | **18** |

### Castillo — customer-facing verified backlinks (4 raw → 3 unique)

| # | Referring page | Destination class | Notes |
|---|---|---|---|
| 1 | `http://www.visit-palma.com/hotel-castillo-son-vida` | Brand Marriott travel URL | Same page as #4 |
| 2 | `https://www.illesbalears.travel/en/mallorca/spa-castillo-hotel-son-vida` | Hotel-owned site | Keep |
| 3 | `https://www.mallorcahouses.com/en/information/things-to-do-in-mallorca/spa/` | Hotel-owned site | Keep |
| 4 | `https://www.visit-palma.com/hotel-castillo-son-vida` | Brand property page | **Duplicate of #1** (http vs https) |

### Castillo — inventory-only verified backlinks (still in research inventory; not in customer PDF)

| Referring page | Destination | Disposition |
|---|---|---|
| `mallorcacomercial.es/en/zona/113/` | Hotel-owned site | Trimmed to Admin inventory (PDF evidence-source trim) — **valid research finding** |
| `mallorcacomercial.es/empresa/850/` | Hotel-owned site | Same |
| `mallorcaconventionbureau.com/.../castillo-hotel-son-vida...` | Brand property page | Same |

### Waterstone — mentions

| Layer | Count | URLs / notes |
|---|---:|---|
| Customer PDF | **1** | `greatlocations.com/...` only |
| Research inventory | **5** | + `exa.ai/library/...` (tool artifact), Amex Hotel Collection, WeddingWire (bot-gated → opportunity, not customer hyperlink), Hilton Boca locations (brand-family / access) |

Verified hotel backlinks remain **0** at both customer and inventory layers after quality filter — consistent with ACCESS_RESTRICTED coverage, not a missing payload.

### Cross-artifact consistency

| Artifact | Castillo BL / mentions | Waterstone BL / mentions | AC BL / mentions | Consistent with customer sources? |
|---|---|---|---|---|
| DA payload `sources` | 4 / 3 | 0 / 1 | 2 / 4 | **Yes — authoritative customer** |
| DA `researchInventory` | 7 / 10 | 0 / 5 | 2 / 18 | **Yes — Admin layer** |
| `discoveryCoverage.verified*` | 7 / 10 | 0 / 5 | 2 / 18 | Matches inventory aggregates (inflated vs PDF if misread as customer) |
| Three-hotel research comparison | 4 / 3 | 0 / 1 | 2 / 4 | **Yes** |
| PDF visibility pilot | 4 / 3 | 0 / 1 | 2 / 4 | **Yes** (renderer fixed to customer counts) |
| DA V2 pilot results `after.verifiedHotelBacklinks` | **7** | 0 | 2 | Research-complete count — **not** customer PDF count |
| DCR `sourceInventory` | 4 URLs (brand + Visit Palma + Golfbreaks) | 5 (hotel site + mention) | 3 (brand ES + CVB) | Overlaps DA; not a second BL ledger |
| Action registers | Visit Palma protect / Elegant Golf opp; WeddingWire opp; CVB URL | Present | Present | Aligned with DA/DCR themes |
| Evidence registers | ADP + public URLs | Present | Present | No contradiction of customer BL counts |
| Platform correction register | 1 open | 2 open | 1 open | Unrelated to BL count gap |
| Certification reports | Mix of 7 vs 4 depending on layer cited | 5 vs 1 mentions | Stable 2 | Explainable — see §2 |

**No silent overwrite performed.** Historical V2 discovery of 7 Castillo backlinks remains in `researchInventory` + certification examples.

---

## 2. Root causes of discrepancies

| Example | Earlier figure | Later figure | Root cause | Data loss? |
|---|---|---|---|---|
| Castillo 7 vs 4 verified backlinks | DA V2 pilot / `discoveryCoverage.verifiedBacklinks=7` | Customer PDF / comparison = **4** | **(a)** Different research layers — inventory vs customer sources. **(b)** Evidence-source trim moved 3 valid BLs (Mallorca Comercial ×2, MCB) to Admin `researchInventory` for PDF hyperlink safety/parity (`ava-six-hotel-final-evidence-source-trim-v1.mjs`). **(c)** Customer row set still double-counts Visit Palma http+https → **4 raw / 3 unique**. | **No** |
| Waterstone 5 vs 1 external mentions | `discoveryCoverage.verifiedExternalMentions=5` | Customer PDF = **1** | Classification + quality filter: Exa library URL, Hilton first-party, Amex, WeddingWire demoted from customer hyperlinks; only greatlocations retained. WeddingWire kept as **opportunity**, not verified mention. | **No** |
| Provider comparison vs “certified V2” | Inventory-inclusive certification language | Customer-facing comparison table | Incorrect payload selection if `discoveryCoverage` or pilot `after.*` is treated as PDF truth | Presentation / selection error, not deletion |
| Destination URL treatment | Hotel-owned `castillosonvidamallorca.com` vs Marriott brand URLs both counted as verified destinations | Unchanged in inventory | Destination class differs; both remain valid when href verified | No |
| Deduplication | Visit Palma listed twice in customer sources | Unique linking page = 1 | Incomplete URL normalization (scheme-only difference) | No — duplicate presentation |

---

## 3. Required data / PDF corrections (do **not** apply until separately approved)

| ID | Change | Why | Risk if skipped |
|---|---|---|---|
| **R1** | Deduplicate Castillo Visit Palma http/https in `digitalAuthority.sources` (keep one canonical https) | Customer BL raw 4 overstates unique referring pages (true unique = 3) | Mild customer overcount |
| **R2** | Optionally **promote** Mallorca Comercial ×2 + MCB from `researchInventory` → customer `sources` after live URL safety recheck | They are verified property hyperlinks already in Admin inventory; trim was PDF-parity, not invalidation | Under-reports destination authority vs research truth |
| **R3** | Annotate / freeze `discoveryCoverage.verified*` as **research-inventory inclusive**; never render as customer PDF snapshot | Prevents 7-vs-4 class regressions (PDF audit already partially fixed renderer) | Recurring founder confusion |
| **R4** | Align stale `discoveryCoverage.customerFacingSources` (Castillo shows 10; actual customer sources = 7) | Counter drift after trim | Internal metric inconsistency only |
| **R5** | Waterstone: keep customer mentions = 1; do **not** promote Exa/Hilton/WeddingWire/Amex to customer verified mentions without new evidence | Correct demotions | Would invent customer-ready authority |

**No PDF regeneration in this preflight.** Corrections above are queued for a later bounded edit pass if founder approves.

---

## 4. Exact provider call manifest (chargeable — **not executed**)

### Account status (verified read-only where possible)

| Provider | Credentials in `.env` | Balance / entitlement | Notes |
|---|---|---|---|
| Exa | Present | Not re-queried | Remains **default** research provider; not in this paid pilot |
| DataForSEO | Login + password present | **USD balance $49.524** via `GET /v3/appendix/user_data` | Usage counters for backlinks endpoints currently `0` (unused, not disabled). Pay-as-you-go. |
| Context.dev | API key present | **Live credit balance not read this turn** (no free balance endpoint; any probe may bill) | Documented: scrape ≥1 cr, search 1/10 results, extract 10. Historical pilot: failed scrapes reported `credits_consumed=10` → scrape hard ceiling **unproven** |
| Parallel | Key present | Deferred | **No paid Parallel calls** |
| Ahrefs | **Absent** | N/A | **No purchase / no calls** |
| `AVA_PAID_RESEARCH_AUTHORIZED` | Unset | — | Must be set only after founder approval of this manifest |

### Pricing basis (documented examples; no live priced task run)

| Endpoint | Documented / example cost | Bound used in max column |
|---|---|---|
| DataForSEO `backlinks/summary/live` | Example ~**$0.020**; marketing ~**$0.024**/request | **$0.05** |
| DataForSEO `backlinks/backlinks/live` | Example ~**$0.020** + ~**$0.000036**/row | limit 100 → **$0.08** |
| DataForSEO `backlinks/referring_domains/live` | Same class as backlinks list | **$0.08** |
| DataForSEO `backlinks/domain_intersection/live` | Same class (~$0.02+) | **$0.10** |
| DataForSEO `business_data/google/my_business_info/live` | Example **$0.0054** | **$0.02** |
| DataForSEO `serp/google/maps/live/advanced` | Example **$0.002** (depth ≤100) | **$0.01** |
| Context.dev search (≤10 results) | **1 credit** | Worst USD: Developer overage **$0.003**/credit |
| Context.dev extract | **10 credits** | Same |
| Context.dev scrape markdown | Docs **1**; observed fail bill **10** | Reserve **10 credits** if scrape authorized |

### Targets

| Hotel | Hotel-operated / owned domain | Brand / booking surface | Competitor intersection peer |
|---|---|---|---|
| Waterstone Resort & Marina | `waterstoneboca.com` | Hilton Curio brand property (ACCESS_RESTRICTED today) | Boca / Curio peer from DA opportunities (e.g. local Hilton Boca) — finalize peer domain at execution from payload competitor notes |
| AC Hotel A Coruña | `hotelacacoruna.com` | `marriott.com/.../lcgco-ac-hotel-a-coruna/...` | ACHM / CVB peer or NH Coruña from inventory (pick 1 at execution) |
| Castillo Hotel Son Vida | `castillosonvidamallorca.com` | `marriott.com/.../pmilc-castillo-hotel-son-vida...` | `Sheraton Mallorca Arabella` / `arabellagolfmallorca.com` |

### DataForSEO calls (cap **$10**)

| Call ID | Provider | Endpoint | Hotel | Target | Purpose | Est. USD | Max USD | Expected incremental value |
|---|---|---|---|---|---|---:|---:|---|
| DFS-01 | DataForSEO | `POST /v3/backlinks/summary/live` | Waterstone | `waterstoneboca.com` | Hotel referring-domain / backlink summary | 0.024 | 0.05 | New RD count vs Exa zero-BL baseline |
| DFS-02 | DataForSEO | `POST /v3/backlinks/summary/live` | Waterstone | brand property URL (Hilton Curio page) | Outdated / alternate destination graph | 0.024 | 0.05 | Detect brand vs hotel domain split |
| DFS-03 | DataForSEO | `POST /v3/backlinks/backlinks/live` `limit=100` | Waterstone | `waterstoneboca.com` | Sample referring pages | 0.028 | 0.08 | Candidate verified BLs for validation |
| DFS-04 | DataForSEO | `POST /v3/backlinks/referring_domains/live` `limit=50` | Waterstone | `waterstoneboca.com` | Referring domains list | 0.028 | 0.08 | Domain-level authority inventory |
| DFS-05 | DataForSEO | `POST /v3/backlinks/domain_intersection/live` | Waterstone | hotel vs 1 peer | Competitor backlink gaps | 0.030 | 0.10 | Actionable gap list (not raw count) |
| DFS-06 | DataForSEO | `POST /v3/serp/google/maps/live/advanced` depth≤20 | Waterstone | `"Waterstone Resort & Marina Boca Raton"` | GBP / Maps listing URL discovery | 0.002 | 0.01 | Unlocks platform correction candidates |
| DFS-07 | DataForSEO | `POST /v3/business_data/google/my_business_info/live` | Waterstone | place from DFS-06 if found | GBP field snapshot | 0.006 | 0.02 | Listing discrepancy → Correction Register |
| DFS-08…14 | DataForSEO | same pattern as 01–07 | AC Hotel A Coruña | `hotelacacoruna.com` + Marriott LCGCO + 1 peer + Maps/GBP | Same purposes | ~0.14 | ~0.39 | CVB/official URL + RD gaps |
| DFS-15…21 | DataForSEO | same pattern as 01–07 | Castillo | `castillosonvidamallorca.com` + Marriott PMILC + Sheraton peer + Maps/GBP | Same; validate inventory BLs vs index | ~0.14 | ~0.39 | Confirm MCB/Comercial; new RDs |

**DataForSEO subtotal:** estimated **≈ $0.45–0.55** · hard max if every call hits bound **≈ $1.17** · **envelope reserved $10** (slack for retries **forbidden** without new approval; one automatic retry only on HTTP 5xx with cost still inside remaining cap).

**Stop rule:** if any single DFS response `cost` would push cumulative DFS spend above **$10**, abort remaining DFS calls.

### Context.dev calls (cap **$5**)

| Call ID | Provider | Endpoint | Hotel | Purpose | Est. credits | Max credits | Est. USD* | Max USD* | Expected incremental value |
|---|---|---|---|---|---:|---:|---:|---:|---|
| CTX-01 | Context.dev | `POST /v1/web/search` ≤10 results | Waterstone | Discover GBP/OTA/listing URLs | 1 | 1 | 0.003 | 0.003 | Listing URLs Exa/HTML missed |
| CTX-02 | Context.dev | `POST /v1/web/extract` | Waterstone | Hilton Curio brand page facts (ACCESS_RESTRICTED today) | 10 | 10 | 0.03 | 0.03 | Previously inaccessible brand facts |
| CTX-03 | Context.dev | `POST /v1/web/extract` | Waterstone | Best GBP/OTA URL from CTX-01/DFS-06 | 10 | 10 | 0.03 | 0.03 | Cross-platform fact consistency |
| CTX-04…06 | Context.dev | search + 2× extract | AC Hotel A Coruña | ES Marriott page + CVB/OTA consistency | 21 | 21 | 0.063 | 0.063 | Address/meetings reconciliation evidence |
| CTX-07…09 | Context.dev | search + 2× extract | Castillo | Brand + destination/CVB facts | 21 | 21 | 0.063 | 0.063 | Spa/meetings fact lock |
| CTX-S1…S3 *(optional)* | Context.dev | `GET /v1/web/scrape/markdown` | One URL/hotel only if extract insufficient | JS/bot-gated HTML | 1 docs / **10 reserved** | **10** | 0.003–0.03 | **0.03** | Only if founder enables scrapes |

\*USD assumes **Developer overage $3 / 1,000 credits** as worst-case cash cost. If the org is on **Free** (1,000 credits/mo included) or **Pro** included allowance, **cash outlay may be $0** while credits still count against the pilot credit budget.

**Context.dev subtotal (extract+search only):** **63 credits** · worst-case cash **≈ $0.19** · with optional scrapes **≤ 93 credits / ≈ $0.28**.  
**Hard stop:** cumulative Context.dev credits > **1,600** (~$4.80 at $0.003) or any unexplained `credits_consumed` > reserved → abort further Context.dev.

**Excluded:** Ahrefs; Parallel paid Core/Lite/Pro tasks; Context.dev Answers/Ultra; DataForSEO depth>100 Maps; bulk 1000-row pulls.

---

## 5. Confirmed projected costs

| Bucket | Estimated | Hard maximum | Cap |
|---|---:|---:|---:|
| DataForSEO (21 calls) | ~$0.50 | ~$1.17 | **$10.00** |
| Context.dev (9 calls; scrapes off by default) | ~$0.19 cash / 63 credits | ~$0.28 cash / 93 credits if scrapes on | **$5.00** |
| **Combined** | **~$0.70** | **~$1.45** (scrapes on) | **$15.00** |

Costs are **bounded reliably** for DataForSEO (per-response `cost` field) and for Context.dev **search + extract** (fixed documented credits).  
Context.dev **scrape** remains `hard_bound_proven=false` in-repo; default manifest **excludes scrapes**. If founder enables CTX-S*, each scrape reserves **10 credits** (observed ceiling) and aborts the Context.dev lane on overrun.

---

## 6. Expected commercially useful outcomes

Primary success metric = **new evidence-supported customer actions**, not higher backlink tallies alone.

| Success signal | How measured post-pilot | Why it matters |
|---|---|---|
| New authoritative sources | Net-new referring domains/pages after quality filter, not already in inventory | Expand Digital Authority without noise |
| New verified backlinks | HTML or Context extract confirms hotel destination href | Customer PDF + actions |
| New actionable competitor gaps | Domain intersection → specific outreach targets with evidence of peer link / hotel absence | Commercial actions |
| New third-party listing discrepancies | Maps/GBP/OTA field diffs → Digital Platform Correction Register | Corrections > vanity metrics |
| Previously inaccessible facts verified | Hilton/Marriott bot-gated pages via Context extract | Unblocks DCR confirmations |
| New Correction Register rows | Count of net-new corrections | Direct customer value |
| Duplicates / unusable | Exa artifacts, first-party, spam RDs | Cost per useful action denominator |
| Cost per meaningful new action | Combined USD / (new actions + new corrections) | Kill/scale decision |

**Not success:** DFS “thousands of backlinks” that fail property-specific quality filter.

---

## 7. GO / NO-GO recommendation

### **CONDITIONAL GO**

| Gate | Result |
|---|---|
| No Ahrefs purchase | **Pass** |
| Exa remains default | **Pass** |
| Parallel paid deferred | **Pass** |
| Three-hotel scope only | **Pass** |
| DataForSEO cost bound + balance ($49.52) | **Pass** |
| Context.dev search+extract cost bound | **Pass** |
| Context.dev scrape hard bound | **Fail unless scrapes stay OFF or 10-credit reservation accepted** |
| Combined ≤ $15 | **Pass** (projected ≪ $15) |
| Founder approval of this exact manifest | **Required — pending** |
| `AVA_PAID_RESEARCH_AUTHORIZED=1` | **Required before any chargeable call** |

### What “GO” means if approved

1. Execute **only** DFS-01…21 and CTX-01…09 as listed (scrapes off).  
2. Stop on cap breach or unbounded Context.dev credit report.  
3. Reconcile outputs into Admin inventory first; promote to customer sources only after quality filter + CQA.  
4. Do **not** regenerate the 38-hotel portfolio; do **not** publish/deploy; do **not** change ADP baselines.

### What remains NO-GO without further approval

- CTX-S scrape lane  
- Expanding hotels, endpoints, limits, or peers beyond this manifest  
- Treating DFS raw backlink volume as certification success  
- Overwriting historical V2 inventory counts

---

## Spending authorization block (founder reply)

Reply with one of:

1. **`APPROVE MANIFEST AS WRITTEN`** — authorize DFS-01…21 + CTX-01…09; scrapes off; combined hard cap $15.  
2. **`APPROVE MANIFEST + ENABLE SCRAPES`** — also authorize CTX-S1…S3 at 10-credit reservation each.  
3. **`APPROVE DATAFORSEO ONLY`** — CTX deferred until Context.dev dashboard credit balance is confirmed in-chat.  
4. **`NO-GO`** — no chargeable calls.

Until one of the above is explicit in this thread, **no chargeable provider calls will run**.

---

## Provenance references

- Payloads: `lib/ai-visibility-audit-action-plan/payloads/castillo-hotel-son-vida-v1.json`, `waterstone-boca-raton-v1.json`, `ac-hotel-a-coruna-v1.json`  
- Trim script: `scripts/ava-six-hotel-final-evidence-source-trim-v1.mjs`  
- DA V2 results: `AVA_DIGITAL_AUTHORITY_V2_PILOT_RESULTS.json`  
- Comparison: `AVA_THREE_HOTEL_RESEARCH_COMPARISON.md`  
- PDF presentation audit: `AVA_PDF_VISIBILITY_PRESENTATION_AUDIT.md`  
- Context bounds: `lib/context-dev/operation-credit-bounds.js`  
- DFS client: `lib/research-engine-v2/dataforseo-client.js`
