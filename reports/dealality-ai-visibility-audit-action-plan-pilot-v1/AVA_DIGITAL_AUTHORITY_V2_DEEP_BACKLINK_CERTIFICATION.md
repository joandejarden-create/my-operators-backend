# AVA Digital Authority V2 — Deep Backlink Certification

**Contract:** `AVA_DIGITAL_AUTHORITY_V2`  
**Identity graph:** `AVA_PROPERTY_URL_IDENTITY_GRAPH_V1`  
**Generated:** 2026-11-05  
**Pilot:** Six commercial-priority hotels only (portfolio-wide rerun deferred)  
**Paid backlink providers:** **not used** · **$0** Ahrefs/Semrush/Majestic  
**Public discovery:** Exa Search (existing approved key) + direct HTML href inspection  
**Publication / founder approval:** unchanged · not authorized  
**Machine results:** `AVA_DIGITAL_AUTHORITY_V2_PILOT_RESULTS.json`

---

## Final verdict: `PASS_WITH_LIMITATIONS`

| Criterion | Result |
|---|---|
| Root cause of V1 “2 backlinks / 38 hotels” diagnosed | Yes |
| URL identity graph (brand vs hotel-operated) | Implemented |
| Multi-strategy public discovery (A–E) | Implemented & executed |
| Hyperlinks verified via HTML inspection | Yes |
| Classification integrity after quality filter | Yes |
| Six-hotel before/after produced | Yes |
| Customer QA + PDF regeneration | 6/6 PASS |
| Material discovery improvement without inventing links | Yes (2 portfolio → **14** quality-filtered third-party backlinks on 6 hotels) |
| Public research still incomplete vs paid indexes | Limitation acknowledged |

Verdict reflects **research completeness, verification integrity, and commercial usefulness** — not raw backlink inflation. First-party brand-family and low-quality referrers were demoted out of `VERIFIED_BACKLINK`.

---

## 1. Root cause of low V1 backlink discovery

| Question | V1 finding |
|---|---|
| Search queries used? | Ad hoc analyst queries; no reusable query library |
| Searches per hotel? | Typically a handful of destination/CVB pages — not tracked |
| Domains examined? | Mostly known CVB / Visit Palma / local guides |
| Mentions only? | Often yes — pages mentioning the hotel by name |
| Referring pages inspected for outbound hrefs? | Inconsistently; many stops at “mention confirmed” |
| Redirects / alternate domains? | Partial; Waterstone historical domain handled as investigation, not graph |
| Brand.com property URLs considered? | Sometimes as evidence sources, rarely as **backlink destinations** |
| Hotel-operated domains considered? | Inconsistent |
| Competitor links discovered? | Rare; often labeled `SUBJECT_COVERAGE_NOT_ESTABLISHED` |
| Mentions misclassified as backlinks? | Occasional risk (first-party site as “external mention” — fixed in CQA) |
| JS-rendered links overlooked? | Yes — V1 HTML-only probes |
| Search-result limitations? | No adaptive multi-strategy recall; no coverage status |

**Root cause:** V1 Digital Authority was a **manual fixture + classification module**, not a discovery engine. Portfolio certification therefore counted only two Visit Palma property hyperlinks as verified third-party backlinks. Low counts reflected **methodology recall**, not proof that hotels lack backlinks.

---

## 2. Research improvements implemented

| Component | Path |
|---|---|
| Property URL identity graph | `property-url-identity-graph-v1.js` |
| Multi-strategy discovery + href verification | `digital-authority-discovery-v2.js` |
| Identity extras (brand/hotel/competitors/segments) | `digital-authority/wave1-da-v2-identity-extras-v1.json` |
| Pilot runner | `scripts/ava-digital-authority-v2-six-pilot-v1.mjs` |
| Quality scrub (first-party / low-quality) | `scripts/ava-da-v2-scrub-first-party-backlinks-v1.mjs` |
| Unit tests | `scripts/test-ava-digital-authority-v2-identity-and-links-v1.mjs` |
| PDF “Current external visibility” | `build-audit-pdf-html-v1.js` |

Strategies executed: **A** direct hotel/domain, **B** hotel mentions, **C** competitor-oriented, **D** destination/industry, **E** mention→link opportunities, plus **seed re-verification** of known destination pages.

---

## 3. URL identity graph

Each hotel tracks separately:

- `BRAND_PROPERTY_PAGE`
- `HOTEL_OWNED_SITE` / `CANONICAL_PROPERTY_URL`
- `ALTERNATE_VERIFIED_URL`
- `HISTORICAL_DOMAIN` (ownership not assumed)
- `BOOKING_URL`
- `UNVERIFIED_PROPERTY_URL`

**Rules:** Corporate brand homepages (e.g. `marriott.com/`) are **not** property-specific destinations. Brand property paths (e.g. `/hotels/pmilc-…`) and hotel-operated hosts match independently.

---

## 4. Search strategies and coverage (pilot totals)

| Metric | Value |
|---:|
| Exa searches completed | 52 |
| Candidate pages discovered | (per-hotel; see results JSON) |
| Candidate pages inspected | 154 |
| Quality-filtered verified backlinks | **14** |
| Verified external mentions | 52 |
| Research cost | **$0** (no Ahrefs/Semrush) |
| Customer-quality certification | 6/6 PASS |

Coverage statuses used: `RESEARCH_SUFFICIENT_NO_ADDITIONAL_LINKS_VERIFIED`, `ACCESS_RESTRICTED`, `RESEARCH_INCOMPLETE`, `PROVIDER_COVERAGE_LIMITATION`.

---

## 5. Six-hotel before / after

| Hotel | V1 searches | V1 verified BL | V2 searches | V2 pages inspected | V2 verified BL (quality-filtered) | V2 mentions | Coverage | Cost |
|---|---|---:|---:|---:|---:|---:|---|---:|
| Waterstone | manual partial | 0 | 8 | 25 | **0** | 5 | ACCESS_RESTRICTED | $0 |
| Castillo Son Vida | manual partial | 1 | 10 | 27 | **7** | 10 | SUFFICIENT | $0 |
| Sheraton Mallorca | manual partial | 1 | 8 | 26 | **3** | 10 | SUFFICIENT | $0 |
| AC Hotel A Coruña | manual partial | 0 | 10 | 27 | **2** | 18 | SUFFICIENT | $0 |
| GFAC Chapultepec | manual partial | 0 | 8 | 24 | **1** | 4 | SUFFICIENT | $0 |
| Condesa Cancún AI | manual partial | 0 | 8 | 25 | **1** | 5 | SUFFICIENT | $0 |

**Interpretation:** Expanded methodology materially improved discovery for destination-rich hotels (Mallorca, A Coruña). Waterstone honestly remains **0** third-party property hyperlinks after quality filtering — mentions exist; destination lodging backlinks are still opportunities, not invented facts.

---

## 6. Verified backlink examples (quality-filtered)

| Hotel | Referring source | Destination class |
|---|---|---|
| Castillo | Visit Palma hotel fiche | Brand property (Marriott) |
| Castillo | Illes Balears Travel spa listing | Hotel-operated site |
| Castillo | Mallorca Houses / Mallorca Comercial spa & zone pages | Hotel-operated site |
| Sheraton | Visit Palma hotel fiche | Brand property |
| Sheraton | Arabella Golf Mallorca hotels page | Brand property |
| Sheraton | mallorca.com directory | Brand property |
| AC A Coruña | Infogal / Páxinas Galegas local directories | Hotel-operated (`hotelacacoruna.com`) |
| GFAC | Forbes Travel Guide hotel profile | Brand property family URL |
| Condesa | ShermansTravel advice/review | Brand property URL |

---

## 7. Verified competitor backlinks

Public HTML in this unpaid pass did **not** establish a large set of `VERIFIED_COMPETITOR_BACKLINK` rows with peer outbound superiority. Competitor strategy pages more often yielded subject mentions or insufficient outbound proof → gaps remain `SUBJECT_COVERAGE_NOT_ESTABLISHED` unless inventory-complete absence is shown. This is correct classification discipline, not a pass/fail on competitor count.

---

## 8. Commercially actionable opportunities

Examples retained for customer action (not generic “build backlinks”):

- **Waterstone:** Convert Boca / Palm Beach destination & leisure guides that already mention marina/dining into listings with `waterstoneboca.com` hrefs.
- **AC A Coruña:** CVB fiches still need official Marriott URL hyperlink after capacity sign-off (mention≠backlink).
- **Condesa / Chapultepec:** Pursue destination lodging inventories (e.g. Turismo Cancún / CDMX partners) with property-specific official URLs — after brand-family self-links are excluded from “third-party” counts.

Each opportunity keeps owner, evidence URL, and verification method; scores remain prioritization aids only.

---

## 9. Remaining coverage limitations

1. Public search + HTML inspection **cannot match** Ahrefs/Semrush backlink indexes.  
2. JS-only rendered anchors may still be missed without browser escalation per page.  
3. Bot/WAF blocks → `ACCESS_RESTRICTED` (Waterstone had elevated inaccessible candidates).  
4. Competitor gap confirmation needs deeper peer-URL graphs than this pilot fully populated.  
5. OTA / social hosts intentionally skipped as low-value directory noise for this module.

---

## 10. Paid-provider comparison and economics

No purchase or paid API call was made. Public terms checked 2026-11-05:

| Provider | Access model (public) | Property-level URLs | Approx. economics | Notes |
|---|---|---|---|---|
| **Ahrefs** | Lite+ plans include API/MCP; units metered (`max(50, per-row×rows)`) | Strong domain & URL backlink lists | Lite listed **~$129/mo** with plan API unit allowances; not a pure per-call $ price | Confirm current plan/API unit docs before buy |
| **Semrush** | Backlinks API needs eligible **Business** + **separately purchased API units** | Strong | Business ~**$500/mo** class + unit packages (unit $ not public) | 45 units/line class costs in v4 docs — verify in-account |
| **Majestic** | Subscription / API (sales-dependent) | Referring domains / topical trust | Quote required | Not evaluated live |
| **DataForSEO** | Pay-as-you-go backlinks | Good for programmatic | Published ~$0.024/task + per-row (third-party summaries; verify live price list) | Already partially configured elsewhere in repo — **not authorized** for this AVA run |
| **Exa (used)** | Existing approved key | Discovery of candidate pages, not a backlink index | Marginal cost in existing Exa usage | Improves recall; not a substitute for SEO backlink DB |

**Recommendation:** Keep V2 public discovery as the default Full E2E path. For portfolio-scale **complete** referring-domain inventories, founder-authorize a bounded DataForSEO or Ahrefs Lite pilot on **3 hotels** first (estimate & unit burn before 38-hotel rollout). Do **not** degrade classification quality to stay at $0 if commercial completeness is required — escalate with a clear cost envelope instead.

---

## 11. Integration-test results

```text
node scripts/test-ava-digital-authority-v2-identity-and-links-v1.mjs  → ok
node scripts/ava-digital-authority-v2-six-pilot-v1.mjs               → 6 hotels
node scripts/ava-da-v2-scrub-first-party-backlinks-v1.mjs            → quality filter
node scripts/ava-da-v2-regen-pdfs-update-results-v1.mjs              → CQA 6/6 PASS
```

Evidence/hyperlink certification + Customer QA remain mandatory. Mentions cannot pass as backlinks. Idempotent merge preserves prior valid sources.

---

## 12. PDF QA results

- Digital Authority section adds **Current external visibility** counts in customer language.  
- Internal research codes remain translated / Admin-only.  
- PDFs regenerated and staged with checksums; founder flags unchanged.  
- Prior PDFs preserved under `pdf-staged/pre-da-v2/`.

---

## 13. Scalability to 38 hotels

| Path | Guidance |
|---|---|
| Public V2 (Exa + HTML) | Ready; ~2–5 minutes + API usage per hotel; expect uneven BL counts by market |
| Quality filter | Mandatory before customer PDF |
| Paid index enrichment | Founder decision; pilot 3 hotels before portfolio |
| Portfolio rerun | **Deferred** until this six-hotel certification is accepted |

Estimated steady-state: **public V2** adds modest runtime to Full E2E; **paid index** adds subscription/units but much higher recall for “who links to this property URL?” questions.

---

## Acceptance notes

- ADP measurements unchanged.  
- No customer publication.  
- No automatic founder approval.  
- No Ahrefs/Semrush/Majestic calls.  
- Stopped after six-hotel pilot as instructed.
