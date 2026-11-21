# GDI Contact Intelligence V2 — Findymail Incremental Analysis & Public Discovery Optimization

**Date:** 2026-11-21  
**Authorization:** Controlled research and development  
**Findymail production integration:** **NOT DONE** (Surfe-first retained)  
**PR vehicle:** Update to open integration PR **#536** (supersedes parallel merge of #484/#534)  
**Dirty `ws-gdi`:** preserved  

---

## Integration state (inspected before changes)

| Item | Status |
| --- | --- |
| PR #484 | Still **OPEN** (Google→Exa→Surfe) |
| PR #534 | Still **OPEN** (permanent verification) |
| PR #536 | **OPEN**, CI green previously; merges #484+#534 + persistence readiness |
| This V2 work | Committed onto `#536` branch (no second parallel pipeline PR) |

---

## 1. Findymail's genuinely incremental contribution

Pilot artifacts used (non-sensitive): Findymail FOUNDER_REPORT + PILOT_RESULTS classifications/redactions + COHORT source URLs. **Provider email local-parts were not persisted or customer-displayed.**

| Bucket | Count | Meaning |
| --- | ---: | --- |
| Strict named persons in cohort | 13 / 15 | 2 rejected as non-person (org/program names) after V2 gate |
| Findymail person-strict accepted | 6 | Domain-matched provider finds on real people |
| Independently corroborated public (free fetch) | **2** | Toni Vitale Chan; Amy Drow — same contact on official pages **without** treating Findymail as proof |
| Findymail-only (no independent public email via automated recovery) | **4** | David Mendoza; Dedra Benjamin; Shaniece Rigans*; Ben Hawkins |
| BOTH_MISS | 6 | No Findymail email + no public email recovered |
| EMPLOYER_MISMATCH rejected | 1 | Kathi Allmendinger (queried DMC domain; provider returned employer.com) |
| Ineligible non-person | 2 | Learning Futures NY; USS Midway Museum |

\*Shaniece: **person confirmed** on `cmss.org` staff HTML; **personal email appears in independent CMSS PDF / GLEAM pages** (WebSearch), but staff HTML alone did not expose mailto → automated recovery classified provider-only until PDF-follow improvement.

**Hard rule applied:** Findymail returns are **never** relabeled as public-source verified merely because the domain matches.

**GDI already had verified work email before pilot:** none of the 15 (selection gate).

**Historical Surfe/public report overlap flag:** 3 rows — treated as corpus overlap only, not matched Surfe spend evidence.

---

## 2. Public-source recoverability analysis

Free fetches of cohort `sourceUrl`s + verification module:

| Contact | Public person? | Public email? | Public phone? | Notes |
| --- | --- | --- | --- | --- |
| Toni Vitale Chan | Yes | **Yes** (FIA event/partner pages) | **Yes** | Should never have needed paid find |
| Amy Drow | Yes | **Yes** (`ndss.org/meet-our-staff`) | No on page | Same |
| Shaniece Rigans | Yes (staff page) | On PDF/GLEAM, not staff HTML | Org phone on pages | PDF discovery gap |
| Dedra Benjamin | **Not on current UCA staff list** | No | Org phones for *other* staff; switchboard | Provider email may be stale |
| David Mendoza | Directory page | Not extracted as direct | — | Gov directory complexity |
| Ben Hawkins | Tournament page | Not on page | — | Provider-only |
| Others | Mixed weak domains | No | — | Aggregator / expo / event microsites |

**Implication:** A large share of Findymail “wins” were **publicly recoverable** or **person-not-current**. True Findymail-only incremental value is smaller than raw 46.7% acceptance.

---

## 3. Causes of missed email and phone discovery

| Gap | Type | Evidence |
| --- | --- | --- |
| Aggregator / expo / event hosts used as employer domain | Discovery | causeiq, expoplatform, chancity, year-branded microsites |
| Org/program strings treated as people | Identity | Midway Museum; Learning Futures |
| Staff directories without mailto; email only on PDF | Discovery | CMSS |
| Stale person not on live staff page | Identity/freshness | UCA / Dedra |
| Stopping before authoritative staff pages exhausted | Budget/ordering | Staff URL scoring previously weak vs news |
| Phone extraction underweighted on association staff pages | Discovery | UCA lists many direct phones for other roles |

---

## 4. Exact improvements implemented

On existing PCD / workflow / hierarchy (no second pipeline; **no Findymail production wire**):

1. **`gdi-public-contact-employer-domain-v1.js`** — classify hosts; prefer employer `.org`/gov; reject aggregators/expo/social; acronym fallback (e.g. AJAS → ajas.org).
2. **`gdi-public-contact-research-budget-v1.js`** — min 2 / max 6 / soft-max 8; stop on complete channels or unproductive streak.
3. **Workflow default effort** — `minQueries: 2`, `maxQueries: 6`, `softMaxQueries: 8`.
4. **URL scoring** — boost `meet-our-staff` / team / partner pages; demote causeiq/expo; raise PDF priority.
5. **`inferOrganizationDomainsFromEvidence`** — uses preferred employer domain; drops weak hosts.
6. **`looksLikePersonName`** — reject museum/futures/exchange/bar/soccer-style org names.
7. **`collectPublicContactSourceUrls`** — honor `candidateUrls`; demote aggregators; prefer staff pages.
8. **PDF link follow** — when a directory page lacks email, enqueue same-origin `.pdf` links (bounded).
9. **Persistence** — unchanged hard-off; provider origins still blocked.
10. **Eval** — `gdi-contact-intelligence-v2-public-recoverability-eval.mjs` + unit tests.

---

## 5. Before / after contact coverage (pilot diagnostic cohort)

| Metric | Before (pilot / prior gates) | After V2 free recoverability |
| --- | ---: | ---: |
| Strict persons | ~13–15 soft | **13** (2 ineligible) |
| Public independently verified emails | 0 counted in Findymail pilot | **2** (Toni, Amy) |
| Public independently verified phones | 0 in pilot accounting | **1** (Toni) |
| Findymail counted as public | Would have been wrong | **0** (forbidden) |
| Wrong employer accepted | 1 Findymail mismatch rejected | Still rejected |
| Org-as-person accepted | 1 (Midway) in Findymail accepted | **Blocked** by name gate |

Paid Findymail not re-run (credit discipline). Surfe not called.

---

## 6. False-positive / employer-mismatch safeguards

- Provider domain must align with preferred employer domain for acceptance (pilot classifier).
- Aggregator/expo hosts cannot become `site:` email domains.
- Org-entity names fail `looksLikePersonName`.
- Verification statuses unchanged: guessed format ≠ verified; provider origin ≠ public verified.
- Persistence readiness still refuses Surfe/Findymail/PDL/Prospeo/Apollo origins.

---

## 7. Research cost and stopping rules

| Rule | Value |
| --- | --- |
| Minimum meaningful attempts | 2 |
| Maximum ordinary | 6 |
| Soft exceptional max | 8 |
| Early stop | Direct email+phone; or 2 consecutive unproductive after min |
| This eval cost | **$0** paid providers |

Optimize for **cost per incremental verified public channel**, not query count.

---

## 8. Cross-hotel contact reuse readiness

- Canonical `personIdentityKey` + workflow process cache remain.
- Persistence readiness proposes GDO / research-inventory embedded `primaryContact*` (no new Contacts table).
- Durable Airtable cross-hotel read path still **not live** (await founder + #536 merge + write auth).
- 15-contact multi-hotel reuse not quantified beyond prior 30-eval (15 multi-hotel people in broader pool).

---

## 9. Canonical Airtable mapping and persistence gates

Unchanged from foundation integration:

- Destination: existing GDO `primaryContact*` + research inventory contact block  
- Gates: named identity, employer/role, source URL, verification status, retain/display rights, person key, opportunity links  
- `GDI_PUBLIC_CONTACT_LIVE_WRITE_ENABLED = false`  
- Classification: **`PERSISTENCE_DESIGNED`** (not validated / not FULL_E2E)

---

## 10. Estimated Surfe request reduction

| Basis | Estimate |
| --- | --- |
| Contacts where public recovery now yields email before Surfe | **≥2 / 15** on this diagnostic set (Toni, Amy) — **label: estimate from free recovery**, not observed avoided Surfe clicks |
| Additional if PDF/secondary page recovery lands Shaniece | +1 potential |
| Findymail-first vs Surfe | **Not recommended**; Surfe remains primary paid path |

---

## 11. PR, CI and integration status

- Changes targeted to **`feat/gdi-contact-intelligence-foundation-integration-v1` (#536)**  
- Local tests: employer-domain V2, permanent verification, persistence readiness — **PASS**  
- GitHub CI: re-run after push  
- GDI chat does **not** merge — `AWAITING_INTEGRATION_CONTROLLER`  
- No production deploy; no customer publication  

---

## 12. Remaining legal / licensing / publication blockers

- Findymail customer display / retention / export: **NOT CONFIRMED** → HOLD for production use  
- Surfe / PDL / Prospeo / FullEnrich storage: prior blocks remain  
- Live Airtable contact writes: founder + schema field review required  
- Public ≠ unrestricted redistribute — retain/display flags still required  

---

## 13. Recommended next implementation phase

1. **IC merge #536** when CI green (includes #484/#534 + V2 public optimizations).  
2. Close #484/#534 as superseded after merge.  
3. Stage-2 public recovery: PDF/secondary-page expansion + cleaner regression cohort (strict persons + official domains only).  
4. Keep **Surfe-first** for paid enrichment; Findymail remains research-only until written SaaS rights.  
5. Only then consider limited Findymail email fallback behind flags — still non-persistable until rights confirmed.

---

## Success criteria check

| Criterion | Status |
| --- | --- |
| Measurable public email/phone improvement without Findymail-as-public | **Met** (+2 email, +1 phone independent) |
| No false-positive identity inflation | **Met** (stricter name/domain gates) |
| No provider persist without rights | **Met** |
| No duplicate pipeline | **Met** |
| No uncontrolled paid spend | **Met** ($0 this phase) |

**STOP** — no live persistence, no paid enrichment, no customer-facing Findymail, no production provider-order change.
