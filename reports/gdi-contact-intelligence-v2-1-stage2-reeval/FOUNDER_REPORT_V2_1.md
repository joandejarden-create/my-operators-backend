# GDI Contact Intelligence V2.1 — Founder Report

**Status:** Implementation complete · PR ready for Integration Controller · **STOP** before live persistence / paid enrichment / customer display / production deploy

**Branch:** `feat/gdi-contact-intelligence-v2-1-recovery-reuse`  
**PR:** [#554](https://github.com/joandejarden-create/my-operators-backend/pull/554) — `OPEN` · `AWAITING_INTEGRATION_CONTROLLER`  
**Baseline:** Stage-2 freeze `STAGE2_COHORT_FREEZE.json` (80 people / 12 hotels)  
**Airtable E2E classification:** `PERSISTENCE_DESIGNED` (writes remain DISABLED)

---

## 1. Repository and PR status

| Item | Value |
|---|---|
| Worktree | Exceptional clean WT (ws-gdi preserved dirty) |
| Base | `origin/main` |
| Stream | GDI — do **not** merge from product chat |
| Live writes | OFF |
| Paid providers | None called ($0) |

## 2. Classification of the 35 unresolved cases (Phase 1)

From frozen Stage-2 `PUBLIC_RESEARCH_INCOMPLETE` rows (unique people = 35):

| Primary root cause | Count |
|---|---:|
| A — Named person cannot be verified (org/UI placeholders) | 10 |
| D — Person viable; direct work email missing | 11 |
| G — Research attempts exhausted | 10 |
| H — Retrieval / parsing / matching defects | 4 |
| B / C / E / F as primary | 0 |

Recovery potential: **HIGH 11 · MEDIUM 4 · LOW 10 · NONE 10**

Notable A-class defects: `Booth Booth`, `Spain Shines`, `Survey Software`, `Boutique Design New`, `Aztron Medtech` (event/UI chrome treated as people).

## 3. Public-source discovery defects identified

1. Process-local person cache **not seeded** on early `REUSED_EXISTING` exit → 0 cross-hotel reuse despite 22 multi-hotel identities.
2. Stage-2 eval researched each person once → never exercised second-hotel lookup.
3. Person-name gate too weak for exhibitor/UI labels.
4. Weak employer / event chrome organization strings (`Powered by A2Z Events`, `Interested in Exhibiting`).
5. Thin URL coverage (0–1 seeds) without official staff/contact path fallback.
6. Embedded Airtable fields adequate for opportunity-scoped channels; **partial** for durable multi-hotel person SoT.

## 4. Improvements implemented

- Seed person research cache on already-verified early exit.
- Cross-hotel reuse tests (same-process + subsequent-run fixture; identity collision guard).
- Stronger `looksLikePersonName` (event UI / repeated tokens / design-product trails).
- `isWeakEmployerOrganizationName` for employer chrome.
- Thin-coverage official staff/contact URL seeding from employer domain.
- Persistence hypothetical mode (`excludeLiveWritesDisabledBlock`) keeping all other gates.
- Unresolved classification module (A–H).
- Frozen-cohort V2.1 re-eval + redacted Airtable dry-run (≤10).

## 5–8. Coverage before / after (same freeze)

| Metric | V2 Stage-2 after | V2.1 after |
|---|---:|---:|
| Verified direct emails | **33** | **33** |
| Verified direct phones | **31** | **32** |
| Verified org email routes | 4 | 4 |
| Verified org phone routes | 0 | 0 |
| Unresolved identities | 35 | **34** |
| Wrong-person / weak-employer rejections | 0 (not gated) | **11** |
| Cross-hotel reuse events | **0** | **9** |
| Avoided duplicate page fetches (reuse) | 0 | 8 |
| Page fetches | 112 | 144 |
| Actual spend | $0 | **$0** |

Incremental vs opportunity “before” in this pass: +2 email, +8 phone (includes recoveries already counted in V2 baseline plus **Eduardo Basulto Jiménez** phone). Net vs V2 published after: **+1 direct phone**, emails flat.

## 9. Research attempts / fetches / spend

- Free official-source fetches only; maxPages raised modestly for unresolved recovery.
- No Surfe / Findymail / PDL / Prospeo / FullEnrich / paid search.
- **Observed spend: $0.** Do not treat avoided Surfe lookups as cash savings.

## 10. Cross-hotel reuse

**Root cause:** cache never populated on early verified exit; eval never re-entered the same `personIdentityKey` under a second hotel.

**Fix + proof:**

- Cache seed on `REUSED_EXISTING`.
- Re-eval second-hotel stripped-channel pass → **9** reuse hits / **8** avoided fetches.
- Unit tests: same-process reuse, subsequent-run fixture reuse, cross-org non-collision.

Process-local ≠ durable Airtable person store (still unauthorized).

## 11. Persistence eligibility

| Gate | Count / 80 |
|---|---:|
| A. Actual (`LIVE_WRITES_DISABLED` enforced) | **0** pass |
| B. Hypothetical (exclude write flag only) | **39** pass |

Hypothetical blockers (contacts may have multiple):

| Blocker | Count |
|---|---:|
| NO_VERIFIED_PUBLIC_CHANNEL | 34 |
| MISSING_SOURCE_EVIDENCE | 20 |
| EMPLOYER_OR_ROLE_UNVERIFIED | 11 |

## 12. Dry-run-ready contacts (8 of 10 requested)

Shortfall: only **8** met verified direct email+phone + person/employer gates with loadable opportunity + non-write readiness.

1. Adam Grosswirth — National Alliance for Musical Theatre  
2. Josh Cacchione — The Broadway League  
3. Larry Schiffer — ARIAS U.S.  
4. Mary Cunningham — CAME  
5. Katie Hollis — SIOR  
6. Jennifer Kearney — NYSSBA  
7. Michele Smith Koontz — SSSP  
8. Maritza Jeanette Medina García — Gobierno de Jalisco / SIOP  

Destination: existing GDO / research inventory **embedded `primaryContact*`** — **no new Contacts table**. Emails/phones redacted in committed dry-run JSON.

## 13. Airtable destination recommendation

- Keep embedded `primaryContact*` for opportunity association + provenance.
- Add/confirm `personIdentityKey`, source URL, verification status, retention/rights metadata on those embedded fields before any live write.
- Multi-hotel reuse needs governed person-key read path; **do not** create a Contacts table until founder confirms SoT requirements.
- Provider-derived channels remain non-persistable.

## 14. CI / regression (local)

- `test:gdi-public-contact-employer-domain-v1` PASS  
- `test:gdi-public-contact-persistence-readiness-v1` PASS  
- `test:gdi-contact-intelligence-v2-1-cross-hotel-reuse` PASS  
- `test:gdi-public-contact-discovery-v1` PASS  
- Secret scanning / full `dealality:ci-precheck` to be run before PR open

## 15. Remaining blockers

- Live writes still founder-gated.
- 10 A-class placeholders should be purged from contact research universes.
- Most HIGH-priority unresolved still lack public mailto/phone on official pages within free budget.
- Diminishing returns: additional free page fetches (112→144) yielded +1 phone; further free research is **weakly justified** without better seed URLs or authorized search.
- Durable cross-run person cache / Airtable person index not authorized.

## 16. Recommended next phase

1. Founder review of this PR + 8 dry-run proposals.  
2. Optional: purge A-class placeholders from GDI opportunity contact fields.  
3. If authorizing persistence: enable write flag for **fixture/authorized subset only**, with embedded-field mapping for personIdentityKey + rights.  
4. Keep Surfe-first paid waterfall unchanged; Findymail remains inactive.  
5. Do **not** expand paid public search until seed-URL quality improves for the remaining HIGH list.

---

**Final states**

- Product PR: `AWAITING_INTEGRATION_CONTROLLER` after open  
- Persistence: `PERSISTENCE_DESIGNED`  
- Customer publication: `CUSTOMER_BLOCKED`  
- **STOP** for founder before live persistence / paid enrichment / display / deploy
