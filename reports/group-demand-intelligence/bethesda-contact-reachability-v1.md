# Bethesda GDI — Contact Reachability v1

**Mode:** LIVE EVAL · `CONTACT_INTELLIGENCE_PAID_ENRICHMENT_ENABLED=0` (global OFF)
**Cohort:** 9 frozen named people · 9 gated eligible · 0 blocked
**Generated:** 2026-09-15T20:48:44.422Z
**Identity law:** Dealality owns WHO. Surfe may help HOW TO REACH.

## A. Bethesda reachability

| Metric | Value |
|---|---:|
| Cohort size | 9 |
| Email calls | 2 |
| Phone/mobile calls | 9 |
| Accepted emails | 2 |
| Accepted phones | 3 |
| Holds | 0 |
| Identity AMBIGUOUS (fields rejected) | 5 |
| Meaningful improvements | 4 |
| Credits actual (email / mobile / total) | 2 / 6 / 8 |
| Credits per meaningful improvement | 2 |
| Credit balances before → after (email) | 957 → 955 |
| Credit balances before → after (mobile) | 61 → 55 |

### Wins

- **Amy Drow** — ACCEPTED · new direct email + mobile → Grade C→A
- **Ben Hawkins** — ACCEPTED · new direct email → Grade C→B
- **Jamie McCormick** — LIMITED · accepted mobile (official email retained) → C→A
- **Kelly Frere** — LIMITED · accepted mobile (official email retained) → C→A

### Non-wins (correct gate behavior)

- Brad Roos (×2), Elizabeth Lancaster, Meg Novak, Karen Bertani — Surfe identity **AMBIGUOUS** → no field merge

## Founder table

| Opportunity | Person | Role | WHO Conf | Before Email | After Email | Before Phone | After Phone | Before Grade | After Grade | Provider | Merge |
|---|---|---|---|---|---|---|---|---|---|---|---|
| Bethesda Premier Cup 2026 — stay-to-play weekend hotel demand (Nov 13–15 & 20–22) | Brad Roos | Tournament Director | MEDIUM | broos@bethesdasoccer.org | broos@bethesdasoccer.org | — | — | C | C | id=AMBIGUOUS e=IDENTITY_REJECTED p=IDENTITY_REJECTED | e=REJECT_FIELD p=REJECT_FIELD |
| 2027 MSYSA Spring State Cup Championships — multi-weekend SoccerPlex demand | Brad Roos | Cups Director | MEDIUM | Brad@msysa.org | Brad@msysa.org | — | — | C | C | id=AMBIGUOUS e=IDENTITY_REJECTED p=IDENTITY_REJECTED | e=REJECT_FIELD p=REJECT_FIELD |
| ACTS Translational Science 2027 (TS27) — Washington, DC (hotel/venue not named) | Elizabeth Lancaster | Events Manager | HIGH | elancaster@actscience.org | elancaster@actscience.org | — | — | C | C | id=AMBIGUOUS e=IDENTITY_REJECTED p=IDENTITY_REJECTED | e=REJECT_FIELD p=REJECT_FIELD |
| NDSS Down Syndrome Advocacy Conference 2027 — hotel block TBA | Amy Drow | Director of Events | HIGH | — | adrow@ndss.org | — | +14074962293 | C | A | id=ACCEPTED e=NEW_DIRECT_WORK_EMAIL p=NEW_MOBILE_PROFESSIONAL | e=ACCEPT_NEW_FIELD p=ACCEPT_NEW_FIELD |
| ACC Legislative Conference 2027 — Washington, DC (hotel not named) | Meg Novak | ACC Legislative Conference contact | HIGH | mnovak@acc.org | mnovak@acc.org | — | — | C | C | id=AMBIGUOUS e=IDENTITY_REJECTED p=IDENTITY_REJECTED | e=REJECT_FIELD p=REJECT_FIELD |
| NADO & DDAA Washington Conference 2028 — Arlington, VA (hotel not yet named) | Jamie McCormick | Events Manager | HIGH | jmccormick@nado.org | jmccormick@nado.org | — | +15189447999 | C | A | id=ACCEPTED_WITH_LIMITED_EVIDENCE e=NOT_FOUND p=NEW_MOBILE_PROFESSIONAL | e=REJECT_FIELD p=ACCEPT_NEW_FIELD |
| BEBPA US Bioassay Conference 2027 — College Park, MD (room block coming soon) | Karen Bertani | Director of Events | HIGH | karen.bertani@bebpa.org | karen.bertani@bebpa.org | — | — | C | C | id=AMBIGUOUS e=IDENTITY_REJECTED p=IDENTITY_REJECTED | e=REJECT_FIELD p=REJECT_FIELD |
| Alexandria Soccer Kickoff 2027 — stay-to-play housing (Traveling Teams) | Ben Hawkins | Tournament Director | HIGH | — | ben@alexandria-soccer.org | — | — | C | B | id=ACCEPTED e=NEW_DIRECT_WORK_EMAIL p=IDENTITY_REJECTED | e=ACCEPT_NEW_FIELD p=REJECT_FIELD |
| ASAE Annual Meeting & Exposition 2029 — Washington, DC (early watch) | Kelly Frere | Director, Meeting Operations and Engagement | HIGH | kfrere@asaecenter.org | kfrere@asaecenter.org | — | +13015095526 | C | A | id=ACCEPTED_WITH_LIMITED_EVIDENCE e=NOT_FOUND p=NEW_MOBILE_PROFESSIONAL | e=REJECT_FIELD p=ACCEPT_NEW_FIELD |

## B. Contact funnel (hotel-agnostic calculator)

```
29 qualified
→ 24 credible WHO (18 named + 6 entity)
→ 5 unresolved WHO
→ 9 reachability enrichment eligible
→ 21 usable email (72.4%)
→ 10 usable phone (34.5%)
→ 10 both (34.5%)
```

| Rate | Value |
|---|---:|
| WHO Resolution Rate | 82.8% |
| High-Confidence WHO Rate | 48.3% |
| Email Reachability Rate | 72.4% |
| Phone Reachability Rate | 34.5% |
| Full Reachability Rate | 34.5% |
| Paid Enrichment Eligibility Rate | 31% |
| Paid Enrichment Success Rate (this pass) | 44.4% of cohort |
| Unresolved Identity Rate | 17.2% |
| Credits per Meaningful Improvement | 2 |

## C. Portability

**Verdict: PORTABLE**

- Core contact coverage / eligibility / identity gates have no Bethesda hotelId hardcode
- Hotel-specific facts stay in discovery data, cohort freeze, and hotel config
- Synthetic non-Bethesda fixture passes the same calculator

See `reports/group-demand-intelligence/gdi-cross-hotel-contact-portability.md`

## D. GDI Operating Law audit

- **DMV geography keywords in candidate scoring** · REUSABLE · `lib/group-demand-intelligence/contact-candidate/scoring.js — geographyHints from opportunity; neutral default` · test: test:gdi-contact-coverage-portability
- **Contact funnel / coverage metrics** · REUSABLE · `lib/group-demand-intelligence/contact-coverage.js` · test: test:gdi-contact-coverage-portability
- **Bethesda hotelId / cohort freeze / discovery seeds** · HOTEL_SPECIFIC · `data/group-demand-intelligence/evals/*cohort*.json · official-person-discoveries-v1.js (data)` · test: N/A — data fixtures
- **Provider phone owned by another person must be rejected (Danielle→Prebil)** · REUSABLE · `surfe-identity-acceptance.js — detectOtherPersonPhoneCollision + OTHER_PERSON_PHONE_COLLISION` · test: test:surfe-identity-acceptance · test:gdi-contact-coverage-portability
- **Identity acceptance + Fessler→Kessler surname gate** · REUSABLE · `lib/hotel-intelligence/contact-intelligence/surfe-identity-acceptance.js` · test: test:surfe-identity-acceptance
- **Official sourceUrl host → enrichment domain when email missing** · REUSABLE · `contact-coverage.js inferDomainFromOfficialSourceUrl + freeze join from discovery sourceUrl` · test: test:gdi-contact-coverage-portability
- **After-grade must not improve without accepted fields** · REUSABLE · `simulateGradeAfterAcceptedFields — no-op when nothing accepted` · test: test:gdi-contact-coverage-portability
- **Provider gate return shape (allowed/rejected) wiring** · REUSABLE · `gdi-run-contact-reachability.mjs gateSubject maps allowed[]` · test: dry-run eligibleAfterGate=9

## E. Next step

**CONTACT STACK READY; MOVE TO NEXT GDI LAYER**

Global paid enrichment remains OFF. Controlled multi-hotel test is the next bounded eval after share/canonical merge policy is defined — not auto-enabled.
