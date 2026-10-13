# Bethesda GDI — Surfe Contact Smoke (Evaluation Only)

**Date:** 2026-09-15  
**Mode:** Offline evaluation · **NO production writes** · **NO share updates** · **NO priority/grade mutation**  
**Adapter:** `lib/surfe/client.js` (`surfe-client-v1`)  
**Key:** `SURFE_API_KEY` present (value not printed)  
**Script:** `npm run gdi:surfe-contact-smoke`  
**Cohort freeze:** `data/group-demand-intelligence/evals/bethesda-surfe-contact-cohort-v1.json`  
**Machine JSON:** `data/group-demand-intelligence/evals/bethesda-surfe-contact-smoke.json`

---

## Cohort

Named, already-qualified Bethesda GDI contacts with weak direct reachability (n=4):

| Person | Opportunity | Why included |
|---|---|---|
| **Andrea Snader** | AFCEA HITS 2027 | Named event VP; only `registrar@` + main switchboard |
| **Justin Fessler** | AFCEA HITS 2027 | Named co-VP; same weak reachability |
| **Jessica Mitchell** | SHOW 2026 NIH | Named; official email OK; phone missing |
| **Jami Sims** | NAR GAD 2027 | Named Grade A; role inbox `GADInst@` (test direct-email lift) |

**Excluded:** HBC housing desk, `info@`-only, unnamed AHIMA/CMSS, Watchlist/Disqualified.

**Surfe input model:** name + employer org/domain (not “find a planner”). AFCEA volunteers enriched against **LogicMonitor** / **Guidehouse** employer domains from the board page.

---

## Baseline

| Person | Grade | Email | Email type | Phone | Phone type |
|---|---|---|---|---|---|
| Andrea Snader | B | registrar@afceabethesda.org | ROLE_BASED | 571-323-2587 | MAIN ORGANIZATION |
| Justin Fessler | B | registrar@afceabethesda.org | ROLE_BASED | 571-323-2587 | MAIN ORGANIZATION |
| Jessica Mitchell | B | Jessica.Mitchell@nih.gov | DIRECT_WORK | — | UNKNOWN |
| Jami Sims | A | GADInst@nar.realtor | ROLE_BASED | 202-383-1221 | OFFICE |

---

## Surfe Results

### Andrea Snader — NO RESULT
- Enrichment: LogicMonitor / logicmonitor.com  
- Identity: OK (name+domain)  
- Email: **NOT_FOUND**  
- Phone: **NOT_FOUND**  
- Incremental value: **NONE**  
- Hypothetical grade: B → B  

### Justin Fessler — IDENTITY FAILURE (false positive caught)
- Enrichment: Guidehouse / guidehouse.com  
- Person payload name matched “Justin Fessler”, but Surfe returned:
  - email `jkessler@guidehouse.com`
  - LinkedIn `…/justin-kessler-…`
  - title “Senior Consultant” (not Co-VP Health IT Summit)
- Email class: **IDENTITY_AMBIGUOUS** (local-part `jkessler` ≠ last name `fessler`)  
- Phone `+15712455057` **held back** (same enrich; wrong-person risk)  
- Incremental value: **NONE (blocked)**  
- Hypothetical grade: B → B  
- **Lesson:** name+companyDomain alone is insufficient — require email local-part agreement / LI token agreement before accepting Surfe fields.

### Jessica Mitchell — PHONE WIN
- Enrichment: NIH / nih.gov  
- Identity: OK  
- Email: `jessica.mitchell@nih.gov` · **CORROBORATES_OFFICIAL** (not counted as new)  
- Phone: `+18645672166` · **MOBILE / PUBLIC PROFESSIONAL** (provider-reported; business-use unknown)  
- Improvement: **MISSING → DIRECT/MOBILE**  
- Hypothetical grade: B → **A** (eval only)  
- Meaningful success: **YES**

### Jami Sims — DIRECT EMAIL WIN
- Enrichment: NAR / nar.realtor  
- Identity: OK · title “Manager of Political Programs”  
- Email: `jsims@nar.realtor` · **PROVIDER_VERIFIED_DIRECT_WORK** (VALID / professional)  
- Improvement: **ROLE-BASED → DIRECT**  
- Official `GADInst@` preserved as stronger official channel; Surfe direct is additive candidate  
- Phone: not requested (office already useful)  
- Hypothetical grade: A → A  
- Meaningful success: **YES**

---

## Before vs After (hypothetical — not applied to production)

| Person | Current Grade | Current Email | Current Phone | Surfe Email | Surfe Phone | Hyp. Grade | Incremental Value |
|---|---|---|---|---|---|---|---|
| Andrea Snader | B | registrar@… | Main | — | — | B | NONE |
| Justin Fessler | B | registrar@… | Main | jkessler@… **REJECTED** | held back | B | IDENTITY_AMBIGUOUS |
| Jessica Mitchell | B | Jessica.Mitchell@nih.gov | — | corroborates | +18645672166 | **A** | phone missing→mobile |
| Jami Sims | A | GADInst@… | Office | **jsims@nar.realtor** | — | A | role→direct email |

---

## Cost

| Bucket | Before | After email | After mobile |
|---|---:|---:|---:|
| Email | 17 | 994* | 994 |
| Mobile | 4 | 99* | 97 |
| Search | 0 | 0 | 0 |

\*Surfe balances **increased mid-run** (account top-up / plan refresh). Do not treat raw before→after email delta as spend.

**Accounted spend (conservative):**
- Email: **4** credits (people_requested; worst-case)  
- Mobile: **2** credits (measured 99→97 after top-up)  
- **Total ≈ 6 credits**  
- Credits per meaningful enrichment: **3** (6 / 2 useful)

Jobs (audit):  
- email `ccb162b4-e720-4afc-9281-9b591f24b1f2`  
- mobile `5a4ef16d-87b3-4fa0-8b3f-42a8ba78d312`

---

## Success Rate

| Metric | Value |
|---|---:|
| People tested | 4 |
| Emails returned | 3 |
| Direct emails **accepted** | 1 |
| Phones accepted | 1 |
| Materially improved | **2** |
| Ambiguous / false-positive blocked | 1 |
| No results | 1 |
| Meaningful improvement rate | **50%** |

---

## Failures

- **Andrea Snader:** no email/phone returned for LogicMonitor identity  
- **Justin Fessler:** Surfe matched **Justin Kessler** (wrong person) — email + phone rejected by local-part / LinkedIn token check  

---

## Economics (illustrative only — tiny n)

Assumptions: ~8 Surfe-eligible named weak-reach contacts / hotel / month; ~1.5 credits average from this run’s mix; **not** a production forecast.

| Scale | Projected credits / month |
|---|---:|
| 1 hotel | ~12 |
| 10 hotels | ~120 |
| 100 hotels | ~1,200 |

At 50% meaningful hit rate, ~6 useful enrichments / hotel / month for ~12 credits — **economically interesting if false-positive controls hold**, but n=4 is too small to lock cost.

---

## Future escalation (proposal only — **not enabled**)

```
Qualified GDI Opportunity
→ Named relevant person identified
→ Official-source contact research
→ Canonical Contact Intelligence
→ If email/phone still weak AND CONTACT_INTELLIGENCE_PAID_ENRICHMENT_ENABLED=1
→ Surfe (email, then mobile if still weak)
→ Identity gates (name + domain + email local-part + LI token)
→ Review / validation
→ Canonical merge by evidence strength
  (official > multi-source > provider verified > inferred)

Default: CONTACT_INTELLIGENCE_PAID_ENRICHMENT_ENABLED=0
```

### Eligibility rule (proposed)

Surfe MAY be called only if ALL of:
- opportunityQualification ∈ {VERIFIED_OPEN, STRONG}
- priority ∈ {HIGH_PRIORITY, strong MEDIUM}
- named person exists (not org desk / HBC entity alone)
- targetRoleMatch ∈ {DIRECT_DECISION_MAKER, EVENT_MEETINGS_OWNER, EVENT_OPERATIONS_CONTACT, HOUSING_SOURCING_CONTACT} when person-named
- AND (contactGrade ∈ {C,D,E} OR emailType ∈ {ROLE_BASED, GENERIC_ORGANIZATION} OR useful phone missing)

Surfe MUST NOT be called for:
- WATCHLIST / DISQUALIFIED
- Grade A with direct work email + useful phone
- generic org with no named person
- adequate official housing desks (e.g. HBC already actionable)
- opportunities where person discovery is incomplete

**Accept Surfe fields only if:** strong name match + domain/org match + email local-part agrees with person (or explicit LI token agreement). Otherwise `IDENTITY_AMBIGUOUS` — do not merge.

---

## Recommendation

# KEEP EVALUATING

**Why not “ADD AS PAID ESCALATION” yet**
- Real wins: Jami Sims direct email; Jessica Mitchell mobile  
- Real risk: Justin Fessler → Justin Kessler false positive (caught only by post-checks)  
- Andrea miss shows employer-domain enrich is not reliable for chapter volunteers  
- n=4; credit metering distorted by mid-run top-up  

**Why not “DO NOT USE”**
- 50% meaningful rate with clear salesperson value on 2/4  
- False positive was detectable with Dealality identity gates  

**Next eval (before any production wiring)**
1. Larger cohort (10–15 named Grade B/C contacts)  
2. Hard-fail when LinkedIn slug / email local ≠ target last name  
3. Prefer enrich against the org that owns the published work email domain when known  
4. Keep share/production untouched until CI merge path + flag exist  

---

## Success question

> "For an already-qualified named GDI contact, does Surfe materially improve the salesperson's ability to reach the right person?"

**Answer this run:** **Sometimes — yes for Jami Sims (direct email) and Jessica Mitchell (mobile); no for Andrea; Justin blocked as wrong-person risk.**

**Verdict:** **KEEP EVALUATING** · paid enrichment stays **OFF** · no production writes performed.
