# Bethesda GDI — Surfe Contact Enrichment Evaluation Phase 2

**Date:** 2026-09-15
**Mode:** Evaluation-only · **NO production writes** · **NO share updates** · **Paid enrichment OFF**
**Cohort:** `bethesda-surfe-contact-cohort-v2.json`
**Adapter:** `lib/surfe/client.js` (surfe-client-v1)

## Executive Verdict

# ADD AS GATED PAID ESCALATION

Accepted-result precision ≥98%, no bad identity passed the gate, meaningful enrichment ≥40%, deterministic production-gate simulation available. Keep CONTACT_INTELLIGENCE_PAID_ENRICHMENT_ENABLED=0 until CI merge path ships. Surfe must never create/replace person identity.

### Success question

> "Can Dealality safely use Surfe as a reachability enrichment layer for an already-established person identity, with near-zero risk of surfacing the wrong person?"

**Answer this run:** Yes, with gates — Surfe can safely enrich reachability for already-established identities when multi-signal acceptance holds and official sources remain authoritative.

## Cohort

| Person | Segment | Grade | Opp | Baseline email | Baseline phone | Control |
|---|---|---|---|---|---|---|
| Andrea Snader | association | B | gdi_opp_afcea_hits_2027 | registrar@afceabethesda.org (ROLE_BASED) | 571-323-2587 (MAIN_ORGANIZATION) | no |
| Justin Fessler | government_contractor | B | gdi_opp_afcea_hits_2027 | registrar@afceabethesda.org (ROLE_BASED) | 571-323-2587 (MAIN_ORGANIZATION) | no |
| Jessica Mitchell | medical_scientific | B | gdi_opp_show_2026_nih | Jessica.Mitchell@nih.gov (DIRECT_WORK) | — (UNKNOWN) | no |
| Jami Sims | association | A | gdi_opp_nar_gad_institute_2027 | GADInst@nar.realtor (ROLE_BASED) | 202-383-1221 (OFFICE) | yes |
| Kathy Hauschild | sports_tournament | B | gdi_opp_potomac_memorial_2027 | tournament@potomacsoccer.org (ROLE_BASED) | 301-519-8070 (MAIN_ORGANIZATION) | no |
| Brad Roos | sports_tournament | A | gdi_opp_bethesda_premier_cup_2026 | broos@bethesdasoccer.org (DIRECT_WORK) | — (UNKNOWN) | yes |
| Karen Wetzel | government | A | gdi_opp_nice_2027 | karen.wetzel@nist.gov (DIRECT_WORK) | 240-439-0767 (OFFICE) | yes |
| Susana Barraza | government | A | gdi_opp_nice_2027 | susana.barraza@nist.gov (DIRECT_WORK) | 240-457-2638 (OFFICE) | yes |
| Danielle Santos | government | C | gdi_opp_nice_2027 | — (UNKNOWN) | — (UNKNOWN) | no |
| Jessica Price | conference_event_ops | C | gdi_opp_afcea_hits_2027 | — (UNKNOWN) | 571-323-2587 (MAIN_ORGANIZATION) | no |
| Cody Mruk | conference_event_ops | C | gdi_opp_afcea_hits_2027 | — (UNKNOWN) | 571-323-2587 (MAIN_ORGANIZATION) | no |
| Michael Prebil | government | C | gdi_opp_nice_2027 | — (UNKNOWN) | 202-308-3909 (OFFICE) | no |

## Identity Safety

- ACCEPTED: **9**
- ACCEPTED_WITH_LIMITED_EVIDENCE: **0**
- AMBIGUOUS: **2**
- REJECTED: **1**
- NOT_FOUND: **0**
- Provider raw false matches: **1**
- Guardrail rejection success: **1** / 1

- **Andrea Snader**: AMBIGUOUS — signals: name:exact_full_name, org:domain_match
- **Justin Fessler**: REJECTED — local_part_contradicts_surname local=jkessler last=fessler; linkedin_slug_contradicts_surname slug=justin-kessler-6b6a0414 last=fessler
- **Jessica Mitchell**: ACCEPTED — signals: name:exact_full_name, org:domain_match, email_local:last_name_in_local
- **Jami Sims**: ACCEPTED — signals: name:exact_full_name, org:domain_match, email_local:last_name_in_local, linkedin:slug_first_last_agreement
- **Kathy Hauschild**: AMBIGUOUS — signals: name:exact_full_name, org:domain_match
- **Brad Roos**: ACCEPTED — signals: name:exact_full_name, org:domain_match, email_local:last_name_in_local
- **Karen Wetzel**: ACCEPTED — signals: name:exact_full_name, org:domain_match, email_local:last_name_in_local, title:title_plausible_overlap
- **Susana Barraza**: ACCEPTED — signals: name:exact_full_name, org:domain_match, email_local:last_name_in_local, linkedin:slug_first_last_agreement, title:title_plausible_overlap
- **Danielle Santos**: ACCEPTED — signals: name:exact_full_name, org:domain_match, email_local:last_name_in_local, linkedin:slug_first_last_agreement, title:title_plausible_overlap
- **Jessica Price**: ACCEPTED — signals: name:exact_full_name, org:domain_match, email_local:last_name_in_local, linkedin:slug_first_last_agreement, title:title_plausible_overlap
- **Cody Mruk**: ACCEPTED — signals: name:exact_full_name, org:domain_match, linkedin:slug_first_last_agreement, title:title_plausible_overlap
- **Michael Prebil**: ACCEPTED — signals: name:exact_full_name, org:domain_match, email_local:last_name_in_local

## Email Results

- NEW_DIRECT_WORK_EMAIL: 4
- NEW_ROLE_BASED_EMAIL: 0
- CORROBORATES_OFFICIAL_EMAIL: 4
- CONFLICTS_WITH_OFFICIAL: 0
- NOT_FOUND / IDENTITY_REJECTED: 4

| Person | Baseline | Surfe email | Outcome | Provider label |
|---|---|---|---|---|
| Andrea Snader | registrar@afceabethesda.org | — | IDENTITY_REJECTED | — |
| Justin Fessler | registrar@afceabethesda.org | jkessler@guidehouse.com | IDENTITY_REJECTED | SURFE_PROVIDER_VALID |
| Jessica Mitchell | Jessica.Mitchell@nih.gov | jessica.mitchell@nih.gov | CORROBORATES_OFFICIAL_EMAIL | SURFE_PROVIDER_VALID |
| Jami Sims | GADInst@nar.realtor | jsims@nar.realtor | NEW_DIRECT_WORK_EMAIL | SURFE_PROVIDER_VALID |
| Kathy Hauschild | tournament@potomacsoccer.org | — | IDENTITY_REJECTED | — |
| Brad Roos | broos@bethesdasoccer.org | broos@bethesdasoccer.org | CORROBORATES_OFFICIAL_EMAIL | SURFE_PROVIDER_VALID |
| Karen Wetzel | karen.wetzel@nist.gov | karen.wetzel@nist.gov | CORROBORATES_OFFICIAL_EMAIL | SURFE_PROVIDER_VALID |
| Susana Barraza | susana.barraza@nist.gov | susana.barraza@nist.gov | CORROBORATES_OFFICIAL_EMAIL | SURFE_PROVIDER_VALID |
| Danielle Santos | — | danielle.santos@nist.gov | NEW_DIRECT_WORK_EMAIL | SURFE_PROVIDER_VALID |
| Jessica Price | — | jessica.price@nuaxis.com | NEW_DIRECT_WORK_EMAIL | SURFE_PROVIDER_VALID |
| Cody Mruk | — | — | NOT_FOUND | — |
| Michael Prebil | — | michael.prebil@nist.gov | NEW_DIRECT_WORK_EMAIL | SURFE_PROVIDER_VALID |

## Phone Results

- NEW_MOBILE_PROFESSIONAL: 3
- NEW_OFFICE / DIRECT: 0
- SAME_MAIN_LINE: 0
- CORROBORATION_ONLY: 0
- NOT_FOUND: 3

| Person | Baseline | Surfe phone | Outcome |
|---|---|---|---|
| Andrea Snader | 571-323-2587 | — | IDENTITY_REJECTED |
| Justin Fessler | 571-323-2587 | — | IDENTITY_REJECTED |
| Jessica Mitchell | — | — | IDENTITY_REJECTED |
| Jami Sims | 202-383-1221 | — | NOT_FOUND |
| Kathy Hauschild | 301-519-8070 | — | IDENTITY_REJECTED |
| Brad Roos | — | — | IDENTITY_REJECTED |
| Karen Wetzel | 240-439-0767 | — | NOT_FOUND |
| Susana Barraza | 240-457-2638 | — | NOT_FOUND |
| Danielle Santos | — | +12023083909 | NEW_MOBILE_PROFESSIONAL |
| Jessica Price | 571-323-2587 | +17032826259 | NEW_MOBILE_PROFESSIONAL |
| Cody Mruk | 571-323-2587 | +14433925014 | NEW_MOBILE_PROFESSIONAL |
| Michael Prebil | 202-308-3909 | — | IDENTITY_REJECTED |

### Phone quality audit (post-run)

- **Danielle Santos** Surfe “mobile” `+12023083909` equals **Michael Prebil’s published NIST office line** (`202-308-3909`) already on the official NICE staff page. Treat this phone as **HOLD FOR REVIEW** — do not auto-merge as Danielle’s mobile. Her **email** `danielle.santos@nist.gov` remains a valid meaningful improvement under ACCEPTED identity.
- **Jessica Mitchell** Phase 1 mobile win was **not** accepted here: stricter Phase 2 mobile identity gate blocked phone salvage when the mobile payload lacked multi-signal identity (email corroboration only).
- Production phone rule recommendation: reject/hold Surfe phones that collide with another known official office/main line in the same org or cohort.

## Precision

- **Accepted-result precision:** 100% (9 accepted; 0 false)
- **Provider raw false-match rate:** 8.3%
- **Guardrail rejection success:** 100%

## Yield

- Meaningful improvement rate: **41.7%** (5/12)
- Direct email improvement rate: 33.3%
- Phone improvement rate: 25%
- Corroboration-only rate: 33.3%
- Not-found rate: 0%
- Ambiguous rate: 16.7%
- Rejected false-match rate: 8.3%

## Economics

| Bucket | Starting | After email | Ending |
|---|---:|---:|---:|
| Email | 988 | 979 | 979 |
| Mobile | 95 | 95 | 89 |

- Credits used (email): **9**
- Credits used (mobile): **6**
- Credits used (total): **15**
- Credits / meaningful improvement: **3**
- Credits / accepted new email: **3.8**
- Credits / useful phone: **5**
- Accounting note: Balances decreased monotonically — deltas are measured.

This run consumed 15 credits across 12 people (5 meaningful). Credits per meaningful ≈ 3.

| Scale | Assumption | Projected credits / month |
|---|---|---:|
| 1 hotel / month | 6 eligible × 1.25 credits | 8 |
| 10 hotels | linear × 10 (no volume discount assumed) | 75 |
| 100 hotels | linear × 100; Grade≤C + missing reachability gate lowers actual volume | 750 |

*n=12; assumes ~6 Surfe-eligible named weak-reach contacts per hotel per month after production gate; credits/person from this run ≈ 1.25; NOT a production forecast.*

## Stratification

| Bucket | n | Meaningful | Accepted | Rejected | Meaningful % |
|---|---:|---:|---:|---:|---:|
| grade_B | 4 | 0 | 1 | 1 | 0% |
| segment_association | 2 | 1 | 1 | 0 | 50% |
| role_EVENT_MEETINGS_OWNER | 3 | 1 | 1 | 1 | 33.3% |
| title_known | 12 | 5 | 9 | 1 | 41.7% |
| org_domain_known | 12 | 5 | 9 | 1 | 41.7% |
| segment_government_contractor | 1 | 0 | 0 | 1 | 0% |
| segment_medical_scientific | 1 | 0 | 1 | 0 | 0% |
| role_EVENT_OPERATIONS_CONTACT | 5 | 3 | 5 | 0 | 60% |
| grade_A | 4 | 1 | 4 | 0 | 25% |
| segment_sports_tournament | 2 | 0 | 1 | 0 | 0% |
| role_DIRECT_DECISION_MAKER | 4 | 1 | 3 | 0 | 25% |
| segment_government | 4 | 2 | 4 | 0 | 50% |
| grade_C | 4 | 4 | 4 | 0 | 100% |
| segment_conference_event_ops | 2 | 2 | 2 | 0 | 100% |

## Eligibility Recommendation

```
HYPOTHETICAL future Surfe eligibility (NOT ENABLED):
- opportunityQualification IN (VERIFIED_OPEN, STRONG)
- priority IN (HIGH_PRIORITY, strong MEDIUM_PRIORITY)
- named person already established by Dealality (canonical identity exists)
- role relevance strong (EVENT_MEETINGS_OWNER | DIRECT_DECISION_MAKER | EVENT_OPERATIONS_CONTACT | HOUSING_SOURCING_CONTACT)
- canonical identity confidence high
- Contact Grade <= C (or Grade A only when email is ROLE_BASED and phone weak)
- useful direct email OR useful phone is missing

HARD RULES:
- Surfe MUST NOT create or replace canonical person identity
- identity acceptance requires >=2 positive signals and zero contradictions
- name + company domain alone is NEVER sufficient
- REJECTED identities salvage zero fields
- official source verified fields always win conflicts
- CONTACT_INTELLIGENCE_PAID_ENRICHMENT_ENABLED remains 0 until merge path ships
```

## Production Gate Simulation

- WOULD_ACCEPT: **5**
- WOULD_ACCEPT_AS_CORROBORATION_ONLY: **4**
- WOULD_HOLD_FOR_REVIEW: **2**
- WOULD_REJECT: **1**

| Person | Identity | Email | Phone | Meaningful | Merge sim |
|---|---|---|---|---|---|
| Andrea Snader | AMBIGUOUS | IDENTITY_REJECTED | IDENTITY_REJECTED | no | WOULD_HOLD_FOR_REVIEW |
| Justin Fessler | REJECTED | IDENTITY_REJECTED | IDENTITY_REJECTED | no | WOULD_REJECT |
| Jessica Mitchell | ACCEPTED | CORROBORATES_OFFICIAL_EMAIL | IDENTITY_REJECTED | no | WOULD_ACCEPT_AS_CORROBORATION_ONLY |
| Jami Sims | ACCEPTED | NEW_DIRECT_WORK_EMAIL | NOT_FOUND | YES | WOULD_ACCEPT |
| Kathy Hauschild | AMBIGUOUS | IDENTITY_REJECTED | IDENTITY_REJECTED | no | WOULD_HOLD_FOR_REVIEW |
| Brad Roos | ACCEPTED | CORROBORATES_OFFICIAL_EMAIL | IDENTITY_REJECTED | no | WOULD_ACCEPT_AS_CORROBORATION_ONLY |
| Karen Wetzel | ACCEPTED | CORROBORATES_OFFICIAL_EMAIL | NOT_FOUND | no | WOULD_ACCEPT_AS_CORROBORATION_ONLY |
| Susana Barraza | ACCEPTED | CORROBORATES_OFFICIAL_EMAIL | NOT_FOUND | no | WOULD_ACCEPT_AS_CORROBORATION_ONLY |
| Danielle Santos | ACCEPTED | NEW_DIRECT_WORK_EMAIL | NEW_MOBILE_PROFESSIONAL | YES | WOULD_ACCEPT |
| Jessica Price | ACCEPTED | NEW_DIRECT_WORK_EMAIL | NEW_MOBILE_PROFESSIONAL | YES | WOULD_ACCEPT |
| Cody Mruk | ACCEPTED | NOT_FOUND | NEW_MOBILE_PROFESSIONAL | YES | WOULD_ACCEPT |
| Michael Prebil | ACCEPTED | NEW_DIRECT_WORK_EMAIL | IDENTITY_REJECTED | YES | WOULD_ACCEPT |

## Official source precedence

Official verified email/phone remains stronger than Surfe. Surfe may corroborate or fill missing fields; it must not overwrite stronger verified fields. Provider labels are never called OFFICIAL_SOURCE_VERIFIED.

## Risks

- Identity collision (same first name, different surname / LinkedIn) — **observed:** Justin Fessler → Kessler (guardrail REJECTED)
- Stale employment (board employer ≠ current employer)
- Wrong LinkedIn mapping
- Guessed / unverified email
- Wrong phone type (mobile vs office vs main) — **observed:** Danielle Santos phone equals Michael Prebil office line
- Provider conflicts with official source
- Name + domain alone insufficient (Andrea Snader, Kathy Hauschild held as AMBIGUOUS — correct)

## Recommendation

Verdict stands for **gated** escalation of **email** reachability after multi-signal identity acceptance. Phone merge needs an extra collision check against known official lines before production. Keep `CONTACT_INTELLIGENCE_PAID_ENRICHMENT_ENABLED=0` until CI merge path + phone collision gate ship. Surfe must never create/replace person identity.

Keep `CONTACT_INTELLIGENCE_PAID_ENRICHMENT_ENABLED=0`. No production writes from this evaluation.
