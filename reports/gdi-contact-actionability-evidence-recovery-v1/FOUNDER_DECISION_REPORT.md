# GDI Contact Actionability & Evidence Recovery V1 — Founder Decision Report

Generated: 2026-11-20T15:42:10.612Z

**Status:** READ-ONLY design + evidence audit · **STOP** before implementation or spending

## Founder decisions honored

- APPROVED design direction: `EXISTING_RULES_WITH_CONTACT_TIERS`
- REJECTED as publication gate: `STRICT_EMAIL`
- No publication authorization changes
- No paid provider calls in this phase
- No fabricated / inferred / backfilled source evidence

## Exact inventory (before / after)

| Measure | Count |
|---|---:|
| Customer-visible opportunities (frozen baseline) | 182 |
| After adopting tiers-only (no gate change) | 182 |
| Unauthorized inventory delta | **0** |
| Matches leakage audit baseline (182) | YES |

EXISTING_RULES_WITH_CONTACT_TIERS does not remove opportunities from customer publication. Actionability tiers are labels/workflow only.

## 1. Proposed contact-actionability data contract

### Separation of concerns

- **opportunityEligibility:** commercial demand + hotel fit + contact eligibility class
- **contactVerification:** named person identity + source-backed channels
- **contactActionability:** SALES_READY | REACHABLE | CONTACT_RESEARCH_REQUIRED | CONTACT_IDENTIFICATION_REQUIRED
- **customerPublication:** founder-authorized CURRENT / registry PUBLISHED only

### Tiers

| Tier | Customer label | Requires |
|---|---|---|
| `SALES_READY` | Sales-ready contact | named person; commercially relevant (DIRECT_BUYER or RELEVANT_COMMERCIAL_ENTRY_POINT — labeled distinctly); person-specific business email with independent source confirmation |
| `REACHABLE` | Reachable contact | named commercially relevant person; usable person-specific business phone or office extension with evidence |
| `CONTACT_RESEARCH_REQUIRED` | Contact research needed | named person identified |
| `CONTACT_IDENTIFICATION_REQUIRED` | Contact not yet identified |  |

### DIRECT_BUYER vs RELEVANT_COMMERCIAL_ENTRY_POINT

- **DIRECT_BUYER:** Hotel/travel/meetings/lodging/purchasing remit
- **RELEVANT_COMMERCIAL_ENTRY_POINT:** Verified commercially relevant intermediary/referral — valid contact, never mislabeled as direct decision-maker
- **Rule:** Both may achieve SALES_READY actionability when channels are source-confirmed; UI must show distinct role labels

### Shared contact reuse

- Identity: `personIdentityKey(name|organization|domain)`
- Verified person channels reusable across hotels
- Commercial relevance / buyerVsEntry re-evaluated per opportunity-hotel pair

### Hard forbids (Sales Ready)

- inferred email patterns
- generic/org mailboxes counted as personal email
- fabricated source URLs
- provider-only channels without display rights

Machine-readable: `ACTIONABILITY_CONTRACT_V1.json`

## 2. Bethesda evidence-recovery findings

Cohort: **34** person-specific emails without frozen contact source URL (leakage reported 34).

| Recovery class | Count |
|---|---:|
| EMAIL_LITERAL_IN_PACKAGE_TEXT_NO_ATTRIBUTABLE_URL | 5 |
| REVERIFIABLE_VIA_PUBLIC_DISCOVERY | 29 |

- Potential SALES_READY if evidence recovered: **14**
- Already phone-reachable within cohort: **19**

### Definitions used (no invention)

- **RECOVERABLE_ATTACH_EXISTING_URL:** email literal found in frozen evidence AND at least one candidate URL exists — attach real URL only, never invent.
- **EMAIL_LITERAL_IN_PACKAGE_TEXT_NO_ATTRIBUTABLE_URL:** email appears in narrative/prose but no URL to attribute — public discovery required; do not fabricate sourceUrl.
- **REVERIFIABLE_CANDIDATE_URLS_PRESENT:** opportunity has official/evidence URLs but email not literal in package — bounded public fetch of those URLs only.
- **REVERIFIABLE_VIA_PUBLIC_DISCOVERY:** named person + org/domain anchors exist — Google-first public discovery then validate.
- **UNSUPPORTED_IN_FROZEN_PACKAGE:** insufficient anchors; hold unverified; no customer verified badge.

Detail: `BETHESDA_EVIDENCE_RECOVERY.json` (emails fingerprinted; full addresses not required for founder review).

## 3. Portfolio-wide opportunity impact

| Actionability tier | Opportunities |
|---|---:|
| SALES_READY | 5 |
| REACHABLE | 15 |
| CONTACT_RESEARCH_REQUIRED | 159 |
| CONTACT_IDENTIFICATION_REQUIRED | 3 |

### By hotel

| Hotel | Visible | SALES_READY | REACHABLE | RESEARCH_REQUIRED | IDENTIFICATION_REQUIRED | Person email w/o source |
|---|---:|---:|---:|---:|---:|---:|
| Radisson Blu Toronto Downtown | 66 | 2 | 0 | 61 | 3 | 0 |
| Bethesda Marriott | 67 | 1 | 15 | 51 | 0 | 34 |
| YOTEL Geneva Lake | 42 | 2 | 0 | 40 | 0 | 0 |
| Spice Island Beach Resort | 7 | 0 | 0 | 7 | 0 | 0 |

### Cross-hotel duplicated research

- Shared identities across ≥2 hotels: **0**
- Estimated duplicate research pairs avoidable via shared contact model: **0**

_No cross-hotel shared person identities detected in the frozen customer-visible set._

## 4. Contact-recovery workflow & cost controls

```
1) Identity preflight (named person + employer/domain/LinkedIn)
2) Scan frozen package evidence for literal email/phone (free)
3) Re-fetch only existing opportunity URLs (bounded, free)
4) Google-first public discovery → validate on official HTML
5) STOP if stop-rules hit
6) Paid providers ONLY with separate founder budget (HOLD now)
```

| Upgrade path | Est. cost / success |
|---|---:|
| Recover from frozen evidence | $3.75 |
| Re-verify existing URLs | $10.05 |
| Public discovery + validate | $18.8 |
| Paid providers | HOLD (not authorized) |

### Suggested Bethesda cohort caps (analyst time only)

- Phase A attach existing URL only: ~$0
- Phase B public discovery (34 − attachable): ~$639.2
- Paid: $0

### Research limits / stop rules

- Min attempts before stop classification: 1
- Max public discovery passes / contact: 2
- Max URL reverify fetches / contact: 5
- Stop when: two public discovery passes yield no person-specific channel
- Stop when: all candidate official URLs fetched and email/phone absent
- Stop when: identity mismatch / lookalike LinkedIn quarantine
- Stop when: only generic mailbox remains after named-person search

## 5. Recommended UI treatment

| Tier | Customer-visible label | Treatment |
|---|---|---|
| SALES_READY | Sales-ready contact | Show verified email; badge Direct buyer **or** Commercial entry point |
| REACHABLE | Reachable contact | Show verified phone; email CTA = research/find if missing |
| CONTACT_RESEARCH_REQUIRED | Contact research needed | Named person visible; channels not marked verified; no fake source |
| CONTACT_IDENTIFICATION_REQUIRED | Contact not yet identified | Prompt identification; do not invent names |

Unsupported person-specific emails (no source): **do not** present as verified Direct Business Email. Keep internal ownership metadata; customer copy stays honest.

## 6. Tests & permanent regression gates (proposed)

1. `test:gdi-contact-actionability-contract-v1` — tier classification fixtures
2. `test:gdi-contact-actionability-no-strict-publication-gate` — publication eligibility ≠ actionability
3. `test:gdi-bethesda-person-specific-without-source-quarantine` — no verified badge without sourceUrl
4. `test:gdi-buyer-vs-entry-point-labeling` — intermediaries never labeled Direct buyer
5. `test:gdi-shared-contact-identity-reuse` — personIdentityKey reuse across hotels
6. `test:gdi-actionability-inventory-freeze` — 182 baseline count unchanged by tier labeling
7. Leakage audit regression: STRICT leakage reported, not enforced

## 7. Implementation sequence (after founder GO)

| Step | Work | Acceptance |
|---|---|---|
| 0 | Founder approves this report | Explicit GO; still no STRICT gate |
| 1 | Land contract module + unit tests (no UI yet) | Gates 1–2 green; inventory delta 0 |
| 2 | Compute tiers on read path / DTO only | Customer labels honest; no CURRENT switch |
| 3 | Bethesda Phase A: attach only frozen-recoverable sources | Recovery class counts drop; no invention |
| 4 | Bethesda Phase B: bounded URL reverify (free) | Reverified emails gain sourceUrl; stop-rules enforced |
| 5 | Shared contact store design (identity key) | Cross-hotel reuse without duplicating research |
| 6 | Optional paid ladder | Separate budget PR; caps ≤ authorized credits |

## Recommendation

Proceed with **tiers-as-workflow-labels** on top of existing publication rules. Prioritize Bethesda frozen-evidence recovery and URL reverification before any paid spend. Keep DIRECT_BUYER and RELEVANT_COMMERCIAL_ENTRY_POINT visually distinct. Do not activate STRICT_EMAIL as a publication gate.

## STOP

STOP for founder review. No implementation wiring, no provider spend, no merge/deploy/publication in this phase.

