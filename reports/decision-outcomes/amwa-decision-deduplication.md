# AMWA decision deduplication

Generated: 2026-09-17T16:40:00.000Z  
Mode: audit (no merge required)

## Finding

The three Bethesda “AMWA” Decision rows are **not duplicates of the same subject/version lineage**.

| decisionId | subjectId | status | events | notes |
|---|---|---|---|---|
| `dec_mu4qcpxz_5394a4c6` | `gdi_opp_amwa_2027_annual` | VALIDATED | 72 validations | Opportunity-linked; Rad feedback lineage |
| `dec_mu5kpw7x_d10847ae` | `gdi_opp_amwa_2027_interim` | RECOMMENDED | 0 | Separate opportunity |
| `dec_mu5kpxvb_cbfe3bb4` | `gdi_opp_amwa_2028_annual` | RECOMMENDED | 0 | Separate opportunity |

## Selection rule (for true duplicates)

1. Opportunity-linked `decisionId`
2. Most complete event lineage
3. Earliest `createdAt`
4. Stable `decisionId` ascending

## AMWA 2027 annual (the feedback subject)

ACTIVE DECISIONS FOR SAME SUBJECT/VERSION: **1**  
RETIRED/SUPERSEDED: **0**  
EVENTS LOST: **0**  
EVENTS DUPLICATED: **0**  
OPPORTUNITY LINK: `dec_mu4qcpxz_5394a4c6`

## Root cause of the “3 decisions” signal

Earlier coverage count used `FIND('amwa', subjectId)`, which matched **three different opportunities**, not three copies of one decision.

## Idempotency hardening (implemented)

Even though no merge was required, `createDecision` now:

1. Looks up by idempotency key
2. Looks up by hotel + module + type + subject + recommendationVersion among **active** (non-CLOSED) decisions
3. Returns the existing active decision instead of creating a second row

Regression: `test:decision-outcomes` + `test:adp-gdi-hpc-identity` cover same-key idempotency; added subject-level active reuse check.
