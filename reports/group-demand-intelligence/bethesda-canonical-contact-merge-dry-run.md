# Bethesda Canonical Contact Merge — Dry Run

**Mode:** `DRY_RUN` · Production writes: **PROHIBITED** · AUTO_ACCEPT_SAFE: **DISABLED**
**Source:** `bethesda-contact-reachability-v1.json`
**Generated:** 2026-09-15T21:15:24.883Z

## A. Canonical merge decisions

| Action | Count |
|---|---:|
| ACCEPT_NEW_FIELD | 5 |
| REPLACE_WEAKER_FIELD | 0 |
| CORROBORATE_EXISTING | 0 |
| HOLD_FOR_REVIEW | 0 |
| REJECT_FIELD | 13 |
| NO_INCREMENTAL_VALUE | 0 |

Audit entries: **18** · Any production mutation: **false**

## B. Bethesda projected reachability (cohort people)

| | Before (funnel) | After proposed merge (cohort people) |
|---|---:|---:|
| Usable email | 21 | 9 |
| Usable phone | 10 | 3 |
| Both | 10 | 3 |

> Funnel before = hotel opportunity grain (29). After = distinct people in reachability cohort with proposed fields (dry-run).

## Per-person proposals

| Person | Identity | Decisions | Proposed email | Proposed phone/mobile |
|---|---|---|---|---|
| Brad Roos | AMBIGUOUS | EMAIL:REJECT_FIELD; PHONE:REJECT_FIELD | broos@bethesdasoccer.org | — |
| Brad Roos | AMBIGUOUS | EMAIL:REJECT_FIELD; PHONE:REJECT_FIELD | Brad@msysa.org | — |
| Elizabeth Lancaster | AMBIGUOUS | EMAIL:REJECT_FIELD; PHONE:REJECT_FIELD | elancaster@actscience.org | — |
| Amy Drow | ACCEPTED | EMAIL:ACCEPT_NEW_FIELD; MOBILE:ACCEPT_NEW_FIELD | adrow@ndss.org | +14074962293 |
| Meg Novak | AMBIGUOUS | EMAIL:REJECT_FIELD; PHONE:REJECT_FIELD | mnovak@acc.org | — |
| Jamie McCormick | ACCEPTED_WITH_LIMITED_EVIDENCE | EMAIL:REJECT_FIELD; MOBILE:ACCEPT_NEW_FIELD | jmccormick@nado.org | +15189447999 |
| Karen Bertani | AMBIGUOUS | EMAIL:REJECT_FIELD; PHONE:REJECT_FIELD | karen.bertani@bebpa.org | — |
| Ben Hawkins | ACCEPTED | EMAIL:ACCEPT_NEW_FIELD; PHONE:REJECT_FIELD | ben@alexandria-soccer.org | — |
| Kelly Frere | ACCEPTED_WITH_LIMITED_EVIDENCE | EMAIL:REJECT_FIELD; MOBILE:ACCEPT_NEW_FIELD | kfrere@asaecenter.org | +13015095526 |

## C. Reuse

This dry-run seeds from reachability only (no prior canonical store). Cross-opportunity/hotel reuse is validated in the multi-hotel suite.

## D. Verdict

**READY FOR CONTROLLED CANONICAL MERGE PILOT**

Next controlled step: REVIEW_REQUIRED apply path for TIER_1_SAFE fields only — still not global Surfe enablement.
