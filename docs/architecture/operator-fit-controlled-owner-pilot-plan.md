# Operator Fit — Controlled Owner Pilot Plan

**Date:** 2026-08-04  
**Status:** Ready to authorize — **NOT ENABLED**  
**First deal:** Deal C (redacted) · Backup: Deal D

---

## Corrected gate basis

- Candidate-bearing Round 2: **4/5 Strong** (A, C, D, F Strong · E Useful)  
- Deal B: Truthfulness **Passed** (excluded from denominator)  
- Deal F: provisional synthetic — founder acknowledgment required  

## Design (when founder authorizes enablement)

| Control | Requirement |
| ------- | ----------- |
| Scope | **One** owner · **one** approved deal (Deal C) |
| Access | Server allowlist (user + deal ID) |
| Kill switch | `OPERATOR_FIT_ENGINE_V2=0` disables owner surface immediately |
| UI | **Owner View only** |
| Hidden | Advisor diagnostics · Research Stage in ranked list · internal-only claims · Ranking Ready jargon · `/100` headline |
| Shortlist | Airtable Shortlist only if explicitly approved for this pilot |
| Outreach | **No** automatic ODR / outreach |
| Logging | Deal · event · engine version · timestamp (minimal PII) |
| OAS | Legacy OAS remains available (coexistence) |

## Rollback

1. Set `OPERATOR_FIT_ENGINE_V2=0` (and any pilot allowlist empty).  
2. Confirm My Deals does not surface Fit.  
3. No DB migration required to disable.  
4. Shortlist snapshots remain for audit (immutable).  

## Explicit non-goals until separate approval

Multi-owner rollout · Deal B as first pilot · weight changes · OAS deprecation · My Deals global wiring · Deal F as live owner deal without real replacement

## Enablement checklist (human)

- [ ] Founder accepts provisional Deal F **or** real Deal F scored Strong  
- [ ] Freeze docs acknowledged (terminology + Option C)  
- [ ] Allowlist entries written  
- [ ] Kill-switch tested in staging  
- [ ] Deal C preview reviewed with founder  

**Do not implement owner access in this assignment.**
