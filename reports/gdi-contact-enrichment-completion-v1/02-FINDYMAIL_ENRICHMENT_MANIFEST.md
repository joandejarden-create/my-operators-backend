# Findymail Enrichment Manifest — Wyndham locked-26 tip

**Date:** 2026-11-27  
**Package:** `gdi_hotel_wide_fqo_recnPB3Q02NO8Ma1o_20261127_6e217b8a` (+ post-identity rematerialize)  
**Spend authorized this cycle:** **$0**

## Integration audit

| Item | Finding |
|---|---|
| Dedicated Findymail client in repo | **None** (`lib/` / `api/`) |
| Gate | `gdi-contact-research-stage-v1.js` requires `authorizeFindymail` + `allowFindymail` |
| Auth env | `FINDYMAIL_API_KEY` (HE audit docs; not in `.env.example`) |
| Persistence default | **Off** — provider PII not auto-persisted |
| Surfe | Explicit-click only; no durable email/phone persist |
| Plan credits (docs) | Basic $49/mo · 1,000 finder · 1,000 verify · rollover 2× |

## Eligibility on current tip

Strict rule: named person + org + **no** usable email/phone/**LinkedIn**.

**Eligible primaries: 0**

Every primary already has at least one usable outreach route (email, phone, or personal LinkedIn). Prior eligibility queue rows (Pamela Ballinger, Connie Withers) are **not** in the locked-26 inventory.

## Decision

| Action | Status |
|---|---|
| Findymail API calls this cycle | **Not executed** |
| Incremental budget request | **None** — no eligible unpaid gaps under policy |
| Public Stage 4 | Already annotated on all 26; remaining email/phone gaps are post-research unavailable or need public directory follow-up outside Findymail |
| Identity before spend | Lily Y. → Lily Yuan (LinkedIn slug); Suzanne K. surname **not** invented |

## Expenditure

| Provider | Credits used | USD |
|---|---:|---:|
| Findymail | 0 | 0 |
| Surfe | 0 | 0 |
| Exa (this cycle) | 0 | 0 |
