# Funding summary correction — six-combo Admin delivery

**As of:** 2026-10-16T22:15:00Z  
**No paid provider calls or top-ups in this task.**

## 1. Renaissance GDI — AlphaSense $0.25 already in ledger

| Field | Value |
|-------|------:|
| Authorization | `auth_gdi_renaissance_ts_12usd_v1` |
| Cap | **$12.00** |
| Incurred (authoritative) | **$4.31** |
| Available | **$7.69** |
| Ledger path | `data/group-demand-intelligence/renaissance-gdi-budget/authorization-ledger-v1.json` |

The reported AlphaSense Surfe attempt is the **third** `surfe_contact_enrichment` entry at **$0.25** (`2026-10-16T19:54:56.810Z`). It is already included in the $4.31 total. **Do not duplicate** this charge.

Earlier $4.06 readings came from a stale MOB/worktree copy that lacked this third Surfe row. Authoritative copies (exec + main-deploy) already reconciled at $4.31 / $7.69.

## 2. Wyndham San Diego ≠ new research authorization

| Envelope | Hotels | Cap | Incurred | Reserved | Accounting hold | Available | Notes |
|----------|--------|----:|---------:|---------:|----------------:|----------:|-------|
| Mayflower / San Diego GDI (shared) | Mayflower `rec0eTEnCqlsGJJt9`, Wyndham SD `recnPB3Q02NO8Ma1o` | **$11.00** | **$5.25** | **$0** | **$1.25** | **$4.50** | Authoritative ledger `paid-multisegment-budget/authorization-ledger-v1.json` (`reconciliation.safelyAvailableUsd`). The earlier $3.85 / $7.15 worktree reading is stale. Hold and restored LMC transactions stay in the ledger. |
| Renaissance GDI | Ren NYTS only | $12.00 | $4.31 | $0 | $0 | $7.69 | Separate — do not draw Mayflower/SD funds. AlphaSense $0.25 is the third `surfe_contact_enrichment` row already inside $4.31. |
| ADP Renaissance | Ren NYTS ADP only | $15.00 | $10.53 | — | — | ~$4.47 | Separate from all GDI envelopes |

Wyndham Admin client-link / PDF delivery uses **existing** Mayflower/SD research already in Airtable (82 opportunities / 51 customer-facing). It does **not** authorize new paid research scope for Wyndham or Westin.

Westin Tampa Waterside has 21 Airtable opportunities but **0 customer-facing** — GDI client list is empty until publication eligibility is completed under an approved scope (not this delivery task).

## 3. Internal authorization ≠ provider credits / cash

| Layer | What it means | Do not confuse with |
|-------|---------------|---------------------|
| Internal auth ledgers (Ren $12, Mayflower/SD $11, ADP Ren $15) | Dealality spend caps for approved work | Provider wallet / card balances |
| Surfe usage ledger | Contact enrichment usage under those caps | Surfe account credits |
| Parallel / LLM provider balances | Cash or prepaid credits at the vendor | Internal available USD on auth ledgers |

Provider credit checks must not block Admin client-link / PDF delivery fixes.


## Consolidation update (2026-10-17)

- Mayflower/SD shared authoritative: **$5.25 incurred / $1.25 hold / $4.50 available**
  (MOB `data/group-demand-intelligence/paid-multisegment-budget/authorization-ledger-v1.json`
  `reconciliation.safelyAvailableUsd`).
- Do not restore older $3.85 / $7.15 worktree summaries.
- Renaissance GDI: **$4.31 incurred / $7.69 available**
  (`data/group-demand-intelligence/renaissance-gdi-budget/authorization-ledger-v1.json`).
- Renaissance ADP remains separate from GDI ledgers.
