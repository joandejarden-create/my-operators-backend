# Commercial Contact Intelligence — founder report

**Status:** STOP for founder review before full-database backfill  
**Integration path:** Sequential extension of open PR [#475](https://github.com/joandejarden-create/my-operators-backend/pull/475) (PCD 2.0). PR #474 Surfe auth remains separate.  
**Date:** 2026-11-20

## 1. Architecture audit

See `ARCHITECTURE_AUDIT.md`. CCI extends PCD 2.0; no competing engine; drawer not redesigned.

## 2. Commercial contact selection design

Relevance-first selection:

1. Verified hotel / meetings / travel buyer (evidence-gated)
2. Best accessible entry point (when buyer lacks channels)
3. Relevant backup
4. Organizational housing / meetings route

Never silently replace a verified decision-maker with an easier-to-email contact.

## 3. Dual scoring rubrics + evidence gates

| Score | Weights |
|-------|---------|
| CHANNEL (0–100) | Email 40 · Phone/ext 30 · Profile/route 20 · Provenance 10 |
| COMMERCIAL (0–100) | Buying authority evidence 40 · Demand relationship 30 · Influence/referral 20 · Current role 10 |

Gates: DIRECT_HOTEL_BUYER and VERIFIED_MEETINGS_OR_TRAVEL_BUYER require confirmed responsibility evidence. Title alone insufficient. Scores cannot override gates (unit-proven).

## 4. Ecomondo + Maritza regressions

| Case | Outcome |
|------|---------|
| Alejandra | Kept as RELEVANT_COMMERCIAL_ENTRY_POINT · email recovered · **not** hotel buyer · gap recorded · Find Phone only |
| Paulina | Secondary/backup retained |
| Housing research | Ecosystem queries include hoteles/hospedaje; no invented housing buyer |
| Maritza | Email + office phone preserved · **not** inferred hotel buyer from procurement · entry-point classification |

Evidence: `REGRESSION.json` / `REGRESSION.md`

## 5. 100-contact pilot (before/after)

| Metric | Value |
|--------|-------|
| Opportunities | 100 |
| Provider | EXA_ONLY |
| Meaningful commercial upgrades | **2** |
| Better relevant entry points | 2 |
| New verified direct buyers | **0** (correct — Exa did not invent buyers) |
| Incorrect buyer promotions rejected | 1 |
| Contacts retained correctly | 98 |
| New emails / phones | 1 / 0 |
| Surfe avoided | 26 |
| **Actual Exa spend** | **$2.64 / $10** |
| Surfe spend | **0** |

Note: Same cohort previously ran PCD 2.0 channel recovery; CCI correctly emphasizes commercial relevance over raw channel count. New-person ≠ automatic improvement (enforced in comparison metric).

## 6–9. Persistence / Surfe / spend

- Airtable: same inventory path as PCD 2.0 (two-case already certified on #475). CCI annotations store on `primaryContact.commercialContactIntelligence` + contact-level weekly change layer without creating demand opportunities.
- Surfe: 0 automated calls; CTAs only for unresolved channels.
- Exa: $2.64 this pilot (+ prior PCD pilot $1.25 on same cohort is separate).

## 10. Local validation

Local STAGED drawer already shows Maritza/Alejandra public channels from PCD 2.0 restage. CCI adds sales-friendly `whyRelevant` copy (e.g. “Hotel accommodation responsibility not yet confirmed”).

Railway: awaits IC merge of #475 (+ this extension).

## 11. PR / SHA

- Branch: `feat/gdi-public-contact-discovery-2-v1`
- Push updates PR #475 (CCI commits follow PCD 2.0)
- Deployed SHA: not until IC merge

## 12. Remaining gaps / calibration

- Live Exa rarely surfaces *new named* housing buyers without curated official pages — need more official housing-page seed URLs per opportunity.
- Stratified founder ranking sample recommended before weighting changes.
- Branch still behind `origin/main` — IC should reconcile on merge.
- Serper hybrid not evaluated.
- Full DB backfill **not started**.

## STOP

Await founder review of pilot results before entire-database backfill.
