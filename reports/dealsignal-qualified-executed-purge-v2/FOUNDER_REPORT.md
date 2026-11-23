# DealSignal — Qualified Opportunities Executed-Deal Purge V2

**CUSTOMER_BLOCKED** · No Airtable deletes · No schema · No Surfe · No paid research

## A. Root cause

Reader `classifyBucket` promoted every `COMMERCIAL_OPPORTUNITY_BCT|MCT` finding to Qualified (`PRINCIPAL_OPEN_SIGNAL`) unless closed markers appeared in **notes**. Track A stored outcome tags such as `timing=CLOSED` / `ACTIVE_COMMITTED` in **valueObserved**. VERIFIED relevance labels were treated as open opportunity.

## B. Authoritative eligibility rules

Module: `hif-deal-opportunities-commercial-eligibility-v1.js`

Qualified only when all are true:

1. Relevant BCT/MCT decision  
2. Prospective + unresolved (not executed / retained / renewed / committed / awarded)  
3. Commercially actionable (expiry-only insufficient; contacts not required)

VERIFIED ≠ open. Track B `find_tb_*` stays watch without explicit OPEN_PROCESS.

## C–E. Population audit (live dump reclassify)

See `QUALIFIED_POPULATION_AUDIT_LEDGER.json`.

| Metric | Count |
|--------|------:|
| Qualified before | 45 |
| Removed from prior Qualified | 35 |
| Retained from prior Qualified | 10 |
| Proxy after Qualified (why-only; notes further constrain) | ~16 |
| Historical executed destinations | 16 |
| Historical committed | 6 |
| Expiry-only → watch | 8 |
| Emerging / relationship | 5 |
| Internal owned-brand program | 2 |

Airtable records deleted: **0**  
Schema changes: **0**

## F. Historical destinations

Existing Emerging / All Intelligence views. Status labels: Historical — executed / retained / committed. Dedicated Historical Decisions tab proposed separately (not shipped).

## G–H. Drawer + sources

Historical disposition forces MONITOR priority, historical narrative (no “upcoming brand decision” / “approach to compete”), and preserves original source URLs on findings.

## I. Track A handoff

`TRACK_A_CLASSIFICATION_HANDOFF.md`

## J–K. Tests / browser

- `npm run test:dealsignal-qualified-commercial-eligibility-v1`  
- `npm run test:deal-opportunities-reader`  
- `npm run test:deal-opportunities-drawer-v2`  
- Live reader smoke + browser verification recorded in PR completion notes.

## L. Defect regressions (must stay out of Qualified)

- AC Hotel Cartagena — historical_executed  
- GHL / Sonesta MFA through 2034 — historical_executed  
- HI Monterrey retention — historical_executed  
- Courtyard GDL conversion — historical_executed  
- Devossion Isla Mujeres — historical_committed  
- Courtyard PDC committed — historical_committed  
- HI Puebla La Noria internalization — historical_executed  
- Track B Gwen HMA table — watch_contractual  

## M. Ambiguous / review

Proxy promotions (Maldives public bid plots; Voir Beach; Baha Mar brand-open) — commercially may be open; live notes historically marked some Emerging/Closed. Full notes at runtime remain authoritative. Retained Qualified set of 10 listed in ledger.
