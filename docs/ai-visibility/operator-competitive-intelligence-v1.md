# Operator Competitive Intelligence V1

**Status:** Competitive Gap **partially production-certified** (CORE same-model rows only)  
**UI:** frozen (`OPERATOR_UI_DIFF = 0`)  
**Provider calls:** 0  
**Presence wave:** `aiv_operator_presence_validation_20260818_1342_20ee11`

Operator competitive intelligence measures **Questions Missing**, a derived **All Providers** view, and **Competitive Gap** on certified Operator Presence. Associations, Narrative/Sources, Recommendation signals, and an Operator Presence Index remain blocked.

## Grain

Presence remains **prompt × provider**. Questions Missing and Competitive Gap roll to **operator × scenario** (and provider scope). Failed, missing, or unavailable providers are excluded from the denominator. Missing provider is never filled with zero. OpenAI is never a proxy.

Out-of-scope absence is **Not Applicable**, not a Questions Missing weakness.

## Questions Missing vs Competitive Gap

- **Questions Missing** = the subject operator does not appear across comparable providers for an applicable owner-intent scenario.
- **Competitive Gap** = that absence **plus** one or more **CORE_COMPARABLE** alternative operators present in the same scenario / provider / geography context.

Every gap is an absence. Not every absence is a gap. SECONDARY / CONDITIONAL / NON_COMPARABLE alternatives cannot create `TRUE_COMPETITIVE_GAP`.

## Commercial comparability

Classes: `CORE_COMPARABLE` · `SECONDARY_CONTEXT` · `CONDITIONAL` · `NON_COMPARABLE`.

CORE requires: both `ELIGIBLE`, overlapping geography, commercially substitutable models **for that scenario**, production-required truth, and neither operator is Arbor.

Brand-managed vs third-party is scenario-specific (usually SECONDARY or NON_COMPARABLE). GHL mixed regional platform vs TPM is SECONDARY. Brittain US Southeast is never a CALA CORE comparable. Brand peer sets and Census are not used.

Production-required comparability fields: operator model, managed-brand affiliation, third-party management, geographic operating scope, brand-agnostic capability. Luxury / lifestyle / resort / conversion remain DETAIL_ONLY.

## Customer disclosure

Show Owner Intent, Decision Context, provider scope, Presence, missing/gap result, selected relevant operators. Protect exact prompts, variants, prompt IDs, the full classification matrix, and raw evidence internals.

Unpromoted gap candidates must not leak (`toCustomerSafeCompetitiveGapRow` returns `null` unless `clientPromoted`).

## Arbor

Holdout metrics can be 100% with **zero live positive gold mentions**. Status remains `INSUFFICIENT_OPERATOR_SPECIFIC_EVIDENCE`. Arbor is never CORE for customer gap certification and cannot appear as a promoted subject.

## Institutional / commercial-revenue scenarios

Both **DETAIL_ONLY**. Low Presence is not competitive weakness. Appearance is not “stronger capability.”

## Tests

```bash
npm run test:operator-competitive-intelligence-v1
npm run test:ai-visibility-operator-intelligence-foundation
npm run test:operator-presence-validation-v1
```

## Next

No customer UI until this partial inventory is reviewed. No Operator index yet. Arbor remains blocked until live positive Presence exists.
