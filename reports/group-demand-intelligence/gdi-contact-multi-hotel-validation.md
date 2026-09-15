# GDI Contact Multi-Hotel Validation

**Verdict: PORTABLE**

Synthetic fixtures only — no paid research.

## Hotels

| | Hotel A | Hotel B |
|---|---|---|
| ID | `recSynthUrbanBusiness001` | `recSynthResortGroup002` |
| Type | Urban full-service business | Resort / group-heavy |
| Geography | miami, florida | tampa, florida |

## Checks

- PASS different hotel IDs and geographies load as data
- PASS WHO scoring portable across geographyHints
- PASS coverage calculator hotel-agnostic for both hotels
- PASS Hotel A establishes person → Hotel B reuses without Surfe
- PASS merge engine rejects ambiguous identity identically for any hotel
- PASS functional entity not enrichment-eligible (portable)
- PASS core merge module has no Bethesda hotelId hardcode
- PASS dry-run write mode never sets mutated=true via applyMergeDecision
- PASS hotel feedback wrong-person blocks cross-hotel reuse

## Proven reuse scenario

1. Hotel A discovers **Casey Planner** with accepted email + mobile  
2. Hotel B encounters same person on a different opportunity  
3. Canonical lookup reuses reachability → **Surfe not called**  
4. Hotel B stores its own opportunity relationship  
5. Person-canonical fields remain shared; event roles stay separate  

## Operating Law

| Issue | Classification | Implementation |
|---|---|---|
| Merge / reuse engine | REUSABLE_PRODUCT_LOGIC | `canonical-merge-policy.js` |
| Synthetic hotel IDs / events | HOTEL_SPECIFIC_DATA | this validation script fixtures |
| Bethesda pilot eval | HOTEL_SPECIFIC_DATA | `evals/bethesda-*` |

## Regression

```bash
npm run test:canonical-contact-merge-policy
npm run test:gdi-contact-multi-hotel-validation
```
