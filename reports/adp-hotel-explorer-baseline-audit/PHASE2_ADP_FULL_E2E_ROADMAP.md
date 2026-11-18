# Phase 2 roadmap — reliable, repeatable ADP Full E2E

Depends on founder approval of Phase 1 (Radar HE progressive enrichment).  
No calendar estimates — effort = technical size.

## Goals

1. One supported Full E2E entrypoint for any hotel (OS orchestrator), with hotel-specific baselines as thin wrappers only.  
2. End legacy grandfathering for commercial hotels on their next official period.  
3. Materialize / mount durable published fixtures so permanence certification runs in CI.  
4. Unify HI ADP Attributes ↔ Reality Gap attribute taxonomies for Explorer enrichment.  
5. Expand Radar HE host coverage (Scout / radar-share) after ranked-list proves Wave 0.

## Sequenced work

| Step | Item | Effort | Blockers |
|---|---|---|---|
| 2.1 | Materialize `data/adp-durable-published/current` from PRIMARY_SOT with documented sync script (still gitignored or volume-mounted) | Medium | Infra / Railway volume decision |
| 2.2 | Make `test:adp-permanent-full-e2e-certification-v1` green in CI once fixtures available | Medium | 2.1 |
| 2.3 | Single `adp-autonomous-hotel-full-e2e` CLI as only supported path | Large | Founder auth ledger |
| 2.4 | Re-cert next official periods for active commercial hotels under `ADP_REQUIRE_CERTIFICATION_BEFORE_PUBLISH=1` | Medium/hotel | Paid remeasure approval |
| 2.5 | Attribute taxonomy unification → Explorer attribute strip | Large | Schema stewardship |
| 2.6 | Scout + radar-share HE wiring (same progressive model) | Medium | Phase 1 host proof |
| 2.7 | Optional Bethesda internal-only deep-link QA (still no public share pack) | Small | Founder confirm |

## Explicit non-goals until separately approved

- Customer share packs for Bethesda or other hotels  
- Production Airtable mutations  
- Paid Webhound / Parallel research factory runs  
- Weakening permanence or customer-surface assertions  

## Unresolved from Phase 1 (carry forward)

1. **Durable published fixtures missing in repo** (`data/adp-durable-published/**` gitignored) → permanence cert wrapper exits 2 with clear code.  
2. **Customer-surface-contract** may still fail on deeper evidence-card edge cases after null-intent fix — re-run and triage remaining failures without weakening gates.  
3. **Certification baseline** restored as reconstruction from published manifests — founder should confirm accepting reconstructed authority vs locating original freeze blob.
