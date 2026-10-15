# Implementation proof

## Code

| Path | Role |
|------|------|
| `lib/group-demand-intelligence/priority-action-window/assignment-contract-v1.js` | Shared assign + apply; persists reasons + version |
| `lib/group-demand-intelligence/reviewed-prospect-import-v1.js` | Calls `applyPriorityActionAssignment` after contact package merge (replaces blind Watch stamp) |
| `lib/group-demand-intelligence/opportunity-list-dto.js` | Exposes `prospectCommercialClassLabel`, reasons, version |
| `lib/group-demand-intelligence/prospect-publication-eligibility-v1.js` | Reads `contactUrl` / URL `contactMethod` for usable route |
| `scripts/gdi-priority-action-window-simulate-v1.mjs` | Five-hotel freeze + simulation + change set |
| `scripts/test-gdi-priority-action-window-assignment-v1.mjs` | Focused regression cases |

## Tests

```text
node scripts/test-gdi-priority-action-window-assignment-v1.mjs
→ test-gdi-priority-action-window-assignment-v1: PASS
```

Cases covered:

1. Strong fit + sourcing unconfirmed + usable contact → Medium + Contact  
2. Placed host + unconfirmed overflow → Contact, not Pursue Now  
3. Ongoing venue partnership → Contact (relationship)  
4. Future occasion unknown planning → Too Early + Watch priority  
5. No usable contact → enrichment Watch action; Medium priority if fit credible  
6. Same occasion different hotel suitability → Medium vs Watch  
7. `onlyIfMissing` does not overwrite evidence-backed High  
8. Default Watch stamps upgraded when evidence supports Contact  
9. Confirmed open buying → High + Pursue Now  
10. Commercial class independent of Priority filter  

## Simulation

```text
node scripts/gdi-priority-action-window-simulate-v1.mjs
→ san_diego 21→ Medium/Contact; mayflower 11→ Medium/Contact; Bethesda High preserved
```

## Repeatability

Future reviewed imports assign via the same module. Re-run simulation anytime. Production apply (when authorized) should consume only `production-change-set.json` patches and re-verify facet counts.
