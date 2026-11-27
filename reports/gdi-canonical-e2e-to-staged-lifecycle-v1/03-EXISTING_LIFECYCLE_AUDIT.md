# Existing GDI lifecycle audit (read-only)

**At:** 2026-11-27  
**Scope:** Canonical Full E2E → STAGED → CURRENT before/after permanent simplification

## Architecture reused (no parallel engines)

| Layer | Existing component | Role |
|---|---|---|
| Orchestrator | `Invoke-GdiFullE2E.ps1` → `gdi-full-e2e-v3-orchestrator-v1.js` | Five research stages + mandatory STAGED progression |
| Entry | `gdi-canonical-full-e2e-entry-v1.js` | Hotel-wide finalization + auto-stage |
| Qualification | FQO six-gate + hotel-wide finalization | Commercial qualification |
| Persistence (packages) | `gdi-publication-package-store-v1.js` | Durable STAGED/CURRENT packages + pointers |
| Persistence (Airtable GDO) | `gdi-full-e2e-research-persistence-bridge-v1.js` | Opt-in / separately authorized production writes |
| Local review | `:8080` GDI app + `resolveFounderInventoryMode` | Founder STAGED default |
| CURRENT promotion | `publishCustomerVisible` + `Invoke-GdiPromoteCurrent.ps1` | Explicit founder gate only |
| Validate | `Invoke-GdiValidateFullE2E.ps1` | Fixtures + lifecycle + optional :8080 |

## Intermediate gates that blocked auto-STAGED (removed or demoted)

1. **Hotel-wide `pointerActivated: false` as terminal “complete”** — candidate written but STAGED pointer never activated → founder saw CLEAN11-only / empty. **Fix:** orchestrator/entry call `stageQualifiedInventoryFromE2e` after qualification.  
2. **Proposed Qualified founder UI / `?preview=proposed` review gate** — treated write-ready inventory as a separate founder step. **Fix:** internal classification only; STAGED is the review inventory.  
3. **HOLD / continuation inventory selectors** — implied extra founder gates. **Fix:** collapsed; HOLD stays outside qualified STAGED.  
4. **Docs stating “never activate STAGED from hotel-wide / E2E”** without a mandatory follow-on activator. **Fix:** lifecycle module + PS1 doctrine; hotel-wide remains package writer, orchestrator owns activation.  
5. **“Proposed write-set QA” as operational E2E complete** — reports alone were treated as done. **Fix:** doctrine requires STAGED visible on :8080.

## Gates that remain (intentional)

| Gate | Why retained |
|---|---|
| FQO six-rule commercial qualification | Do not relax |
| DryRun non-mutating | Budget + safety |
| Paid research authorization | Spend control |
| Airtable GDO apply authorization | Separate production write auth / freezes |
| Explicit CURRENT promotion auth + exact version | Single founder customer-publish gate |
| CLEAN11 / Bethesda freezes | Explicit founder freezes |
| Auth + hotel ACL on :8080 | Localhost ≠ founder privilege |

## Contradictory docs reconciled

- Constitution §20, BUILD_DECISIONS, three-layer contract, `GDI_CANONICAL_E2E_TO_STAGED_LIFECYCLE_V1.md`, `GDI_FULL_E2E_COMPLETION_AND_PUBLICATION_LIFECYCLE_V1.md`  
- Hotel-wide header comment updated (activation owned by orchestrator lifecycle)  
- CAD doc still correctly states `FQO_CANDIDATE ≠ FQO PASS ≠ STAGED` (classification ≠ completion)

## Open IC coordination

PRs still OPEN (do not merge from GDI chat): **#618, #620, #624, #626** (+ older GDI PRs). Cumulative surface lives on `:8080` / PR **#626**.
