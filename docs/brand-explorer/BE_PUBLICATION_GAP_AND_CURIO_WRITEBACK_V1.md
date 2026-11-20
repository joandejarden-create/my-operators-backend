# Brand Explorer — publication gap & Curio writeback prep

**Contract:** `BE_PUBLICATION_GAP_AND_CURIO_WRITEBACK_V1`  
**Status:** Documentation only · **no Airtable writes** · Gate 2 / activation remain fail-closed

## Three layers (do not conflate)

| Layer | What it is | Where it lives | What the founder sees |
|-------|------------|----------------|------------------------|
| **1. Latest finalized staged candidate** | Immutable cumulative Phase 2H overlay + scorecard + manifest | `public/brand-explorer-phase2e-preview/*-phase2h-candidate.json` via `BrandExplorerStagedCandidateRegistry` | **Proposed Update** (admin-only) and Comparison Studio **RIGHT** |
| **2. Current canonical Airtable Presentation** | Saved Presentation fields on Brand Basics / presentation rows | Airtable (product SoT for “saved”) | **Current Saved** in product atelier |
| **3. Published customer profile** | Customer-visible Brand Explorer after External Display / quality locks | Same Presentation path without `beInternalPreview` | Ordinary customers — **Current Saved only**; no preview control |

Today layers **1 ≠ 2 ≠ 3** for Curio and Ascend. That is why Compare Studio can look “finished” while the default product route still shows older saved Presentation.

## Founder access (product route — no manual preview URL)

Authorized platform admins opening:

`http://localhost:8080/app#/brand-explorer-combined`

receive automatic `beInternalPreview=1` injection from the app shell (`founderProductPreview` on the Brand Explorer route). The **Current Saved | Proposed Update** control appears in the real atelier iframe. Customers never receive that injection.

| Mode | Data | Mutation |
|------|------|----------|
| Current Saved | Layer 2 (Airtable Presentation) | None |
| Proposed Update | Layer 1 (registry Phase 2H cumulative) | None — read-only mount |

## Curio publication procedure (controlled — do not run until approved)

Fail-closed sequence. **Do not skip Gate 2, scorecard review, local parity, or activation protections.**

1. **Founder Gate 1 approval** recorded for renderer/product parity (see `BE_PHASE2H_A_GATE1_RENDERER_PARITY_FOUNDER_REVIEW.md`).
2. **Gate 2 content / standards** PASS for the exact candidate version (`phase2h-component-parity`) — `BE_PERMANENT_CONTENT_STANDARDS_V1`, scorecard green.
3. **Local product parity gate** PASS — `BE_LOCAL_PRODUCT_PARITY_GATE_V1` (Proposed Update ≡ Compare RIGHT ≡ approved candidate hash).
4. **Full 10-tab product popup quality certification** PASS — `FULL_10_TAB_PRODUCT_QUALITY_CERTIFIED` on the actual authenticated ` /app#/brand-explorer-combined` popup in Proposed Update (`verify:be-final-ten-tab-product-popup-cert-v1`, lesson `BE-QL-FINAL-TEN-TAB-PRODUCT-CERT-001`). Hash/parity alone is not sufficient.
5. **Immutable candidate pin** — record brandId `receQkxgjlezsc1xg`, candidateVersion, content hash, overlay path, and scorecard path in the publication run report.
6. **Dry-run Airtable Presentation writeback** for Curio only — preview field diffs; no `--apply`.
7. **Founder authorize apply** — then idempotent writeback of the **exact** pinned candidate into Presentation.
8. **Retrieval proof** — reload product **Current Saved** (no Proposed) and confirm hash/content match published Presentation.
9. **Customer publication / External Display** only after Current Saved matches the approved candidate and founder activation checklist passes.
10. **Registry status** — update `founderApprovalStatus` / publication pointers only after apply; never overwrite another brand’s pointer.

Ascend follows the same procedure independently (`reclkgOzvAcBheUSo`).

## Current blockers (Curio)

- `founderApprovalStatus: NOT_APPROVED` on staged registry entry  
- Gate 2 / scorecard / local parity activation not founder-cleared for production writeback  
- Airtable Presentation still holds pre-candidate content (layer 2 ≠ layer 1)  
- Customer default remains layer 2/3 until authorized writeback + activation  

## Explicit non-goals

- Silent production Airtable writes  
- Bypassing Gate 2 or parity gates because Proposed Update “looks good”  
- Treating standalone HTML or Compare Studio alone as customer publication  
- Declaring publication-ready from a single-tab fix or LEFT/RIGHT hash match without full popup subsection certification  

## Related

- `docs/brand-explorer/BE_FOUNDER_PRODUCT_PREVIEW_V1.md`  
- `docs/brand-explorer/BE_LOCAL_PRODUCT_PARITY_GATE_V1.md`  
- `reports/brand-explorer/final-ten-tab-product-certification-v1/`  
- `docs/ai-build-system/AIRTABLE_PERSISTENCE_GATE.md`  
