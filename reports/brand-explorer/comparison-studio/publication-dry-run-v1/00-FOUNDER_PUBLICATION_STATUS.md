# Brand Explorer — Approve & Publish (dry-run)

**Production writes authorized:** false  
**Curio / Ascend:** NOT APPROVED  
**Gate 1:** AWAITING_FOUNDER_APPROVAL  
**Gate 2:** BLOCKED  
**Airtable writes in this run:** 0

## Workflow

The Comparison Studio **Approve & Publish** action runs:

1. Lock candidate + immutable content hash  
2. Validate Gate 1 / Gate 2 / founder approval / content gates  
3. Build exact Presentation write plan (before/after)  
4. Verify Brand Basics identity + concurrent modification  
5. Create recoverable snapshot  
6. Apply writes (**gated off**)  
7. Staged publication boundary (no public exposure)  
8. Readback verify plan  
9. Promote to customer (**gated off**)  
10. Post-publish verify plan  
11. Record audit  
12. Rollback path from snapshot  

Activation file: `configs/brand-explorer-founder-publication-activation-v1.json`  
(`productionWritesAuthorized: false`)

## Dry-run results

### curio-collection
- status: **BLOCKED_GATES**
- hash: `c0cc375b048668a2ace9acce4bb395f27ea2b63067a2bd80607f427e6430a089`
- writes: 0
- plan: {"update":168,"create":46,"deactivate":9,"skippedNotApproved":0}

### ascend
- status: **BLOCKED_GATES**
- hash: `b90ae03e42ec071ed6a7a7f5d801cd46d8eaf843b551515166d43f8b02523235`
- writes: 0
- plan: {"update":0,"create":214,"deactivate":0,"skippedNotApproved":0}


Ordinary approved publications will use this path once gates pass and the founder separately authorizes production publishing. No separate manual Airtable editing is required for ordinary approved publications after activation.
