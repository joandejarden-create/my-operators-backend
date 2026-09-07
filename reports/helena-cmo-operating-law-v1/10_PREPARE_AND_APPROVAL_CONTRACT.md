# 10 — PREPARE and Approval Contract

## PREPARE metadata (required)

ICP · GTM Track · Product · Product Maturity · Claim Class · Evidence · Confidence · CTA · Pricing Class (if applicable) · Approval Required · Provenance  

Helena may prepare LinkedIn, articles, briefs, proposals, campaigns, product-marketing resources, technical work items — **not** externally execute.

## Approval statuses

DRAFT · PENDING · APPROVED · APPROVED_WITH_CHANGES · REJECTED · HOLD  

## Semantics

- Helena cannot self-approve  
- Ambiguous approval does not count  
- APPROVED ≠ PUBLISHED ≠ MERGED ≠ DEPLOYED  
- Operating Law v1 defaults: EXECUTE remains OFF even if Joan approved a draft (until separate unlock)

Modules: `prepare-contract.js`, `approvals.js`
