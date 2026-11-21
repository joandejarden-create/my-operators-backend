# Phase 0 — Reconcile existing Image Integrity work

**Date:** 2026-11-21  
**Workspace:** `C:\Dev\dealality\ws-brand-explorer`  
**Branch:** `feat/be-factory-image-integrity-v3`

## Verified completed (do not reopen)

| Item | Evidence |
|------|----------|
| PR #491 Factory 2.0 image discovery / websites / loyalty | Merged on `main` prior to V3 |
| PR #494 Image Integrity V2 durable attachment ingest + delivery | Merge commit `be99076b1` |
| AC Hotels three Overview scenarios render on authenticated :8080 | V2 pilot + DOM diagnosis reports under `portfolio-image-integrity-v2/` |
| Courtyard valid attachments protected from expired-URL overwrite | V2 `PRESERVE_VALID_CURRENT` path |
| AC scenarios + gallery content-API import + read-back | V2 pilot apply reports |

## Infrastructure reused (not duplicated)

- `api/brand-library.js` + `api/brand-explorer-presentation-image.js` delivery
- `lib/brand-explorer/factory-2/be-factory-image-attachment-ingest-v2.js`
- Brand Asset Registry / Brand Basics / Presentation tables
- Wave gallery pools under `fixtures/`
- Cumulative proposal rehydration for ephemeral Proposed CDN

## V3 additions

- Exact-brand identity permanent contract + failure codes
- Shared Verified Hotel Image Library (on-disk; Airtable schema proposed only)
- Full CURRENT + Factory Proposed audits with V3 classifications
- Duplicate slot deterministic resolution (no deletes)
- Hotel Explorer reuse gate (permission-independent)
- Portfolio remediation for Aloft / Bunkhouse / Suburban / SLH / Radisson Individuals
