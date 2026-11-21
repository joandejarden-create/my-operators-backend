# Shared Verified Hotel Image Library V1

**Contract:** `BE_SHARED_VERIFIED_HOTEL_IMAGE_LIBRARY_V1`  
**Module:** `lib/brand-explorer/factory-2/be-shared-verified-hotel-image-library-v1.js`  
**Exact-brand gate:** `BE_FACTORY_PERMANENT_EXACT_BRAND_IMAGE_IDENTITY_V1` / `BE-QL-IMAGE-EXACT-BRAND-001`  
**Status:** On-disk durable ledger implemented · Airtable schema **PROPOSED only** (no unapproved schema apply)

## Purpose

Every Dealality hotel photograph must be traceable to:

- Actual hotel property (Census ID when available)
- Exact Brand Basics ID (not parent company alone)
- Geography, source provenance, usage rights
- Permitted products (Brand Explorer / Hotel Explorer / customer-facing independently)

## Reused canonical structures

| Concept | Existing SoT | Notes |
|--------|--------------|-------|
| Brand | Brand Setup — Brand Basics (`tbl1x6S7I7JwTcRdV`) | `brandBasicsId` |
| Property | Hotel Property Census | `censusPropertyId` when linked |
| Presentation display | Brand Explorer Presentation (`tblZyZF4JsRkNeCbU`) | `presentationRecordIds[]` |
| Asset governance | Partner Intelligence — Brand Asset Registry (`tblwNaf9DZt8Lth4t`) | Optional `brandAssetRegistryId` |
| Delivery | `/api/brand-library/presentation-image/:recordId` (V2) | Fresh signed fetch |

**Do not** create parallel hotel or brand records.

## On-disk ledger (implemented)

```
data/brand-explorer/verified-hotel-image-library/
  index-v1.json
  assets/svhil_<hash24>.json
```

Idempotent upsert by SHA-256 content hash. Query by brand / census / registry before discovering new images.

## Airtable schema proposal (NOT applied)

Optional future fields on Brand Asset Registry **or** a thin link table — founder/IC approval required before apply:

| Field | Type | Purpose |
|-------|------|---------|
| Content Hash | Single line | SHA-256 |
| Census Property | Link → Hotel Property Census | Exact property |
| Exact Brand | Link → Brand Basics | Exact brand (not parent) |
| Scene Category | Single select | exterior/lobby/guest_room/… |
| BE Display Permission | Checkbox | Brand Explorer |
| HE Display Permission | Checkbox | Hotel Explorer |
| Customer-Facing Permission | Checkbox | Customer surfaces |
| Exact Brand Decision | Single select | ELIGIBLE / REJECT_SIBLING / HOLD_… |
| Library Asset ID | Single line | `svhil_…` |

Compatibility: additive only; no rename of existing Attachment / Source URL fields.

## Product permission rule

A publicly accessible image does **not** automatically grant customer-facing reuse. Each product flag is independent.

## Commands

```bash
npm run test:be-factory-exact-brand-image-identity-v1
npm run be-factory-image-integrity-current-audit-v3
npm run be-factory-image-integrity-current-audit-v3 -- --probe
```
