# SHARE_WORKFLOW_TRACE

Canonical ADP customer-share workflow (control hotels: Bethesda Marriott, Spice Island Beach Resort; Waterstone via `adp_waterstone_boca_raton`):

1. **FOUNDER APPROVAL** — AVA lifecycle `FOUNDER_REVIEWED_STAGED` → `FOUNDER_APPROVED_FOR_CUSTOMER` with `founderApprovedForCustomer=true` (+ timestamp) via `upsertAuditMeta` / admin status transition (`api/admin-ai-visibility-audit.js`).
2. **CUSTOMER READY** — `CUSTOMER_READY` with `customerReadyAuthorized` / `customerReadinessStatus=CUSTOMER_READY` and founder + customer publication flags.
3. **PUBLISHED MANIFEST / DURABLE CUSTOMER PACKAGE** — classic `data/ai-demand-positioning/published/{propertyId}/` via `publishExistingHotelAdpSnapshot` + durable published-current; report payload carries Core + LOYALTY_BPP portfolio.
4. **SHARE TOKEN** — `issueShareCapability` → `config/client-share/adp-share-registry/active-tokens.json` (`ACTIVE`) + sealed row in `production-share-contract-tokens.json`.
5. **EXTERNAL URL** — `{SHARE_PUBLIC_BASE}/owner-ai-demand-share.html?share=adpshare.v1.…` (no localhost, no admin route).

Toronto uses this same path (no new share mechanism). Prior customer release minted token `sht_a07147cf287fc81fa9afca26`; this activation applies founder authorization + confirms registry/contract/Open-Copy probe.
