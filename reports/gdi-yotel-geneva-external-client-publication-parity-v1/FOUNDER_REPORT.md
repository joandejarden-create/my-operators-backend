# YOTEL Geneva Lake — External client publication + local parity V1

**Status:** PREPARATION COMPLETE — BLOCKED at founder approval gate  
**Hotel:** YOTEL Geneva Lake (`recrPQcZg7SFARRb2`)  
**Fingerprint:** `353430092ceea5626c85e04bffbeeb84d0738bf7a5382c5177d5fdf37413eba3`  
**Inventory:** 42 total · 34 Hotel Market · 8 CMI  

## Publication readiness

| Gate | Result |
|---|---|
| Staged fingerprint | PASS |
| Package integrity | PASS |
| 42 unique IDs | PASS |
| 34 / 8 inventory | PASS |
| Certified drawers | 42/42 · truncated=0 |
| Essential prose | PASS |
| Auth↔simulated share commercial parity | PASS |
| Prior package 41a1b1ca preserved | PASS |
| CURRENT present | NO (expected) |

## Founder approval gate (HARD STOP)

Required exact phrase:

```
FOUNDER_APPROVED_FOR_PUBLICATION 353430092ceea5626c85e04bffbeeb84d0738bf7a5382c5177d5fdf37413eba3
```

This chat has **not** supplied that phrase. Activation remains dry-run only.

After authorization:
1. Set `FOUNDER_APPROVED_FOR_PUBLICATION.json` approved=true
2. `node scripts/gdi-activate-yotel-geneva-35343009-approved.mjs --apply --confirm-fingerprint=353430092ceea5626c85e04bffbeeb84d0738bf7a5382c5177d5fdf37413eba3`
3. Commit CURRENT + receipt → PR → merge
4. Railway production release for exact merge SHA
5. `railway run node scripts/gdi-issue-share.mjs --hotelId=recrPQcZg7SFARRb2 --label="YOTEL Geneva Lake GDI"`
6. Optional feedback grant via `gdi-grant-share-lifecycle-capabilities.mjs` after separate approval
7. Production browser QA of all 42 drawers

## Capability differences (approved, not parity failures)

- Local uses founder auth + `preview=staged`; external uses share token + CURRENT only
- Surfe stays on-click / founder-gated — not silent paid enrichment for clients
- Feedback (Bethesda-style) requires explicit `CAN_VALIDATE` / action / outcome capabilities

## External URL

**Not issued.** Production CUSTOMER_PUBLISHED pointer and share token await founder phrase + deploy.

Planned path shape (token redacted):  
`https://my-operators-backend-production.up.railway.app/group-demand-intelligence-share.html?share=gdishare.v1.<redacted>`

## GitHub

Filled when activation PR merges (post-approval).
