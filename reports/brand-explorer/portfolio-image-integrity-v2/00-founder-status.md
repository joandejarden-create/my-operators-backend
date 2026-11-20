# Brand Explorer Image Integrity V2 — Founder Status

**Generated:** 2026-11-20T20:10Z  
**Branch:** `feat/be-factory-image-integrity-v1` (extends open PR #494)  
**Base:** Deal Capture MVP `appvtnDurnMSjINP6`  
**Paid research:** $0  

## Phase 1 reconciliation

| Item | Status |
|------|--------|
| PR #491 | **MERGED** (`98c326a12`) |
| PR #494 Image Integrity V1 | **OPEN**, CI green |
| BE workspace | `ws-brand-explorer` on integrity branch; dirty work preserved |
| Authoritative image table | `Brand Setup - Brand Explorer Presentation` (`tblZyZF4JsRkNeCbU`).`Image` |
| Asset Registry | Staging only (`tblwNaf9DZt8Lth4t`) |

## What shipped in this cycle (code)

1. **`BE_FACTORY_PERMANENT_IMAGE_INTEGRITY_V2`** — slot classifications + lifecycle vocab + writeback preserve rules.
2. **Content-API ingest + independent read-back** — `be-factory-image-attachment-ingest-v2.js` (URL-only PATCH is not success).
3. **Writeback image preservation** — never overwrite CURRENT with unverified / ephemeral Airtable CDN proposed URLs; separate `hashPresentationImages`.
4. **Fresh image delivery** — `GET /api/brand-explorer/presentation-image/:recordId` (allowlisted by Presentation record id; no arbitrary URL proxy). Brand-library blocks prefer this durable path.
5. **Live CURRENT audit runner** + **pilot remediate runner**.
6. **Regression tests** — `npm run test:be-factory-permanent-image-integrity-v2`.

## Pilot results (founder-authorized)

### Courtyard by Marriott (`rec6hye5H8zJmAGv3`)

| Slot | Record | Result |
|------|--------|--------|
| overview.scenario.1–3 | `recJcNQUm5mltOCHN` / `reciP5wtZYcgiFZY2` / `recX6N7od73A3JC1j` | **PRESERVE_VALID_CURRENT** — JPEG attachments present; fresh fetch 200; not replaced |

Root cause of founder “broken” view: ephemeral `airtableusercontent.com` signed URLs cached in browser / Proposed frozen payloads — not missing attachments. Delivery endpoint + fresh API metadata address recurrence.

### AC Hotels by Marriott (`rec9aZp7GHtzUEg0c`)

| Slots | Result |
|-------|--------|
| overview.scenario.1–3 | **IMAGE_READBACK_VERIFIED / VALIDATED** via content-API upload |
| materials.gallery.1–6 | **IMAGE_READBACK_VERIFIED / VALIDATED** via content-API upload |

Sources: Asset Registry candidates (revalidated) + Guadalajara Marriott DAM pool (JPEG). Rights remain **Needs Review** / not Company Validated.

Evidence: `10-pilot-remediation-APPLY-*.json`

## Live CURRENT audit snapshot (presence-only, pre-reprobe after race)

See `01-current-audit-summary.json` (presence layer; run may have raced pilot apply for AC):

- 68 Active/Live Basics; 67 brands with required rows
- 619 required slot rows (includes duplicate slot rows)
- Presence: ~593 attached / ~26 missing (Aloft still fully missing; Autograph/Bunkhouse/Tapestry/Vignette partial scenario gaps)

**Re-verify AC after apply:** attachments present + probe OK on scenarios + gallery.1.

## Still outstanding (not yet PORTFOLIO_IMAGE_AUDIT_COMPLETE)

- Portfolio-wide automated remediation for Aloft + partial brands + unverified 577
- `--probe` full CURRENT HTTP decode pass
- Factory PROPOSED frozen-payload audit matrix (separate denominator)
- Authenticated browser screenshots for every repaired brand on `:8080`
- Wire integrity V2 into QUALITY_CERTIFIED gate permanently beyond writeback
- Commit + update PR #494; Integration Controller merge + post-merge sync

## Lifecycle legend (do not collapse)

`IMAGE_DISCOVERED` → `IMAGE_STAGED` → `IMAGE_ATTACHMENT_IMPORTED` → `IMAGE_READBACK_VERIFIED` → `IMAGE_BROWSER_VALIDATED` → `IMAGE_CERTIFIED`
