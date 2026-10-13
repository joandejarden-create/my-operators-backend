# Official-List Decomposition QA

## Shared path

`official event/association → participant/exhibitor/sponsor list → named account → traveling entity → buyer → contact → lodging → future decision → packet → Ready/Watch`

Implementation: `campaign-decomposition-orchestrator.js` + evidence packs + second-gen classifiers.

## Invocation proof

| Hotel | Campaigns on disk | Orchestrator gate | Actually invoked this corpus |
|-------|-------------------|-------------------|------------------------------|
| YOTEL | 10 | Shared + YOTEL ensure | **YES** (persisted children / FUTURE_WATCH=24) |
| AC | 0 | Shared (runs when campaigns exist) | **NO** — skipped_no_visible_campaigns |
| RAD | 0 | Shared (runs when campaigns exist) | **NO** — skipped_no_visible_campaigns |

## Parity classification impact

AC/RAD share the **same live hook** after this repair. Missing runtime yield is **no demand campaigns**, not a hotel-specific fork.
