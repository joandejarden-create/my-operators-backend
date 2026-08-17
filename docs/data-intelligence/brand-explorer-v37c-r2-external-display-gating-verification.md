# v37C-R2 External Display Gating Verification

Read-only verification that API `brandExplorerDisplayState` gates owner-facing staging copy.

```bash
npm run brand-explorer-v37c-r2-external-display-gating-verification -- --brands hotel-indigo,mgallery-collection --dry-run
npm run test:brand-explorer-external-display-gating -- --brands hotel-indigo,mgallery-collection
```

## Display states
- `hidden_incomplete` — suppress scenario/helper/fallback bullets externally
- `internal_preview_only` — same suppression; internal preview via `?beInternalPreview=1`
- `draft_applied_with_defects`
- `founder_review_ready`
- `external_owner_ready`

Source Library seeding alone must never set `external_owner_ready`.