# v46 — Brand Explorer Image Remediation Batch

Read-only batch for the remaining incomplete brands routed by OS to **`image_remediation`**:

- `hotel-indigo`
- `mgallery-collection`
- `small-luxury-hotels-of-the-world`

Protects released golden brands: Everhome, Kimpton, Radisson Individuals, Design Hotels.

## Command

```bash
npm run brand-explorer-v46-image-remediation-batch -- --brands hotel-indigo,mgallery-collection,small-luxury-hotels-of-the-world --dry-run
```

No `--apply` in this stage (no Presentation Image materialization, no draft apply, no unlock).

## What it does

1. Confirms OS `image_remediation` routing + external lock for targets
2. Protects v44 released baseline (fail on golden regression / incomplete unlock)
3. Builds asset-pack candidates (gallery / property / scenario)
4. Rejects logos, lifestyle-only, generic IHG heroes, InterContinental, Accor brand graphics, registry-only without render `imageUrl`
5. Prefers CALA → U.S. → global property-specific photography
6. Classifies eligibility from **accepted candidates** (not live Presentation counts): `still_blocked_by_images` | `asset_pack_ready` | `build_draft_ready` (never `apply_draft_allowed`)

## Hotel Indigo remediation note

Legacy catalog used invalid MARSHA stubs (`guan` / `gdl` / `lim` / `bna`) that resolved to shared IHG shell pages and the Maldives brand hero. v46 corrects codes to **BJXGD / GDLAL / LIMMD / BNAUS** and accepts only `digital.ihg.com/.../hotel-indigo-{city}-*` property photography.

## Accor / MGallery note

`ahstatic.com/photos/{code}_ho_*` (and `_ro_*`) are treated as official property-specific photography — not generic Accor brand graphics.
## Outputs

- `reports/brand-explorer-v46-image-remediation-batch.{json,md}`
- `reports/brand-explorer-v46-hotel-indigo-image-remediation.md`
- `reports/brand-explorer-v46-mgallery-image-remediation.md`
- `reports/brand-explorer-v46-slh-image-remediation.md`
- `reports/brand-explorer-v46-released-baseline-protection.md`

## Guardrails

- No active release / unlock
- No Company Validated changes
- No released brand changes
- No Presentation writes
- No generic / registry-only images counted as render-ready

## Modules

- `lib/partner-intelligence/brand-explorer-v46-image-remediation-batch.js`
- `scripts/brand-explorer-v46-image-remediation-batch.mjs`

OS `image_remediation` next command points at this dry-run.
