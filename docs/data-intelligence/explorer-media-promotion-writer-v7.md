# Explorer Media Promotion Writer v7

**Status:** Explorer-facing media promotion writer (dry-run first, heavily gated)  
**Module:** `lib/partner-intelligence/explorer-media-promotion-writer.js`  
**Script:** `npm run explorer-media-promotion-writer -- --brand tribute-portfolio --dry-run`

## Purpose

Promote only formally approved, attachment-ready registry assets into Explorer-facing media fields:

- `Brand Setup - Brand Basics` (logo + hero fields)
- `Brand Setup - Brand Explorer Presentation` (`materials.gallery.*`, value-driver scenario image slots)

This is the first governed Explorer-facing write path. It is dry-run by default.

## Commands

```bash
# Default (no writes)
npm run explorer-media-promotion-writer -- --brand tribute-portfolio --dry-run

# Apply (required gate)
npm run explorer-media-promotion-writer -- --brand tribute-portfolio \
  --apply --approve-explorer-media-promotion

# Optional overwrite gates
--allow-logo-overwrite
--allow-nonblank-hero-overwrite
--allow-presentation-slot-overwrite
--allow-presentation-slot-image-patch
```

## Eligibility (required)

Registry asset is promotable only when all are true:

- Formal approval (v5.1):  
  `Asset Status = Approved For Explorer Use`  
  `Explorer Use Permission = Approved For Explorer`  
  `Usage Review Status = Usage Review Complete|Reviewed`  
  Review notes include v5 human-approval stamp
- Attachment exists or Local File Path exists
- Source URL exists
- Recommended Explorer Slot exists
- Visual Slot Validation Status is not disallowed (`Mock/Demo Guard`, `Provenance Only`, `Do Not Use`, `Not Enough Context`)
- Not Recent Openings, PR placeholder, or source-reference slot

## Slot promotion rules

- **Logo:** from approved logo asset only; no overwrite of populated non-mock logo without `--allow-logo-overwrite`.
- **Hero:** from approved hero asset only; populated non-mock hero requires `--allow-nonblank-hero-overwrite`.
- **Gallery:** promote only approved `materials.gallery.1`, `.2`, `.4`, `.5`, `.6`; do not fill `.3` with unapproved assets.
- **Value Drivers:** promote only approved Resort/Urban visuals into scenario image slots; leave Conversion/Boutique/Mixed-Use unchanged until approved.
- **Recent Openings / PR / FDD:** never promoted by v7.

## Image patch mode

When existing presentation slot rows are present but their `Image` attachments are blank, use:

```bash
npm run explorer-media-promotion-writer -- --brand tribute-portfolio \
  --apply --approve-explorer-media-promotion --allow-presentation-slot-image-patch
```

In this mode, v7 updates only the presentation slot image field (no logo writes, no text/body/title edits).

## Report outputs

- `reports/explorer-media-promotion-writer.md`
- `reports/explorer-media-promotion-writer.json`

Report includes:

- Current Brand Setup media state
- Current presentation slot media state
- Approved + eligible/ineligible registry assets
- Exact proposed Airtable field/record updates
- Overwrite risks and apply blockers
- Exact apply command

## Does not do

- Auto-approve assets
- Download assets (handled by v6)
- Touch non-approved assets
- Write Company Validated / Company Validation Date
- Modify non-media text/content/scoring/governance fields
- Change Airtable schema

