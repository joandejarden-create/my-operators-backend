# Brand Explorer Radisson Individuals Image / Asset / Openings Root-Cause Audit v31I

- Generated: 2026-07-10T18:49:46.342Z
- Brand: **Radisson Individuals by Choice**
- v31I exists: **yes**
- Mode: **dry-run** (audit only — no Airtable writes)
- Company Validated untouched: **yes**
- Airtable modified: **no**

## Files read
- AGENTS.md
- reports/brand-explorer-choice-expansion-partial-profile-backfill-writer.md
- reports/brand-explorer-radisson-individuals-final-qa-reconciliation-writer.md
- reports/brand-explorer-brand-asset-registry-discovery-writer.md
- reports/brand-explorer-radisson-individuals-openings-suppression-writer.md
- reports/brand-explorer-radisson-individuals-final-visible-ui-cleanup-writer.md
- reports/brand-explorer-radisson-individuals-asset-registry-normalization-writer.md
- reports/brand-explorer-radisson-individuals-gallery-restore-writer.md
- reports/brand-explorer-radisson-individuals-approved-asset-materialization-writer.md
- reports/brand-explorer-radisson-individuals-momentum-editorial-repair-writer.md
- reports/brand-explorer-choice-expansion-partial-profile-backfill-writer.json
- reports/brand-explorer-radisson-individuals-final-qa-reconciliation-writer.json
- reports/brand-explorer-brand-asset-registry-discovery-writer.json
- reports/brand-explorer-radisson-individuals-openings-suppression-writer.json
- reports/brand-explorer-radisson-individuals-final-visible-ui-cleanup-writer.json
- reports/brand-explorer-radisson-individuals-asset-registry-normalization-writer.json
- reports/brand-explorer-radisson-individuals-gallery-restore-writer.json
- reports/brand-explorer-radisson-individuals-approved-asset-materialization-writer.json
- reports/brand-explorer-radisson-individuals-momentum-editorial-repair-writer.json
- live Radisson Individuals Brand Explorer Presentation rows
- live Radisson Individuals Brand Asset Registry rows
- live Radisson Individuals Source Library records
- live Radisson Individuals Partner Facts
- live Radisson Individuals API response
- live Tribute Portfolio Brand Asset Registry rows
- live Tribute Portfolio presentation rows
- api/brand-library.js
- public/js/brand-explorer-atelier-from-api.js
- public/js/brand-explorer-gold-detail.js
- docs/brand-explorer-presentation-slots.md
- lib/partner-intelligence/brand-asset-registry-workflow.js
- lib/partner-intelligence/brand-explorer-brand-asset-image-governance.js
- lib/partner-intelligence/brand-explorer-openings-ui-quarantine-governance.js

## Prior writer reports present
- v31a: yes
- v31recon: yes
- v31b: yes
- v31c: yes
- v31d: yes
- v31g: yes
- v31dr1: yes
- v31e: yes
- v31f: yes

## 1. Image restoration audit
- Visual rows audited: **17**
- Cleared by v31D: **6**
- Restored by v31D-R1: **6**
- Gallery with images now: **0**
- Non-openings still empty after v31D: **0**

### Notable rows
- `materials.gallery.2` Hotel Casa Don Luis by Faranda Boutique,a member o — cleared v31D: true, restored: true, has image: false, rec: **restored_pending_review**
- `materials.gallery.4` Faranda Collection Bogota, a member of Radisson In — cleared v31D: true, restored: true, has image: false, rec: **restored_pending_review**
- `materials.gallery.1` Hotel Bambito By Faranda Boutique, a member of Rad — cleared v31D: true, restored: true, has image: false, rec: **restored_pending_review**
- `materials.gallery.3` Hotel Faranda Guayacanes, a member of Radisson Ind — cleared v31D: true, restored: true, has image: false, rec: **restored_pending_review**
- `materials.gallery.6` Hotel Faranda Bolivar Cucuta, a member of Radisson — cleared v31D: true, restored: true, has image: false, rec: **restored_pending_review**
- `materials.gallery.5` Hotel Casa La Factoria by Faranda Boutique, a memb — cleared v31D: true, restored: true, has image: false, rec: **restored_pending_review**

## 2. Registry completeness vs Tribute
- Radisson registry rows: **17**
- Tribute registry rows: **27**

### Fields populated in Tribute but mostly blank in Radisson
- **Explorer Section** — Tribute 100% vs Radisson 35% (v31E: optional)
- **Visual Slot Validation Notes** — Tribute 100% vs Radisson 0% (v31E: optional)
- **Slot Purpose** — Tribute 100% vs Radisson 35% (v31E: optional)
- **CALA Relevant?** — Tribute 100% vs Radisson 35% (v31E: optional)
- **Brand Confirmed?** — Tribute 100% vs Radisson 35% (v31E: optional)
- **Hotel / Property Confirmed?** — Tribute 100% vs Radisson 18% (v31E: optional)

## 3. Source URL expiration
- temporary_attachment_url: **6**
- durable_source_page_url: **0**
- source_reference_only: **6**
- founder-approved not materializable: **6**

## 4. Duplicate registry
- Duplicate groups: **1**
- true_duplicate_same_asset: 1
- gallery_restore_duplicate: 0
- do_not_use_guard_duplicate: 0
- placeholder_needs_image: 0
- source_reference_vs_image_asset: 0

## 5. Openings root cause
- Rows audited: **15**
- `footprint.openings` Radisson Individual — Barranquilla, Colo — keep_quarantined (quarantined: true)
- `valueOwners.scenario.1` Boutique Independent Conversion — rebuild_from_official_source (quarantined: false)
- `footprint.openings` Radisson Individuals — Cartagena, Colomb — keep_quarantined (quarantined: true)
- `footprint.openings` Radisson Individuals — Panama City, Pana — eligible_for_reactivation_after_copy_image_repair (quarantined: true)
- `footprint.openings` Radisson Individual — Panama, Panama — keep_quarantined (quarantined: true)
- `footprint.openings` Radisson Individuals — Medellín, Colombi — eligible_for_reactivation_after_copy_image_repair (quarantined: true)
- `overview.scenario.2` CALA Hand-Selected Growth — pending_image_review (quarantined: true)
- `footprint.openings` Radisson Individual — Cucuta, Colombia — keep_quarantined (quarantined: true)
- `overview.scenario.1` Boutique Independent Conversion — pending_image_review (quarantined: true)
- `footprint.openings` Radisson Individual — Cali, Colombia — keep_quarantined (quarantined: true)
- `footprint.openings` Radisson Individual — Bogota, Colombia — keep_quarantined (quarantined: true)
- `overview.scenario.3` Preserve Uniqueness + Choice Scale — pending_image_review (quarantined: true)

## 6–7. Openings text / image quality
- Text issue rows: **6**
- Image issue rows: **15**

## 8. UI/API rendering
- API blocks: **0** / presentation rows: **219**
- Quarantined rows: **11**
- Quarantined leaked to API: **0**
- Visual API slots without image: **0**

## 9. Root-cause map

### writersThatRemovedCorrectImages
- **v31D**: clear_unapproved_image on all visible visual slots including materials.gallery.*
### writersThatCreatedIncompleteRegistry
- **v31B**: Initial discovery staged registry without Source Page URL on many rows
- **v31D-R1**: Created 6 gallery registry rows with Airtable attachment URLs as Source URL
- **v31G**: Normalized metadata but did not backfill Source Page URL or attachments
- **whySourceUrlsExpire**: Source URL field stores v5.airtableusercontent.com signed attachment URLs from presentation Image fields — these expire. Source Page URL (durable choicehotels/radisson page) was left blank.
- **whyDuplicatesExist**: Separate writer batches (v31B discovery, v31D-R1 gallery restore, v31G normalization) created rows per slot without deduping against existing Do Not Use guards and press-kit references.
### whyV31eCannotMaterialize
- 6 founder-approved rows are source_reference_only (no image URL)
- 12 approved rows missing v31E-required fields (Source URL / attachment)
- Opening placeholders approved without property image files attached
- **whyOpeningsNeedsRebuild**: 11+ footprint.openings rows quarantined (v31C); wrong-brand images on Do Not Use registry rows; internal/census language in copy; no durable official source linkage
### dataVsCodeVsUi
- dataIssues: Missing Source Page URL on registry rows; Temporary attachment URLs used as Source URL; Approved opening placeholders without image files
- codeIssues: v31D originally cleared all unapproved images (fixed to wrong-brand/Do Not Use only); v31E requires sourceUrl on approved asset — no fallback to presentation attachment
- uiIssues: Gallery renders empty shell cards when image cleared; Pending gallery images now visible in draft (intentional post v31D-R1)

## 10. Recommended repair sequence

### A. Restore/preserve non-Openings images that were working
- Writer: `v31I follow-up: non-openings image preservation writer (or extend v31D-R1 pattern)`
- Action: Audit shows gallery restored; verify overview.scenario / hero / valueOwners slots

### B. Normalize Brand Asset Registry schema and source URLs
- Writer: `v31J registry source-url repair writer (proposed)`
- Action: Backfill Source Page URL with durable choicehotels/radisson pages; move attachment URLs to Image/Attachment only
- Blocked by: 6 rows with temporary_attachment_url pattern

### C. Deduplicate / canonicalize registry records
- Writer: `v31K registry dedupe planner (proposed)`
- Action: Mark canonical per slot; supersede gallery_restore_duplicate and press-kit duplicates logically
- Blocked by: 1 duplicate groups detected

### D. Patch UI to avoid blank image shells
- Writer: `Frontend: brand-explorer-atelier-from-api.js gallery/openings card rendering`
- Action: Hide empty visual shells in owner-facing mode; keep admin hint in draft only

### E. Rebuild Openings / Examples from official sources
- Writer: `v31L openings rebuild writer (proposed)`
- Action: Official announcement copy, durable source pages, approved property images only
- Blocked by: 6 rows quarantined

### F. Materialize approved assets
- Writer: `v31E (re-run after B/C/E)`
- Action: Requires Source URL + Source Page URL on approved opening assets

### G. Final QA / complete-build
- Writer: `brand-explorer-final-qa-auditor + brand-explorer-complete-build`
- Action: Target active-profile readiness after above phases

## Current readiness
- Final QA: **80** (almost_ready)
- Active-profile ready: **no**
