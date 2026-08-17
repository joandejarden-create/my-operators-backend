# Brand Explorer — Radisson Individuals Restoration

**Canonical:** `radisson-individuals-by-choice` / `recRyvM8OmLlDj9G7`  
**Goal:** Restore `active_profile_ready` by fixing visual + field-gate defects only.  
**Reports:** `reports/brand-explorer-radisson-individuals-restoration.json` · `.md`

## Verdict

Radisson Individuals by Choice is **active_profile_ready → no_action** after visual uniqueness/role-match remediation and field-gate Presentation (plus one Brand Basics segment adjacency) remediation.

Sibling Radisson brands were not written. Company Validated, Source Library status, Registry approval, Founder Visual Review Pass, and Active Profile Approved were not written (Founder / Active were already true).

## What was wrong

| Gate | Before | Root cause |
| --- | --- | --- |
| Property uniqueness | `propertyExampleDistinctCount=2` | Bogotá + Cúcuta shared placeholder; Barranquilla reused a gallery asset |
| Role-match | Hard fails on empty openings | Empty/press-kit openings still in factory context |
| Completeness / golden | 39 thin fields → then residual | Under-depth Presentation copy; golden `generic_audience_prose` from Target Guest Segments adjacency in HTML |
| Forbidden URLs | Quarantined openings still scanned | OS presentation corpus previously included Do Not Display rows |

## What we changed

### Visual (Presentation Image + External Display Status)

- Assigned three distinct Faranda OG property images to live openings: Barranquilla, Bogotá, Cúcuta.
- Quarantined empty openings (Medellín, Cartagena, Panama City, Panama corridor, Cali) to **Do Not Display**.
- Stripped raw URLs from quarantined opening bodies (defense in depth).

### Field-gate content (Presentation Body / Title / Case Summary*)

- Deepened Overview scenarios, proofs, why-value, featured application, commercial differentiators, portfolio context, relative positioning.
- Deepened lifecycle phases, ops model, compliance, opening path, portfolio mix chips (extra mix stubs quarantined).

### Brand Basics (narrow golden fix only)

- `Target Guest Segments`: removed `Luxury / Discerning` so rendered Audience no longer matches golden `generic_audience_prose` (`Luxury / Discerning, Leisure…`).
- Left as `Experience-Oriented`, `Leisure`.

### Code gate hygiene

- `presentationCorpus` in OS gate evaluator now skips Do Not Display / Internal Only / inactive rows (matches “visible” gate naming).
- Uniqueness / role-match already skip non-visible rows.

## Tooling

```bash
npm run brand-explorer-radisson-individuals-restoration -- --dry-run
npm run brand-explorer-radisson-individuals-restoration -- --content-only --apply \
  --approve-radisson-individuals-restoration \
  --confirm-no-company-validation-changes \
  --confirm-no-source-library-status-changes \
  --confirm-no-registry-approval-changes \
  --confirm-no-sibling-radisson-writes \
  --confirm-canonical-record-recRyvM8OmLlDj9G7 \
  --confirm-property-distinct-three \
  --confirm-image-role-match-pass \
  --confirm-field-gate-content-remediation \
  --confirm-no-owner-narrative-beyond-failed-fields
```

## Acceptance (verified)

| Check | Result |
| --- | --- |
| galleryDistinct ≥ 6 | pass |
| scenarioDistinct ≥ 3 | pass |
| propertyExampleDistinct ≥ 3 | pass |
| imageRoleMatchPass | pass |
| rendered field completeness | pass (`failFindings=0`) |
| no empty rendered components | pass |
| source provenance | pass (via tab-factory / OS) |
| OS | **active_profile_ready → no_action** |

## Data contract snapshot

- **Tables:** Brand Explorer Presentation; Brand Basics (Target Guest Segments only).
- **Mapping:** patches use explicit `fieldMapping` to Presentation / Basics field names.
- **Required for live property examples:** Image URL on three visible `footprint.openings` rows.
- **Forbidden writes:** Company Validated, Company Validation Date, Source Library status, Registry approval, sibling Radisson records, Founder/Active release flags.

## Change impact

**High** (Airtable Presentation + one Basics select; OS corpus visibility filter).

**Rollback:** Revert Presentation Image / External Display Status / Body patches for Radisson Individuals; restore prior Target Guest Segments; revert `presentationCorpus` visibility filter if needed.

## Regression checklist

- Retest Radisson Individuals founder preview + public lock behavior.
- Confirm Radisson Blu / RED / Collection / Radisson by Choice unchanged.
- Re-run uniqueness, role-match, tab-factory, OS release-readiness for this slug only.
- Confirm no raw URLs in owner-facing Presentation corpus.
