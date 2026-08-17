# Brand Explorer Scenario Display Parity Audit v31H

- Generated: 2026-07-10T21:16:51.276Z
- Slot: **overview.scenario.1**
- Left: **Radisson by Choice** (`radisson`)
- Right: **Radisson Individuals by Choice** (`radisson-individuals-by-choice`)
- v31H exists: **yes**
- Mode: **dry-run** (audit only)
- Company Validated untouched: **yes**
- Airtable modified: **no**

## 1. Airtable row comparison

### Radisson by Choice
- Record: `recA5WPVy4xTIAAAS`
- Title: Gateway & Airport-Adjacent Repositioning
- Image: attached
- External Display Status: —
- Quarantined: false
- Registry (slot): 0 asset(s)
- Governance: **pending_image_review**
### Radisson Individuals by Choice
- Record: `recpe1vIxIsaKq1XX`
- Title: Boutique Independent Conversion
- Image: attached
- External Display Status: Do Not Display
- Quarantined: true
- Registry (slot): 0 asset(s)
- Governance: **pending_image_review**

## 2. API comparison

- Left in blocks: **true** (imageUrl: true)
- Right in blocks: **false** (imageUrl: false)

## 3. Frontend rendering

- Left renders image: **true**
- Right renders image: **false**
- Right blank placeholder: **true**
- Same component: **yes** (atelier scenario-card--visual)

## 4. Root cause

- **data**: Image attachment / imageUrl differs between brands
- **data**: External Display Status differs — API filter may exclude one brand
- **api**: Slot present in API blocks for one brand only
- **api**: API imageUrl differs — mirrors Airtable attachment state
- **data**: Frontend uses identical scenario-card logic; blank placeholder appears when imageUrl is empty — not a brand-specific render branch
- **image_governance**: Radisson Individuals is expansion_backlog — stricter image governance applies; Radisson by Choice uses active_registry path

## 5. Recommended fix

- [P1] restore_or_assign_scenario_image: Radisson Individuals overview.scenario.1 needs an approved scenario image (registry row + presentation attachment) to match Radisson display parity — or explicitly accept text-only card until founder approves image.
- [P2] review_quarantine_status: If overview.scenario.1 is quarantined (Do Not Display), it will not appear in API blocks — verify External Display Status is intentional.
- [P3] optional_ui_text_only_mode: Optional frontend patch: hide scenario-card__visual--empty shell when imageUrl missing and card is text-only eligible — applies to both brands uniformly (not Radisson-specific logic).
- [P2] registry_then_approval: Create/link Brand Asset Registry asset for overview.scenario.1, then founder-approve before v31E materialization — expansion brands require approved registry for active-profile evidence.
