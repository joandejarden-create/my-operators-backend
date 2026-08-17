# Brand Explorer Radisson Individuals Durable Gallery Source + Registry Repair v31J
- Generated: 2026-07-10T20:43:11.394Z
- Brand: **Radisson Individuals by Choice**
- v31J exists: **yes**
- Mode: **dry-run**
- Company Validated untouched: **yes**
- Airtable modified: **no**
## Gallery image diagnosis
- Slots audited: **6**
- Should restore: **6**
- Expired/temporary image URLs: **6**
- `materials.gallery.1` Hotel Bambito By Faranda Boutique, a member of Rad — image: attached, url: temporary_expired, restore: yes
- `materials.gallery.2` Hotel Casa Don Luis by Faranda Boutique,a member o — image: attached, url: temporary_expired, restore: yes
- `materials.gallery.3` Hotel Faranda Guayacanes, a member of Radisson Ind — image: attached, url: temporary_expired, restore: yes
- `materials.gallery.4` Faranda Collection Bogota, a member of Radisson In — image: attached, url: temporary_expired, restore: yes
- `materials.gallery.5` Hotel Casa La Factoria by Faranda Boutique, a memb — image: attached, url: temporary_expired, restore: yes
- `materials.gallery.6` Hotel Faranda Bolivar Cucuta, a member of Radisson — image: attached, url: temporary_expired, restore: yes
## Durable source page findings
- Resolved: **6** / 6
- `materials.gallery.1` Hotel Bambito By Faranda Boutique — https://www.choicehotels.com/colon/cerro-punta/radisson-individuals-hotels/pn007 — image: resolved
- `materials.gallery.2` Hotel Casa Don Luis by Faranda Boutique — https://www.choicehotels.com/colombia/cartagena/radisson-individuals-hotels/cb017 — image: resolved
- `materials.gallery.3` Hotel Faranda Guayacanes — https://www.choicehotels.com/herrera/chitre/radisson-individuals-hotels/pn009 — image: resolved
- `materials.gallery.4` Faranda Collection Bogota — https://www.choicehotels.com/colombia/bogota/radisson-individuals-hotels/cb012 — image: resolved
- `materials.gallery.5` Hotel Casa La Factoria by Faranda Boutique — https://www.choicehotels.com/colombia/cartagena/radisson-individuals-hotels/cb018 — image: resolved
- `materials.gallery.6` Hotel Faranda Bolivar Cucuta — https://www.choicehotels.com/colombia/cucuta/radisson-individuals-hotels/cb010 — image: resolved
## Registry field repair plan
- Assets to update: **6**
- `materials.gallery.1` rec0tjE3JvH7pyP5J — missing before: Explorer Section, Related Property Name, Country / Region, Visual Slot Validation Status — reset approval: no
- `materials.gallery.2` recXlmzJTtHbKIxJs — missing before: Explorer Section, Related Property Name, Country / Region, Visual Slot Validation Status — reset approval: no
- `materials.gallery.3` recm6isRdCaotG3aU — missing before: Explorer Section, Related Property Name, Country / Region, Visual Slot Validation Status — reset approval: no
- `materials.gallery.4` rec3fCxZEdL7lwR6e — missing before: Explorer Section, Related Property Name, Country / Region, Visual Slot Validation Status — reset approval: no
- `materials.gallery.5` recLVkWOnyPszJYGt — missing before: Explorer Section, Related Property Name, Country / Region, Visual Slot Validation Status — reset approval: no
- `materials.gallery.6` recdxfVt2sISpG95w — missing before: Explorer Section, Related Property Name, Country / Region, Visual Slot Validation Status — reset approval: no
## Apply summary
- Presentation rows to update: **6**
- Registry assets to update: **6**
- Images approved: **no**
- Images restored/materialized: **no (dry-run or blocked)**
- Dry-run clean: **yes**
## Expected UI result
Materials gallery shows up to 6 property images sourced from durable Choice pages; draft/internal profile renders gallery cards with pending-review posture.
## Expected active-profile result
- Ready: **no**
- Note: Gallery images remain pending review after v31J — active-profile blocked until founder approves registry assets and v31E materializes.
## Current readiness
- Final QA: **—** (—)
- Active-profile ready: **no**
## Exact apply command
```bash
npm run brand-explorer-radisson-individuals-durable-gallery-source-repair-writer -- --brand radisson-individuals-by-choice --apply --approve-brand-explorer-v31J-durable-gallery-source-repair --restore-gallery-images-from-durable-sources --confirm-no-image-approval --confirm-no-company-validation-claim
```
