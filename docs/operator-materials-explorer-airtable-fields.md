# Operator Materials — Airtable fields

**Canonical (Brand parity):** `Operator Setup - Explorer Materials`  
**Legacy fallback:** `Operator Setup - Governance, Delivery & Diligence` (JSON + URL lines)  
**UI:** `public/js/operator-dna-materials.js` (presentation-first, then Governance JSON)  
**Maps:** `api/lib/operator-materials-explorer-presentation-map.js`, `api/lib/operator-materials-explorer-field-map.js`

## Operator Setup - Explorer Materials

Child table linked to **Operator Setup - Master** (`Operator` link field). Same slot pattern as `Brand Setup - Brand Explorer Presentation`.

| Column | Type | Use |
|--------|------|-----|
| `Slot Key` | Single line text | `materials.file` (repeat for multiple files), `materials.gallery.1` … `materials.gallery.6` |
| `Title` | Single line text | File card title or gallery caption |
| `Body` | Long text | File description; optional `https://…` URL line; optional `Badge: …` line |
| `Image` | Attachments | PDF or image URL at create time (`[{ url: "https://…" }]`) — first URL used when Body has no link |
| `Sort Order` | Number | Order within slot |
| `Active` | Checkbox | Inactive rows omitted from API |
| `Operator` | Link → Master | Required |
| `Company Name` | Single line text | Optional fallback label |

**API shape:** `operator.operatorExplorerMaterials` → `{ version: 1, blocks: [{ recordId, slotKey, title, body, sort, imageUrl }] }`

## Legacy — Governance table

| Form / prefill key | Airtable field | Type | Use |
|--------------------|----------------|------|-----|
| `operator_materials_json` | `operator_materials_json` | Long text (JSON) | Fallback file cards + gallery |
| `operator_materials_gallery_json` | `operator_materials_gallery_json` | Long text (JSON) | Fallback gallery only |
| `diligenceDocumentLinks` | `diligenceDocumentLinks` | Long text | Fallback file URLs (one per line) |

## Schema + seed scripts

Create table (once per base):

```bash
node scripts/create-operator-setup-explorer-materials-table.mjs
```

Seed presentation rows (all operators, replaces existing rows per operator):

```bash
node scripts/seed-operator-explorer-materials-presentation.mjs --apply
node scripts/seed-operator-explorer-materials-presentation.mjs --apply --master recTUjuDxL96yWcQA
node scripts/seed-operator-explorer-materials-presentation.mjs --apply --fixture fixtures/operator-materials-presentation-antillano-norte.json
```

Legacy Governance JSON seed (still supported as fallback):

```bash
node scripts/ensure-operator-materials-explorer-schema.mjs --apply
node scripts/seed-operator-materials-explorer-data.mjs --apply --fixture fixtures/operator-materials-antillano-norte.json
```
