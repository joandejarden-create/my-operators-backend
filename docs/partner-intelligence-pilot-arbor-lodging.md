# Partner Intelligence — Pilot: Arbor Lodging (Operator)

**Profile type:** Operator  
**Company:** Arbor Lodging (CALA)  
**Master record:** `recF5Z87OAqFgndoq`  
**Reference folder:** `G:\My Drive\Dealality™\Platform Design & Build\Brand Reference Material\Arbor Lodging\`

## Explorer entry points

| Surface | URL |
|---------|-----|
| Operator Explorer (11 tabs) | `/operator-explorer-gold-mock.html?id=recF5Z87OAqFgndoq` |
| Operator list | `/operator-explorer` → click Arbor Lodging tile |
| Operator Setup | `/third-party-operator-setup-new-two.html?recordId=recF5Z87OAqFgndoq` |

## Phase 2 artifacts

| File | Purpose |
|------|---------|
| `scripts/ensure-partner-intelligence-tables.mjs` | Create 4 Airtable tables |
| `api/lib/partner-intelligence-explorer-field-registry.js` | Field catalog + pilot constants |
| `api/lib/partner-intelligence-field-map.js` | Airtable column names |

## Create tables in Airtable

**Recommended:** use the ensure script (validates field names against code maps).

```bash
# Preview
npm run ensure-partner-intelligence-tables

# Create tables + fields
npm run ensure-partner-intelligence-tables -- --apply
```

Report written to `reports/ensure-partner-intelligence-tables.json`.

**Alternative:** create tables manually using:

- `docs/partner-source-library-airtable-fields.md`
- `docs/partner-extracted-facts-airtable-fields.md`
- `docs/partner-explorer-published-fields-airtable-fields.md`
- `docs/partner-helena-intake-airtable-fields.md`

Manual creation is fine if you prefer UI review first; keep **exact table and field names** matching the docs.

## Suggested first sources (capture only — Phase 8)

Public URLs from existing Arbor research (`reports/arbor-cala-explorer-seed.md`):

1. https://www.arborlodging.com/platforms — Website Capture, Medium
2. https://www.hotelinvestmenttoday.com/Development/Owners/What-makes-sense-today-for-Arbor-Lodging — trade pub, Medium/Low
3. https://www.arborlodging.com/press — Press Release, Medium

Save downloaded PDFs/decks under the **Arbor Lodging** reference folder when received.

## Phase 3 — Source Library API

All routes require Memberstack auth + Dealality user + admin role (`isAdmin` or `PARTNER_INTELLIGENCE_ADMIN_ROLES`).

| Method | Route | Purpose |
|--------|-------|---------|
| GET | `/api/partner-intelligence/pilot` | Pilot operator + suggested public sources |
| GET | `/api/partner-intelligence/sources?operatorId=recF5Z87OAqFgndoq` | List sources |
| GET | `/api/partner-intelligence/sources/:recordId` | Source detail |
| POST | `/api/partner-intelligence/sources` | Create source (JSON body) |
| PATCH | `/api/partner-intelligence/sources/:recordId` | Update status, quality, approvals |
| POST | `/api/partner-intelligence/sources/:recordId/upload` | Multipart `file` → reference folder |

### Seed public URL sources (already run)

```bash
npm run seed-arbor-pilot-sources -- --apply
```

Creates 3 **Found** rows linked to `recF5Z87OAqFgndoq` (see `reports/seed-arbor-pilot-sources.json`).

### Example: create source

```json
POST /api/partner-intelligence/sources
{
  "profileType": "Operator",
  "operatorId": "recF5Z87OAqFgndoq",
  "sourceTitle": "Arbor owner presentation (received)",
  "sourceType": "Owner Presentation",
  "sourceOrigin": "Operator Provided",
  "visibility": "Private",
  "sourceQuality": "Medium",
  "status": "Captured"
}
```

### Example: upload file

```bash
POST /api/partner-intelligence/sources/recXXX/upload
Content-Type: multipart/form-data
file=@brochure.pdf
```

Files save under `PARTNER_REFERENCE_ROOT/Arbor Lodging/` (or `data/partner-sources/Arbor Lodging/` if Drive path unavailable).

## Phase 4 next

1. Extraction run → Extracted Facts (Pending)
2. Admin review UI
3. Publish one approved field to Operator Explorer (e.g. `op.snapshot.companyDescription`)

## Out of scope for publish merge

- **Dealality Insights** tab (tab 11) — Dealality-derived
- **Alignment Context** tab — OAS deal scoring

## Known data caveat

Current Explorer content for Arbor includes **research-based seed copy** (486 inventory fields). Partner Intelligence publish workflow will **not overwrite** Setup fields until an approved fact is explicitly published. Treat existing seed as **unverified** until source-backed facts are approved.
