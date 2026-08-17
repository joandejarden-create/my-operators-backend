# Partner Reference Material — Collection Guide

**Archive root:** `G:\My Drive\Dealality™\Platform Design & Build\Brand Reference Material\`  
**Env:** `PARTNER_REFERENCE_ROOT` (same path)  
**Airtable:** Partner Intelligence - Source Library → **Local File Path** = relative path under root

Dealality does **not** auto-scrape. You build a **disciplined collection** from official portals, targeted searches, FDD libraries, regional sites, and (when needed) direct development contacts.

---

## Workflow (recommended)

```text
1. INIT folder     npm run partner-reference:init-folder -- --company "IHG Hotels & Resorts" --apply
2. DISCOVER        npm run partner-reference:search -- --brand "Kimpton" --parent ihg
3. DOWNLOAD        npm run partner-reference:download -- --url "https://…" --company "IHG Hotels & Resorts" --brand "Kimpton" --type development-brochure --title "Kimpton development brochure" --apply --register
4. REVIEW in Airtable Source Library (Status: Captured → Approved for Extraction when ready)
5. EXTRACT         Partner Intelligence review UI → Run extraction
6. APPROVE + PUBLISH human review only — never auto-publish
```

---

## Folder layout (per company)

```
{Company Name}/
  README.md                 ← auto-generated capture notes
  PORTAL-LINKS.md           ← when parent is in development portal registry
  _capture-log.json         ← machine log of CLI downloads
  development/              ← brochures, one-sheets, brand essence
  fdd/                      ← U.S. FDD PDFs
  regional/                 ← CALA, EMEA, Mexico, Australia materials
  press/                    ← press / media (supporting only)
  operator-materials/       ← mgmt decks, case studies
  brands/{Brand Name}/      ← optional brand-specific subfolder (--brand flag)
  inbox/                    ← uncategorized until sorted
```

**Existing examples:**

- `Choice Hotels International/` — FDD inventory (see `docs/choice-fdd-inventory.md`)
- `Arbor Lodging/` — operator decks (English/Spanish overview, regional experience)

---

## Source priority (use in this order)

### 1. Official hotel development websites (primary)

| Parent | Portal |
|--------|--------|
| Marriott | https://hotel-development.marriott.com/ |
| Hilton | https://www.hilton.com/en/corporate/development/ |
| IHG | https://development.ihg.com/ |
| Hyatt | https://www.hyatt.com/development |
| Choice | https://www.choicehotels.com/development |
| Wyndham | https://development.wyndhamhotels.com/ |
| Accor | https://group.accor.com/en/hotel-development |
| BWH | https://www.bwhhotels.com/development |
| Radisson | https://www.radissonhotels.com/en-us/brand-partnership/hotel-development |

Full registry + search patterns: `api/lib/partner-development-portal-registry.js`

### 2. Brand-specific PDF search (Google)

Many brochures are indexed but not linked in nav. CLI:

```bash
npm run partner-reference:search -- --brand "Sofitel" --parent accor
```

Patterns (also in `docs/partner-source-discovery-patterns.md`):

- `site:hotel-development.marriott.com ResourceFiles [brand] pdf`
- `site:development.ihg.com [brand] development brochure pdf`
- `site:development.wyndhamhotels.com [brand] one sheet pdf`
- `site:assets.group.accor.com [brand] Development Brochure pdf`
- `"[brand]" "development brochure" hotel`

### 3. FDD sources (U.S. franchise brands)

- Marriott development site indexes FDD PDFs (e.g. AC Hotels, Residence Inn).
- **Choice:** local folder + `scripts/extract-choice-fdd-item19.mjs` pipeline.

**FDD is essential for:** fees, royalties, marketing/tech fees, training, transfer, termination — **not** a substitute for positioning brochures.

Default `--type fdd` → `fdd/` subfolder, Source Quality **High** (still Verified = No until human review).

### 4. Regional franchise / development sites

CALA / Mexico / EMEA materials may live off the global parent site:

- Choice Australia — brand brochure downloads
- Choice Mexico — franchise brochure links
- Wyndham EMEA development downloads

Save under `regional/` or `brands/{Brand}/`.

### 5. Press / media / investor (supporting only)

Use for positioning momentum — **not** primary Brand Explorer sources. Save under `press/`.

### 6. Direct from brand development teams (best gated materials)

Some decks, standards, and prototypes are inquiry-only. IHG explicitly routes FDD/additional docs through local developers.

**Helena outreach template** (copy from registry `HELENA_OUTREACH_TEMPLATE`):

> We are building a structured brand intelligence profile for hotel owners and developers evaluating brand fit. Could you please share the latest owner/developer-facing materials for **[Brand]**, including development brochure, prototype overview, conversion guidance, support model, regional development priorities, and current FDD where applicable?

Log responses in **Partner Intelligence - Helena Outreach Intake**.

---

## CLI reference

### Initialize folder structure

```bash
npm run partner-reference:init-folder -- --company "Wyndham Hotels & Resorts" --apply
```

### Search query URLs

```bash
npm run partner-reference:search -- --brand "Moxy" --parent marriott
npm run partner-reference:search -- --operator "Hotel Equities" --domain hotelequities.com
```

### Download + register

```bash
npm run partner-reference:download -- \
  --url "https://example.com/brochure.pdf" \
  --company "Accor" \
  --brand "Sofitel" \
  --type development-brochure \
  --title "Sofitel development brochure" \
  --apply \
  --register \
  --brand-id recXXXXXXXX
```

**`--type` values:** `development-brochure`, `one-sheet`, `prototype`, `fdd`, `press`, `media-kit`, `regional`, `operator-deck`, `case-study`, `other`

---

## Source Library defaults on capture

| Material | Status | Quality | Verified? | Approved for Extraction |
|----------|--------|---------|-----------|-------------------------|
| Official dev PDF | Captured | Medium | No | No (until you approve) |
| FDD (manual verify) | Captured | High | No → Yes after review | No until ready |
| Press / trade | Captured | Low/Medium | No | No |
| Operator deck (provided) | Captured | Medium | No | No |

---

## Rules (non-negotiable)

- Do **not** bypass gated logins or paywalls.
- Do **not** use confidential materials without written permission.
- Do **not** treat third-party hosts as verified.
- Do **not** auto-publish extracted facts to Brand/Operator Explorer.
- Always store **relative path** in Source Library **Local File Path**.
- Prefer **owner/developer-facing** PDFs over consumer marketing.

---

## Operators vs brands

| Profile | Folder name | Typical contents |
|---------|-------------|------------------|
| **Brand** | Parent company (`Marriott International`) or `brands/{Brand}` | FDD, brochures, one-sheets |
| **Operator** | Operator legal name (`Arbor Lodging`, `Hotel Equities`) | Capability decks, regional experience, case studies |

Use `--profile-type Operator` and `--operator-id rec…` when registering operator sources.

---

## Related docs

- `docs/partner-source-discovery-patterns.md` — search pattern catalog
- `docs/partner-source-library-airtable-fields.md` — Source Library schema
- `docs/choice-fdd-inventory.md` — Choice FDD on-disk inventory
- `docs/partner-intelligence-pilot-arbor-lodging.md` — operator pilot example
