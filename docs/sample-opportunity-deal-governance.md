# Sample opportunity & sample deal governance

Dealality uses **real hotels only as public reference comps**. Each **sample opportunity** is a **fictional Dealality deal** inspired by that property type—not a claim about the real hotel.

---

## Two layers (required)

### 1. Reference property layer

Use **public, verifiable facts** about a real hotel (or anonymized comp label + public facts) for:

| Category | Example intake fields |
|----------|------------------------|
| Location | City, submarket, country, address *if public* |
| Room count | Total keys, suite mix |
| Amenities | Pool, fitness, spa, parking |
| F&B | Outlet count, program type, concepts *as publicly described* |
| Meeting space | SF / sqm, ballroom count *if public* |
| Design / story | Architecture era, renovation history *if public* |
| Service model | Full-service, select-service, extended-stay, etc. |
| Brand positioning | Chain scale, flag, parent company *as operating today* |
| Market context | Submarket descriptors, demand drivers *from public sources* |
| Physical features | Stories, building type, site constraints *if public* |

**Reference layer rules**

- Cite or note the public source in `referenceProperty.sources[]` when possible (brand site, STR directory page, municipal filing, etc.).
- Use `referenceProperty.displayLabel` such as *“Public reference comp — upscale airport suburban select-service, Amsterdam Schiphol corridor”* when the UI should not show the trademarked hotel name.
- Optional `referenceProperty.publicName` only when needed for internal authoring; default user-facing copy should not imply the real hotel is the deal.

### 2. Fictional Dealality sample deal layer

Create a **fictional** project inspired by the reference type:

| Category | Example intake fields |
|----------|------------------------|
| Owner strategy | Hold period, goals, flexibility vs prestige |
| Deal structure | Franchise, mgmt, lease, JV |
| CapEx / PIP | Budget ranges, timeline |
| Operator need | Self-manage vs third party, operator criteria |
| Brand preferences | Preferred brands, chain scales, soft vs hard |
| Timeline | Decision date, proposal deadline, opening target |
| Deal breakers | Top 3 breakers |
| Must-haves | Brand/operator requirements |
| Contacts | **Fictional** names, entities, emails (`@example.com`, `@dealality.sample`) |
| Financial assumptions | IRR targets, equity/debt split, total project cost |

**Fictional layer rules**

- `fictionalDeal.projectName` / `Property Name` must be **clearly fictional** (e.g. *Alcove Gloria*, *Harborline 42*, *Vista Norte Sample Resort*).
- Never use the reference hotel’s exact name as the project name unless explicitly marked internal-only and never shown in product UI.
- Owner, advisor, and broker names must be fictional unless a **public** executive is cited solely as market context (rare; avoid in samples).

---

## Prohibited implications (never state or imply)

For any **real** reference hotel, do **not** suggest or imply it is:

- For sale  
- Distressed  
- Seeking a brand  
- Seeking an operator  
- Participating in Dealality  
- Available for conversion  
- Available for acquisition  

**Required disclaimer** (UI, briefs, radar cards, seed README):

> *Sample deal for product demonstration only. Reference property is a public comp for factual context; it is not offered for sale and is not participating in Dealality.*

---

## Field source types (per field)

Tag every populated field with exactly one `sourceType`:

| `sourceType` | When to use |
|--------------|-------------|
| `public_reference` | Copied or closely paraphrased from a named public source |
| `inferred_from_reference` | Reasonable derivative (e.g. suite % from total keys + public “all suites” claim) |
| `fictional_sample_assumption` | Invented for the sample deal narrative (owner, economics, preferences, timeline) |
| `needs_validation` | Placeholder or uncertain; must be resolved before production use |

**Shape** (see `fixtures/sample-deals/*.example.json`):

```json
"fieldSources": {
  "Total Number of Rooms/Keys": {
    "sourceType": "public_reference",
    "layer": "reference_property",
    "note": "Brand fact sheet, 2024"
  },
  "Preferred Brands (up to 4)": {
    "sourceType": "fictional_sample_assumption",
    "layer": "fictional_deal",
    "note": "Illustrative owner preference for demo"
  }
}
```

---

## Mapping: Deal Setup / Airtable fields → layer

| Layer | Deal Setup fields (non-exhaustive) |
|-------|-----------------------------------|
| **Reference** | Property Name (public comp label only), Full Address, City & State, Country, Hotel Submarket & Location, Hotel Chain Scale, Hotel Type, Hotel Service Model, Total Number of Rooms/Keys, room mix, Building Type, Number of Stories, Meeting Space*, F&B fields, Additional Amenities, Parking, Primary Demand Drivers, Key Competitors (public comps), Current Brand Affiliation, Operator Name Current (as public), Zoning / site facts if public |
| **Fictional** | Project Name, Stage of Development, Expected Opening*, Who should receive bids*, Ownership Structure, Preferred Deal Structure, PIP/CapEx*, financial ranges, brand/operator preferences, breakers, must-haves, timelines, contact fields, Company Executive Summary, broker/advisor fields, strategic intent |
| **Mixed** | “Is the hotel currently branded?” — **reference** if describing the comp’s today state; **fictional** if describing the sample owner’s repositioning plan. Split into two fields in JSON seeds when ambiguous. |

\* If the fictional deal assumes a future state, tag `fictional_sample_assumption` and document in `note`.

---

## Authoring workflow

1. Pick a **reference property** and record `referenceProperty` + public sources.  
2. Define **fictional deal** identity (name, owner entity, strategy) — no overlap with reference name in UI.  
3. Fill intake: maximize **reference** for physical/market facts; use **fictional** for economics and process.  
4. Tag each field in `fieldSources`.  
5. Run `node scripts/validate-sample-deal-fixture.mjs <path>` before import.  
6. Set `meta.isSample: true` and `meta.sampleTier: "demo" | "qa" | "sales"`.

---

## Current codebase gaps (audit)

| Location | Issue | Remediation |
|----------|--------|-------------|
| Airtable Deals (e.g. *Courtyard by Marriott Amsterdam Airport*) | Real flag names used as **project names** | Rename to fictional project; move real facts to reference block + disclaimer |
| `public/deal-summary.html` → `alcoveGloriaDemo` | Fictional name OK; lacks layer/source metadata | Migrate to `fixtures/sample-deals/alcove-gloria.example.json` |
| `api/dashboard-home.js` | Generic “Sample” labels | OK; keep non-specific |
| Opportunity Radar seeds | Not yet in repo | Use `fixtures/sample-deals/` schema when adding `data/opportunity-radar/` |

---

## Files

- Schema constants: `lib/sample-opportunity-deal-schema.js`  
- Example fixture: `fixtures/sample-deals/alcove-gloria.example.json`  
- Validator: `scripts/validate-sample-deal-fixture.mjs`
