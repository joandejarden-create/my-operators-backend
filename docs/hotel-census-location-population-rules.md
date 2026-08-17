# Hotel Census `Location` — population rules

**Scope:** `Hotel Census` on Deal Capture Platform (`AIRTABLE_BASE_ID_ALT`).

**Field:** `Location` (multilineText)

---

## What `Location` means (important)

`Location` is **not** geographic placement. It does **not** hold city, country, address, or STR Market.

| Field | Meaning |
|-------|---------|
| `city`, `country`, `Address 1` | Where the hotel is |
| `Market`, `Submarket` | STR commercial geography |
| **`Location`** | **STR location type** — the physical *setting* of the hotel |

Brand Explorer and `aggregate-presence-summary.js` use `Location` as **`locationTypeMix`** (Urban vs Resort vs …).

---

## Allowed values (STR location type vocabulary)

Exact labels used in census today:

| Value | Share (Jun 2026) | Definition |
|-------|------------------|------------|
| **Urban** | ~44% | Major metro / city-center or primary urban market hotels |
| **Suburban** | ~22% | Metro periphery, suburban corridors, non-CBD metro |
| **Resort** | ~17% | Leisure destinations — beach, mountain resort towns, destination resorts |
| **Small Metro/Town** | ~10% | Secondary cities, smaller towns, low-density markets |
| **Airport** | ~2% | Airport-proximate / airport-capture product |
| **Interstate** | &lt;1% | Highway / roadside corridor (rare in CALA; more common in US STR) |

**Do not invent new labels** without updating `LOCATION_TYPE_LABELS` in `lib/hotel-census/location-enrichment-contract.js` and Brand Explorer consumers.

---

## How to decide the correct value (steward playbook)

Use the hotel's **actual physical context**, not brand name alone.

### Urban
- CBD, downtown, primary business district of a major city
- Examples: `Innside by Melia Lima Miraflores` (Lima), `Providencia Bed & Breakfast` (Santiago)

### Suburban
- Within a metro but outside dense urban core; office-park / residential corridors
- Examples: `Aparthotel Maria Alexandra` (San Miguel de Escazú), `Confort Ejecutivo Suites Valle` (Monterrey)

### Resort
- Leisure destination product; beach/mountain/island resort markets; all-inclusive coastal
- Strong signals: beachfront, destination island, resort town (Gramado leisure can be Resort *or* Small Metro/Town — use peer context)
- Examples: `Windsong Resort` (Providenciales), `Club Del Mar` (Jacó)

### Small Metro/Town
- Secondary / smaller cities and towns without major-urban CBD character
- Examples: `St Andrews Gramado` (Gramado), `Hotel Vasco` (Cuautla), `Selina Paraty` (Paraty)

### Airport
- Primary demand driver is airport capture (shuttle, crew, connections)
- Name or site clearly airport-oriented
- Examples: `ibis Styles Confins Aeroporto`, `Hilton Garden Inn Guanacaste Airport`

### Interstate
- Highway roadside / interstate exit lodging (limited CALA footprint)
- Examples: `Belem Soft Hotel` (Belém), `Hathor Hotels Mendoza` (Mendoza highway markets)

---

## Source priority (future population)

| Tier | Source | When to use | Write rule |
|------|--------|-------------|------------|
| 1 | Steward-verified `Location` | Field already set to valid STR label | **Keep** — never overwrite in automation |
| 2 | STR legacy import | Full STR property export includes location type | Preferred for bulk historical census |
| 3 | Brand steward plan | Curio / HE CALA manual rows | `curio-census-location-type.js`, steward CSV |
| 4 | Name tokens | `airport` / `aeropuerto` → Airport; `resort` / `beach` / `all-inclusive` → Resort | Fill-blank only; high confidence |
| 5 | `Resort (Y/N)` amenity | `Y` → Resort | Medium confidence |
| 6 | Census peer mode | Same `city` + `country` as rows with known `Location` | Medium/low by peer count |
| 7 | Manual review | Ambiguous pipeline, no peers, conflicting signals | Leave blank until reviewed |

**Never derive from:**
- `country` or `Region` alone
- `Market` / `Submarket` alone (except as steward context)
- Chain scale or affiliation defaults (Hampton ≠ always Suburban)

---

## Automated inference module

`lib/hotel-census/location-enrichment-contract.js`

- `proposeLocationType(row, peerIndex)` — fill-blank proposals
- `buildLocationPeerIndex(records)` — city+country mode from existing census
- `LOCATION_TYPE_LABELS` — allowed values

### Plan / apply

```bash
node scripts/plan-hotel-census-location-enrichment.mjs
node scripts/plan-hotel-census-location-enrichment.mjs --apply
```

---

## Current census status (Jun 2026)

| Metric | Value |
|--------|------:|
| Total rows | 15,642 |
| `Location` filled | 14,931 (95%) |
| Blank | 711 |

Blanks cluster on:
- Recent Hilton / brand-directory adds
- Pipeline hotels in cities without STR peer rows
- Curio rows (partially addressed via `apply-curio-census-location-type.mjs`)

**Estimated automated fill-blank coverage (without steward):**
- ~579 via city+country peer mode
- ~24 via airport name tokens
- ~84 via resort/beach name tokens
- ~132 need steward review (no peers, no strong name signal)

---

## Related fields (do not confuse)

| Field | Relationship |
|-------|----------------|
| `Property Type` | Dealality product class (Full Service, Select Service, …) — separate from STR location type |
| `Resort (Y/N)` | STR amenity flag; `Y` supports Resort location type but does not replace steward judgment |
| `Dealality Market` | Commercial market — not location type |

---

## Data contract snapshot

| Item | Value |
|------|--------|
| Table | `Hotel Census` |
| Mapping | `MAP_LOCATION_ENRICHMENT` |
| Required for automation | `city`, `country` (for peer mode); `name` (for token rules) |
| Allowed values | `LOCATION_TYPE_LABELS` (6 STR labels) |
| UI consumer | `locationTypeMix` in Brand Explorer census metrics |

---

## Change impact

| Change | Impact |
|--------|--------|
| Location backfill | **Medium** — shifts Brand Explorer location-type mix charts |
| Overwriting steward values | **High** — avoid except `--force` steward scripts |

---

## Regression checklist

- Brand Explorer footprint `locationTypeBreakdown`
- `GET /api/brand-presence/location-types`
- Curio location steward plan (`apply-curio-census-location-type.mjs`)
- Rows with invalid/non-STR `Location` text (should be flagged, not auto-kept)

---

## Known limitations

- STR Excel files in `data/str-imports/` do **not** include location type — peer/name inference only for new rows until full STR export is available.
- `Small Metro/Town` vs `Suburban` vs `Urban` can be subjective in mid-size LATAM cities — prefer peer mode over rules when peers exist.
- Interstate is uncommon in CALA; do not default to Interstate without highway-corridor evidence.
