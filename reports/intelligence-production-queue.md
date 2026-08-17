# Dealality Intelligence Production Queue

Generated: 2026-07-06T20:05:34.688Z
Queue: **v1.1**
Mode: **plan** (read-only)

## Executive summary

| Metric | Count |
|--------|------:|
| Total packages | 10 |
| Resolved | 9 |
| Unresolved (missing rec ID) | 1 |
| Complete / Stage 8 | 6 |
| Platform-ready | 6 |
| Blocked (in progress) | 3 |
| Needs sources | 3 |
| Needs source approval | 0 |
| Needs extraction | 0 |
| Needs fact approval | 0 |
| Needs governance publish | 0 |

## Queue table

| Entity | Type | Stage | Status | Sources (appr/total) | Facts (appr/total) | Blockers | Next action |
|--------|------|------:|--------|---------------------:|-------------------:|----------|-------------|
| GHL Hoteles (GHL Holding) | operator | 8 · platform_usage | platform_ready | 5/6 | 5/7 | source_status_not_ready; approved_for_explorer_use_no | Monitor / optional enrichment (stronger live governance preserved when applicable) |
| Hotel Equities (CALA) | operator | 8 · platform_usage | platform_ready | 3/5 | 5/9 | source_status_not_ready; approved_for_explorer_use_no | Monitor / optional enrichment (stronger live governance preserved when applicable) |
| Arbor Lodging (CALA) | operator | 8 · platform_usage | platform_ready | 7/7 | 1/283 | — | Monitor / optional enrichment (stronger live governance preserved when applicable) |
| Kimpton Hotels | brand | 8 · platform_usage | platform_ready | 4/4 | 4/48 | stronger_live_governance_preserved; would_downgrade_existing_validation | Monitor / optional enrichment (stronger live governance preserved when applicable) |
| Curio Collection by Hilton | brand | 8 · platform_usage | platform_ready | 1/15 | 2/149 | approved_for_explorer_use_no | Monitor / optional enrichment (stronger live governance preserved when applicable) |
| Aimbridge Hospitality (LATAM/CALA) | operator | — | unresolved | — | — | No Operator Setup - Master record found in Airtable (2026-07-06 search); PI sources empty | Resolve Airtable record ID in priority tracker |
| Best Western Plus | brand | 1 · source_discovery | blocked | 0/0 | 0/0 | no_linked_sources; no_approved_explorer_sources; no_approved_facts | Discover and link official sources |
| Hilton Garden Inn | brand | 1 · source_discovery | blocked | 0/0 | 0/0 | no_linked_sources; no_approved_explorer_sources; no_approved_facts | Discover and link official sources |
| Radisson Blu by Choice | brand | 8 · platform_usage | platform_ready | 4/4 | 8/11 | — | Monitor / optional enrichment (stronger live governance preserved when applicable) |
| Viento Sur Gestión Hotelera | operator | 1 · source_discovery | blocked | 0/0 | 0/0 | no_linked_sources; no_approved_explorer_sources; no_approved_facts | Discover and link official sources |

## Platform-ready profiles

- **GHL Hoteles (GHL Holding)** (`operator:reciI2tYQBfMoMK9G`) — AI-Assisted Profile · change class `no_op`
- **Hotel Equities (CALA)** (`operator:recWPKu5laVZxsvpn`) — AI-Assisted Profile · change class `no_op`
- **Arbor Lodging (CALA)** (`operator:recF5Z87OAqFgndoq`) — Source-Informed Profile · change class `no_op`
- **Kimpton Hotels** (`brand:recCKuXCmGvxHPfb3`) — AI-Assisted Profile · change class `downgrade`
- **Curio Collection by Hilton** (`brand:receQkxgjlezsc1xg`) — AI-Assisted Profile · change class `no_op`
- **Radisson Blu by Choice** (`brand:recWPEvxBQxVVzSq3`) — AI-Assisted Profile · change class `no_op`

## Blocked profiles

### Aimbridge Hospitality (LATAM/CALA)

- Record: `null` (operator)
- Stage: 0 — resolve_entity
- Blocker: unresolved_record_id

### Best Western Plus

- Record: `rec5KPgalPPAFl7UZ` (brand)
- Stage: 1 — source_discovery
- Blocker: no_linked_sources
- Blocker: no_approved_explorer_sources
- Blocker: no_approved_facts

### Hilton Garden Inn

- Record: `recrvdAjRlXxPvPPF` (brand)
- Stage: 1 — source_discovery
- Blocker: no_linked_sources
- Blocker: no_approved_explorer_sources
- Blocker: no_approved_facts

### Viento Sur Gestión Hotelera

- Record: `recZPHT2zqc8K6itx` (operator)
- Stage: 1 — source_discovery
- Blocker: no_linked_sources
- Blocker: no_approved_explorer_sources
- Blocker: no_approved_facts

## Next actions

### Aimbridge Hospitality (LATAM/CALA) (`TBD`)

**Resolve Airtable record ID in priority tracker**

```bash
npm run intelligence-profile-workflow -- --entity-type {type} --target-rec-id rec... --plan
```

### Best Western Plus (`rec5KPgalPPAFl7UZ`)

**Discover and link official sources**

```bash
npm run partner-reference:search -- --operator "Best Western Plus"
```

```bash
npm run partner-reference:init-folder -- --company "Best Western Plus" --dry-run
```

### Hilton Garden Inn (`recrvdAjRlXxPvPPF`)

**Discover and link official sources**

```bash
npm run partner-reference:search -- --operator "Hilton Garden Inn"
```

```bash
npm run partner-reference:init-folder -- --company "Hilton Garden Inn" --dry-run
```

### Viento Sur Gestión Hotelera (`recZPHT2zqc8K6itx`)

**Discover and link official sources**

```bash
npm run partner-reference:search -- --operator "Viento Sur Gestión Hotelera"
```

```bash
npm run partner-reference:init-folder -- --company "Viento Sur Gestión Hotelera" --dry-run
```

## Safety (v1.1)

- Read-only queue — no Airtable writes
- No apply orchestration — use printed commands with explicit approval
- Does not write: Company Validated
- Does not write: Company Validation Date
- Does not write: source/fact approval
- Does not write: governance publish
- Does not write: Airtable schema
