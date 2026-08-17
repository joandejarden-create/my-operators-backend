# Brand & Operator Explorer — coverage tracker

**Updated:** 2026-07-03  
**FPP master task:** Create master completion tracker (`recHls6zriLafoqJT`, Pilot Delivery step 5)  
**Source of truth:** `npm run choice-brand-explorer:manifest` + platform Operator Explorer

---

## Summary

| Area | Status |
|------|--------|
| **Choice CHI brands (22)** | All **L1 slot-complete** in Airtable Brand Explorer |
| **L2 Blu-parity (4)** | Radisson Blu, Radisson (Choice), Ascend Hotel Collection, Radisson RED |
| **L2 enrichment queue (5)** | Country Inn, Park Inn, Park Plaza, Radisson Collection, Radisson Individual |
| **Non-CHI enriched (2)** | Kimpton (IHG), Curio Collection by Hilton |
| **Operator Explorer** | Platform live; profile depth varies by Operator Setup completion |

**Completion levels** (see `docs/choice-brand-explorer-completion-runbook.md`):

- **L1** — All presentation slots populated (generator baseline)
- **L2** — Radisson Blu parity: brand-specific copy, CALA case studies, economics, momentum

---

## Choice Hotels International — by completion level

### L2 complete (Blu parity)

| Brand | Record ID |
|-------|-----------|
| Radisson Blu (Choice) | recWPEvxBQxVVzSq3 |
| Radisson (Choice) | recywbx1YQSTCPqW1 |
| Ascend Hotel Collection | reclkgOzvAcBheUSo |
| Radisson RED (Choice) | recmKqo7M7mLZgRqQ |

### L1 slot-complete (generator baseline)

Cambria Hotels, Clarion, Clarion Pointe, Comfort Inn & Suites, Econo Lodge, Everhome Suites, MainStay Suites, Quality Inn, Radisson Inn & Suites, Rodeway Inn, Sleep Inn, Suburban Studios, WoodSpring Suites

### Needs L2 enrichment (Radisson-family P1 queue)

| Brand | Record ID | Notes |
|-------|-----------|-------|
| Country Inn & Suites by Radisson (Choice) | recaayt9u7YYg8h7Y | Split fixtures started |
| Park Inn by Radisson (Choice) | recKXAaJYUZSZVc2D | Full JSON exists |
| Park Plaza (Choice) | recnVdGwNaaJNn0eH | No full fixture file |
| Radisson Collection (Choice) | recPAB0PgJyKE2v09 | No full fixture file |
| Radisson Individual (Choice) | recRyvM8OmLlDj9G7 | Full JSON exists |

---

## Non-CHI pilot brands

| Brand | Parent | Status |
|-------|--------|--------|
| Kimpton | IHG | L2 split fixtures (`fixtures/brand-explorer-presentation-kimpton-*.json`) |
| Curio Collection by Hilton | Hilton | L2 split fixtures (`fixtures/brand-explorer-presentation-curio-*.json`) |

---

## FPP structure

- **Step 5:** Master completion tracker
- **Steps 6–65:** Brand Explorer — one row per **chain scale × parent company** (60 tasks)
- **Steps 66–69:** Operator Explorer by **operator type**

Sync: `npm run sync:fpp-explorer-chain-scale`


---

## Next actions (priority)

1. **Joan:** Sign off this tracker → FPP master task `recHls6zriLafoqJT` → Completed
2. **L2 queue:** Radisson Collection, Radisson Individual, Country Inn, Park Inn, Park Plaza (`docs/choice-brand-explorer-completion-runbook.md` P1)
3. **QA:** Spot-check segment tasks at Needs Review in Brand Explorer UI
4. **Operators:** Hydrate CALA pilot operators in Operator Setup → Explorer (steps 14–15)

---

## Commands

```bash
npm run choice-brand-explorer:manifest
npm run sync:fpp-explorer-coverage -- --dry-run
npm run audit-choice-explorer-presentation-gaps -- --brand "Comfort Inn & Suites"
```

## Chain-scale × parent-company brand tasks

**60 rows** - one FPP task per parent company within each chain-scale segment.

### Luxury / Ultra-Luxury

| Step | Parent | Brands | In repo | L2 | Progress | Status |
|------|--------|--------|---------|-----|----------|--------|
| 6 | Marriott | 8 | 0 | 0 | 0% | Not Started |
| 7 | Hilton | 4 | 0 | 0 | 0% | Not Started |
| 8 | Hyatt | 5 | 0 | 0 | 0% | Not Started |
| 9 | IHG | 5 | 1 | 1 | 20% | In Progress |
| 10 | Accor | 9 | 0 | 0 | 0% | Not Started |
| 11 | Minor Hotels | 4 | 0 | 0 | 0% | Not Started |
| 12 | BWH / WorldHotels | 2 | 0 | 0 | 0% | Not Started |
| 13 | Radisson | 2 | 2 | 1 | 73% | In Progress |
| 14 | Iberostar | 1 | 0 | 0 | 0% | Not Started |

### Upper-Upscale / Premium

| Step | Parent | Brands | In repo | L2 | Progress | Status |
|------|--------|--------|---------|-----|----------|--------|
| 15 | Marriott | 11 | 0 | 0 | 0% | Not Started |
| 16 | Hilton | 7 | 1 | 1 | 14% | In Progress |
| 17 | Hyatt | 7 | 0 | 0 | 0% | Not Started |
| 18 | IHG | 6 | 0 | 0 | 0% | Not Started |
| 19 | Accor | 8 | 0 | 0 | 0% | Not Started |
| 20 | Choice / Radisson Americas | 5 | 5 | 3 | 84% | In Progress |
| 21 | BWH | 3 | 0 | 0 | 0% | Not Started |
| 22 | Minor Hotels | 3 | 0 | 0 | 0% | Not Started |
| 23 | Iberostar | 1 | 0 | 0 | 0% | Not Started |

### Upscale / Lifestyle / Boutique

| Step | Parent | Brands | In repo | L2 | Progress | Status |
|------|--------|--------|---------|-----|----------|--------|
| 24 | Marriott | 6 | 0 | 0 | 0% | Not Started |
| 25 | Hilton | 6 | 1 | 1 | 17% | In Progress |
| 26 | Hyatt | 5 | 0 | 0 | 0% | Not Started |
| 27 | IHG | 5 | 1 | 1 | 20% | In Progress |
| 28 | Accor / Ennismore | 13 | 0 | 0 | 0% | Not Started |
| 29 | BWH | 4 | 0 | 0 | 0% | Not Started |
| 30 | Wyndham | 5 | 0 | 0 | 0% | Not Started |
| 31 | Radisson | 2 | 2 | 1 | 73% | In Progress |

### Upper-Midscale / Select-Service

| Step | Parent | Brands | In repo | L2 | Progress | Status |
|------|--------|--------|---------|-----|----------|--------|
| 32 | Marriott | 6 | 0 | 0 | 0% | Not Started |
| 33 | Hilton | 3 | 0 | 0 | 0% | Not Started |
| 34 | Hyatt | 2 | 0 | 0 | 0% | Not Started |
| 35 | IHG | 5 | 0 | 0 | 0% | Not Started |
| 36 | Choice / Radisson Americas | 8 | 8 | 0 | 68% | In Progress |
| 37 | Wyndham | 7 | 0 | 0 | 0% | Not Started |
| 38 | BWH | 3 | 0 | 0 | 0% | Not Started |

### Midscale / Economy

| Step | Parent | Brands | In repo | L2 | Progress | Status |
|------|--------|--------|---------|-----|----------|--------|
| 39 | Choice | 2 | 2 | 0 | 75% | In Progress |
| 40 | Wyndham | 6 | 0 | 0 | 0% | Not Started |
| 41 | Accor | 6 | 0 | 0 | 0% | Not Started |
| 42 | IHG | 3 | 0 | 0 | 0% | Not Started |
| 43 | BWH | 3 | 0 | 0 | 0% | Not Started |

### Extended-Stay / All-Suites

| Step | Parent | Brands | In repo | L2 | Progress | Status |
|------|--------|--------|---------|-----|----------|--------|
| 44 | Marriott | 6 | 0 | 0 | 0% | Not Started |
| 45 | Hilton | 5 | 0 | 0 | 0% | Not Started |
| 46 | Hyatt | 1 | 0 | 0 | 0% | Not Started |
| 47 | IHG | 4 | 0 | 0 | 0% | Not Started |
| 48 | Choice | 4 | 4 | 0 | 75% | In Progress |
| 49 | Wyndham | 3 | 0 | 0 | 0% | Not Started |
| 50 | BWH | 2 | 0 | 0 | 0% | Not Started |
| 51 | Accor | 2 | 0 | 0 | 0% | Not Started |

### Resort / All-Inclusive

| Step | Parent | Brands | In repo | L2 | Progress | Status |
|------|--------|--------|---------|-----|----------|--------|
| 52 | Hyatt Inclusive Collection | 9 | 0 | 0 | 0% | Not Started |
| 53 | IHG / Iberostar | 4 | 0 | 0 | 0% | Not Started |
| 54 | Wyndham | 1 | 0 | 0 | 0% | Not Started |
| 55 | TUI | 3 | 0 | 0 | 0% | Not Started |
| 56 | Accor | 4 | 0 | 0 | 0% | Not Started |
| 57 | Minor | 3 | 0 | 0 | 0% | Not Started |

### Soft Brand / Collection

| Step | Parent | Brands | In repo | L2 | Progress | Status |
|------|--------|--------|---------|-----|----------|--------|
| 58 | Marriott | 6 | 0 | 0 | 0% | Not Started |
| 59 | Hilton | 3 | 1 | 1 | 33% | In Progress |
| 60 | Hyatt | 3 | 0 | 0 | 0% | Not Started |
| 61 | IHG | 3 | 0 | 0 | 0% | Not Started |
| 62 | Choice | 2 | 2 | 1 | 73% | In Progress |
| 63 | Wyndham | 2 | 0 | 0 | 0% | Not Started |
| 64 | BWH / WorldHotels | 7 | 0 | 0 | 0% | Not Started |
| 65 | Accor | 3 | 0 | 0 | 0% | Not Started |

## Operator Explorer segments

Steps 66–69 (by operator type, not chain scale).

| Step | Segment | Operators |
|------|---------|-----------|
| 66 | Third-party / institutional operators | 14 |
| 67 | Regional / CALA / owner-operators | 11 |
| 68 | Lifestyle / boutique operators | 7 |
| 69 | Resort / all-inclusive operators | 10 |

Sync: `npm run sync:fpp-explorer-chain-scale`

