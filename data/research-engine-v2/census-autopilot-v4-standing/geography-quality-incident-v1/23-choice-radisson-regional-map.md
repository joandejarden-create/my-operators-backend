# Choice / Radisson Americas Regional Map

**Status:** Incident documentation (read-only). V4 production writes **PAUSED**.

## Principle

Do **not** use stale global Radisson Hotel Group relationships for Americas properties when current evidence indicates Choice affiliation.

Do **not** collapse every former Radisson Americas property into a generic `Choice` / `Choice Hotels` Current Brand.

## Distinct fields (preserve separately)

| Layer | Meaning | Example |
| --- | --- | --- |
| Current Brand | Hotel-level brand/collection | Sleep Inn, Radisson Blu, Radisson Individuals, Country Inn & Suites |
| Parent Company | Corporate parent | Choice Hotels International |
| Regional platform / relationship | Operating/distribution structure | Radisson Americas under Choice; Choice Privileges |
| Historical affiliation | Prior brand/parent when reflagged | Prior RHG-era claim (temporal) |
| Physical Property Identity | Immutable hotel identity | unchanged across reflag |

## Choice family hotel brands (registry seed — not a static truth table)

- **Sleep Inn** (`sleep-inn`) — choice_global
- **Comfort** (`comfort`) — choice_global
- **Quality Inn** (`quality-inn`) — choice_global
- **Cambria** (`cambria`) — choice_global
- **Ascend Hotel Collection** (`ascend-hotel-collection`) — choice_global
- **Country Inn & Suites** (`country-inn-suites`) — radisson_americas_under_choice
- **Radisson** (`radisson`) — radisson_americas_under_choice
- **Radisson Blu** (`radisson-blu`) — radisson_americas_under_choice
- **Radisson Individuals** (`radisson-individuals`) — radisson_individuals_americas_under_choice
- **Clarion** (`clarion`) — choice_global

## URL slug map (property URL evidence)

- `…/sleep-inn-hotels/{id}` → **Sleep Inn**
- `…/comfort-hotels/{id}` → **Comfort**
- `…/comfort-inn-hotels/{id}` → **Comfort Inn**
- `…/comfort-suites-hotels/{id}` → **Comfort Suites**
- `…/quality-hotels/{id}` → **Quality Inn**
- `…/quality-inn-hotels/{id}` → **Quality Inn**
- `…/clarion-hotels/{id}` → **Clarion**
- `…/cambria-hotels/{id}` → **Cambria**
- `…/ascend-hotels/{id}` → **Ascend Hotel Collection**
- `…/country-inn-hotels/{id}` → **Country Inn & Suites**
- `…/country-inn-suites-hotels/{id}` → **Country Inn & Suites**
- `…/radisson-hotels/{id}` → **Radisson**
- `…/radisson-blu-hotels/{id}` → **Radisson Blu**
- `…/radisson-red-hotels/{id}` → **Radisson RED**
- `…/radisson-individuals-hotels/{id}` → **Radisson Individuals**
- `…/park-hotels/{id}` → **Park Inn**
- `…/park-inn-hotels/{id}` → **Park Inn**
- `…/park-plaza-hotels/{id}` → **Park Plaza**
- `…/econo-lodge-hotels/{id}` → **Econo Lodge**
- `…/rodeway-inn-hotels/{id}` → **Rodeway Inn**
- `…/mainstay-suites-hotels/{id}` → **MainStay Suites**
- `…/suburban-hotels/{id}` → **Suburban Studios**
- `…/woodspring-suites-hotels/{id}` → **WoodSpring Suites**

## Known bug in audited production

All 70 Choice-family V3/V3.1 production rows audited had `Current Brand = "Choice"` (parent/source-family default), including properties whose official URL encodes Sleep Inn / Comfort / etc.

## Required model after fix

`Physical Hotel → Current Brand → Parent Company → Distribution/Loyalty Platform`
