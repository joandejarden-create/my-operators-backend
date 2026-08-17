# Operator Explorer — Brand-managed Core 5

> **Created:** 2026-07-24  
> **Updated:** 2026-07-24 (source-backed MxM / Accor re-fill)  
> **Wave:** C — brand-management companies as Operator Explorer profiles  
> **Scope:** Marriott, IHG, Hilton, Accor, Minor only (Playa is Wave B — see below)

## Decision lock

- **Entity:** Parent **brand-management** companies as Operator Setup Masters (owner lens: brand-managed operating model).
- **Plus:** Registry maps Brand parent companies → those Operator Masters (for tooling / Operator Explorer).
- **Not:** Brand Explorer hero chips linking to Operator profiles; not duplicating Brand Explorer brand tabs; not claiming these are third-party independents.

## Source authority (binding)

**Local Brand / Operator Reference Material captures before Airtable write.** Live web is for harvest only when a capture is missing. Do not keep thin homepage paraphrase when a managed-ops / development page exists.

| Operator | Primary source | Local path |
|---|---|---|
| Marriott International (Managed) | [Managed by Marriott (MxM)](https://www.hotel-development.marriott.com/how-we-work-together/managed-by-marriott) | `Brand Reference Material/Marriott International/development/Managed by Marriott (MxM).html` |
| Accor (Managed) | [Develop with Accor](https://group.accor.com/en/hotel-development) + Solutions / Overview 2026 | `Brand Reference Material/Accor/development/…` |
| IHG / Hilton / Minor | Official managed/development captures when present | Brand Reference Material company folders |

Pack modules:

- `lib/partner-intelligence/operator-setup-source-packs-marriott-managed.js` (`verifyMarriottMxmLocalCapture`)
- `lib/partner-intelligence/operator-setup-source-packs-accor-managed.js` (`verifyAccorManagedLocalCaptures`)

Harvest Accor: `npm run partner-reference:harvest-accor-materials -- --apply`

Every narrative must:

1. Use the **(Managed)** lens in the company name.
2. Label **enterprise scale vs CALA managed footprint** (do not invent CALA managed counts without census/source evidence). Enterprise examples: MxM ~2,100 hotels; Accor Overview 5,800+ hotels — never as CALA managed counts.
3. Contrast vs third-party: best-fit = brand-managed / brand-operator paths; not-ideal = pure third-party independent of brand affiliation.

## Naming

| Slug | Master `company_name` | Domain |
|---|---|---|
| `marriott-international-managed` | Marriott International (Managed) | marriott.com / hotel-development.marriott.com |
| `ihg-managed` | IHG Hotels & Resorts (Managed) | ihg.com |
| `hilton-managed` | Hilton (Managed) | hilton.com |
| `accor-managed` | Accor (Managed) | group.accor.com |
| `minor-hotels-managed` | Minor Hotels (Managed) | minorhotels.com |

## Brand Explorer link contract

Registry: `lib/partner-intelligence/brand-managed-operator-link-registry.js`  
Browser mirror: `public/js/brand-managed-operator-link-registry.js` (ID/alias resolution only)

| Parent Company (and aliases) | Master ID | Explorer |
|---|---|---|
| Marriott International | `recGmiPhRt6hiayd9` | `/operator-explorer-gold-mock.html?id=recGmiPhRt6hiayd9` |
| IHG Hotels & Resorts | `rec7IXYQYpKMYsrDl` | `/operator-explorer-gold-mock.html?id=rec7IXYQYpKMYsrDl` |
| Hilton | `rec3Uwxe6ovpiokuN` | `/operator-explorer-gold-mock.html?id=rec3Uwxe6ovpiokuN` |
| Accor / AccorHotels | `recF2WqLqNVyKGz9E` | `/operator-explorer-gold-mock.html?id=recF2WqLqNVyKGz9E` |
| Minor Hotels | `rec8SrT3VjRkkYTxm` | `/operator-explorer-gold-mock.html?id=rec8SrT3VjRkkYTxm` |

**Brand Explorer:** do **not** render a “View brand-managed operator profile” chip in Brand Explorer heroes. Operator profiles are opened from Operator Explorer only. Do not merge Brand tabs into Operator IA.

## Related: Playa Hotels & Resorts (Wave B — not brand-managed)

Playa is an **all-inclusive owner/operator** (Mexico / Jamaica / DR), not a brand parent. No Brand Explorer parent chip.

| Field | Value |
|---|---|
| Slug | `playa-hotels-resorts` |
| Master | `rec3TUHT9Z4AnFp5P` (Active) |
| Domain | playaresorts.com |
| Content | `lib/partner-intelligence/operator-setup-playa-hotels-content.js` |
| Create | `npm run create-playa-hotels-resorts-operator-master` |
| Harvest | `npm run partner-reference:harvest-operators -- --operator playa --apply` |
| Gold mock | `/operator-explorer-gold-mock.html?id=rec3TUHT9Z4AnFp5P` |

Note Hyatt’s public acquisition of Playa (2025) in diligence copy; underwrite current ownership/management structure.

## Headquarters format (all Operator Setup)

**Rule:** Profile `headquarters` = `City, Country` only (e.g. `Bethesda, United States`). No country-only, state abbreviations as middle segments, or narrative strings.

Canonical map: `lib/partner-intelligence/operator-setup-headquarters-registry.js`  
Normalize fleet:

```bash
npm run normalize-operator-setup-headquarters -- --dry-run
npm run normalize-operator-setup-headquarters -- --apply --approve-normalize-operator-setup-headquarters
```

## Pipelines

```bash
# Source-backed re-fill (Marriott MxM / Accor after local capture)
npm run operator-setup-website-content-apply -- --apply --approve-operator-setup-website-content-apply --operators marriott-international-managed,accor-managed
npm run operator-setup-profile-deepen -- --apply --approve-operator-setup-profile-deepen --operators marriott-international-managed,accor-managed

# Promote Active after detail smoke
npm run promote-operator-explorer-brand-managed-active -- --apply --approve-promote-operator-brand-managed-active
```

## Live Masters (2026-07-24)

| Slug | Master ID | Status |
|---|---|---|
| marriott-international-managed | `recGmiPhRt6hiayd9` | Active (MxM source-backed) |
| ihg-managed | `rec7IXYQYpKMYsrDl` | Active |
| hilton-managed | `rec3Uwxe6ovpiokuN` | Active |
| accor-managed | `recF2WqLqNVyKGz9E` | Active (Accor development source-backed) |
| minor-hotels-managed | `rec8SrT3VjRkkYTxm` | Active |
| playa-hotels-resorts | `rec3TUHT9Z4AnFp5P` | Active (Wave B) |

## Out of scope (this wave)

- Hyatt / Choice / Wyndham as brand-managed Operator Explorers
- Multi-row Operating Platform / Leadership / Case Studies population
- Live Airtable fixture overlay apply
- Invented CALA managed hotel counts
- Full Arbor/HE Tab Factory `auditPass=true` (Phase 2). Thin fixtures intentionally leave gaps.
