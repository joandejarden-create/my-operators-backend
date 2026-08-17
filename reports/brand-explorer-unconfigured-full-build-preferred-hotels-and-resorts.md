# Unconfigured Full Build — Preferred Hotels & Resorts

| Field | Value |
| --- | --- |
| Slug | `preferred-hotels-and-resorts` |
| Record ID | `recwl5JOYxlChuCAr` |
| Brand Status | Active |
| Presentation rows | 3 |
| Content pack exists | true |
| Gallery pool exists | false |
| Openings fixture exists | true |
| Build ready | true |
| Blocked reason | — |

## Owner lens

Independent hotel representation / soft affiliation platform — distinguish from SLH, Design Hotels, Autograph, Tribute, Curio, Vignette.

## Required steps

1. Author brand-explorer-full-build-content-<slug>.js (~70+ presentation rows)
2. Register in brand-explorer-full-build-content.js (UNCONFIGURED_ACTIVE_FULL_BUILD_SLUGS)
3. Create fixtures/lane2-<slug>-gallery-pool.json (6 gallery + 3 scenario + 3 property URLs)
4. Add property catalog entries for openings cards
5. Dry-run full-tab-factory-build → apply content POSTs
6. Image asset pack → image materialization apply
7. Founder minor cleanup (Recent Momentum contract)
8. Run full gate suite + PVQL
9. Founder packet → approve_for_active_release
10. Public restore governance apply (Basics release fields only)

