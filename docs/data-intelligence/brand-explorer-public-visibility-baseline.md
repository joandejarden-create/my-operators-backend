# Public Visibility Baseline

**Status:** **FROZEN** (2026-07-22) — all 11 public-full profiles pass PVQL.

Artifacts: `reports/brand-explorer-public-visibility-baseline.json` · `reports/brand-explorer-public-visibility-baseline.md`

## Scope

Only profiles with `shouldRenderFullProfile === true` (externally rendering full tabs).

Locked / remediation / founder-preview-only profiles are **out of scope**.

## Public-full cohort (baseline)

| Slug | Cohort |
| --- | --- |
| `hotel-indigo` | primary (protected passer) |
| `mgallery-collection` | primary (protected passer) |
| `radisson-individuals-by-choice` | primary (protected passer) |
| `small-luxury-hotels-of-the-world` | primary (protected passer) |
| `everhome-suites` | primary (stabilized) |
| `kimpton` | primary (stabilized) |
| `design-hotels` | primary (stabilized) |
| `ascend` | legacy public (stabilized) |
| `comfort-inn-suites` | legacy public (stabilized) |
| `curio-collection` | legacy public (stabilized) |
| `tribute-portfolio` | legacy public (stabilized) |

## Required gates (existing — no new gates)

1. Rendered field completeness  
2. No empty rendered components  
3. Tab factory audit  
4. Source provenance by tab  
5. Image uniqueness  
6. Image role/caption match  
7. Public visibility quality lock  

## Re-check

```bash
npm run test:brand-explorer-public-visibility-quality-lock -- --public-full-only
```

## After freeze

Use the **Tab Factory** as the build process for all future brands. Do not broaden public visibility until new brands pass the same gate set.
