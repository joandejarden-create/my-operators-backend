# Brand Explorer Active Profile Factory v34C



Generic factory layer — brand config + shared stages (no suburban-only writer chain).



## Architecture



| Module | Purpose |

|--------|---------|

| `brand-explorer-active-profile-brand-config.js` | Per-brand config model |

| `brand-explorer-active-profile-asset-pack-builder.js` | Gallery / property / scenario asset discovery |

| `brand-explorer-active-profile-draft-builder.js` | Dry-run presentation + registry patch proposals |

| `brand-explorer-active-profile-factory.js` | Stage orchestration |



## Factory commands



| Stage | Command |

|-------|---------|

| Preflight | `brand-explorer-active-profile-preflight` |

| Asset pack | `brand-explorer-active-profile-asset-pack` |

| Build draft | `brand-explorer-active-profile-build-draft` |

| Copy governance | `brand-explorer-active-profile-copy-governance` |

| Founder review | `brand-explorer-active-profile-founder-review` |

| Apply approved | `brand-explorer-active-profile-apply-approved` |

| Final QA | `brand-explorer-active-profile-final-qa` |



## Suburban assessment (2026-07-14T11:07:17.897Z)



- Factory pass: **no**

- Asset pack readiness: **full**

- Recommendation: **proceed**

- Path required: **config + asset pack**

- Custom code required: **no**

- Draft patches (dry-run): **12**



## Global rules



1. Gallery — minimum 6 visible `materials.gallery` with API `imageUrl`

2. Property examples — hotel/property images only; U.S. fallback labeled

3. Scenario images — hide when no source; no IMAGE placeholders

4. Registry traceability — durable source URLs + approved registry rows

5. Copy safety — ADR/FDD/performance claims blocked in sanitizer

6. Apply gated — founder review + explicit confirm flags



## Validation brands



- **suburban-studios** — first generic factory test

- **woodspring-suites** — validation reference

- **everhome-suites** — validation reference (property catalog TBD)

