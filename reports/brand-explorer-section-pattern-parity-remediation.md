# Section Pattern Parity Remediation

Generated: 2026-07-23T06:43:50.977Z
Version: section-pattern-parity-remediation-v1
Applied: false

## Summary

- Brands planned: 3
- Brands with patches: 3
- Patch count: 29
- Already passing: 0
- Missing pack: (none)

| Brand | Section | Current Pattern | Expected Pattern | Status | Failure Reason | Proposed Patch | Benchmark Reference |
| --- | --- | --- | --- | --- | --- | --- | --- |
| hotel-indigo | recent_momentum | benchmark_card_list | Tribute / Kimpton / Design Hotels named openings with date + geography + owner relevance | **needs_patch** | linked_announcement_url_below_min:0 | Replace with 2–4 named momentum cards (activity type, geography/property, timeframe, owner relevance, linked source when available). Remove Illustrative activity / directional themes framing. | Tribute / Kimpton / Design Hotels named openings with date + geography + owner relevance |
| hotel-indigo | geographic_footprint | regional_breakdown | Tribute / Kimpton / Radisson Blu regional cards + brand-specific geo_intro | **pass** |  |  | Tribute / Kimpton / Radisson Blu regional cards + brand-specific geo_intro |
| hotel-indigo | portfolio_context | present | Benchmark overview.portfolio_context ladder + owner-facing positioning | **pass** |  |  | Benchmark overview.portfolio_context ladder + owner-facing positioning |
| hotel-indigo | growth_priorities | themes_plus_narrative | Benchmark footprint.growth_themes + growth_editorial/fit chips and narrative | **pass** |  |  | Benchmark footprint.growth_themes + growth_editorial/fit chips and narrative |

### Patches — hotel-indigo (11)
- `PATCH` footprint.momentum recDu7L9MOVN0tWFM — quarantine_wrong_pattern_momentum
- `PATCH` footprint.momentum recR0xqsVc2f5VrGD — quarantine_wrong_pattern_momentum
- `PATCH` footprint.momentum recQBKwlCdB3e4adf — quarantine_wrong_pattern_momentum
- `PATCH` footprint.momentum recFKX9gLVC9gFKWS — quarantine_wrong_pattern_momentum
- `PATCH` footprint.momentum_label rec5U5cerWwWaRpby — quarantine_wrong_pattern_momentum_label
- `POST` footprint.momentum_label (create) — section_pattern_momentum_label
- `POST` footprint.momentum (create) — section_pattern_momentum_card
- `POST` footprint.momentum (create) — section_pattern_momentum_card
- `POST` footprint.momentum (create) — section_pattern_momentum_card
- `POST` footprint.momentum (create) — section_pattern_momentum_card
- `PATCH` footprint.geo_intro recIziC1bkwcIZxoO — section_pattern_geo_intro
| mgallery-collection | recent_momentum | benchmark_card_list | Tribute / Kimpton / Design Hotels named openings with date + geography + owner relevance | **needs_patch** | linked_announcement_url_below_min:0 | Replace with 2–4 named momentum cards (activity type, geography/property, timeframe, owner relevance, linked source when available). Remove Illustrative activity / directional themes framing. | Tribute / Kimpton / Design Hotels named openings with date + geography + owner relevance |
| mgallery-collection | geographic_footprint | regional_breakdown | Tribute / Kimpton / Radisson Blu regional cards + brand-specific geo_intro | **pass** |  |  | Tribute / Kimpton / Radisson Blu regional cards + brand-specific geo_intro |
| mgallery-collection | portfolio_context | present | Benchmark overview.portfolio_context ladder + owner-facing positioning | **pass** |  |  | Benchmark overview.portfolio_context ladder + owner-facing positioning |
| mgallery-collection | growth_priorities | themes_plus_narrative | Benchmark footprint.growth_themes + growth_editorial/fit chips and narrative | **pass** |  |  | Benchmark footprint.growth_themes + growth_editorial/fit chips and narrative |

### Patches — mgallery-collection (9)
- `PATCH` footprint.momentum recc9nLaUZ2smFTzN — quarantine_wrong_pattern_momentum
- `PATCH` footprint.momentum recyM9JTC3yVQZXwm — quarantine_wrong_pattern_momentum
- `PATCH` footprint.momentum rec3MojdhsxxB5ab6 — quarantine_wrong_pattern_momentum
- `PATCH` footprint.momentum_label recDbuzgsgJjPRknP — quarantine_wrong_pattern_momentum_label
- `POST` footprint.momentum_label (create) — section_pattern_momentum_label
- `POST` footprint.momentum (create) — section_pattern_momentum_card
- `POST` footprint.momentum (create) — section_pattern_momentum_card
- `POST` footprint.momentum (create) — section_pattern_momentum_card
- `PATCH` footprint.geo_intro recOpY8kMmLXaR23W — section_pattern_geo_intro
| small-luxury-hotels-of-the-world | recent_momentum | benchmark_card_list | Tribute / Kimpton / Design Hotels named openings with date + geography + owner relevance | **needs_patch** | linked_announcement_url_below_min:0 | Replace with 2–4 named momentum cards (activity type, geography/property, timeframe, owner relevance, linked source when available). Remove Illustrative activity / directional themes framing. | Tribute / Kimpton / Design Hotels named openings with date + geography + owner relevance |
| small-luxury-hotels-of-the-world | geographic_footprint | regional_breakdown | Tribute / Kimpton / Radisson Blu regional cards + brand-specific geo_intro | **pass** |  |  | Tribute / Kimpton / Radisson Blu regional cards + brand-specific geo_intro |
| small-luxury-hotels-of-the-world | portfolio_context | present | Benchmark overview.portfolio_context ladder + owner-facing positioning | **pass** |  |  | Benchmark overview.portfolio_context ladder + owner-facing positioning |
| small-luxury-hotels-of-the-world | growth_priorities | themes_plus_narrative | Benchmark footprint.growth_themes + growth_editorial/fit chips and narrative | **pass** |  |  | Benchmark footprint.growth_themes + growth_editorial/fit chips and narrative |

### Patches — small-luxury-hotels-of-the-world (9)
- `PATCH` footprint.momentum recxi5I5DO0BuAApP — quarantine_wrong_pattern_momentum
- `PATCH` footprint.momentum recrV8chBu7uTgpdd — quarantine_wrong_pattern_momentum
- `PATCH` footprint.momentum recHGKLjbTbCDW1rs — quarantine_wrong_pattern_momentum
- `PATCH` footprint.momentum_label recw8qiBym9bfM4rK — quarantine_wrong_pattern_momentum_label
- `POST` footprint.momentum_label (create) — section_pattern_momentum_label
- `POST` footprint.momentum (create) — section_pattern_momentum_card
- `POST` footprint.momentum (create) — section_pattern_momentum_card
- `POST` footprint.momentum (create) — section_pattern_momentum_card
- `PATCH` footprint.geo_intro recNryqEtgtMWDsWf — section_pattern_geo_intro

## Guardrails

- Presentation Title/Body/chips/quarantine only
- No Company Validated / Source / Registry / release / public restore
