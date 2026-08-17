# Wave 13 SO/ — Section Pattern Failures

Generated: 2026-07-28T09:58:38.196Z
Brand: **SO/** (`so-hotels-and-resorts`, `recTJdPlr4mDs9app`)
Tab-factory auditPass: **false** · sectionPatternPass: **false**

| Brand | Section | Field | Failure Type | Required Pattern | Proposed Fix | Source Support |
| --- | --- | --- | --- | --- | --- | --- |
| SO/ | Recent Momentum | Title / Body dateLine | `dated_cards_below_min` | ≥2 cards with title + year/month dateLine (e.g. Mar 2026) + geography + owner summary + https source; no raw URL in prose | Rebuild 2 momentum cards from source pack: Accor Brandbook Mar 2026 + SO/ Paris brand-site listing 2025 (International Reference) | reports/brand-explorer-wave13-source-pack-so-hotels-and-resorts.md Recent Momentum candidates |
| SO/ | Geographic Footprint | Body / Active / External Display Status | `empty_mea_region_panel` | Fill with source-supported MEA copy OR suppress (Active=false + Do Not Display). No empty visible panel. | No source-supported SO/ MEA operating inventory — suppress MEA panel cleanly | Source pack CALA/MEA: none_found; hold remediation geo package omitted MEA intentionally |
| SO/ | Geographic Footprint | Body | `footprint_not_brand_specific` | Brand-specific geo_intro + ≥3 filled regions; SO/ Hotels & Resorts / resorts language present | Refresh geo_intro (+ keep filled EU/APAC/AM/CALA) with explicit SO/ Hotels & Resorts / resorts wording; suppress MEA | so-hotels.com Paris/Maldives + Accor SO/ brand page (International Reference) |
| SO/ | Growth Priorities | Body | `growth_priorities_not_brand_specific` | ≥2 theme chips + ≥30-word SO/-specific growth editorial (fashion/design-led hotels and resorts, destination energy, F&B intensity) | Rewrite growth_themes + growth_editorial/fit as SO/ Hotels & Resorts selective luxury lifestyle growth — distinguish from Mama Shelter / Fairmont / MGallery / generic Accor | Accor Brandbook + SO/ brand page positioning |

## Details

### Recent Momentum — `dated_cards_below_min`

- Slot: `footprint.momentum`
- Current: dateLine values Directory / Collection (structured but undated vs year/month gate); datedCount=0
- Proposed: Rebuild 2 momentum cards from source pack: Accor Brandbook Mar 2026 + SO/ Paris brand-site listing 2025 (International Reference)
- Failures: dated_cards_below_min:0

### Geographic Footprint — `empty_mea_region_panel`

- Slot: `footprint.region.mea`
- Current: footprint.region.mea visible with empty body (words=0)
- Proposed: No source-supported SO/ MEA operating inventory — suppress MEA panel cleanly
- Failures: footprint_not_brand_specific

### Geographic Footprint — `footprint_not_brand_specific`

- Slot: `footprint.geo_intro (+ regions)`
- Current: brandSpecific() tokens for Basics name SO/ collapse to slug token 'resorts'; corpus often lacks 'resorts'
- Proposed: Refresh geo_intro (+ keep filled EU/APAC/AM/CALA) with explicit SO/ Hotels & Resorts / resorts wording; suppress MEA
- Failures: footprint_not_brand_specific

### Growth Priorities — `growth_priorities_not_brand_specific`

- Slot: `footprint.growth_editorial / footprint.growth_themes / footprint.growth_fit`
- Current: Themes present (5) + editorial ~32 words but brandSpecific fails (missing 'resorts' token match)
- Proposed: Rewrite growth_themes + growth_editorial/fit as SO/ Hotels & Resorts selective luxury lifestyle growth — distinguish from Mama Shelter / Fairmont / MGallery / generic Accor
- Failures: growth_priorities_not_brand_specific

