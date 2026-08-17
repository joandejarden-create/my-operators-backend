# Webhound Active Brand Setup Coverage Gap Pass

**Status:** `webhound_active_brand_coverage_gap_pass_complete_ready_for_adapter_queue`  
**Executive recommendation:** **C — Add several low-effort adapters together**  
**Airtable / Census / Brand Setup / Brand Explorer writes:** false  
**Webhound role:** learning input only (not Census SoT)

**Session:** https://webhound.ai/session/0abff16f-9e6e-4203-ae91-9ec79556cea6 ($5, budget_complete)

## Gap list summary

Active/Live Brand Setup: **62** brands.

| Parent | Brands | Adapter status | Webhound? |
| --- | ---: | --- | --- |
| Marriott / IHG / Hilton / Choice | 44 | **supported** (multi-parent sprint wired) | **No** |
| Accor | 9 | partial (extractors exist, not Autopilot-wired) | Yes |
| Wyndham | 4 | partial (extractors exist, not Autopilot-wired) | Yes |
| BWH Hotels | 2 | partial / live blocked | Yes |
| Preferred | 1 | missing | Yes |
| SLH | 1 | missing | Yes |
| Bunkhouse | 1 | missing (tiny CALA) | Yes |

Gap list: `reports/research-engine-v2/active-brand-discovery-coverage-gap-list.{md,json}`

**Note:** Many Brand Setup rows lack Parent Company; Autopilot Active Setup discovery previously skipped IHG/Accor/etc. unless parent is inferred from slug.

## Adapter build queue

1. **Accor** — wire `accor-brand-directory-extract` + continent browse into Autopilot (low effort)  
2. **Wyndham** — wire `wyndham-brand-directory-extract`; CALA filter via page country (not path keywords)  
3. **Preferred** — new static `/directory` adapter (medium)  
4. **BWH** — defer; sitemap OK, property pages 403 → steward/seed  
5. **SLH** — defer steward  
6. **Bunkhouse** — manual steward only  

## Code vs Webhound

| Pattern | Classification |
| --- | --- |
| Marriott / IHG / Hilton / Choice | `already_supported` |
| Accor sitemap + continent | `new_adapter_needed` (extend existing) |
| Wyndham property sitemaps | `new_adapter_needed` (extend existing) |
| Preferred `/directory` | `new_adapter_needed` |
| BWH property HTML | `blocked_source` |
| SLH country pages | `generic_extractor_can_handle` / steward |
| Bunkhouse SPA | `do_not_use` (adapter) |

## Do not use / do not learn

- OTAs, TripAdvisor as SoT, Wikipedia lists, CoStar, VIC as production table  
- Webhound as Census SoT or hotel-count authority  
- Accor short continent paths (403)  
- Wyndham path keyword “CALA” (e.g. panama-city-florida)  
- Owner/operator/dates, Recent Momentum, Company Validated / Brand Verified  

## Code probe companion

`reports/research-engine-v2/webhound-active-brand-coverage-gap-code-probe.json`

- Accor continent pages: South America 300 / Central America 151 / North America 131 (page 1)  
- Accor sitemap: ~5895 hotel locs  
- BWH sample property page: **403**  
- Preferred directory / SLH home: reachable  

## Next

Implement Accor + Wyndham Autopilot discovery adapters (no apply until controlled runs). Prefer Dealality extractors over Webhound field claims where they conflict (JSON-LD / CALA counts).
