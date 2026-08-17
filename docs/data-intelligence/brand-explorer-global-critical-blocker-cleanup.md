# Global Active — Critical Semantic Blocker Cleanup

**Ready:** `global_active_critical_semantic_blockers_clean`  
**Freeze:** Do **not** freeze 54 in this task. High/Medium remain for later review. Protected baseline stays **46**.

## Summary

| Item | Result |
|------|--------|
| Fresh global semantic audit (post Wave 14) | 54 Active/Live reconciled |
| Pre-cleanup Critical findings | 45 across 20 brands |
| Critical patches applied (pass 1) | 46 |
| Residual Critical after pass 1 | 1 (`so-hotels-and-resorts` Stage language; Brand Name alias miss) |
| Residual patches (pass 2 + operator-compat polish) | 2 + 1 Body polish |
| Post-cleanup Critical findings | **0** |
| Wave 14 Critical in global audit | **0** |
| Quiet PVQL | **PASS** 54/54 public-full |
| Quiet 24-tab | 53 approve / 1 minor (`mgallery-collection`); **0 blockers** |
| AI-Assisted footnote (enriched) | **PASS** 55/0 |
| Momentum evidence quality | **PASS** |
| Mandatory release gates | **PASS** |
| Active universe SoT | **54** |
| Four Points Flex / House of Originals / Morgans / Radisson Collection | Untouched / excluded |
| Brand Status / release / CV / Source / Registry / images | **No writes** |

## What was patched (Critical-only)

Targeted Presentation **Title/Body**:

- Internal language: `source pack`, `source-supported`, `steward-matched`, Stage process clauses → owner-facing
- Design Hotels `source-supported` → `verified`
- SpringHill / TownePlace steward-matched footprint language
- SO/ `standards.requirement` Stage 3 / Brand Basics process clause removed; operator-compat residual cleaned
- Suburban Studios: false-positive archetype match on brand prefix `Suburban *` — restored verified catalog properties (Orlando / Columbus / Kennesaw); audit/cleanup regex now excludes `Suburban Studios`

## Tooling

- `npm run brand-explorer-global-active-semantic-audit -- --dry-run --fresh`
- `npm run brand-explorer-global-critical-blocker-cleanup` (`global-critical-blocker-cleanup-v2`)
  - Prefers refresh audit JSON
  - SO/ Brand Name aliases + presentation-record fetch fallback

## Reports

- `reports/brand-explorer-global-active-semantic-audit-refresh.*`
- `reports/brand-explorer-global-critical-blocker-failures.*`
- `reports/brand-explorer-global-critical-blocker-cleanup.*`
- `docs/data-intelligence/brand-explorer-global-critical-blocker-cleanup.md`

## Next (out of scope)

- High-severity semantic review (**179** High remain)
- Explicit 54 freeze only after High review + founder acceptance
