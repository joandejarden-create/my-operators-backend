# Global Active — High-Severity Semantic Cleanup Batch 1

**Ready:** `global_high_semantic_cleanup_complete_ready_for_medium_review`  
**Freeze:** Do **not** freeze 54 yet — Medium (12) remains; founder review before protected 54 freeze.

## Summary

| Item | Result |
|------|--------|
| Pre-Batch-1 High (refresh audit) | **179** |
| Critical before / after | **0 / 0** |
| Primary Batch 1 patches applied | **174** across **42** brands |
| Residual lifecycle patches | **5** (`Confirm owner reporting expectations…`) |
| Mama Shelter momentum residual | Hide duplicate brand-page card; restore Paris East `Directory` date-line contract |
| Post-cleanup High | **0** |
| Post-cleanup Medium | **12** (deferred — not Batch 1) |
| Internal Language section | **54/54 pass** |
| Portfolio Mix section | **54/54 pass** |
| Recent Momentum section | **54/54 pass** |
| Freeze buckets | **48 freeze_safe / 6 minor_cleanup** |
| Fresh audit ready statement | `global_active_semantic_audit_complete_review_freeze_decision` |

## What Batch 1 patched

### A. Lifecycle / internal language
- Removed `Confirm owner, operator, and brand responsibilities… stays deliverable after affiliation` boilerplate
- Rewrote `Confirm owner versus operator reporting…` and `Confirm owner reporting expectations…`
- Rewrote `Confirm operator capacity/responsibilities…`
- `Keep sibling lines…` → adjacent-brand owner language (ibis)
- `fee-stack` → `affiliation fee` (Quality Inn)

### B. Portfolio Mix prose → curated sample %
Brands: everhome-suites, fairmont, ibis, mama-shelter, mercure, novotel, pullman, so-hotels-and-resorts  
Labeled **Curated sample mix** (illustrative, not disclosed census).

### C. Recent Momentum weak semantics
- Mama Shelter brand-page card hidden (`Do Not Display`) — stronger Mexico City pipeline card already existed
- Paris East directory card restored to structured `Directory` date-line + As of 2026 owner context + URL

## Forbidden writes avoided

- Brand Status / release / Active Profile Approved / Ready for Active Profile / Founder Visual Review Pass
- Company Validated / Company Validation Date
- Source Library status / Registry approval
- Images
- Four Points Flex / House of Originals / Morgans Originals / Radisson Collection
- Broad profile rewrites / baseline freeze artifacts

## Tooling

```bash
npm run brand-explorer-global-high-severity-cleanup -- --batch 1 --dry-run
npm run brand-explorer-global-high-severity-cleanup -- --batch 1 --apply [confirm flags…]
```

Lib: `lib/partner-intelligence/brand-explorer-global-high-severity-cleanup.js`  
Script: `scripts/brand-explorer-global-high-severity-cleanup.mjs`

## Reports

- `reports/brand-explorer-global-high-severity-failures.*`
- `reports/brand-explorer-global-high-severity-batch1-plan.md`
- `reports/brand-explorer-global-high-severity-cleanup-batch1.*`
- `reports/brand-explorer-global-active-semantic-audit-refresh.*` (post-apply)

## Next

- Medium review (12 findings) before any protected 54 freeze
- Quiet PVQL / 24-tab / footnote / evidence / mandatory gates (post-apply validation)
