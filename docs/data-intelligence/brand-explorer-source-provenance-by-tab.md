# Brand Explorer source provenance by tab

Validates that each Brand Explorer tab/section uses an acceptable source mix.

## Hierarchy

1. Brand-specific official site  
2. Brand-specific development page  
3. Brand-specific property pages  
4. Parent-company brand page  
5. Parent-company corporate page  
6. Third-party (supplementary only)

## Canonical rules

| Brand | Required | Parent context only |
| --- | --- | --- |
| hotel-indigo | hotelindigo.com | ihg.com, ihgplc.com, development.ihg.com |
| mgallery-collection | mgallery.accor.com | group.accor.com, all.accor.com, accor.com |
| small-luxury-hotels-of-the-world | slh.com | none (no forced franchise logic) |

Parent pages may support ownership / family / platform / portfolio. They may **not** dominate brand positioning, audience, design story, property examples, images, scenarios, owner fit, or differentiators.

## Command

```bash
npm run brand-explorer-source-provenance-by-tab -- --brands hotel-indigo,mgallery-collection,small-luxury-hotels-of-the-world --dry-run
npm run test:brand-explorer-source-provenance-by-tab -- --brands hotel-indigo,mgallery-collection,small-luxury-hotels-of-the-world
```

Reports: `reports/brand-explorer-source-provenance-by-tab.{json,md}`
