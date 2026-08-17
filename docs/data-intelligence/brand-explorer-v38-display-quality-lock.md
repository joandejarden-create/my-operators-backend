# v38 Brand Explorer Display Quality Lock

Global external rendering contract: only `external_owner_ready` and `active_profile_ready` render full tabs.

```bash
npm run brand-explorer-v38-display-quality-lock-audit -- --brands hotel-indigo,mgallery-collection,design-hotels,small-luxury-hotels-of-the-world,everhome-suites,kimpton-hotels,radisson-individuals-by-choice --dry-run
npm run test:brand-explorer-external-quality-lock -- --brands hotel-indigo,mgallery-collection,design-hotels,small-luxury-hotels-of-the-world,everhome-suites,kimpton-hotels,radisson-individuals-by-choice
```

## External rule
All other display states render **Profile in Preparation** only — no tabs, no fallback cards, no helper text.

## Internal preview
Append `?beInternalPreview=1` to see incomplete sections labeled **Internal preview · Not owner-ready**.