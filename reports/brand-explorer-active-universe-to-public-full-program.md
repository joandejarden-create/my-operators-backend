# Active Universe → Public-Full Program

Version: `active-universe-to-public-full-v1` · Generated: 2026-07-23T13:22:20.336Z
Mode: **dry-run**

## Active universe source of truth

- Brand Basics Brand Status Active/Live
- `OR({Brand Status}='Active', {Brand Status}='Live')`

## Lanes

### final-public-full-validation

Targets: —

```json
{
  "activeCount": 24,
  "publicFullCount": 24,
  "notPublicFullCount": 0,
  "unconfiguredCount": 0,
  "publicFullSlugs": [
    "ascend",
    "autograph-collection",
    "bw-premier-collection",
    "bw-signature-collection",
    "comfort-inn-suites",
    "country-inn-suites",
    "curio-collection",
    "design-hotels",
    "everhome-suites",
    "handwritten-collection",
    "hotel-indigo",
    "kimpton",
    "mgallery-collection",
    "preferred-hotels-and-resorts",
    "quality-inn",
    "radisson-blu",
    "radisson",
    "radisson-individuals-by-choice",
    "radisson-red",
    "small-luxury-hotels-of-the-world",
    "suburban-studios",
    "tribute-portfolio",
    "vignette-collection",
    "woodspring-suites"
  ],
  "notPublicFullSlugs": [],
  "unconfiguredSlugs": []
}
```

```json
{
  "activeCountIs24": true,
  "publicFullIs24": true,
  "noUnconfigured": true,
  "excludedStatusConflicts": [
    "radisson-collection",
    "tapestry-collection-by-hilton"
  ],
  "nextCommands": [
    "npm run test:brand-explorer-public-visibility-quality-lock -- --public-full-only",
    "npm run brand-explorer-os -- --stage release-readiness --dry-run --skip-regression",
    "npm run test:brand-explorer-mandatory-release-gates",
    "npm run brand-explorer-active-universe-source-of-truth -- --dry-run"
  ]
}
```

## Excluded from active universe

- `radisson-collection` — Brand Status Draft — not Active/Live
- `tapestry-collection-by-hilton` — Brand Status Under Review — not Active/Live

## Final acceptance snapshot

- Active universe: **24**
- public-full: **24**
- PVQL inventory lockPass: **24** (`reports/_tmp-active-universe-pvql-inventory.json`)
- OS PRIMARY_RELEASE: **7 / 7** `active_profile_ready` → `no_action`
- Mandatory release gates: PASS
- Company Validated / Source Library / Registry / Brand Status: untouched
- Status conflicts remain excluded (no Brand Status writes)

