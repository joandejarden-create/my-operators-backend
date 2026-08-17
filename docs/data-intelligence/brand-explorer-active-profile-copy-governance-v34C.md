# Brand Explorer Active Profile Copy Governance v34C

Generic safety rules + **brand-specific** rewrites. No interchangeable boilerplate.

## Stage

```bash
npm run brand-explorer-active-profile-copy-governance -- --brand suburban-studios --dry-run
```

Pipeline position:

`preflight` → `asset-pack` → `build-draft` → **`copy-governance`** → `founder-review` → `apply-approved` → `final-qa`

## Modules

| Module | Role |
|--------|------|
| `brand-explorer-active-profile-copy-governance-config.js` | Per-brand slot rewrites, positioning pillars, founder notes |
| `brand-explorer-active-profile-copy-governance-builder.js` | Audit, rewrite, founder review queue, apply |

## Rules

1. **Audit** — FDD, item 19, ADR, RevPAR, net contribution, fee stack, consumer site, booking path, source metadata, etc.
2. **Rewrite** — Brand-specific slot packages (Suburban / WoodSpring / Everhome); regex repairs; sanitize-only fallback.
3. **No generic filler** — Rewrites blocked if interchangeable or missing brand anchor terms.
4. **Source support** — Each rewrite traces to consumer URL, development URL, or approved Source Library record.
5. **Founder queue** — Rows without a brand-specific rewrite are **not** filled with generic text.

## Apply (copy only)

```bash
npm run brand-explorer-active-profile-apply-approved -- --brand suburban-studios --apply \
  --approve-brand-explorer-active-profile-copy-governance
```

Copy apply blocked when `founderReviewQueue` is non-empty.

## Brands configured

- **suburban-studios** — economy extended-stay studio positioning
- **woodspring-suites** — practical extended-stay (imports v33B slot packages)
- **everhome-suites** — new-construction extended-stay (imports v32D slot packages)
