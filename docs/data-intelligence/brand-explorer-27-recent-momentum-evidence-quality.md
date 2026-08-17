# Recent Momentum / Openings Evidence Quality (27-wave)

Gate version: `recent-momentum-evidence-quality-v1`  
Audit version: `27-recent-momentum-evidence-audit-v1`

## Purpose

Permanent factory gate ensuring Recent Momentum and Openings/Examples/Properties use source-backed dates/links, correct region labels (CALA first when available; otherwise International Reference), brand-correct evidence, and no raw URLs in public rendered HTML.

## Targets

- `dazzler-by-wyndham` (CALA available)
- `trademark-collection-by-wyndham` (no CALA in pack → International Reference)
- `tapestry-collection-by-hilton` (no CALA in pack → International Reference)

## npm

```bash
npm run brand-explorer-27-recent-momentum-evidence-audit -- --brands dazzler-by-wyndham,trademark-collection-by-wyndham,tapestry-collection-by-hilton --dry-run
npm run test:brand-explorer-recent-momentum-evidence-quality -- --brands dazzler-by-wyndham,trademark-collection-by-wyndham,tapestry-collection-by-hilton
```

## Gate failure conditions

- Missing/invalid date
- Missing source URL (Body third unit for frontend link render)
- Raw URL visible in public HTML text (outside href) — Recent Momentum and Openings / Examples / Properties
- Wrong-brand / sibling-brand evidence
- Invented year on property-listing-only cards without announcement framing
- CALA available but unused / not prioritized
- Non-CALA examples missing International Reference label
- Thin momentum body (<35 words)
- Generic / diligence-filler momentum copy
- Openings source URL does not match property-distinctive title tokens

## Data contract

- Table: Brand Setup - Brand Explorer Presentation
- Momentum Body shape: `dateLine`, summary, and `https://url` as separate units (blank-line *or* single-newline separators). Atelier extracts the URL into `momentum-feed__link`.
- Openings: Case Summary Tags + structured Body chips; trailing URL matched **by property name** to Lane2 catalog (never by array index)
- No CV / Source Library / Registry / Brand Status / release / Image writes

## Renderer / test notes

- Public atelier recovers trailing URLs from momentum description and hardens `isSafeHttpUrl` when `URL` is unavailable.
- Node HTML gates must expose `URL` / `URLSearchParams` in the atelier VM sandbox (`brand-explorer-atelier-render-test-loader.js`).

## Acceptance (2026-07-24)

| Check | Result |
| --- | --- |
| Evidence gate (Dazzler / Trademark / Tapestry) | PASS |
| Tab section quality audit dry-run (27) | PASS — all `approve_for_baseline_freeze` |
| PVQL `--public-full-only` (27) | PASS |
| OS release-readiness dry-run | PASS (exit 0) |
| Mandatory release gates | PASS |
| Openings source URLs match property names | PASS (index-mismatch fixed) |
| Protected 24 / CV / Source / Registry / Brand Status | Untouched |

Last report: 2026-07-24T10:47:08.834Z
