# Brand Explorer — Wave 13 Partial Release

> **Ready (write stages):** `wave13_six_brand_partial_release_complete_so_held`  
> **Acceptance complete:** false until PVQL public-full-only + 45 quality freeze pass  
> **Validation:** 2026-07-27T19:07:34.453Z

## What shipped

Stages **9–10** promoted and publicly released **six** founder-approved Accor brands only.

| Brand | Slug | Brand Status | Release fields |
| --- | --- | --- | --- |
| Mama Shelter | `mama-shelter` | Active | yes |
| Mercure | `mercure` | Active | yes |
| ibis | `ibis` | Active | yes |
| Novotel | `novotel` | Active | yes |
| Pullman | `pullman` | Active | yes |
| Fairmont | `fairmont-hotels-and-resorts` | Active | yes |

## Held / excluded

| Brand | Treatment |
| --- | --- |
| SO/ (`so-hotels-and-resorts`) | **Held** — Brand Status remains Under Review; no release fields; not in intentional restore registry; not in Active universe |
| The House of Originals | Excluded (unchanged) |
| Morgans Originals | Untouched |
| Radisson Collection | Untouched |

## Universe

- Before Stage 9: **39** Active/Live
- After Stage 9–10: **45** Active/Live
- Public-full render: **45/45** `shouldRenderFullProfile=true`
- SoT buckets: **39** `public_full_clean` + **6** `public_full_failing_pvql` (Wave 13 six)

## Protected-field safety

- Company Validated / Company Validation Date: untouched
- Source Library status: untouched
- Registry approval/status: untouched
- Presentation content / images: no Stage 9–10 rewrites
- Protected original 39 Basics: untouched

## Validation results

| Gate | Result |
| --- | --- |
| Active universe SoT | 45 |
| PVQL overallPass | true (legacy may flag) |
| PVQL `--public-full-only` | **FAIL** (6 Wave 13) |
| 24-tab section quality | **do_not_freeze_remediation_required** (39/6) |
| Tab-factory (six) | failFindings=0, auditPass=false (geo + momentum section pattern) |
| Recent momentum evidence (permanent targets) | PASS |
| Mandatory release gates | PASS |

## May we freeze a 45-brand baseline now?

**No.** Wait for:

1. Wave 13 section-pattern cleanup on the six (geo regions ≥3 + momentum card structure) so PVQL public-full-only and quality freeze pass, and
2. SO/ minor cleanup + founder re-review + promotion (or an explicit interim-45 founder decision).

Prefer keeping protected baseline at **39** until then.

## Reports

- `reports/brand-explorer-wave13-partial-status-promotion.{json,md}`
- `reports/brand-explorer-wave13-partial-public-release.{json,md}`
- Fresh PVQL / SoT / 24-tab / tab-factory audits under `reports/`

## Next

Do **not** start SO/ cleanup in this packet. Queue a dedicated Wave 13 post-release section-pattern remediation for the six public brands, then re-run PVQL `--public-full-only` and 24-tab quality before any 45 baseline freeze.
