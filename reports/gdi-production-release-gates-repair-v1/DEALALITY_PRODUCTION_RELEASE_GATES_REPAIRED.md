# DEALALITY_PRODUCTION_RELEASE_GATES_REPAIRED

**Status:** gates repaired; focused PR prepared from `fix/gdi-production-release-gates-v1`  
**Prior blocked candidate:** `6373c019bba940210f78a32c20c3653f9e779c6b`  
**Scope:** release-stabilization only — no research merge, no customer publication changes, no Airtable writes, no Railway deploy

## 1. Customer visibility — root cause

**A — outdated fixture** (+ minimal commercial-relevance guard).

`keepOpp` used `Housing Desk` / `Meetings` with no usable route → `HELD_FOR_WHO_RESEARCH` (`buyer_contact_path_insufficient`, `account_quality_insufficient`). Fixture predated the commercial contact-path taxonomy.

## 2. YOTEL approved publication?

**No.** Durable registry `listPublished(recrPQcZg7SFARRb2)` = **0**. Portfolio audit: `NOT_PUBLISHED`. Research `opportunities.json` ≠ publication. Canonical HPC: `recrPQcZg7SFARRb2`.

## 3. YOTEL test failure — root cause

Invariant required research-side `facing.length >= 1` without frozen `nowDate`. AidEx events ended 2026-10-22; under 2026-11-07 facing = 0. Treated research as approved publication.

## 4. Corrections

| File | Change |
|---|---|
| `scripts/test-gdi-customer-visibility.mjs` | Named buyer + entry-point positives; desk/press/unsupported negatives |
| `lib/group-demand-intelligence/buyer-contact-path-taxonomy-v1.js` | Reject press/media/IR named contacts |
| `scripts/test-gdi-customer-publication-invariants-v1.mjs` | YOTEL = no approved publication; research ≠ publish; Spice retained |

## 5. Commercial standards preserved

Named commercially relevant buyer OR functional commercial entry point + hotel demand thesis → Ready. Unrelated press / desk-without-route / unsupported demand → held. No booking-authority / RFP / two-contact universal requirement.

## 6. Publication inventories unchanged

Spice durable seven retained. YOTEL unpublished. No publish/copy from research. Bethesda/Waterstone inventories not mutated.

## 7. Local gate results (this worktree)

| Gate | Exit |
|---|---:|
| `test:gdi-customer-visibility` | 0 |
| `test:gdi-customer-publication-invariants-v1` | 0 |
| `test:gdi-full-content-parity-v1` | 0 |
| `test:gdi-local-published-inventory-mirror-v1` | 0 |
| `test:gdi-research-audit-admin-relocation-v1` | 0 |
| `test:gdi-need-date-customer-presentation-v3` | 0 |
| `test:gdi-one-click-publish-auth-v1` | 0 |
| `assert:production-assets` | 0 |
| `test:dealality-production-release-coordinator` | 0 |
| `test:dealality-production-release-github-actions-contract` | 0 |
| `test:adp-admin-workspace-navigation-v1` | env miss (no `playwright` in clean WT; covered by research-audit relocation unit gate) |

Secret scan: CI `secret-scan-pr.yml` / gitleaks (local gitleaks binary absent).

## 8–11. PR / SHA / dry-run

Filled after push/merge.

## Aimbridge cohort (separate; not in this PR)

Branch `research/aimbridge-three-hotel-full-e2e-v1` — identity test **PASS**:
1. Sheraton Guadalajara Expo `recCAGqkERSXgiku4`
2. voco Cancún Zona Hotelera `recTk5I8VOzsPhxDz`
3. Wyndham Alltra Punta Cana `recXh4NgF8sDbi9cQ`
Forbidden: Monterrey `recsn3BUKJ9PNfeZW` (archived), duplicate `reccax9nT5J90Sc0Y`.
