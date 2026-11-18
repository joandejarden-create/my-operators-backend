# Phase 1.1 — Certification baseline authority & reconciliation

**Status:** STOP for founder review  
**Generated:** 2026-11-18 (Europe/Paris)  
**Machine artifact:** `CERTIFICATION_BASELINE_AUTHORITY_RECONCILIATION.json`  
**Rule:** Publication ≠ certification. No production certification states were modified.

## Verdict

The reconstructed 27-property baseline is accepted as a **provisional historical inventory only**. It is **not** independent certification authority.

## 27 vs 28 filesystem reconciliation

| Count | What |
|------:|------|
| 27 | Hotel folders under `data/ai-demand-positioning/published/` |
| 1 | Non-hotel file `_cache_token.json` |
| 27 | Properties in reconstructed baseline JSON |
| 0 | Set diff baseline ↔ published hotel folders |

The previously reported “28” counted `_cache_token.json`. **Hotel inventory is 27 = 27.**

## Authority classification (published latest)

| Class | Count | Meaning |
|-------|------:|---------|
| `CERTIFIED_FORMAL_COMPARABLE` | 3 | Certification-era + CERTIFIED + `formalComparisonUsable=YES` (Hilton TS, Renaissance TS, YOTEL Geneva) |
| `LEGACY_PUBLISHED_MANIFEST_CLAIMS_CERTIFIED` | 20 | Manifest stamps `CERTIFIED` without independent certification-era formal authority |
| `LEGACY_PUBLISHED_NO_CERT_FIELD` | 4 | Published Live with null/missing certification stamp |

## Provenance preserved

| Artifact | Role |
|----------|------|
| `reports/ai-demand-positioning/certification-baseline/ADP_EXISTING_HOTEL_CERTIFICATION_BASELINE_V1.json` | Reconstructed inventory (`…_reconstructed`, 2026-11-18T08:09:36.200Z) |
| `reports/adp/certification-inventory-reconciliation/*` | Orthogonal status model (QA ≠ CERTIFIED; LEGACY_QA_PASS ≠ CERTIFIED) |
| Published manifests under `data/ai-demand-positioning/published/*/manifest.json` | Filesystem Primary SoT for **publication**, not certification |

## Freeze / original certification hunt

| Artifact | Found? |
|----------|--------|
| Original pre-reconstruction certification baseline freeze | **No** |
| Reconstructed baseline (this tree) | Yes |
| Renaissance six-hotel `BASELINE_FREEZE.json` | Yes (property-specific) |
| CALA / Monterrey Valle identity freezes | Yes (identity, not ADP cert authority) |

## Inventory CSV coverage gap

`CANONICAL_PROPERTY_INVENTORY.csv` has **20** rows. **7** published hotels lack inventory rows:

- `adp_castillo_hotel_son_vida`
- `adp_mayflower_hotel_washington_dc`
- `adp_radisson_blu_toronto_downtown`
- `adp_sheraton_mallorca_arabella_golf`
- `adp_westin_grand_munchen`
- `adp_westin_tampa_waterside`
- `adp_wyndham_san_diego_bayside`

## Comparability

Baseline `priorComparablePeriodId` is claimed from prior published period presence. **Not validated** as measurement-contract comparable unless `formalComparisonUsable=YES`. Do not treat previous periods as comparable by default.

## Uncertainties (remaining)

1. Original independent certification freeze still missing.
2. Inventory CSV incomplete vs 27 published hotels.
3. Manifest `CERTIFIED` stamps remain common and are **not** treated as certification authority.
4. No production certification states were rewritten to “correct” stamps (intentional).
