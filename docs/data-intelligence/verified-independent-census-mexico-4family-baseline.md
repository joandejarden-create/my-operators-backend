# Mexico VIC 4-Family Baseline (Locked)

> **Status:** `mexico_vic_4family_baseline_locked_staging_ready`  
> **Locked at:** 2026-08-04T23:13:45.164Z  
> **Artifacts:** `data/research-engine-v2/verified-independent-census-mexico-combined-4family/`  
> **Reports:** `reports/research-engine-v2/verified-independent-census-mexico-combined-4family.{md,json}`  
> **Freeze hash:** `c1cb244a95d7311b4ab2cf31d4988685879ef492f4f6420710633267d0effda3`

## Snapshot

| Family | Records | Data-eligible |
|--------|--------:|--------------:|
| IHG | 195 | 191 |
| Hilton | 102 | 102 |
| Choice | 68 | 50 |
| Marriott | 301 | 237 |
| **Total** | **666** | **580** |

## Constraints

Staging only · No Airtable · No Webhound · No BE activation · No production overwrite · No cross-family auto-merge · Marriott steward overlay only (Wave 1D freeze unmodified)

## Re-lock

```bash
npm run research-engine-v2:lock-mexico-vic-4family-baseline
```
