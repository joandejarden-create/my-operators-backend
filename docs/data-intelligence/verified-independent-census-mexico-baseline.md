# Verified Independent Census — Mexico Baseline (Locked)

> **Status:** `mexico_vic_baseline_locked_ready_for_marriott_wave1d`  
> **Authority:** Staging research baseline for Dealality VIC — not production Hotel Census.  
> **Locked artifacts:** `data/research-engine-v2/verified-independent-census-mexico-combined/`  
> **Reports:** `reports/research-engine-v2/verified-independent-census-mexico-combined.{md,json}`

## What is locked

Independently reconstructed Mexico hotel universe across three parent companies:

| Family | Wave | Independent hotels |
|--------|------|--------------------|
| IHG | 1A | 195 |
| Hilton | 1B | 102 |
| Choice | 1C | 68 |
| **Combined** | — | **365** |

## Non-negotiables (still in force)

1. No Airtable writes from this baseline alone  
2. No Brand Explorer activation  
3. No Webhound / no credit spend for reconstruction lock  
4. No legacy values as independent research evidence  
5. No fuzzy-only property merges  
6. No fabricated temporal affiliation start dates  
7. No production overwrite of legacy Hotel Census  
8. 403 / Blocked ≠ closed / reflagged  

## Architecture locked in

- Research firewall (fail-closed until freeze)
- Verified Independent record model + field provenance
- Freeze hashes (per-wave + combined)
- Property Identity V1
- Temporal Affiliation V1
- Production eligibility (data vs image gates)
- Source-rights registry updates per wave

## Completeness snapshot

| Family | Core | Material | Data-eligible |
|--------|------|----------|---------------|
| IHG | 100% | 56% | 191 |
| Hilton | 100% | 71% | 102 |
| Choice | 97% | 56% | 50 |

## Next step

**Marriott Mexico Wave 1D** — launch only with explicit steward approval. Do not auto-start from this document.

## Related paths

- IHG: `data/research-engine-v2/verified-independent-census-v1/`
- Hilton: `data/research-engine-v2/verified-independent-census-wave1b-hilton/`
- Choice: `data/research-engine-v2/verified-independent-census-wave1c-choice/`
- Combined lock: `data/research-engine-v2/verified-independent-census-mexico-combined/`
