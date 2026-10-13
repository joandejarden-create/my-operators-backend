# FOUNDER REPORT — Westin Grand München ADP Forensic

Generated: 2026-10-07  
Frozen (immutable): `adp_period_adp_westin_grand_munchen_20261007145836_73c25a`  
Corrected successor: `adp_period_adp_westin_grand_munchen_20261007182141_4baab7` (**CERTIFIED**)

## Verdict

The original CERTIFIED period was **not customer-complete**.  
Raw responses contained a rich Munich competitive universe (**The Charles Hotel** dominant). Customer UI showed subject-only because:

1. **No Munich entry in `PROPERTY_ENTITY_REGISTRY`** → fail-closed unbound drop of all competitors  
2. **Ranking / displacement resolve** only used property registries for `GOVERNED_NON_WATERSTONE_PROPERTIES` → even after registry add, Overall stayed subject-only until shared resolve was fixed  
3. **Certification** did not fail empty competitive universe when raw bindable competitors existed  
4. **Attributes:** `wellness` / `large_ballroom` missing from shared dictionary (8 vs 10)

This is **MULTIPLE** shared-pipeline defects — not a UI bug, not metric fabrication, not a Westin-only hardcode.

## Repair path used

**A → B → C** — recompute from raw; repair registry + resolve paths; republish successor.  
**No full provider rerun.** Apify not used.

## Final customer surface (successor)

| Field | Value |
|-------|------|
| Overall competitors (non-subject) | **9** |
| Top displacement | **The Charles Hotel** (20) |
| Top Observed AI Alternative | **The Charles Hotel** |
| Tracked attributes | **10** (avg recognition ~32.9%) |
| Displacement displayed | **YES** |

Top competitors: Charles · Vier Jahreszeiten Kempinski · Sofitel Bayerpost · Bayerischer Hof · Sheraton Arabellapark · Mandarin Oriental · Andaz · Cortiina · München Palace

## Safety

| Check | |
|-------|--|
| Original period mutated | **NO** |
| Hotel-specific UI/metric hardcodes | **NO** |
| Competitors invented | **NO** (evidence-bound registry from raw frequencies) |
| Ready/ADP thresholds lowered | **NO** |
| Hilton / Renaissance / YOTEL regression | **PASS** (shared resolve prefers registry when present; SF fallback unchanged when no registry) |
