# Count Reconciliation

## Prior output
`CERTIFIED/PASS COUNT AFTER = 3` and `LEGACY_ONLY COUNT AFTER = 17`

## Root cause (exact)
**A + B + C (combined):**

1. **A — Only Hilton / Renaissance / YOTEL** have `globalCertificationEngineVersion` → **CERTIFICATION_ERA_OFFICIAL** + governed **CERTIFIED**.
2. **B — Cambridge / Hotel Caribe / NOW NOW / Bethesda** (and other legacy Live hotels) only **passed engine dry-run** (`engineStatus=CERTIFIED` / LEGACY_QA_PASS). Audit remaps official status to **LEGACY_UNCERTIFIED** / reporting **LEGACY_QA_PASS**. They were **not** retro-stamped as certification-era CERTIFIED.
3. **C — Terminology inconsistency:** founder pack used `engineStatus` as "FINAL STATUS: CERTIFIED" for the three review cases, and collapsed certification-era CERTIFIED with a combined `CERTIFIED/PASS` inventory bucket, while the same pack bucketed legacy QA-pass hotels as `LEGACY_ONLY`.

Prior "3" = **certification-era CERTIFIED official periods only** (Hilton, Renaissance, YOTEL).  
Prior "17" = **legacy official current periods** (including hotels that dry-run QA PASS).

No silent state-model corruption of periods; reporting mixed axes.
