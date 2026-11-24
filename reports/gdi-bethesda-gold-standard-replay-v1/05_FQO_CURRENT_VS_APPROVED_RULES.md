# 5. FQO current vs founder-approved rules

| Rule | Founder standard | Current implementation | Verdict |
|------|------------------|------------------------|---------|
| 1 Demand thesis | Plausible / inferred / confirmed OK; reject speculation | Requires **top-level** evidence URL arrays (`sourceUrls`/`evidence`/`sources`) | **FAIL** vs Bethesda gold — 67/67 presentation rows fail `MISSING_EVIDENCE_URLS` even when demand narrative exists |
| 2 Hotel-specific fit | Defensible commercial rationale; not narrative length | `hotelFitOk` passes when thesis **length ≥ 40** (or ≥20 with market flag) | **MISALIGNED** — length proxy |
| 3 Actionable timing | Future / recurring / overflow pathways; past OK with next cycle | `hasFuturePathway` + historical exclusion | **PARTIAL** — 11 gold rows fail pathway; recurring logic incomplete |
| 4 Named person | Mandatory verified named individual | Named gate + junk-name filters | **MOSTLY ALIGNED** — 1 fail; 67/67 gold have names |
| 5 WHO ≠ HOW; org routes OK if labeled | Direct email **not** mandatory if named person + credible pathway | `PERSON_SPECIFIC_CHANNEL` requires email/direct phone/person LinkedIn; org routes rejected | **FAIL** — 17 gold rows fail channel despite named people / agency lines |
| 6 Commercial recommendation | Why / who / when / next action; not string length | `commercialActionOk` uses **length ≥ 20** | **MISALIGNED** — length proxy; gold 67/67 have actions |
| Historical compatibility | Do not retro-eliminate approved Bethesda | Hotel-wide marks 38 as `PREVIOUSLY_APPROVED_REVIEW` | **CORRECT INTENT**, but blocks candidate inclusion until FQO softens inheritance |

Additional defect: competitive incumbent regex hardcodes **Westin New York at Times Square** (Hilton NYC era) — inappropriate for Bethesda competitive rows (10 fails).
