# V3.1 Final Report — 250-Property Unseen Scale Proof

**Run:** `cav31_2026-08-08T16-48-19-242Z`  
**Elapsed:** ~10.8 min (647650 ms)  
**Artifacts:** `data/research-engine-v2/census-autopilot-v3-1-scale-proof/`

---

## COHORT
1. Frozen properties: **250**
2. Outside repaired original 150: **YES** (excluded `cav3_2026-08-08T15-04-05-566Z`)
3. Countries: MX 159, BR 47, DO 12, AR 10, BB 10, JM 7, CR 5
4. Families: IHG 176, Choice 50, Marriott 24 (Hilton NEW_INSERT pool exhausted after exclude — queue-driven, not hand-tuned)
5. Mix: **74 NEW_INSERT** / **176 EXACT_EXISTING**
6. Frozen before research: **YES**
7. Property-specific tuning after freeze: **NO**

## RESEARCH
8. Researched: **250**
9. Independently verified (auto-write eligible): **250**
10–12. Official/native first; SerpApi-assisted where EV justified (**75** searches)
13. Research failures tracked in cost.failed (source-health)

## IDENTITY
14. Exact/High eligible: **250** (74 new + 176 exact)
15. Probable: **0** in write cohort
16. Conflicts: **0** in write cohort
17. Duplicate insert attempts prevented: **yes** (re-query + index)
18. Actual duplicate inserts: **0**

## GOLDEN COMPLETENESS
19. Avg staging Priority Completeness: **95.5%**
20. Avg production-eligible Priority Completeness: **95.5%**
21. Median staging: **100%**
22. Hotels ≥95% staging: **173/250**
23. Hotels ≥95% production: **173/250**
24. Hotels ≥95% excl Rooms: **173/250**
25. Golden Complete: **0**
26. Verified — Rooms Pending: **250**
27. Material Gaps (<80%): **23**

## GEOGRAPHY
28. State/Region: **89.6%** (224/250)
29. Market: **100%**
30. Applicable Submarket: **98.2%** (108/110; 140 NA)
31. Address: **70%**
32. Coordinates: **90.4%**
33. Phone (conditional applicable): **100%** of applicable (173/173; others NA)
34. READY: **Mexico, Dominican Republic, Costa Rica**
35. PARTIAL: **Brazil**
36. NOT READY: **Argentina, Jamaica, Barbados** (thin official city/address on this unseen slice)

## ROOMS
37–38. Resolved **0** / unresolved **250**
39. Resolution **0%**
40–42. First-party validation by family in `23-first-party-validation-queue.json`
43. Rooms dominant remaining Priority gap: **YES**

## SERPAPI
44. Searches used: **75**
45. Searches/property: **0.3**
46. Searches/verified hotel: **0.3**
47. Material fields gained: contact/geo gap fills (rights-gated for production)
48. Official-only avoidance: EV path avoided most calls
49. SerpApi-only blocked production claims: claim-level rights enforced
50. Blocked SerpApi suppressed official: **NO**

## PRODUCTION
51. Pilot A attempted: **25**
52. Pilot A passed: **YES** (100% expected/actual; continuation gate pass)
53. Pilot B executed: **YES** (225)
54. Actual inserts: **74**
55. Actual updates: **174**
56. Skips: **2**
57. Blocked: **0**
58. Steward review: Rooms Pending (not written)
59. Fields written: **2595** (556 Pilot A + 2039 Pilot B)
60. Expected-vs-actual: **100%**

## SAFETY
61. Duplicate inserts: **0**
62. Unintended overwrites: **0**
63. Identity errors: **0**
64. Cvent leakage: **0**
65. Legacy leakage: **0**
66. Provenance failures: **0**
67. Rights violations: **0**
68. Unexpected mutations: **0**
69. Transaction-log failures: **0**
70. Rollback complete: **YES** (`19-rollback.json` / `26-rollback-simulation.json`)

## GENERALIZATION
71. Original repaired 150 staging: **~97.5%**
72. New unseen 250 staging: **95.5%**
73. Difference: **-2.0 pp**
74–75. Geography: original V3.0.3 hit 100% State on repaired 150; unseen 250 State **89.6%** with Caribbean/AR weak
76. Deterministic geography generalized: **PARTIAL** (strong MX/DO/CR; weak AR/JM/BB on this cohort)
77. Canonical field research generalized: **YES** (95.5% staging)
78. Write safety generalized: **YES**
79. Material degradation: Address/State on Caribbean + some Argentina/Jamaica rows with sparse official city/address

## FULL UNIVERSE
80. Freeze universe: **1467**
81. Remaining outside production (approx): freeze − prior waves − this 250 (see `29-full-universe-forecast.json`)
82–88. Forecast uses observed V3.1 cost/runtime — SerpApi ~0.3/property; Rooms Pending dominant; runtime ~11 min / 250

## AUTOPILOT
89. Safe properties research→Airtable without per-property Joan approval: **YES** (after standing auth)
90. Exceptions separable: **YES** (Rooms Pending / steward / blocked rights)
91. Paid-search EV: **YES** (75/250)
92. Stop failing adapter: source-health tracked (breaker policy retained)
93–94. Checkpoint/resume: research checkpoints every 25; `--resume-research` supported
95. Manually authorized 250-waves still technically necessary: **NO** if standing auth granted
96. Replace with: **CENSUS AUTOPILOT V4 STANDING AUTHORIZATION**

## MOST IMPORTANTLY
97. Unseen production cohort (not repaired 150): **YES**
98. Autonomous high-quality Census from independent research: **YES** (with Rooms Pending)
99. Safe production Airtable writes: **YES**
100. Evidence/provenance maintained: **YES**
101. Rooms principal unresolved Golden field: **YES**
102. Ready for standing Census Autopilot authorization: **YES — prepare V4; do not auto-run another batch**

---

## FINAL VERDICTS

| Area | Verdict |
|------|---------|
| **V3.1 SCALE PROOF** | **PASS** |
| **RESEARCH ENGINE** | **AUTONOMOUS** |
| **DETERMINISTIC GEOGRAPHY** | **PARTIAL** |
| **PRODUCTION WRITES** | **SCALE PROVEN** |
| **ROOMS** | **DOMINANT REMAINING GAP** |
| **FULL CENSUS AUTOPILOT** | **READY FOR STANDING AUTHORIZATION** |

No further wave launched from this task. No Webhound calls. No Brand/Operator Explorer writes.
