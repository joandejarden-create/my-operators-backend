# GDI WHO Recall V12 — Results

Marker: `gdi_wave1_who_recall_v12_20260921`  
V11 checkpoint: `cbfd1af96de62e594f4f9d781f3572c1b69bc6f1`  
Precision law: **V11 unchanged**

## Summary

| Metric | Value |
|---|---:|
| Baseline named WHO opps | 8 / 20 (40%) |
| New named WHO opps | **5** |
| Total named WHO opps | **13 / 20 (65%)** |
| New valid people | **9** |
| New invalid (accepted) | **0** |
| Preferred threshold (≥60%) | **MET** |

## New WHO (all V11-confirmed)

| Hotel | Opportunity | New WHO | Role | Evidence | Valid | Public Email | Public Phone |
|---|---|---|---|---|---|---|---|
| St. Regis Mexico City | SAH 79th Annual International Conference | Christopher Kirbabas | Director of Programs | sah.org/staff | VALID | ckirbabas@sah.org | 312-573-1365 |
| St. Regis Mexico City | AoIR 2026 Conference | Ann McLean | Conference Coordinator | aoir.org/professional-staff | VALID | confcoordinator@aoir.org | — |
| St. Regis Mexico City | AoIR 2026 Conference | Stacy Wood | Program Chair | aoir.org AoIR2026 materials | VALID | aoirconfchair@aoir.org | — |
| Hotel Phillips KC | Kansas City Developers Conference 2026 | Nathan Mills | Executive Director | kcdc.info/about | VALID | staff@kcdc.info | — |
| Hotel Phillips KC | Kansas City Developers Conference 2026 | Heather Downing | Hospitality Coordinator | kcdc.info/about | VALID | staff@kcdc.info | — |
| Hotel Phillips KC | Big 12 Men's Basketball Tournament | Dominic Drury | Director – Men's Basketball Operations | big12sports.com staff | VALID | (Surfe) | 469-524-1000 |
| Hotel Phillips KC | Big 12 Men's Basketball Tournament | Brad Clements | Senior Director – Competition and Events | big12sports.com staff | VALID | (Surfe) | 469-524-1000 |
| Hotel Phillips KC | 2026 Animal Health Summit | Emily McVey | VP, KC Animal Health Corridor | onekc.org FAQ | VALID | mcvey@OneKC.org | 816-522-6168 |
| Hotel Phillips KC | 2026 Animal Health Summit | Kimberly Young | President, KC Animal Health Corridor | onekc.org FAQ | VALID | young@OneKC.org | 816-654-3617 |

## Recovery patterns (reusable)

| Pattern | Example | Rule |
|---|---|---|
| Org staff directory → conference owner | SAH / Christopher Kirbabas | STAGE_2_ORG_STAFF |
| Official event staff + program chair | AoIR / Ann + Stacy | STAGE_1 + STAGE_2 |
| Aggregator failure → official event domain | KCDC / Nathan + Heather | ALIAS_DOMAIN_FAILURE repair |
| Role-first championship ops | Big 12 / Dominic + Brad | STAGE_4_ROLE_SPECIFIC |
| Functional FAQ pivot | Animal Health / Emily | STAGE_7_FUNCTIONAL_PIVOT |

## Still unresolved (7)

See gap audit + research paths. Classified as researched-not-found / functional-only after gap-directed pass.
