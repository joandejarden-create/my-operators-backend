# Census Autopilot V2.3 — Final Report

**Version:** census-autopilot-v2.3-independent-universe  
**Mode:** DRY-RUN · No Airtable · No Webhound · Cvent never production evidence  
**Firewall:** Independent discovery cannot read Cvent challenge hotels (FAIL CLOSED)

---

## INDEPENDENT DISCOVERY

1. **Countries tested:** Mexico, Dominican Republic, Costa Rica, Colombia, Brazil, Argentina, Jamaica, Barbados
2. **Hotels independently discovered:** **1467**
3. **Branded:** **1065**
4. **Independent:** **402**
5. **Resorts:** **193**
6. **Urban:** **781**
7. **Soft/collection:** **156**
8. **Official-directory discoveries:** **1002**
9. **SerpApi discoveries:** **446**
10. **Other approved-source discoveries:** **19**

## CVENT BLIND COMPARISON

11. **Cvent challenge hotels in same geography:** **10521**
12. **BOTH:** **804**
13. **Independent-only:** **363**
14. **Cvent-only:** **9417**
15. **Probable:** **300**
16. **Conflicts:** **0**
17. **Overall blind rediscovery rate:** **10%**
18. **Rediscovery branded:** **59%** (n=1010)
19. **Rediscovery independent:** **5%** (n=9511)
20. **Rediscovery resort:** **13%**
21. **Rediscovery small hotels (proxy):** **5%**
22. **Rediscovery by country:** Mexico:20%, Dominican Republic:24%, Costa Rica:19%, Colombia:16%, Brazil:3%, Argentina:7%, Jamaica:16%, Barbados:38%

## CVENT-ONLY

23. **Independently resolved after freeze (sample):** **28**
24. **Remain unresolved (sample):** **12**
25. **Top reasons:** weak_match; no_serpapi_candidate
26. **Any Cvent factual field copied into production evidence?** **NO**

## INDEPENDENT-ONLY

27. **Legitimate independent-only hotels (count):** **363**
28. **Why absent from Cvent:** Non-meetings inventory, small hotels, branded directory hotels outside Cvent meetings index, soft brands, possible new openings / coverage gaps
29. **Meaningful inventory beyond Cvent?** **YES** — 363 independent-only in pilot

## CVENT RETIREMENT

30. **Remove as routine discovery dependency?** **Not yet for full 48** — pilot recall 10% vs 90% target
31. **Exact recall gap:** Long-tail independents + families without adapters (Hyatt/Accor/Wyndham/regional) + Cvent-only meetings venues
32. **Retirement threshold:** ≥90% blind rediscovery + meaningful independent-only + challenge-resolvable remainder
33. **Retain after resolution:** challenge_id, outcome, audit timestamp, matching status — **not** Cvent factual content

## VERIFIED CENSUS

34. **Lifecycle states:** DISCOVERED · IDENTITY VERIFIED · VERIFIED — MATERIAL GAPS · VERIFIED — ROOMS PENDING · VERIFIED — FIRST-PARTY VALIDATION PENDING · VERIFIED — GOLDEN COMPLETE · RESEARCH ESCALATION · INACTIVE / HISTORICAL · IDENTITY CONFLICT
35. **VERIFIED without Rooms?** **YES**
36. **VERIFIED without Golden Complete?** **YES**
37. **Minimum identity gate:** Exact/High independent confirmation + durable property_identity_id + country + non-Cvent provenance

## ROOMS

38. **Rooms validation queue size:** **1467**
39. **Native route:** IHG/Hilton/Choice/Marriott first
40. **First-party route:** Primary for empty official Rooms fields
41. **Deep research:** Independents / blocked official
42. **Top 10 FP orgs:** Marriott, Independent, IHG, Independent portfolio, Hilton, Choice, Accor, Wyndham, Hyatt, Best Western

## SERPAPI

43. **Updated rights classification:** Nuanced dimensions (see `19-serpapi-rights-state.json`) — **not** binary RIGHTS_BLOCKED
44. **Research allowed?** **YES**
45. **Persistence:** **CUSTOMER_RESPONSIBILITY_REVIEW — pending explicit persistence clarification**
46. **Images:** **SEPARATELY GATED — NOT APPROVED**
47. **Discovery calls used:** **25**
48. **Enrichment calls used:** **118**
49. **Calls per independently discovered hotel:** **0.02**

## GOLDEN ENRICHMENT

50. **Sample size:** **250**
51. **Average Priority Completeness:** **94%**
52. **≥95%:** **31%**
53. **≥95% excluding Rooms:** **47%**
54. **Rooms coverage:** **5%**
55. **First-party validation required:** **130**

## FULL UNIVERSE

56. **Forecast independently discoverable:** **~1584**
57. **Forecast official/native share:** see `27-full-48-country-forecast.json`
58. **Forecast SerpApi discovery share:** see forecast artifact
59. **Forecast hard/deep share:** see forecast artifact
60. **Forecast build search volume:** see forecast artifact

## BRAND EXPLORER

61. **Census foundation for Brand Explorer?** **YES (staging path)**
62. **Inactive brands completable via pipeline?** **YES — with governed gates**
63. **Gates before activation:** Brand Status, PVQL, Tab Factory, protected baseline, no Cvent provenance

## OPERATOR EXPLORER

64. **Naturally generate operator seeds?** **YES (sparse)** — brand-family seeds today
65. **Additional architecture needed:** dedicated operator research lanes, owner/management corroboration, temporal affiliation graph

## MAINTENANCE

66. **Detect new hotels?** **YES** — directory deltas + discovery queries
67. **Detect closures/reflags?** **YES (design)** — contradiction-first + temporal affiliation
68. **Build → continuous maintenance?** **YES (designed)** — see `26-build-to-maintenance-design.md`

## MOST IMPORTANTLY

69. **Independently constructed rather than copied from Cvent?** **YES — architecture + this pilot support the claim**
70. **Code enforces the claim?** **YES** — Cvent firewall FAIL CLOSED; discovery has no Cvent import path
71. **Cvent → temporary blind challenge / eventually disappear?** **YES as target; currently TEMPORARY CHALLENGE SET ONLY until recall threshold**
72. **Build/maintain own LATAM/Caribbean universe?** **PROMISING → path to PROVEN with adapter expansion**
73. **Verified Census → Brand Explorer / Operator Explorer foundation?** **YES**

---

## FINAL VERDICTS

| Area | Verdict |
|------|---------|
| **INDEPENDENT UNIVERSE** | **PROMISING** |
| **CVENT DEPENDENCY** | **TEMPORARY CHALLENGE SET ONLY** |
| **VERIFIED CENSUS** | **STAGING ONLY** |
| **SERPAPI** | **DOWNSTREAM-USE REVIEW** |
| **AIRTABLE** | **DRY-RUN ONLY** |

### Change Impact: **High** (discovery architecture / rights model) — no Airtable writes.  
### Rollback: ignore `data/research-engine-v2/census-autopilot-v2-3-independent-universe/`; firewall remains safe default.
