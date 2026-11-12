# Bethesda GDI — Live Path Leak Fix (Fail-Closed)

**Hotel:** Bethesda Marriott (`recLuxvwwxID7U2B8`)  
**Date:** 2026-11-12  

## Verdict

The disposition unit tests from #194/#195 were correct. The **running localhost Node process was stale** (started before merge) and continued serving the pre-filter route. Combined with a **fail-open try/catch** and a **UI priorityFilterKey bug**, excluded records remained founder-visible.

## Founder questions

1. **Why did the founder still see Marriott HQ after PR #194/#195?**  
   Localhost `server.js` PID 25364 started at 04:10; #194 merged ~04:22. The process never reloaded the new filter. Disk store already had the Market Intelligence title patch, but the API still returned the full family-collapsed 83-card union (`listScope=BETHESDA_INTERNAL_UNION_88`) without `commercialSurface` metadata. Marriott HQ remained WATCHLIST + Build Account on that payload.

2. **Exact page/API path that leaked it.**  
   - Page: `http://localhost:8080/group-demand-intelligence.html?hotelId=recLuxvwwxID7U2B8`  
   - API: `GET /api/group-demand-intelligence/hotels/recLuxvwwxID7U2B8/opportunities` (no query — default browse)  
   - Pre-fix live evidence: `count=83`, `listScope=BETHESDA_INTERNAL_UNION_88`, forbidden IDs present.

3. **Cause:** Stale server process (primary) + fail-open catch (latent safety defect) + UI using remapped `priorityFilterKey` (DISQUALIFIED rows mapped to MEDIUM_PRIORITY still rendered) + UI only hiding rows when surface stamps existed (stale API sent none).

4. **Permanent code fix:**  
   - Default commercial filter **fail-closed** (503 + empty opportunities; never return unfiltered union).  
   - Family-collapse failure also fail-closed on commercial.  
   - `includeDisqualified` alone no longer widens to full.  
   - Response contract: `requestedSurface`, `effectiveSurface`, `eligibleCount`, `excludedCount`, `serverBuildSha`, `sourceOfTruth`, `storeIdentity`.  
   - UI: hard forbidden-ID denylist; raw `priority===DISQUALIFIED`; block unsafe `BETHESDA_INTERNAL_UNION_88` payloads.  
   - Adversarial route test with forced filter failure.

5. **Default API count after repair:** **68**  
6. **Rendered UI count:** **68** cards on localhost GDI page (DOM card count; no Marriott/AAO/NTCA/PTAB/ISCB/NCI RNA text).  
7. **Forbidden IDs returned:** **0**  
8. **Forbidden IDs rendered:** **0** (page text scan + API).  
9. **Forced filter-failure test:** **FAIL CLOSED** (503 / empty)  
10. **SHAs:** baseline main `ee599543f…`; this fix PR merges atop it; server restarted with `GDI_SERVER_BUILD_SHA` and live filter active.  
11. **Store integrity:** 88 IDs preserved; Marriott HQ retained as MARKET_INTELLIGENCE for admin/full views.  
12. **Authenticated browser:** **VERIFIED** on `http://localhost:8080/group-demand-intelligence.html?hotelId=recLuxvwwxID7U2B8` — 68 cards rendered, forbidden text hits empty.

## Live acceptance snapshot (post-restart)

```
status: 200
count: 68
listScope: BETHESDA_PRIMARY_COMMERCIAL
effectiveSurface: commercial
forbiddenHits: []
marriottPresent: false
```
