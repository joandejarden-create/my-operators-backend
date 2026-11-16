# BROWSER QA

Server restarted (new process on :8080). Hard-refresh:

`http://localhost:8080/owner-ai-demand.html?propertyId=adp_radisson_blu_toronto_downtown&cb=bppfix2`

| Check | Result |
|------|--------|
| Stale fallback absent | YES |
| Actual portfolio section | YES |
| Choice Privileges | YES |
| #1 of 6 | YES |
| Portfolio AI Presence 45.5% | YES |
| #1 Appearance 0.0% / Top-3 92.9% | YES |
| Six-hotel overview + shared columns | YES (e.g. Saint James shared 16) |
| Evidence drawers work | YES |
| Drawers use Loyalty period | YES — Positive drawer: 51× `loy_*` scenario ids; 0 Core / c8bd8b |
| Server restart verified | YES |
| data-bpp-ready | 1 |
