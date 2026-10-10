# PURSUIT REGRESSION

## Data (filesystem)

| Hotel | Pursuits | Sample status / follow-up / hotel inclusion |
|-------|----------|-----------------------------------------------|
| AC Coruña | 2 | OUTREACH_READY / PREPARE — follow-ups 2027-01-10, 2026-12-31 — PROCESS_IDENTIFIED / NOT_YET_OPEN |
| Radisson SD | 3 | OUTREACH_READY ×3 — follow-ups intact — PROCESS_IDENTIFIED / HOTEL_LIST_PENDING / COMPETITOR_HOST_CONFIRMED |
| **Total** | **5** | Spanish draft subjects present |

## Code preserved

| Asset | Status |
|-------|--------|
| `lib/group-demand-intelligence/pursuit/` | Unchanged |
| `api/gdi-pursuit.js` | Unchanged |
| `server.js` pursuit routes | Unchanged |
| Start / View Pursuit buttons | Unchanged |
| Pursuit panel HTML | Unchanged |

## Ready / Watch counts

| Hotel | Ready / Watch gates | Changed by this task? |
|-------|---------------------|------------------------|
| AC | Ready 0 / Watch (IAPS + BioCultura corpus) | NO |
| Radisson | Ready 0 / Watch corpus | NO |
| YOTEL | Ready/Watch gates untouched | NO |
| Bethesda | Customer All = 28 on live UI | NO |

## Live HTTP note

`GET /api/group-demand-intelligence/hotels/:id/pursuits` returned `API route not found` on the currently running localhost:8080 process. Repo still registers the routes. Restart Node to bind pursuit HTTP; filter cleanup does not depend on that restart for nav correctness.
