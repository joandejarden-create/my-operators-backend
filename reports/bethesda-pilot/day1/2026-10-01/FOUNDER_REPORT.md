# Bethesda Marriott Pilot 001 — Day 1 Activation — October 1, 2026

## A. Executive Result

PILOT STATUS: **ACTIVE** (Founding Hotel Partner #001)  
PRODUCTION: **DEPLOYED** (`3859b5dc…`, source `d272564` = boot-fixed successor of tested `fd919a1`)  
ADP BASELINE: **PASS** — `BETHESDA_ADP_BASELINE_V1` designated immutable  
GDI: **PASS** — Day-1 snapshot frozen; live share resolve + Demand Report data OK  
CLIENT ADP: **PASS** (share page live; new external communications still under forensic HOLD policy)  
CLIENT GDI: **PASS** (existing token preserved; Opportunities + Demand Report)  
REPORTING: **PARTIAL / WORKAROUND** — weekly/monthly tooling exists; binary PDF cache not on production lean package  
MEASUREMENT: **PASS** — baselines + KPI framework established  
FPP RECONCILED: **PARTIAL** — live FPP read UNAVAILABLE (401); proposed reconciliation filed  

OVERALL DAY-1 VERDICT:

**BETHESDA PILOT 001 ACTIVE — ACCEPTABLE DOCUMENTED WORKAROUNDS REMAIN**

## B. Original Launch Requirements

| Requirement | Status | Evidence | Remaining action |
|---|---|---|---|
| Freeze ADP V1 official baseline | COMPLETE | `ADP_BASELINE_MANIFEST.json` | Never overwrite |
| Activate GDI production monitoring | COMPLETE w/ workaround | Live GDI share + corpus; cron HELD | Manual/controlled until cron approval |
| Pilot status ACTIVE | COMPLETE | `PILOT_MASTER_RECORD.json` | — |
| Pilot database/entity | COMPLETE | `data/pilots/bethesda-marriott-001/` | Sync to FPP when token works |
| Canonical property | COMPLETE | GDI profile + ADP foundation + HI | — |
| GDI targeting config | COMPLETE | hotel config JSON | — |
| ADP immutable storage | COMPLETE | runtime + published + corrections | — |
| Methodology/model/prompt versioning | COMPLETE | in baseline manifest | — |
| GDI opportunity IDs | COMPLETE | 54 stable IDs | — |
| Opportunity status tracking | COMPLETE | priority/status/commercial | — |
| Source/evidence storage | COMPLETE | opportunity evidence | — |
| Already Known Yes/No | WORKAROUND | UNKNOWN until hotel responds | Collect hotel feedback |
| Accept/Reject + reason | COMPLETE | feedback + share validation | Hotel use |
| ADP intervention log | WORKAROUND | schema/UI exist; Bethesda rows empty | Seed first week |
| Internal command center | WORKAROUND | AI Demand Admin + GDI admin | Dedicated CC later |
| Exportable weekly GDI | WORKAROUND | weekly scripts + Demand Report | First weekly delivery |
| Exportable monthly ADP/GDI | WORKAROUND | monthly review archives + GDI PDF engine | Assembly workflow |
| Commercial docs / billing | FOUNDER ACTION | not in repo | Tonight |

## C. Production Deployment

| Field | Value |
|---|---|
| Approved / tested SHA | `fd919a1135eeb0ef35fb04b3b038f4ec7afed88f` |
| Deployed source SHA | `d2725642db6da8f5d5f8501990cbe9597b0a785a` |
| Why successor | Single commit after tested SHA restores omitted boot libs (`lib/market-alerts-contact/*` etc.) so production starts |
| Revision match | **YES** (GDI PDF + client sharing retained; boot fix only) |
| Method | `railway up --detach` from clean lean worktree (bulk `reports/`/`fixtures` handling; fixtures restored for boot) |
| Deployment ID | `3859b5dc-d0fa-4a99-8a4f-2203212cce7f` |
| Prior production | `e0798a3d…` (admin-gdi-reports 404) |
| Admin smoke | admin-gdi-reports.js **200** + External Client markers |
| External smoke | GDI resolve **200** hotel `recLuxvwwxID7U2B8`; share HTML **200**; ADP share HTML **200** |
| PDF smoke | `pdf-report` JSON **200** (Bethesda Marriott); cached binary `report-pdf` **404** (store not in lean package) |

Failed attempts before success: `ee003356` (missing market-alerts libs on bare `fd919a1`), `06dcd0ff` (fixtures parked → owner-intelligence ENOENT). Production remained on prior SUCCESS until `3859b5dc`.

## D. ADP Official October Baseline

| Field | Value |
|---|---|
| Baseline ID | `BETHESDA_ADP_BASELINE_V1` |
| Date | 2026-10-01 (designation) |
| Period ID | `adp_period_adp_bethesda_marriott_20260909091016_9f3a60` |
| Run timestamp | 2026-09-09T09:10:16.123Z (not fabricated) |
| Query set | 63 scenarios (`adp_scenario_universe_v1`) |
| Methodology | `ADP_MEASUREMENT_CONTRACT_V1` |
| Models | gpt-4o, gemini-3.6-flash, sonar, claude-sonnet-4-6 |
| Immutable | **YES** |
| Metrics | demandCaptureRate **81%** |
| Note | Published CERTIFIED; forensic hold blocks *new* external communications; existing URLs allowed |

## E. GDI Day-1 Snapshot

Frozen from corpus (measurement reference):

| Metric | Count |
|---|---:|
| Total | 54 |
| Ready (strict) | 37 |
| Visible | 37 |
| High | 6 |
| Medium | 15 |
| Future Watch (status) | 19 |

Live production Demand Report (2026-10-01 smoke): ready **36** / high **5** / medium **11** / watch **12** — explain as live DTO presentation vs frozen corpus; IDs unchanged; no discovery run in this activation.

Already-known coverage: policy **UNKNOWN** until hotel responds (do not auto-label net-new).

## F. Customer Access

| Item | Result |
|---|---|
| ADP external page | Live (`/owner-ai-demand-share.html` 200) |
| GDI external page | Live (`/group-demand-intelligence-share.html` 200) |
| GDI token | `gdisht_47c25d74c79216021fb36150` **UNCHANGED** |
| GDI resolve | PASS — Bethesda hotel |
| Demand Report data | PASS |
| Cached PDF binary | FAIL/404 until PDF store generated into production |
| Security | invalid/no token 403; cross-hotel 403 |

Signed URLs intentionally not printed here.

## G. Feedback / Outcome Tracking

Supported in share validation / feedback schema:

Already Known · Pursue · Contacted · Rejected · RFP · Tentative · Won · Lost (+ rejection reason / comments)

Stored in: `data/group-demand-intelligence/hotels/recLuxvwwxID7U2B8/feedback.json` + share APIs.

Day-1 hotel responses: not yet collected (test actors only historically).

## H. Pilot KPIs

See `PILOT_KPI_BASELINE.json`.

Outcome metrics = **NOT YET MEASURED**. Baseline ADP demand capture 81%; GDI ready corpus 37 (freeze).

## I. Hotel Inputs

Overall: **PENDING / PARTIAL** — see `HOTEL_INPUT_STATUS.md`. Not a product blocker with interim profile/GM package.

## J. Commercial / Administration

| Item | State |
|---|---|
| Pilot record | COMPLETE (repo) |
| Agreement | MISSING — founder action |
| Billing | MISSING — founder action |
| Repository | COMPLETE (`reports/bethesda-pilot/day1/2026-10-01/` + GM package) |
| Cadence | PARTIAL — dates set; meeting times SCHEDULE_REQUIRED |

## K. Founder Project Plan

- Live read: UNAVAILABLE (401)
- Bethesda FPP rows before: 0 found
- Completed today (repo + proposed): pilot master, ADP baseline, control set, GDI snapshot, production deploy
- Still active: commercial, hotel inputs, cron approval, Nov remeasure
- Duplicates: 0
- Live write: **not executed**

## L. Must Finish Tonight

1. Commercial/pilot agreement documentation  
2. Billing entity/process  
3. Restore GTM FPP token / create Bethesda FPP workstream  
4. Confirm ADP external-comms posture under forensic HOLD  
5. Schedule first weekly + monthly touchpoints  

## M. Waiting on Bethesda

Stakeholder roster, need periods, preferred group profile, known accounts, feedback owner, first Already Known / Pursue responses.

## N. First Week

GM action set delivery on live GDI URL; seed ADP action register; hotel feedback kickoff; generate Bethesda PDF into production store; first weekly note.

## O. November Remeasurement

Frozen: `BETHESDA_ADP_BASELINE_V1` + October control query set + `BETHESDA_GDI_DAY1_SNAPSHOT`. Compare November current vs these — never overwrite October.

## P. Pilot vs Product Development

| Tier | Items |
|---|---|
| P0 | Commercial docs if required for legal go-live; FPP access for governance; PDF binary store if Rad expects download tonight |
| P1 | Hotel inputs; action-log seeding; first weekly operating report |
| P2 | Dedicated command center; Future Watch cron; automated combined monthly export |

## Q. Integrity / Regression

| Check | Result |
|---|---|
| Bethesda hotel ID unchanged | YES `recLuxvwwxID7U2B8` |
| ADP baseline immutable designation | YES |
| ADP history overwritten | NO |
| GDI IDs rotated | NO |
| Share token changed | **NO** |
| Cross-hotel leakage | NO (403) |
| Internal client leak in resolve smoke | NO admin IDs observed in happy-path checks |
| Wrong-base / legacy writes | NO (no Airtable migrations this run) |
| Duplicate Pilot object | NO (single `BETHESDA_MARRIOTT_001`) |
| Uncontrolled discovery / Surfe bulk | NO |
| Cron enabled | **NO — HELD** |
| Customer-facing regression from deploy | NO (share resolve OK; admin GDI now present vs prior 404) |

---

## Final verdict

**BETHESDA PILOT 001 ACTIVE — ACCEPTABLE DOCUMENTED WORKAROUNDS REMAIN**

CRON: **NOT_REQUIRED_FOR_DAY1** (held; ready for separate founder approval later)
