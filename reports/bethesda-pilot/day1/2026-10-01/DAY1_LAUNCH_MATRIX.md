# Bethesda Day-1 Launch Matrix — 2026-10-01

| Requirement | Original FPP Task | Launch Critical? | Current Evidence | Current Status | Remaining Action | Owner | Must Complete Today? | Can Be Workaround? | Workaround | Final Day-1 State |
|---|---|---|---|---|---|---|---|---|---|---|
| Pilot master record | Pilot/customer record + commercial setup | YES | `data/pilots/bethesda-marriott-001/pilot-master.json` created this run | COMPLETE (repo) | Sync into FPP when live write available | Dealality | YES | YES | Repo pilot master | COMPLETE |
| Pilot status ACTIVE | Pilot status → ACTIVE | YES | Pending proven production deploy | BLOCKED until REVISION_MATCH | Complete controlled deploy + smoke | Dealality | YES | NO | — | PENDING_DEPLOY |
| Start date 2026-10-01 | — | YES | Pilot master `start_date` | COMPLETE | — | Dealality | YES | — | — | COMPLETE |
| End/term recorded | — | YES | 6 months → 2027-03-31 | COMPLETE | Confirm commercial docs | Founder | PARTIAL | YES | Repo term recorded | COMPLETE_WITH_COMMERCIAL_PARTIAL |
| Products ADP+GDI | — | YES | Pilot master products | COMPLETE | — | Dealality | — | — | — | COMPLETE |
| Canonical property | Canonical Bethesda property profile | YES | GDI profile + ADP foundation + HI | COMPLETE | — | Dealality | — | — | — | COMPLETE |
| ADP official baseline | ADP official immutable baseline | YES | Period `adp_period_…9f3a60` designated `BETHESDA_ADP_BASELINE_V1` | COMPLETE | Do not overwrite | Dealality | YES | — | Designate existing certified period | COMPLETE |
| ADP immutable storage | — | YES | runtime + published + corrections | COMPLETE | — | Dealality | — | — | — | COMPLETE |
| ADP methodology version | — | YES | `ADP_MEASUREMENT_CONTRACT_V1` | COMPLETE | — | Dealality | — | — | — | COMPLETE |
| ADP model/provider version | — | YES | gpt-4o / gemini-3.6-flash / sonar / claude-sonnet-4-6 | COMPLETE | — | Dealality | — | — | — | COMPLETE |
| ADP prompt/query version | — | YES | `adp_scenario_universe_v1` | COMPLETE | — | Dealality | — | — | — | COMPLETE |
| Locked October control query set | ADP demand-family/query set | YES | `ADP_CONTROL_QUERY_SET.json` locked | COMPLETE | — | Dealality | YES | — | — | COMPLETE |
| ADP intervention/action log | ADP intervention/action log | YES | Monthly Action Register schema + Admin UI; Bethesda persistent log empty | PARTIAL | Seed first actions from GM package in first week | Dealality | NO | YES | Use monthly action register + GM 30-day plan as interim log | WORKAROUND |
| GDI production availability | GDI production activation | YES | Code ready; production revision not yet matching tested SHA | BLOCKED | Deploy + smoke | Dealality | YES | NO | — | PENDING_DEPLOY |
| GDI targeting config | GDI targeting configuration | YES | `config/group-demand-intelligence/hotels/recLuxvwwxID7U2B8.json` | COMPLETE | — | Dealality | — | — | — | COMPLETE |
| GDI persistent IDs | GDI opportunity IDs | YES | 54 stable `gdi_opp_*` IDs | COMPLETE | — | Dealality | — | — | — | COMPLETE |
| GDI lifecycle statuses | Opportunity status tracking | YES | priority + status + commercialStatus fields | COMPLETE | — | Dealality | — | — | — | COMPLETE |
| Already Known tracking | Already Known Yes/No | YES | feedback `familiarity` + share enums; Day-1 policy UNKNOWN until hotel responds | PARTIAL→ACCEPTABLE | Collect hotel responses | Hotel/Rad | NO | YES | UNKNOWN default until hotel reply | WORKAROUND |
| Pursue/Reject feedback | Accept/Reject + rejection reason | YES | feedback.json + validation APIs | COMPLETE | Hotel use | Hotel | NO | — | — | COMPLETE |
| Rejection reason | — | YES | feedback schema supports | COMPLETE | — | — | — | — | — | COMPLETE |
| Evidence/source/confidence | Source/evidence storage | YES | opportunity evidence fields + corpus | COMPLETE | — | Dealality | — | — | — | COMPLETE |
| Weekly GDI reporting | Exportable weekly GDI report | YES | `gdi:weekly-refresh` + PDF engine | PARTIAL | First weekly delivery workflow documented | Dealality | NO | YES | Manual weekly PDF + corpus delta | WORKAROUND |
| Monthly ADP/GDI reporting | Exportable monthly ADP/GDI report | YES | ADP monthly review archives + GDI PDF | PARTIAL | Combined assembly workflow | Dealality | NO | YES | Assemble ADP monthly + GDI PDF | WORKAROUND |
| Pilot KPIs | Pilot KPI framework | YES | `PILOT_KPI_BASELINE.json` | COMPLETE | Outcome metrics NOT YET MEASURED | Dealality | YES | — | — | COMPLETE |
| Internal command center | Internal Bethesda pilot dashboard | YES | AI Demand Admin + GDI admin surfaces | PARTIAL | Dedicated CC not required Day-1 | Dealality | NO | YES | Use Admin ADP+GDI pages | WORKAROUND |
| External ADP client URL | — | YES | share architecture; prod deploy pending | PENDING_DEPLOY | Deploy + smoke | Dealality | YES | NO | — | PENDING_DEPLOY |
| External GDI client URL | — | YES | token `gdisht_47c25d74…` frozen | PENDING_DEPLOY | Deploy + smoke; token must not change | Dealality | YES | NO | — | PENDING_DEPLOY |
| ADP PDF/report | — | YES | published report + monthly review | PARTIAL | Client PDF via share after deploy | Dealality | YES | YES | Admin/published report | PENDING_DEPLOY |
| GDI PDF/report | Exportable weekly GDI report (engine) | YES | GDI PDF engine tested at fd919a1 | PENDING_DEPLOY | Deploy | Dealality | YES | NO | — | PENDING_DEPLOY |
| Document repository | — | YES | `reports/bethesda-pilot/day1/2026-10-01/` + GM package | COMPLETE | — | Dealality | YES | — | — | COMPLETE |
| Operating cadence | — | YES | weekly GDI / monthly ADP+GDI / Nov 1 remeasure | PARTIAL | Meeting times SCHEDULE_REQUIRED | Founder+Hotel | NO | YES | Cadence dates without clock times | WORKAROUND |
| November remeasurement | Oct baseline → Nov remeasure | YES | Baseline frozen; target 2026-11-01 | COMPLETE | Execute Nov | Dealality | NO | — | — | COMPLETE |
| Commercial documentation | Pilot agreement/IP/data | YES | No written agreement found in repo | MISSING | Founder action | Founder | YES as founder action | YES for product ACTIVE with caveat | Document gap; do not invent terms | FOUNDER_ACTION |
| Billing setup | — | YES | Not found in repo | MISSING | Founder action | Founder | YES as founder action | YES for product ACTIVE | Document gap | FOUNDER_ACTION |
| Hotel stakeholder inputs | Hotel Team / priorities / need periods | YES | Structured profile partial; checklist not received as hotel package | PENDING | Collect from Rad/hotel | Hotel/Rad | NO | YES | Interim GDI profile + GM package | HOTEL_INPUT_PENDING |
| Founding Hotel Partner #001 | Founding Hotel Partner record | YES | Pilot master founding_hotel_partner_number=1 | COMPLETE (internal) | No public marketing claim | Dealality | YES | — | — | COMPLETE |
| FPP Bethesda workstream | FPP reconciliation | YES | Live FPP read UNAVAILABLE (401); no Bethesda rows in stale reports | PARTIAL | Founder: restore GTM token / create FPP tasks | Founder | YES | YES | Proposed reconciliation file | FOUNDER_ACTION |
| GDI Future Watch cron | Activate GDI production monitoring | NO for Day-1 UI | Scheduler held; Bethesda not in HOTELS map | HELD | Separate founder approval | Founder | NO | YES | Manual/controlled monitoring | CRON_NOT_REQUIRED_FOR_DAY1 |
