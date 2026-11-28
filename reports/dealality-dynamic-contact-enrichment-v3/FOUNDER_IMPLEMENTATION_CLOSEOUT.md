# DEALALITY_DYNAMIC_CONTACT_ENRICHMENT_V3 — GDI Stage 4 closeout

## Meinrad / Surfe root cause (established)

| Factor | Finding |
|---|---|
| LinkedIn field | Must send `linkedinUrl` (not legacy `linkedInUrl`) — fixed in Surfe client |
| Channel selection | `include.mobile` was historically false → phones in `mobilePhones` missed |
| Async | Must poll until `COMPLETED`; pending ≠ NOT_FOUND |
| API vs Chrome extension | Same coverage when LinkedIn + mobile requested (live diagnostic `5b090384-…`, 2 credits, NOT written to Airtable) |
| GDI impact | Same shared Surfe client + missing-channel planner; Stage 4 never auto-runs Surfe |

Screenshot values are **not** hardcoded.

## What this PR adds (GDI Stage 4)

- Shared planner: `planMissingChannelEnrichment` + `classifyPartialChannelResult`
- Findymail missing-channel: email **and** LinkedIn phone independently
- Stage 4 runner uses `enrichPersonWithFindymailMissingChannels`
- Cumulative hard-cap reserves phone (10) before spend
- PowerShell `Invoke-GdiContactResearch.ps1` documents V3 + no automatic Surfe
- Permanent gate: `npm run test:gdi-stage4-dynamic-contact-enrichment-v3` (mocked)

## Validation mode

| Mode | Status |
|---|---|
| Mocked unit / planner / Stage 4 dry-run | PASS (14/14) |
| Shared global + HE V3 gates | PASS |
| Live Findymail under this assignment | **NOT_VALIDATED** (no authorized spend) |
| Live Surfe under this assignment | Prior diagnostic only; no new spend |

## Consumer table

| Product | Contact Component | Shared Planner | Identity | Missing Channels | Provider Request | Persistence | Regression |
|---|---|---|---|---|---|---|---|
| Shared | `missing-channel-enrichment-v3.js` | Yes | Via person DTO | Yes | Surfe include + Findymail plan | N/A | PASS |
| HE | Contact Intelligence / hybrid API | Yes | LinkedIn preferred | Yes | Surfe missing-only | Surfe session-only | PASS (prior) |
| GDI | Surfe reveal provider | Yes | LinkedIn preferred | Yes | Explicit-click Surfe | Session-only | PASS (prior) |
| GDI | Stage 4 Findymail | Yes | Name+domain / LinkedIn | Yes | Email and/or phone | Durable when permitted | PASS mocked |
| GDI | PowerShell Stage 4 | Via Node stage | — | Via planner | Findymail only | Existing | Source PASS |
