# GDI Exa Agent — Segment-Specific Request Titles V1

**Status:** Merged readiness (observability-only)  
**Contract:** `gdi-exa-segment-request-titles-v1`  
**Does not change:** research missions, qualification, budgets, schemas, contract IDs, publication.

## Root cause

Outbound Exa Agent prompts for the Events family were assembled from static markdown:

- `event-discovery-agent-prompt-v1.md`
- `event-deep-research-agent-prompt-v1.md`

Those files begin with a hardcoded:

`# ASSOCIATION / CONFERENCE / EVENTS — EXA AGENT … CONTRACT V1`

Hotel Full E2E and campaign runners (including YOTEL / Radisson) reused `buildExaEventsAgentCreatePayload` for many of the 14 segments that route through the Association/Events agent family (`education_scientific`, `local_catering_day_meetings`, `professional_event_planners`, etc.). The Exa UI therefore showed Association titles even when the run context was a different segment.

Historical Exa request history cannot be relabeled. Future submitted requests use segment-specific H1 titles.

## Fix

One shared resolver:

`lib/group-demand-intelligence/research/exa-agent/gdi-exa-segment-request-titles-v1.js`

- Canonical display names for all 14 workflow segment IDs
- Alias normalization (`association_events` → `associations_conferences_events`, etc.)
- `buildExaSegmentRequestHeading` / `applyExaSegmentRequestTitle`
- Fail-closed on unknown/missing segment (never silent Association default)
- Fourteen-segment coverage ledger + markdown table (submitted requests only)

Wired into:

- `exa-agent-events-contracts-v1.js` (Stage A/B Events family)
- `exa-agent-contracts-v1.js` + provider adapter (Scaling family)
- Preferred lodging payload compat
- Live discovery bridge + Events ORS previews
- Campaign scripts that call `buildExaEventsAgentCreatePayload` (pass `segmentId`)

## Canonical title mapping

| Segment ID | Display name |
|---|---|
| associations_conferences_events | ASSOCIATION / CONFERENCE / EVENTS |
| corporate_accounts_scaling | CORPORATE ACCOUNTS / SCALING COMPANIES |
| corporate_meetings_incentives | CORPORATE MEETINGS / INCENTIVES |
| government_public_sector | GOVERNMENT / PUBLIC SECTOR |
| sports_entertainment | SPORTS / ENTERTAINMENT |
| education_scientific | EDUCATION / SCIENTIFIC |
| projects_infrastructure | PROJECTS / INFRASTRUCTURE |
| social_special_events | SOCIAL / SPECIAL EVENTS |
| local_catering_day_meetings | LOCAL CATERING / DAY MEETINGS |
| corporate_celebrations_private_events | CORPORATE CELEBRATIONS / PRIVATE EVENTS |
| travel_trade_group_intermediaries | TRAVEL TRADE / GROUP INTERMEDIARIES |
| relocation_extended_stay | RELOCATION / EXTENDED STAY |
| preferred_lodging_partnerships | PREFERRED LODGING / PARTNERSHIPS |
| professional_event_planners | PROFESSIONAL EVENT PLANNERS |

### Examples

Stage A:

`# PROJECTS / INFRASTRUCTURE — EXA AGENT DISCOVERY CONTRACT V1`

Stage B:

`# EDUCATION / SCIENTIFIC — EXA AGENT DEEP RESEARCH CONTRACT V1`

`# LOCAL CATERING / DAY MEETINGS — EXA AGENT DEEP RESEARCH CONTRACT V1`

`# RELOCATION / EXTENDED STAY — EXA AGENT DEEP RESEARCH CONTRACT V1`

## Coverage ledger sample

A segment is **EXECUTED** only when an actual Exa request was submitted for that stage. Taxonomy membership alone does not mark execution.

| Segment | Discovery | Deep Research | Requests | Cost |
|---|---|---|---:|---:|
| EDUCATION / SCIENTIFIC | EXECUTED | EXECUTED | 2 | $0.60 |
| PROJECTS / INFRASTRUCTURE | FAILED | SKIPPED | 1 | $0.00 |
| ASSOCIATION / CONFERENCE / EVENTS | SKIPPED | SKIPPED | 0 | $0.00 |
| …remaining 11… | SKIPPED / … | … | … | … |

## Tests

```bash
npm run test:gdi-exa-segment-request-titles-v1
```

All 12 checks PASS (mocked; no paid Exa calls), including:

1. All 14 segment headings  
2. Stage A → DISCOVERY  
3. Stage B → DEEP RESEARCH  
4. education_scientific ≠ Association  
5. projects_infrastructure ≠ Association  
6. Unknown segment fails closed  
7. Contract IDs / schemas unchanged  
8. Exa quote units unchanged ($0.10 / $0.50)  
9. Ledger reflects submitted requests only  
10. Coverage statuses: EXECUTED / FAILED / SKIPPED  

## Research behavior confirmation

- Contract IDs unchanged (`gdi-exa-events-discovery-v1`, `gdi-exa-events-deep-research-v1`, Scaling contracts)
- Output schemas unchanged
- Missions / tools / budgets / qualification / publication unchanged
- YOTEL $30 campaign not restarted or re-billed for title changes

## GitHub

- Branch: `fix/gdi-exa-segment-request-titles-v1`
- PR: [#333](https://github.com/joandejarden-create/my-operators-backend/pull/333) — MERGED
- CI: Admin tabs + AVA panel, GDI recovery regression suite, Spice fixture + prod isolation, gitleaks — all PASS
- Merge commit SHA: `9d14f892651687387e8a43d01a115ac49bb50b72`
- `main` synchronized to merge commit after closeout
