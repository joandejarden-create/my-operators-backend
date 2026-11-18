# PR4 — Three Local Demand Research Tracks

## Delivered

- Mission packs for social celebrations, planner partnerships, venue lodging partnerships
- Mapped exclusively onto **existing** fourteen segments (no new engines)
- `--research-track` on canonical Full E2E CLI / entry (scopes planned segments)
- Thin PowerShell wrappers under `scripts/gdi-local-tracks/`
- Honesty rules: no invented private events; venue rooms YES/NO/UNKNOWN

## Tracks

| Track | Segments |
|---|---|
| `social_celebrations` | social_special_events, corporate_celebrations_private_events, local_catering_day_meetings |
| `planner_partnerships` | professional_event_planners, travel_trade_group_intermediaries, preferred_lodging_partnerships, corporate_meetings_incentives |
| `venue_lodging_partnerships` | preferred_lodging_partnerships, local_catering_day_meetings, social_special_events, professional_event_planners |

## Commands

```powershell
.\scripts\gdi-local-tracks\Invoke-GdiSocialCelebrationsTrack.ps1 -HotelId recLuxvwwxID7U2B8 -Mode fixture
npm run gdi:canonical-full-e2e -- --hotel recLuxvwwxID7U2B8 --mode fixture --research-track planner_partnerships
```

## Not in PR4

- Live paid validation
- Full Exa mission text rewrites (PR5)
