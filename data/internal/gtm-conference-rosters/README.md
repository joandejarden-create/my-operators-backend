# GTM conference rosters

Drop vendor delegate / attendee rosters here for cross-reference against owner targets.

## ALIS CALA 2026

- Source: `ALISCALA26 Final Delegate Roster for Distribution 4-30.xlsx`
- Archived copy: `alis-cala-2026-delegate-roster.xlsx`
- Cross-ref reports: `reports/gtm-alis-cala-2026-delegate-crossref.*`

```bash
node scripts/report-alis-cala-delegate-crossref.mjs
node scripts/report-alis-cala-delegate-crossref.mjs --roster="path/to/roster.xlsx"
```

Outputs:
- `gtm-alis-cala-2026-delegate-matches.csv` — delegates matched to strike list / branding targets
- `gtm-alis-cala-2026-net-new-owner-leads.csv` — owner-like attendees not yet in target list
- `gtm-alis-cala-2026-delegate-crossref.json` — full structured output

**Note:** Column `x` in column A appears to mark a subset of confirmed registrants (most rows are invitees). Treat roster as "invited + registered" unless vendor confirms otherwise.
