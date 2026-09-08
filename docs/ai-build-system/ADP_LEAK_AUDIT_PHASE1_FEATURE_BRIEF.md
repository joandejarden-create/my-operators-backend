# Feature Brief: AI Demand Leak Audit (Phase 1)

## 1. Objective

Deliver a limited free diagnostic that shows hotel owners whether AI is mentioning their hotel, which competitors appear instead, which demand segments may be leaking, and the first three actions — without contaminating production ADP, Census, Brand Explorer, or Operator Explorer records.

## 2. Primary Users

- Admin (intake, approve, run, mark sent, promote stub)
- Owner / operator prospect (client-safe report recipient)

## 3. User Value

Credible, low-friction proof of AI demand leakage before a paid ADP pilot conversation.

## 4. Recurring Value

Phase 1 is one-shot diagnostic. Paid ADP pilot (Phase 2+) provides monthly monitoring.

## 5. Where It Appears

- `/admin/adp-leak-audits/new` — intake
- `/admin/adp-leak-audits` — list + actions
- `/adp-leak-audit/[reportId]` — client report

## 6. Inputs Required

Hotel name/website/geo, contact, demand segment of interest, source, notes.

## 7. Outputs Generated

Isolated request/run/observation/report records + one-page client-safe report.

## 8. AI Behavior

Phase 1: manual/synthetic observation processing (no live provider spend). Phase 2: limited provider automation.

## 9. Evidence / Interpretation / Next Action

- Evidence: mention / displacement observations from limited sample
- Interpretation: cautious “evidence suggests / may indicate” language
- Next action: first 3 fixes + paid ADP pilot CTA

## 10. Data Model

Filesystem Airtable-shaped collections under `data/ai-demand-positioning/leak-audit/`:
`AdpLeakAuditRequests`, `AdpLeakAuditRuns`, `AdpLeakAuditObservations`, `AdpLeakAuditReports`.

## 11. Permissions / Access

Admin APIs gated by ADP monthly-review admin auth. Client report via unguessable report id.

## 12–16. Done criteria

See acceptance criteria in product ask + gates:

- `npm run test:adp-leak-audit-isolation-v1`
- `npm run test:adp-leak-audit-client-report-safety-v1`
- `npm run adp-leak-audit-founder-qa-v1` (Cambridge Beaches, NOW NOW NOHO, Hotel Phillips)

Client report structure (commercial diagnostic): Bottom Line → Biggest Demand Leak → Competitor Showing Up Instead → Likely Reason → First 3 Fixes → Who Does the Work? → Next Step.
