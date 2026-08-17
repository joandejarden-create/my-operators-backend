# Phase A+B — Fit Shadow Diagnostic (no Fit changes)

OE Fit Data Ready diagnostic uses Assignments / Market Presence / Brand Relationship **row counts**, not Setup form fields. Setup A+B therefore **does not change** the OE diagnostic threshold outcome.

| Metric | Before | After |
| ------ | -----: | ----: |
| Fit Data Ready (OE diag, real ops) | 4 | 4 |
| Conditional | 33 | 33 |
| Production Active Countries populated | 13 | 27 |

## Fit enrichment catalog vs Setup

| Fit domain | Setup hint | Setup improved? | OE available? | Gap class |
| ---------- | ---------- | --------------- | ------------- | --------- |
| Active status | Operator Setup - Master.submission_status | unchanged | yes (intel) | DATA NOW EXISTS IN SETUP |
| Active countries (structured) | Platform.Active Countries (structured multi-select; prose markets do not qualify) | 13→27 | yes (intel) | DATA NOW EXISTS IN SETUP |
| Operating structures supported | Commercial.Management Structures Supported | No (not Phase A+B) | yes (intel) | DATA EXISTS IN NORMALIZED OE |
| Hotel segments / chain scales | Profile.chainScalesSupported | No (not Phase A+B) | yes (intel) | DATA EXISTS IN NORMALIZED OE |
| Meaningful project-experience dimension | Commercial asset/situation + Case Studies | No (not Phase A+B) | yes (intel) | DATA EXISTS IN NORMALIZED OE |
| Identified evidence source for material claims | PI Source Library / Case Studies / Master Source Type | No (not Phase A+B) | yes (intel) | DATA EXISTS IN NORMALIZED OE |
| Brands currently operated | Profile.brands / Brand Relationships | — | yes (intel) | FIT LEGACY / METHODOLOGY ISSUE |
| Brand approval / relationship status | Operator Setup - Brand Relationships (proposed) | — | yes (intel) | FIT LEGACY / METHODOLOGY ISSUE |
| Comparable operator assignments | Operator Setup - Case Studies | No (not Phase A+B) | yes (intel) | DATA EXISTS IN NORMALIZED OE |
| Conversion / reflag experience | Commercial.Conversion / Reflag Experience | — | yes (intel) | FIT LEGACY / METHODOLOGY ISSUE |
| Owner reporting / governance level | Governance.Owner Reporting Level | — | yes (intel) | FIT LEGACY / METHODOLOGY ISSUE |
| Regional resources / capacity | Platform regional fields (proposed) | — | yes (intel) | FIT LEGACY / METHODOLOGY ISSUE |
| Generic offered-services checklist | Governance.Offered Services (table-stakes subset) | — | yes (intel) | FIT LEGACY / METHODOLOGY ISSUE |
| Fee / commercial economics | Outreach / project-specific (Level E) | — | yes (intel) | FIT LEGACY / METHODOLOGY ISSUE |
