# BROWSER QA — Toronto ADP Forensic

Server restarted (new Node process on :8080) before validation.

| Check | Result | Notes |
|------|--------|------|
| Local report loads Core metrics | YES | `owner-ai-demand.html?propertyId=adp_radisson_blu_toronto_downtown` |
| AI Consideration 33.2% | YES | Visible in Executive Summary + metrics |
| Scenario Presence 38.1% | YES | |
| #1 Appearance 44.7% | YES | |
| Top-3 Appearance 85.1% | YES | |
| Reality Gap 66.7% | YES | |
| Owned 9.5% / External 90.5% | YES | |
| Hotel X top alternative | YES | |
| Displacement leaders | YES | Bisha 24, Hotel X 20, 1 Hotel 19, Hazelton 19, Shangri-La 16 |
| Missing Evidence · Business | YES — 16 cards | Drawer header `Missing Evidence · Business`; 16 PROVIDER labels; no empty-state |
| All Business cards from Core c8bd8b | YES | API `periodId` = Core; post-restart |
| Leisure missing (second territory) | YES | API total=21, period=Core |
| Provider / positive evidence period | YES | Core |
| Reality Gap evidence | YES | Recognized/missed lists match forensic |
| Displacement evidence | YES | Canonical counts on report |
| Portfolio lens | YES | Choice Privileges section present (Loyalty family) |
| Server restart verified | YES | Killed PID 8940; new server listening |

API cross-check after restart:

- Report period: `adp_period_adp_radisson_blu_toronto_downtown_20261104094215_c8bd8b`
- Business missing total: 16
- Leisure missing period: Core
