# LINK QA

| Check | AC Hotel A Coruña | Radisson Santo Domingo |
|------|-------------------|------------------------|
| Share page HTTP | 200 | 200 |
| Login required | NO | NO |
| Resolve API | 200 ok | 200 ok |
| Report API | 200 | 200 |
| Correct hotel | AC Hotel A Coruña | Radisson Hotel Santo Domingo |
| Correct period | adp_period_adp_ac_hotel_a_coruna_20260929113035_c74256 | adp_period_adp_radisson_santo_domingo_20260908121004_f947f9 |
| Internal/admin UI | NO | NO |
| Verdict | PUBLIC_LINK_READY | PUBLIC_LINK_READY |

Notes:
- AC had no prior ACTIVE ADP share token; minted via canonical `issueShareCapability` + Railway production secret; registry deployed.
- Radisson token preserved from CALA Six cohort; `externalDistributionHold.allowExistingUrls=true`.
- ADP periods were NOT modified.
