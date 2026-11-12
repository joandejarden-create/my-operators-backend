# Bethesda 14-Segment Offline Replay & Commercial Recovery — Founder Report

Generated: 2026-11-12T13:15:46.339Z
Hotel: Bethesda Marriott (recLuxvwwxID7U2B8)
Closed campaign: `reports/gdi-bethesda-fourteen-segment-live-20261112/`
Analysis package: `reports/gdi-bethesda-fourteen-segment-offline-replay-postmortem-v1/`
Git SHA at analysis: `4b09ae0658bfa25329c087419971b7b2f3772cb3`
Defect fixes in this package: live `disc_*` inventory exclusion; Exa deep `providerRunId` provenance harden.

## A. Original run (historical, closed)

| Metric | Value |
|---|---|
| Discovery candidates (fair queue / ORS) | **170** |
| Discovery-state IDs (pre-retain) | 210 |
| Paid deep executions | **46** |
| Total spend | **$6** / $6.09 |
| Discovery / Deep | $1.4 / $4.6 |
| WATCHLIST | 12 |
| EXCLUDE | 34 |
| Publication-ready | **0** |
| Remaining authorization | $0.09 (NOT a new spend grant) |

## B. Old vs new prioritization (offline counterfactual)

- Candidates skipped before deep under new gates: **42**
- Historical paid deep that new system would have avoided: **13** (≈ **$1.3** COUNTERFACTUAL savings)
- Simulated new paid deep within $4.69 deep budget: **43** (COUNTERFACTUAL — does not prove qualification)
- Cross-segment same-org paid repeats in closed run: **4** orgs

### Top 20 under improved prioritization (Simulation B)

| Rank | Organization | Segment | Pursuit | Investment | Dedup |
|---|---|---|---|---|---|
| 1 | International Consumer Product Health and Safety Organization | associations_conferences_events | RESEARCH_CONTINUATION | 11 | GENUINELY_NEW_COMMERCIAL_ACTIVITY |
| 2 | Corgan | corporate_accounts_scaling | CORPORATE_AND_PROJECT_DEMAND | 99 | GENUINELY_NEW_COMMERCIAL_ACTIVITY |
| 3 | Allied Social Science Associations / American Economic Association | corporate_meetings_incentives | CORPORATE_AND_PROJECT_DEMAND | 111 | GENUINELY_NEW_COMMERCIAL_ACTIVITY |
| 4 | Institute for Professionals in Taxation | government_public_sector | RESEARCH_CONTINUATION | 30 | GENUINELY_NEW_COMMERCIAL_ACTIVITY |
| 5 | Mid-Atlantic Cup / Montgomery County CVB | sports_entertainment | RESEARCH_CONTINUATION | 24 | GENUINELY_NEW_COMMERCIAL_ACTIVITY |
| 6 | U.S. Food and Drug Administration and ISPE | education_scientific | RESEARCH_CONTINUATION | 11 | GENUINELY_NEW_COMMERCIAL_ACTIVITY |
| 7 | Westat, Inc. | projects_infrastructure | CORPORATE_AND_PROJECT_DEMAND | 99 | GENUINELY_NEW_COMMERCIAL_ACTIVITY |
| 8 | Montgomery County Chamber of Commerce | social_special_events | MEETING_AND_CATERING_DISPLACEMENT | 84 | GENUINELY_NEW_COMMERCIAL_ACTIVITY |
| 9 | National Association of Christian Lawmakers | local_catering_day_meetings | MEETING_AND_CATERING_DISPLACEMENT | 102 | GENUINELY_NEW_COMMERCIAL_ACTIVITY |
| 10 | SATShow | corporate_celebrations_private_events | RESEARCH_CONTINUATION | 24 | GENUINELY_NEW_COMMERCIAL_ACTIVITY |
| 11 | NXL Performance (programme/travel-events provider; client unnamed) | travel_trade_group_intermediaries | PARTNERSHIP_AND_REFERRAL_DEVELOPMENT | 92 | GENUINELY_NEW_COMMERCIAL_ACTIVITY |
| 12 | Ullico | relocation_extended_stay | CORPORATE_AND_PROJECT_DEMAND | 99 | GENUINELY_NEW_COMMERCIAL_ACTIVITY |
| 13 | KID Museum | preferred_lodging_partnerships | PARTNERSHIP_AND_REFERRAL_DEVELOPMENT | 92 | GENUINELY_NEW_COMMERCIAL_ACTIVITY |
| 14 | NXL Performance | professional_event_planners | OPEN_HOTEL_SOURCING | 97 | GENUINELY_NEW_COMMERCIAL_ACTIVITY |
| 15 | American Economic Association and allied associations | associations_conferences_events | RESEARCH_CONTINUATION | 11 | GENUINELY_NEW_COMMERCIAL_ACTIVITY |
| 16 | IronCircle | corporate_accounts_scaling | CORPORATE_AND_PROJECT_DEMAND | 99 | GENUINELY_NEW_COMMERCIAL_ACTIVITY |
| 17 | Commercial Real Estate Development Association | corporate_meetings_incentives | CORPORATE_AND_PROJECT_DEMAND | 99 | GENUINELY_NEW_COMMERCIAL_ACTIVITY |
| 18 | North American Millers’ Association | government_public_sector | RESEARCH_CONTINUATION | 11 | GENUINELY_NEW_COMMERCIAL_ACTIVITY |
| 19 | Sand Soccer / EventConnect | sports_entertainment | RESEARCH_CONTINUATION | 24 | GENUINELY_NEW_COMMERCIAL_ACTIVITY |
| 20 | American Geophysical Union | education_scientific | RESEARCH_CONTINUATION | 11 | GENUINELY_NEW_COMMERCIAL_ACTIVITY |

### Newly prioritized vs correctly deprioritized (examples)

Moved up:
- SATShow (corporate_celebrations_private_events): 24 → 10
- International Consumer Product Health and Safety Organization (associations_conferences_events): 121 → 1
- Corgan (corporate_accounts_scaling): 86 → 2
- Allied Social Science Associations / American Economic Association (corporate_meetings_incentives): 59 → 3
- U.S. Food and Drug Administration and ISPE (education_scientific): 76 → 6
- Westat, Inc. (projects_infrastructure): 63 → 7
- Montgomery County Chamber of Commerce (social_special_events): 64 → 8
- National Association of Christian Lawmakers (local_catering_day_meetings): 92 → 9

Deprioritized / skipped under new gates:
- Energy Bar Association (associations_conferences_events): GENUINELY_NEW_COMMERCIAL_ACTIVITY / RESEARCH_CONTINUATION
- KnowBe4 (corporate_accounts_scaling): GENUINELY_NEW_COMMERCIAL_ACTIVITY / OPEN_HOTEL_SOURCING
- Association for Clinical and Translational Science (education_scientific): KNOWN_EXISTING_BETHESDA_OPPORTUNITY / RESEARCH_CONTINUATION
- AstraZeneca (projects_infrastructure): GENUINELY_NEW_COMMERCIAL_ACTIVITY / OPEN_HOTEL_SOURCING
- Ameresco, Inc. (relocation_extended_stay): GENUINELY_NEW_COMMERCIAL_ACTIVITY / OPEN_HOTEL_SOURCING
- Rock Creek Mansion (preferred_lodging_partnerships): KNOWN_EXISTING_BETHESDA_OPPORTUNITY / PARTNERSHIP_AND_REFERRAL_DEVELOPMENT
- Hidden Level (corporate_accounts_scaling): GENUINELY_NEW_COMMERCIAL_ACTIVITY / OPEN_HOTEL_SOURCING
- DRONERESPONDERS (corporate_meetings_incentives): GENUINELY_NEW_COMMERCIAL_ACTIVITY / CORPORATE_AND_PROJECT_DEMAND

## C. Value recovery from $6.00 already spent

- WATCHLIST / priority dossiers reviewed offline: **12**
- Account Development supported (offline, not publication-ready): **10**
- Publication-review-ready bodies staged: **0**
- Blocked as existing-inventory duplicates: **1**

Strongest incomplete dossiers with named contacts (still NOT publication-ready):
- **KnowBe4** [corporate_accounts_scaling] — Neil Brownsword; Erin Manning Hayes; Lyss Madden — ACCOUNT_DEVELOPMENT_SUPPORTED_OFFLINE_NOT_PUBLICATION_READY
- **AstraZeneca** [projects_infrastructure] — Gavin Houston; Ed McAndrews; Danielle Adams; Chad Kellner — ACCOUNT_DEVELOPMENT_SUPPORTED_OFFLINE_NOT_PUBLICATION_READY
- **Ameresco, Inc.** [relocation_extended_stay] — Lydia Nickerson; Jon Spangler — ACCOUNT_DEVELOPMENT_SUPPORTED_OFFLINE_NOT_PUBLICATION_READY
- **Hidden Level** [corporate_accounts_scaling] — Antoinett Dufort; Brian Reynolds; Melanie Sheppard; Michele Cadin — ACCOUNT_DEVELOPMENT_SUPPORTED_OFFLINE_NOT_PUBLICATION_READY
- **X-energy Reactor Company, LLC** [projects_infrastructure] — Nichole Lavalle; Anas Marzook; Jana Cornell — ACCOUNT_DEVELOPMENT_SUPPORTED_OFFLINE_NOT_PUBLICATION_READY
- **Workday, Inc.** [relocation_extended_stay] — Jessica Sorenson; Dan Yoo; Susan Higbee Chong; Matthew T. Cornelius — ACCOUNT_DEVELOPMENT_SUPPORTED_OFFLINE_NOT_PUBLICATION_READY
- **True Anomaly** [corporate_accounts_scaling] — Amanda George, RA; Donnie Loa; Cara Neil — ACCOUNT_DEVELOPMENT_SUPPORTED_OFFLINE_NOT_PUBLICATION_READY
- **Menlo Digital** [projects_infrastructure] — Kimberly Parker; Su Wai — ACCOUNT_DEVELOPMENT_SUPPORTED_OFFLINE_NOT_PUBLICATION_READY
- **True Anomaly** [relocation_extended_stay] — Amanda George, RA; Cara Neil; Ty Smolenski — ACCOUNT_DEVELOPMENT_SUPPORTED_OFFLINE_NOT_PUBLICATION_READY
- **X-energy Reactor Company, LLC (doing business as X-energy)** [corporate_accounts_scaling] — Timothy Bendt; Rachel Lappin Scheinman — ACCOUNT_DEVELOPMENT_SUPPORTED_OFFLINE_NOT_PUBLICATION_READY

## D. Segment quality

- Fourteen-segment coverage in closed run: all segments received deep slots (fair scheduler).
- PLP offline contract: **PASS_OFFLINE_CONTRACT** · live yield **NOT LIVE VERIFIED**
- PEP offline contract: **PASS_OFFLINE_CONTRACT** · contamination in closed run: U.S. IFE, American Society of International Law, Society of Cardiovascular Computed Tomography, American Hospital Association, Society of American Military Engineers, Washington Independent Review of Books, American Academy of Neurology, National Association of Wholesaler-Distributors, Society of Nuclear Medicine and Molecular Imaging, American Geophysical Union
- LCM offline contract: **PASS_OFFLINE_CONTRACT** · contamination in closed run: Association for Financial Counseling and Planning Education

## E. Production readiness

| Stage | Status |
|---|---|
| Fourteen-segment discovery contracts | **PASS** — adapters + PLP/PEP/LCM overlays present |
| Evidence normalization | **PARTIAL** — provenance excerpt showed [object Object] in PLP discovery state |
| Existing-inventory dedup | **PASS** — soft-match + disc_* excluded from baseline inventory (88 union) |
| Cross-segment dedup | **PASS** — paidDeepOrgKeys + pre-paid screen; Sim B one-org-once |
| Commercial fit screening | **PASS** — lowCostCommercialScreen |
| Revenue-based deep prioritization | **PASS** — scoreDeepResearchInvestment |
| Segment-fair allocation | **PASS** — fair scheduler unchanged structure |
| Deep-research eligibility | **PASS** — pursuit-aware eligibility |
| Contact research | **PARTIAL** — named contacts found on some WATCHLIST; many still generic |
| Commercial qualification | **PARTIAL** — 0 publication-ready in closed run |
| Full content generation | **FAIL** — WATCHLIST dossiers not promoted to complete bodies |
| Publication-readiness checks | **PASS** — none auto-published |
| Protected staging | **PASS** — no operational writes this analysis |
| Open sourcing no hotel | **PASS** — offline pursuit tests |
| Planner vs conference brand | **PASS** — offline contamination detection |
| LCM destination conference reject | **PASS** — offline retention gate |
| Deep settle providerRunId | **PARTIAL** — ORS holds run ids; closed ledger lacked them — harden on main |
| No paid API in offline validation | **PASS** — this script |

- Canonical opportunities.json count: **59** (expected 59); inventory union IDs loaded: **88** (expected 88); live disc_* in inventory: **0**
- No paid spend and no publication during this offline package
- Closed-run ledger missing providerRunId on all 46 deep ops; ORS retains run ids for 46/46 — settle harden ships with this fix
- Git SHA: `4b09ae0658bfa25329c087419971b7b2f3772cb3`

## F. Next research investment — GO / NO-GO

### Recommendation: **NO_GO_FOR_NEW_PAID_DISCOVERY**

- Closed campaign: $6.00 → 0 publication-ready opportunities.
- PLP/PEP/LCM offline contracts PASS but live yield remains NOT LIVE VERIFIED.
- Counterfactual ranking would avoid duplicates/contamination spend, but cannot prove publication-ready outcomes without a controlled zero-paid fixture proof or separately authorized live pilot.
- Next step: zero-paid resume/fixture validation of corrected adapters on Bethesda + one non-US hotel before any new paid authorization.

Reconsider paid discovery only after:
- Offline fixture pack proves PEP returns planner firms not conference brands
- Offline fixture pack proves LCM rejects destination conferences
- Inventory soft-match skips known AFCEA-like variants before deep
- ProviderRunId provenance populated on deep settle (ledger, not only ORS)
- At least one WATCHLIST dossier completed to publication-review-ready offline without inventing demand

---

### Appendices in this package

- `00-campaign-freeze.json` … `12-go-no-go.json`
- `02-candidate-master.json` — full reconstructed candidate table
- `04-top30-before-after.json` — explainable ranking deltas
- `07-watchlist-value-recovery.json` — offline recovery detail
- `09-cost-efficiency.json` — verifiable waste vs counterfactual savings
