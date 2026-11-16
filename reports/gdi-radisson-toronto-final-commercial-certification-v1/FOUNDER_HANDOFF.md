# Radisson Blu Toronto Downtown — Founder Handoff

Hotel: **Radisson Blu Toronto Downtown** (`recH9h3McgMNFQEla` / CNC41)  
Reference date: **2026-11-16**  
Paid spend this closeout: **$0**

## A. GitHub

| Item | Value |
| --- | --- |
| PR | [#296](https://github.com/joandejarden-create/my-operators-backend/pull/296) — **MERGED** |
| Head before merge | `c43700ab306e31e6c31092b641839f4bc9983173` |
| Merge commit SHA | `1a498fd7bb59d661f84a01806e21fbaaa67cb9c8` |
| CI | **SUCCESS** — gitleaks + GDI recovery regression suite |
| Primary checkout | `main` @ `1a498fd7bb59d661f84a01806e21fbaaa67cb9c8` |
| Feature branch | Local + remote deleted / pruned |
| Unrelated local dirty files | Preserved (not staged) |

## B. Commercial scorecard (BEFORE → AFTER)

| Metric | Before (`ecafbc4d…`) | After (`d9a6b23b…`) |
| --- | ---: | ---: |
| Hotel Market (customer-visible) | 21 | **15** |
| CMI (customer-visible) | 15 | **13** |
| Total visible | 36 | **28** |
| Sales Ready (Pursue Now) | 0 | **3** |
| Account Development (HM label) | 21 | **0** (12 Qualify Opportunity) |
| Research Continuation (hidden) | 0 | **5** |
| Demoted / not currently actionable | 0 | **3** (1 HM CFEA + 2 CMI) |

**Sales Ready promotions (RELIABLE_INFERRED — not confirmed RFP/block):**

1. Ecological Society of America — 112th Annual Meeting (Christi R. Nam)
2. OHBM 2027 Annual Meeting (Emily Weber)
3. 15th World Chambers Congress (Aimee Welsh)

**Date corrections:** PDAC → Mar 7–10, 2027; Standout → Apr 9–11, 2027; Open Banking Expo → Mar 11, 2027; CILA → Apr 13–Oct 29, 2027. Zero “Multiple Dates” pills remain.

**Contact corrections (examples):** OHBM Beth Slater → Emily Weber; DesignThinkers Kandis Boyd → Youssef Elgaria; City Builders Taylor Curran → Korede Baiyewunmi; Open Banking Kelly Stanley → Lillie Cook; IDS Catia Varricchio → Megan Mauracher.

**Removed from customer view (research preserved):** CFEA 2026; Leadership & Change Management; Open Banking Expo; LabFaB; Toronto Aesthetics Sciences; International Conference on the Image; EDCO CMI; I4PL CMI.

## C. Top 10 Hotel Market priorities

See `16-TOP_10_HOTEL_MARKET_PRIORITIES.json`. Ranked:

1. ESA 112th Annual Meeting — Sales Ready — contact Christi R. Nam  
2. OHBM 2027 — Sales Ready — contact Emily Weber  
3. World Chambers Congress — Sales Ready — contact Aimee Welsh  
4. PDAC 2027 — Qualify / manage existing partner listing — Nicole Sampson  
5. CLIME 1.0 — Qualify — Mary Cunningham (verify email domain)  
6. ISCT 2027 — Qualify — Giovanni Guizzetti via meetings@isctglobal.org  
7. Toronto Anesthesia Symposium — Qualify — Diana Tamir via CPD mailbox  
8. DesignThinkers Toronto 2027 — Qualify — Youssef Elgaria  
9. Canadian Business Hall of Fame — Qualify — Oli Maynard-Langedijk  
10. DCN Toronto 2027 — Qualify — Mariella Greco  

**Shortfall note:** 10 outreach-eligible Hotel Market cards exist; only **3** meet Sales Ready. Ranks 4–10 require qualification before pursuit. Interior Design Show and several speculative cards were intentionally kept out of the top 10.

## D. CMI

| Item | Result |
| --- | --- |
| Final verified CMI | **13** (all eight gates pass; Competitive-only) |
| Demoted | EDCO (missing commitment evidence); I4PL 2026 (imminent / inferred dates) |
| Dual HM/CMI | None |
| Strongest future-cycle | GBTA Canada (Westin/Sheraton/Hilton); Ontario Water & Wastewater (Hotel X / Sheraton / Hyatt / Le Méridien); ROMA (Sheraton / Marriott Eaton / Hilton); SIO (Sheraton Centre); AACR Cancer Evolution (Hilton Toronto) |

CMI actions remain future-cycle account development / overflow — never presented as open Hotel Market principal bookings.

## E. Airtable

| Item | Result |
| --- | --- |
| Mode | **DRY_RUN only** — live writes **not** executed |
| Configured | true |
| Dry-run plan | `wouldCreate: 36`, `wouldUpdate: 0`, `liveWrites: 0` |
| Customer-visible inventory | 28 |
| Hidden / demoted also in dry-run set | 8 IDs flagged `customerVisible=false` in reconciliation |
| Duplicates / failures in dry-run | none reported |
| Live write readiness | **Ready for separate founder authorization**, with constraint below |

**Write-plan constraint for founder auth:** authorize persistence of **all 36 research IDs** only if hidden rows remain non-published and are marked research/continuation — **do not** include the 8 removed IDs in any `CUSTOMER_PUBLISHED` payload. Prefer a bounded write of the **28 customer-visible** opportunities for publication linkage, plus optional research-state upserts for the 8 hidden IDs.

## F. Publication / preview

| Item | Value |
| --- | --- |
| Package | `gdi_pub_recH9h3McgMNFQEla_20261116_radisson_toronto_final_commercial_cert_v1` |
| Fingerprint | `d9a6b23b8eb4ca305b8d35f638146cc266e4c089b0afa1cb5443012b1dd49f78` |
| Prior fingerprint preserved | `ecafbc4d…` unchanged on disk |
| Lifecycle | `STAGED_FOR_FOUNDER_REVIEW` (never `CUSTOMER_PUBLISHED`) |
| Counts | 15 HM + 13 CMI = 28 |
| Browser verification | **PASS** (fingerprint match; 28 cards; no Multiple Dates; CFEA absent; no provider leak) |
| Preview | http://127.0.0.1:8080/group-demand-intelligence.html?hotelId=recH9h3McgMNFQEla&preview=staged |
| Bethesda / Sheraton / Wyndham | Unchanged |

## G. Final verdict

| Certification | Status |
| --- | --- |
| Commercial research & presentation | **PASS** (quality, CMI 4a, actionability 4b, sales-ready contract, browser preview) |
| Full operational / Full E2E complete | **NOT COMPLETE** — `CERTIFIED_WITH_PENDING_GATES` |

**Pending gates**

1. `AIRTABLE_LIVE_PERSISTENCE` — PENDING_FOUNDER_AUTHORIZATION  
2. `FOUNDER_APPROVAL_OF_FINGERPRINT` — PENDING (`d9a6b23b…`)  
3. `PRODUCTION_PUBLICATION` — NOT_STARTED  
4. `PRODUCTION_PARITY` — NOT_STARTED  
5. `WEEKLY_REFRESH_AND_FEEDBACK` — NOT_STARTED  

**Exact next founder actions (in order)**

1. Review staged preview and Top 10 priorities.  
2. Approve or reject fingerprint `d9a6b23b8eb4ca305b8d35f638146cc266e4c089b0afa1cb5443012b1dd49f78`.  
3. Separately authorize Airtable live upsert (bounded plan above) if persistence is required before publication.  
4. Only after fingerprint + (if required) Airtable auth — authorize customer publication.  

**Do not start voco Cancún until this handoff is accepted.** No new paid Exa. Surfe on-click only.
