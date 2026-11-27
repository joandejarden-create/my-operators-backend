# GDI Executive Summary & Commercial Reporting V1  
## Phase 0 — Architecture, Data Readiness & Implementation Audit

**Status:** `GDI_EXECUTIVE_SUMMARY_PHASE_0_AUDIT_COMPLETE — AWAITING_FOUNDER_GO`  
**At:** 2026-11-27  
**Mode:** Read-only (no application/schema/Airtable/paid/deploy changes)  
**Stream workspace:** `C:\Dev\dealality\ws-gdi`

---

## 1. Executive Summary of Findings

GDI already has most of the **read-path ingredients** for an Executive Summary tab and a branded PDF: hotel identity, ACL, customer-facing filters, opportunity drawers/deep links, weekly-delta, contact actionability tiers, need-period/commercial-priority systems, pursuit/feedback APIs, execution observability, and a Playwright + Dealality report-family PDF with cover + “Commercial Demand Snapshot.”

What does **not** exist as a customer product surface:

- An **Executive Summary tab** (customer tabs today: Opportunities · Commercial Priorities · Portfolio).
- A **Research Receipt** contract that is hotel-attributable, period-scoped, and safe for executives.
- **Investment recovery** economics for hotel users.
- A **strict two-page** PDF template with fail-closed page-count validation (current GDI PDF is multi-section and records page count without capping at 2).

**Most important missing piece for genuine executive value:**  
A trustworthy, hotel-specific **Research Receipt + outcome spine** — before/after qualification counts that separate *shared market research* from *hotel-specific qualification*, plus honest commercial outcomes (pursued / booked / realized) that are **not** synthetic ADR × nights. Without that, the briefing risks looking like a pretty inventory digest rather than proof that Dealality did the hard work and moved commercial outcomes.

Secondary blockers: (a) existing PDF revenue scenarios use **hardcoded ADR 165/195/235**; (b) **CURRENT** customer publication exists for only 2 of 5 representative hotels; (c) local `:8080` staging process is stale relative to current `ws-gdi` writes.

---

## 2. Current Repository and Worktree Status

| Field | Value |
|---|---|
| Registered GDI home | `C:\Dev\dealality\ws-gdi` · home branch `ws/gdi` |
| Actual branch (this chat) | `feat/gdi-who-how-checkpoint-a-authorized-path` |
| HEAD | `f0105638b89d2bc90acc6df68afde913dc38e9bf` |
| `origin/main` | `39176c589b2904892bcda362e0b404a24156aa77` |
| Behind / ahead vs main | behind 4 · ahead (feature commits on #615) |
| Dirty / untracked | **Preserved** (large unrelated set; Checkpoint A artifacts; Exa jobs; packages) |
| Open GDI-relevant PRs | **#615** WHO/HOW authorized path; also older GDI PRs (#589, #580, #578, …) |
| Integration conflicts | Do **not** start Exec Summary on dirty Checkpoint A branch without IC sync; coordinate with #615 merge |
| Staging `:8080` | Process up (`pid 24816`, started 2026-11-25) but **stale** vs current workspace file writes |
| Worktrees | Many historical Dealality worktrees exist; **no new worktree created** for this audit |

**Ongoing GDI development (do not disturb):** Checkpoint A closeout / #615 path wiring; Wyndham commercial continuation; WHO/HOW live adapter; Exa job store artifacts; publication packages.

---

## 3. Existing Architecture and Reusable Components

### 3.1 Hotel identity & selection
| Item | Location |
|---|---|
| Selectable hotels | `lib/group-demand-intelligence/repository.js` (`listGdiSelectableHotels`, `loadHotelProfile`) |
| Profile / onboard | `hotel-profile.js`, `hotel-gdi-profile-resolution-v1.js`, `adp-gdi-universal-hotel-roster-v1.js` |
| Canonical base IDs | `canonical-airtable-base.js` |
| Config | `config/group-demand-intelligence/hotels/*.json` |
| API | `GET /api/group-demand-intelligence/hotels`, `…/profile`, `…/summary` |
| UI selector | `#gdiHotel` in `dealality-gdi-ui.js` / `app.js` |

Canonical hotel key = Airtable Hotel Property Census `rec…` when resolved.

### 3.2 Navigation / tabs
| Tab | ID | Registration |
|---|---|---|
| Opportunities | `opportunities` | `GDI_MAIN_TAB` |
| Commercial Priorities | `priorities` | `GDI_PRIORITIES_TAB` |
| Portfolio Demand Intelligence | `portfolio` | `GDI_PORTFOLIO_TAB` |
| **Executive Summary** | — | **Not registered** |

Files: `public/group-demand-intelligence.html`, `public/js/group-demand-intelligence/{dealality-gdi-ui,app,hotel-need-times-priorities-tab,portfolio-demand-intelligence-tab}.js`.

**Recommendation:** Add as first tab in chrome **without** changing default landing until founder GO (Phase 1 can register but keep Opportunities as default).

### 3.3 Authorization & hotel ACL
- Feature flags: `feature-flag.js`
- Hotel scope bags on `req.dealalityUser`: `allowedHotelIds`, `gdiHotelIds`, …
- Contact reveal: `gdi-contact-reveal-authorization-v1.js`
- Share: `share/gdi-signed-share-capability-v1.js` (no Memberstack)
- Portfolio auth: `portfolio-demand-intelligence/gdi-portfolio-operator-authorization-v1.js`

### 3.4 Customer-visible inventory
Chain: `customer-visibility.js` → `canonical-customer-facing-inventory-v1.js` → commercial/actionability/expiration gates → publication packages (`publication-package/*`, `CURRENT.json` pointers).

### 3.5 Opportunities, drawers, deep links
- Store: `airtable-opportunity-store.js`, `opportunity-persistence.js`
- DTO: `opportunity-list-dto.js`
- Detail: `GET …/opportunities/:opportunityId` · deep link `?opportunityId=` → `openDetail`
- WHO/HOW drawer: `workflow/gdi-who-how-agent-contract-v1.js` (`buildWhoHowDrawerPresentation`)

### 3.6 Discovery → WHO/HOW → contact
- Discovery: `discovery-pipeline.js`, `opportunity-discovery-v5/`, weekly discovery orchestrators
- Full E2E V3: `workflow/gdi-canonical-full-e2e-v3-stages-v1.js`, PowerShell `scripts/gdi/Invoke-Gdi*.ps1`
- WHO/HOW: `gdi-who-how-agent-contract-v1.js`, `gdi-exa-who-how-live-adapter-v1.js`
- Contact stage: `gdi-contact-research-stage-v1.js`
- Surfe: on-demand reveal only (`gdi-surfe-live-gate-v1.js`) — **must not** fire from Exec Summary/PDF generation

### 3.7 Weekly Change
- Engine: `weekly-delta.js` (`NEW`, `UPDATED`, `REACTIVATED`, `UNCHANGED`, `CLOSED_DOWNGRADED`, …)
- Durability: `workflow/gdi-weekly-change-durability-v1.js`
- UI filter/pills in `dealality-gdi-ui.js`
- **Caveat:** `NEW` = new-to-GDI inventory identity, not “new to hotel from public web”

### 3.8 Scoring / segments / room nights / priorities / CAD
- Scoring: `scoring.js` + `config/group-demand-intelligence/scoring-weights.json`
- Segments: `demand-segments/*` (8/12/14-segment families)
- Room nights: opportunity fields + PDF derivation (`estimatedRoomNights` / peak rooms)
- Need periods / commercial priorities: `need-date-demand/*`, Priorities tab API `api/gdi-need-dates-portal.js`
- Competitive: `gdi-competitive-account-discovery-*`, `comp-set-demand-mining-v1/`

### 3.9 PDF / reports (reuse)
| Layer | Path |
|---|---|
| Generator | `reports/generate-gdi-report-pdf-v1.mjs` (Playwright) |
| Data | `reports/gdi-pdf-report-data-v1.js` |
| HTML | `reports/gdi-pdf-report-html-v1.js` |
| Cover family | `lib/dealality-report-family/dealality-report-cover-v1.js` |
| Shell/CSS | `dealality-pdf-document-shell-v1.js`, `dealality-pdf-report-css-v1.js` |
| Admin API | `api/admin-gdi-reports.js` |
| Test | `npm run test:gdi-pdf-report-v1` |

Existing PDF already has cover + **Commercial Demand Snapshot** KPIs + opportunity tables — but is **not** a strict 2-page executive briefing.

### 3.10 Activity / feedback / telemetry
- Feedback: `hotel-feedback-subsection-persistence-v1.js`, APIs under `…/feedback`
- Pursuit: `api/gdi-pursuit.js`, `pursuit/*`
- Observability: `observability/gdi-execution-ledger-store-v1.js`, research runs APIs
- Paid ledger: `research/gdi-authorized-paid-budget-ledger-v1.js`
- Exa jobs: `research/exa-agent/gdi-exa-agent-deep-job-store-v1.js`
- Completion queue: `research/gdi-commercial-research-completion-queue-v1.js`
- Persistence bridge counts: `workflow/gdi-full-e2e-research-persistence-bridge-v1.js` (`candidatesAfterDedupe`, exclusion reasons)

### 3.11 Summary API (partial Exec Summary precursor)
`GET …/hotels/:hotelId/summary` → `getGdiSummary` aggregates latest summary + pilot metrics + redaction via `redactCustomerFacingGdiSummary`. Useful Phase 1 seed; **not** a full Executive Summary contract.

---

## 4. Complete Metric Readiness Matrix

| Metric | Purpose | Source | Availability |
|---|---|---|---|
| Active customer-visible opportunities | Headline KPI | `filterCustomerFacingOpportunities` + package/CURRENT | **AVAILABLE_EXISTING** (hotel-dependent CURRENT) |
| New unique opportunities in period | Headline / Weekly | `weekly-delta.js` counts.NEW | **AVAILABLE_DERIVED** (define period vs run) |
| Material updates | Weekly | counts.UPDATED / REACTIVATED | **AVAILABLE_DERIVED** |
| Estimated potential room nights | KPI / outlook | `estimatedRoomNights` / peak rooms / PDF `estimateNights` | **PARTIAL_COVERAGE** |
| Estimated potential room revenue | KPI | PDF `revenueForOpp` | **PARTIAL_COVERAGE** — **blocks executive use until ADR source is hotel-authorized** (today hardcoded 165/195/235) |
| Opportunity of the Week / Top 5 | Priority | scoring + FQO + need-period fit + contact actionability | **AVAILABLE_DERIVED** (compose; no new model required for V1) |
| Demand segment mix | Narrative / outlook | primary demand segment fields | **AVAILABLE_EXISTING** |
| Need-period alignment | Outlook | need-dates portal / commercial profile | **PARTIAL_COVERAGE** |
| Contact readiness tiers | Readiness | `gdi-contact-actionability-v1.js` | **AVAILABLE_DERIVED** |
| WHO/HOW verified buyers / entry points | Readiness | whoHowStatus on opportunities | **PARTIAL_COVERAGE** (post-Checkpoint A uneven vs published inventory) |
| Demand signals reviewed | Research Receipt | discovery/ORS candidate counts in run reports | **TELEMETRY_REQUIRED** (exists in runs; not hotel-period receipt contract) |
| Duplicates consolidated | Receipt | persistence bridge dedupe | **TELEMETRY_REQUIRED** |
| Irrelevant excluded | Receipt | screening dispositions / exclusionReason | **TELEMETRY_REQUIRED** |
| Qualified / FQO | Receipt | FQO contract before/after counts | **AVAILABLE_DERIVED** from runs; not customer receipt |
| Revalidated existing | Receipt | weekly refresh / revalidation events | **PARTIAL_COVERAGE** |
| Stakeholders researched / contact paths validated | Receipt | WHO/HOW jobs + contact stage + Surfe ledger | **PARTIAL_COVERAGE** (Surfe = on-demand; do not over-claim) |
| Shared vs hotel-specific research attribution | Receipt | run context / hotelId on jobs | **TELEMETRY_REQUIRED** (hotelId often present; shared-market split not productized) |
| Forward nights by month | Outlook | demand dates + ranges | **PARTIAL_COVERAGE** — aggregation rules needed; avoid double-count |
| Lead time 30/60/90 | Head start | firstSeen vs demand date | **AVAILABLE_DERIVED** if both dates populated |
| Pursuits / conversations / proposals | Outcomes | pursuit + feedback enums | **PARTIAL_COVERAGE** |
| Booked nights / booked $ / realized $ | Outcomes | feedback/outcome fields | **HOTEL_INPUT_REQUIRED** / **NOT_CURRENTLY_SUPPORTED** as durable $ ledger |
| Dealality monthly investment / break-even | Investment recovery | — | **HOTEL_INPUT_REQUIRED** + authorization |
| Contribution margin / ADR assumptions | Investment recovery | — | **HOTEL_INPUT_REQUIRED** (must not reuse synthetic PDF ADR) |

---

## 5. Research Receipt Telemetry Assessment

| Question | Finding |
|---|---|
| Consistent research run IDs? | **Partial** — execution monitor / research-runs APIs + weekly-runs folders; not one customer receipt schema |
| Candidate counts before/after stages? | **Yes in research reports** (ORS `candidateCount`, persistence bridge dedupe, FQO before/after) — not rolled into hotel executive receipt |
| Dedup / exclusion reasons persisted? | **Yes** in bridge/disposition artifacts; uneven across hotels/periods |
| New vs update distinction? | **Yes** via weekly-delta + firstSeen semantics |
| Revalidation events? | **Partial** — weekly refresh contracts; not always customer-visible |
| Stakeholder / contact validation events? | WHO/HOW job store + contact stage + Surfe usage ledger — **Surfe must stay explicit-click** |
| Hotel attribution? | Job keys / ledgers include `hotelId` often; **shared market runs must not be fully attributed to every hotel** |
| Historical metrics? | Admin historical recovery exists; **do not invent history** for hotels without run evidence |
| Add telemetry without changing qualification? | **Yes** — append-only receipt projection from existing ledgers/queues/reports |

### Proposed Research Receipt data contract (design only — not implemented)

```text
ResearchReceiptV1 {
  hotelId, reportingPeriod {start,end,label}, dataCutoffAt,
  attributionPolicy: "HOTEL_SPECIFIC_PLUS_SHARED_MARKET_CONTEXT",
  sharedMarketContext { marketId?, runIds[], note },
  hotelSpecific {
    demandSignalsReviewed,
    potentialOpportunitiesIdentified,
    duplicatesConsolidated,
    excludedUnsupported,
    qualifiedCount,
    newUniquePublished,
    existingRevalidated,
    stakeholdersResearched,
    contactPathsValidated,
    contactPathsImproved
  },
  evidenceRefs: [ runId | jobKey | reportPath ],
  unknownMetrics: [ metricId… ]  // never coerce to 0
}
```

---

## 6. Representative Hotel Readiness Matrix

| Hotel | ID | Local opps | CURRENT pub | Need periods | Feedback | PDF cover/archive | Exec Summary readiness |
|---|---|---:|---|---|---|---|---|
| Bethesda Marriott | `recLuxvwwxID7U2B8` | 86 | **YES** | Yes (airtable mirror) | Yes (treat foundation samples carefully) | Strong (GDI+ADP archives) | **Best pilot** |
| Wyndham SD Bayside | `recnPB3Q02NO8Ma1o` | 82 | **NO** (STAGED packages) | Yes | No | Local research rich; no CURRENT | **PARTIAL** — inventory ≠ customer-published |
| Renaissance NYTS | `recG66DQJKP2c0UNh` | 29 | **NO** | Yes | No | ADP-strong / GDI pub thin | **PARTIAL** |
| Sheraton Guadalajara Expo | `recCAGqkERSXgiku4` | 43 | **NO** | Market-estimated | No | ADP monthly archive | **PARTIAL** |
| YOTEL Geneva Lake | `recrPQcZg7SFARRb2` | 42 | **YES** | Yes | No | CURRENT package | **Strong second pilot** |

No new opportunities created; no paid research run for this audit.

---

## 7. Existing PDF Infrastructure Assessment

**Reuse:** Dealality report family cover + Playwright shell (same chrome as ADP). GDI already generates branded PDFs with confidential footers and page numbers.

**Gaps vs V1 requirements:**
1. **Page budget:** records `pageCountHint`; **no fail-closed “exactly 2 pages”**.
2. **Cover imagery:** Dealality logo hero — **not hotel property photography** (requirement asks to reuse property imagery if in family; today GDI cover is logo-centric).
3. **Content density:** current HTML has many sections (snapshot, pursuits, opportunities, 30-day plan, pipeline, supporting intel) → naturally multi-page.
4. **Synthetic ADR** in `gdi-pdf-report-data-v1.js` — **must not** feed executive “potential revenue” without hotel-authorized assumptions.
5. Validation approach: Playwright generate → count `/Type /Page` → **fail if ≠ 2**; layout regression screenshots; max narrative/table budgets in composer.

---

## 8. Proposed Executive Summary Data Contract (design)

```text
ExecutiveSummaryViewV1 {
  hotelId, hotelName, reportingPeriod, generatedAt, templateVersion,
  narrative { assessment, segments, materialChanges, implications, managementFocus },
  kpis { activeVisible, newUniqueInPeriod, estimatedRoomNights, estimatedRevenue? },
  opportunityOfTheWeek { opportunityId, why, period, potential, nextAction, deepLink },
  topFiveNextMoves: [ { opportunityId, rankReason, period, nights, readiness } ], // 0..5
  weeklyChanges { new, updated, contactImproved, expiredOrWithdrawn },
  researchReceipt: ResearchReceiptV1 | { unavailableReason },
  forwardOutlook { byMonth[], uncertainDateCount, mixBySegment },
  contactReadiness { salesReady, reachable, contactResearchRequired, identificationRequired },
  commercialHeadStart { medianLeadDays?, pct30?, pct60?, pct90? } | unavailable,
  commercialActivity { pursued, conversations, proposals, bookedNights?, bookedRevenue?, realized? },
  investmentRecovery { … } | { status: "HOTEL_INPUT_REQUIRED"|"UNAUTHORIZED" },
  provenance { opportunityIds[], cutoff, packageId?, snapshotId? },
  generationGuarantees { noPaidDiscovery: true, noContactEnrichment: true }
}
```

Deep links: `/app#/group-demand-intelligence?hotelId=…&opportunityId=…` (existing pattern).

---

## 9. Proposed Research Receipt Data Contract

See §5. Minimal repeatable projection from: execution ledger, completion queue dispositions, persistence-bridge dedupe, weekly-delta counts, WHO/HOW job completions, contact-stage outcomes, Surfe usage (counts only, no PII persistence).

---

## 10. Proposed Two-Page PDF Content Contract

**Page 1 — Cover (existing family)**  
Dealality branding · hotel name · GDI · title “Executive Commercial Briefing” · period · publication date · confidentiality · report id. Property image: **gap** vs current logo-only cover — decide reuse of hotel hero from ADP/HI assets where authorized.

**Page 2 — Briefing (strict)**  
1. Hotel + period  
2. Four KPIs (active / new / nights / revenue-or-unavailable)  
3. Executive assessment (max ~80–100 words)  
4. Top 5 table: name · period · nights (max 5 rows)  
5. Compact outlook (small chart or 3–4 month bars)  
6. Research Receipt (4 metrics only if evidenced)  
7. Commercial impact / investment recovery **conditional**  
8. Management focus (≤3 bullets)  
9. Deep link to hotel in Dealality  

**Overflow:** shorten narrative → drop conditional sections → reduce table rows → **fail validation** rather than page 3. No font-size collapse.

---

## 11. Customer Authorization and Privacy Assessment

| Concern | Control |
|---|---|
| Hotel isolation | Existing ACL bags + hotelId scoping |
| Unpublished opportunities | Customer-facing filters + CURRENT/package scope |
| Surfe emails/phones | Explicit-click only; **never** persist into Exec Summary/PDF |
| Research costs / run IDs | Already admin-redacted in customer summary |
| Financial assumptions | Must be authorized per hotel; never default synthetic ADR |
| Share links | Signed capability tokens; no Memberstack on share |
| Exec Summary generation | Read-only aggregation; **no** discovery/WHO/HOW/Surfe triggers |

---

## 12. Implementation Phases and Dependencies

Adjusted sequence (architecture-supported):

| Phase | Scope | Depends on |
|---|---|---|
| **1 — Foundation** | Tab chrome (non-default), read-only aggregation from summary + customer inventory + deep links + KPI cards that are AVAILABLE_* | #615 synced home optional |
| **2 — Priority & Weekly** | OOTW + Top 5 using existing scoring/FQO/need-period/readiness; Weekly Change panel | Phase 1 |
| **3 — Research Receipt V1** | Receipt projection + instrumentation gaps only (append-only) | Telemetry contract approval |
| **4 — Narrative + Outlook** | Deterministic assessment templates + forward nights aggregation rules | Phase 1–2; nights coverage |
| **5 — Two-page PDF** | New page-budget composer on Dealality family + snapshot store + page-count gate | Phase 1–4 metrics freeze |
| **6 — Outcomes & investment recovery** | Hotel feedback quality + authorized financial inputs | Hotel input + privacy |
| **7 — Distribution** | Suggested email subject/body; later scheduled send | PDF snapshots |

Do **not** duplicate discovery/qualification engines.

---

## 13. Estimated Relative Effort

| Increment | Relative effort |
|---|---|
| Phase 1 Foundation | **S** |
| Phase 2 Priority + Weekly | **S–M** |
| Phase 3 Research Receipt | **M** (telemetry contract + attribution rules) |
| Phase 4 Narrative + Outlook | **M** (aggregation correctness) |
| Phase 5 Strict 2-page PDF | **M** (layout + validation; cover image decision) |
| Phase 6 Outcomes / recovery | **L** (data + auth) |
| Phase 7 Distribution | **S** then **M** for automation |

---

## 14. Risks, Blockers and Open Decisions

1. **Synthetic ADR in current GDI PDF data** — executive revenue KPI blocked until hotel-authorized ADR/contribution.  
2. **CURRENT publication gap** — Wyndham/Renaissance/Sheraton lack CURRENT → customer Exec Summary must not silently use STAGED research inventory.  
3. **Research Receipt attribution** — shared market vs hotel-specific split is the hardest correctness problem.  
4. **Strict 2 pages** — current multi-section PDF will not fit; needs new composer + fail-closed tests.  
5. **Cover property imagery** — not in current GDI cover primitive.  
6. **Feedback quality** — Bethesda feedback may include foundation-test noise.  
7. **Stale `:8080`** — integrated staging process not serving latest `ws-gdi` writes.  
8. **Dirty Checkpoint A branch** — Exec Summary implementation must start from synced `ws/gdi` after IC, not pile onto #615 unless intentional.  
9. Open decision: default tab vs first-tab-but-not-default.  
10. Open decision: weekly vs monthly snapshot identity and retention.

---

## 15. Proposed Files and Components to Modify (future — not now)

| Area | Likely touchpoints |
|---|---|
| Tab chrome | `dealality-gdi-ui.js`, `app.js`, CSS |
| Aggregation module | **new** `lib/group-demand-intelligence/executive-summary/*` |
| API | **new** `GET …/hotels/:hotelId/executive-summary` (+ period query) |
| Receipt projection | **new** + thin hooks into execution ledger / weekly-delta / WHO-HOW job store |
| PDF | **new** `gdi-executive-briefing-pdf-v1.*` reusing `dealality-report-family` |
| Snapshots | reuse/extend `gdi-pdf-store-v1` or publication-package snapshot pattern |
| Tests | new `test:gdi-executive-summary-*` / `test:gdi-executive-briefing-pdf-v1` |
| PowerShell | `scripts/gdi/Invoke-GdiExecutiveSummary.ps1`, `Invoke-GdiExecutiveBriefingPdf.ps1` |

---

## 16. Proposed PowerShell Workflows and Regression Tests

```powershell
# Future (not implemented this phase)
.\scripts\gdi\Invoke-GdiExecutiveSummaryAudit.ps1 -HotelId <rec…> -Period weekly
.\scripts\gdi\Invoke-GdiExecutiveSummary.ps1 -HotelId <rec…> -Mode DryRun -Period weekly
.\scripts\gdi\Invoke-GdiExecutiveBriefingPdf.ps1 -HotelId <rec…> -Mode DryRun
.\scripts\gdi\Invoke-GdiExecutiveBriefingPdfValidate.ps1 -PdfPath <path>  # must be exactly 2 pages
```

**Automated tests to require:**
- Hotel isolation / ACL  
- Published vs unpublished  
- Dedup / period boundaries  
- Shared-market attribution  
- Missing vs zero  
- Demand aggregation no double-count  
- Outcome attribution  
- Financial assumption authorization  
- **No paid API** during summary/PDF (forceMock / network assert)  
- **Exactly 2 PDF pages**  
- Long names / missing-data layout  
- Surfe not invoked  

---

## 17. Recommended First Implementation Increment

**Phase 1 only — Executive Summary Foundation (read-only):**

1. Register Executive Summary tab (visible) but **keep Opportunities as default landing**.  
2. New aggregation module + `GET …/executive-summary` reading **customer-visible** inventory only.  
3. KPIs limited to: active visible count, weekly NEW count, nights-where-populated (show unavailable when missing), **omit revenue** until ADR authorized.  
4. Opportunity of the Week + Top 5 deep links using existing scoring + readiness + need-period signals.  
5. Explicit empty/unavailable states (never fabricate).  
6. Guarantee: generation path cannot call Exa/Surfe/Findymail.

Defer Research Receipt, investment recovery, and 2-page PDF to later phases after telemetry/ADR decisions.

---

## Explicit answer

### What is the most important thing we are missing that could prevent this from delivering genuine executive value?

**A hotel-attributable Research Receipt and commercial-outcome spine that executives can trust** — i.e., evidenced “what Dealality did for *this* hotel this period,” cleanly separated from shared market research, plus non-synthetic economic outcomes.  

A beautiful tab/PDF over inventory counts and hardcoded ADR scenarios would **look** executive-ready while failing the core promise: *we do the hard work so hotels can focus on winning business.*

---

## Acceptance checklist (Phase 0)

- [x] Existing functionality inspected (not assumed)  
- [x] Metrics traced to sources or documented gaps  
- [x] Research Receipt feasibility assessed  
- [x] Report styling/PDF family identified  
- [x] Strict 2-page validation approach proposed  
- [x] Auth / Surfe privacy preserved in design  
- [x] No duplicate discovery/qualification proposed  
- [x] Incremental plan documented  
- [x] No paid API spend for this audit  
- [x] No customer-visible data changed  
- [x] No unrelated repo mutations for product code  

**Final status:** `GDI_EXECUTIVE_SUMMARY_PHASE_0_AUDIT_COMPLETE — AWAITING_FOUNDER_GO`
