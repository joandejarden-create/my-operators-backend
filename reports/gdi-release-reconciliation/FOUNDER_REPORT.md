# GDI release reconciliation — 2026-10-15

**Workspace:** `C:\Dev\dealality\my-operators-backend`  
**Generated:** from live local Airtable-backed API + production read-only inspection + prior freeze/scratch artifacts.

## Inspected baseline (actual, not assumed)

| Item | Actual value |
| --- | --- |
| `origin/main` at branch start | `e0bf9c19` (PR #95) |
| Prior pending HANDOFF docs commit | `e58b6747` (local-only; superseded by this reconciliation PR) |
| Production Railway SUCCESS | `e20f96f9-f1d2-4ae2-b660-f12638c04acd` serving code `e0bf9c19` pre-fix |
| Local + prod assets (pre-fix) | `gdi-tile-contact-toolbar-v1` |
| Persistence | Airtable (`GDI_OPPORTUNITIES_PERSISTENCE=airtable`) |
| `GDI_SHARE_CAPABILITY_ALLOW_DEV_SECRET` (Railway) | `0` |
| Held worktrees | `gdi-release-main`, `gdi-main-consolidate` preserved |

Freeze artifact: `C:\Dev\dealality\gdi-release-main\reports\gdi-research-history-opportunity-type\facing-inventory-freeze.json` (`frozenAt` 2026-10-15T12:27:57Z).  
Prior record-level notes: `reports/gdi-quality-review/INVENTORY_RECONCILIATION_RECORD_LEVEL.md`.  
Machine inspection: `inspection-raw.json`, `scripts/gdi-release-reconciliation-inspect-v1.mjs`.

---

## A. San Diego 45 versus 38

### Authoritative scopes

| Scope | Definition | Count | Source |
| --- | --- | ---: | --- |
| Broad stamp | `prospectHotelFacing \|\| customerVisible` | **46** | Airtable canonical (77 rows); prior integrity/parity |
| Freeze / list gate (canonical fields) | `isCustomerFacingOpportunity` on **unsanitized** Airtable rows | **38** | Freeze + fresh Airtable load |
| Live list API **before fix** | Auth list after `projectOpportunitiesCommercialQuality` then filter | **45** | Local :8080 + production (matched each other) |
| Live list API **after fix** | Filter on canonical, then sanitize for presentation | **38** | Local :8080 after restart |

**45 was not an unsupported historical report.** Local and production both returned 45 under the broken order.  
**38 is the correct customer-list total** under the reviewed-prospect publication gate on durable fields.

### Root cause (proven)

`getGdiOpportunities` / export ran `sanitizeOpportunityForCustomerPresentation` **before** `filterCustomerFacingOpportunities`.

Sanitize fills `summaryWhat` from `summaryWhyMatters` when `summaryWhat` is null.  
`evaluateProspectSuitability` treats non-empty `summaryWhat` as travel evidence.  
Seven PE preferred-lodging rows with `prospectPublicationBasis=REVIEWED_PACKAGE_V1` but missing durable travel evidence therefore flipped from enrichment-queue → hotel-facing **only after sanitize**.

Reproduction (Airtable load):

- before sanitize → filter: **38**
- sanitize → filter: **45**

Share path already filtered before sanitize (no change needed there).

### Discrepant IDs (one row each) — live 45 vs freeze/canonical 38

All seven remain in Airtable canonical (not deleted). Exclusion rule after fix: `reviewed_visibility_gate_excluded` / suitability `travel_or_recurring_evidence` (`ENRICHMENT_QUEUE`, `hotelFacing=false`).

| Opportunity ID | Title | Type | In Airtable | Canonical list-facing | Pre-fix list API | Freeze | Exact exclusion |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `gdi_pe_295cfef5822e63c3` | Sparks Gallery — Preferred Lodging Partnership | VENUE_PARTNERSHIP | yes | no | yes (defect) | no | `travel_or_recurring_evidence` |
| `gdi_pe_879047c3211d2e82` | City Cruises San Diego — Preferred Lodging Partnership | VENUE_PARTNERSHIP | yes | no | yes (defect) | no | same |
| `gdi_pe_5ce20a54b903e243` | University Club San Diego — Preferred Lodging Partnership | VENUE_PARTNERSHIP | yes | no | yes (defect) | no | same |
| `gdi_pe_ca47fef6aa622566` | deck655 — Preferred Lodging Partnership | VENUE_PARTNERSHIP | yes | no | yes (defect) | no | same |
| `gdi_pe_5f72b9d7665314b5` | Flagship Cruises & Events — Preferred Lodging Partnership | VENUE_PARTNERSHIP | yes | no | yes (defect) | no | same |
| `gdi_pe_4b9b49bae4136428` | Nolita House — Preferred Lodging Partnership | VENUE_PARTNERSHIP | yes | no | yes (defect) | no | same |
| `gdi_pe_a7a2fda2a474aa0b` | The House Next Door — Preferred Lodging Partnership | VENUE_PARTNERSHIP | yes | no | yes (defect) | no | same |

### Broad 46 versus list 38 (eighth ID)

| Opportunity ID | Title | Type | In Airtable | List-facing | Freeze | Exact exclusion |
| --- | --- | --- | --- | --- | --- | --- |
| `gdi_sc_188e25b6b3b56637` | The Shipyard — Scaling Corporate Account | CORPORATE_ACCOUNT_RELATIONSHIP | yes | no | no | `missing_publication_basis` (`prospectPublicationBasis` null) |

**Arithmetic:** freeze/list **38** + seven PE (sanitize defect) = live **45**; + Shipyard = broad **46**.  
No bulk restore. Fix is API order only (code), not record-level publication changes.

### Presentation-release “38 unchanged” claim

Compared to freeze (`facingCount` 38, `frozenAt` 2026-10-15T12:27:57Z), live list was **not** unchanged — it was **45** until this filter-order fix. Acceptance scripts that printed 38 were measuring the intended gate, while the browser/API served 45.

---

## B. Mayflower 35 versus 30

**Fully explained** by scope mismatch (broad stamp vs list gate). Live local + production list both **30**, identical to freeze.

| Opportunity ID | Title | Type | In canonical | List-facing | Exact exclusion |
| --- | --- | --- | --- | --- | --- |
| `gdi_opp_prospect_mv8fnmk1_c41c0a4c` | U.S. Travel Association Destination Capitol Hill | OVERFLOW_HOUSING | yes | no | `customer_visible_but_not_prospect_hotel_facing` |
| `gdi_opp_prospect_mv8fnmk1_e4668916` | U.S. Conference of Mayors — next DC cycle | OVERFLOW_HOUSING | yes | no | same |
| `gdi_opp_prospect_mv8fnmk2_a845a23a` | AAD Annual Meeting 2028 | OVERFLOW_HOUSING | yes | no | same |
| `gdi_opp_prospect_mv8fnmk1_ad300879` | Arts Advocacy Summit — next DC cycle | OVERFLOW_HOUSING | yes | no | same |
| `gdi_sc_0fe98055e8df9f75` | Wisedocs — Scaling Corporate Account | CORPORATE_ACCOUNT_RELATIONSHIP | yes | no | `missing_publication_basis` |

No API defect on Mayflower counts. No restore.

---

## C. Research history

### Live summary (local ≡ production, Airtable-backed)

| Field | Mayflower | San Diego |
| --- | --- | --- |
| Last research activity | **unavailable** (`lastResearchAt` null) | **unavailable** |
| Research execution status | **Status unavailable** (`STATUS_UNAVAILABLE`) | same |
| Provenance | `opportunity_run_id_without_completion` (`pe_coverage_expansion_2026_10_15` / `pe_sd_may_canonical_2026_10_15`) | same pattern |
| Last source verification | `2026-10-15T09:08:55.030Z` | `2026-10-15T09:09:05.829Z` |
| Last published update | same as source verification stamps above | same |

### Interpretation

This is **missing durable eligible execution evidence**, not a mapping/rendering defect that hides live successful research.

Eligible ledger events: **0** (Mayflower has ineligible recovered weekly manifests only).

Excluded with audit trail preserved (not clamped):

- Oct 29 simulation `gdi_weekly_rec0eTEn_2026-10-29T12-00-00-000Z_c2ef69` — `dry_run` + `test_or_simulated_clock` + `materially_future_timestamp`
- Oct 15 weekly manifests — `dry_run`

A `runId` alone correctly does **not** imply Completed.  
Publication/source-verification timestamps remain visible and separate.  
No invented completion persisted.

---

## D. Presentation coexistence (local verified after fix; production after deploy)

Checked on shared renderer (`dealality-gdi-ui.js` / `gdi-tile-contact-toolbar-v1`):

- One compact contact block per tile (named / org / referral / Contact form)
- Drawer retains person + organization details
- Desktop controls row + history footer
- Static Why This Hotel score tiles (value + title; no Details expansion)
- HCAOA hotel-fit does not repeat occasion summary as hotel explanation (sanitize `distinctFromOccasion`)
- Provider names kept out of customer contact notes via sanitize

Cases: Mayflower HCAOA + Quorum; San Diego list; org fallback (ACEC/ASID); form-only (Virtru). Bethesda not re-audited in browser this pass beyond shared-renderer smoke — no hotel-specific templates added.

---

## E. Versions / documentation

| Item | Value |
| --- | --- |
| Fix branch | `gdi/release-reconciliation-v1` |
| Code change | Filter-before-sanitize on auth list + CSV export |
| Test | `scripts/test-gdi-list-visibility-before-sanitize-v1.mjs` |
| Local post-fix counts | Mayflower **30**, San Diego **38** |
| HANDOFF | Updated in this PR under `reports/gdi-local-production-parity/HANDOFF.md` |

Deploy SHA / Railway ID: filled after clean `deploy:production`.

---

## F. Remaining blockers

1. **History unavailable** remains honest until a non-dry-run, non-simulated research completion is recorded in the durable ledger.  
2. Seven SD PE rows stay off the customer list until durable travel/recurring evidence satisfies suitability (product decision — not auto-restored).  
3. Shipyard remains off list until `prospectPublicationBasis` is set intentionally.
