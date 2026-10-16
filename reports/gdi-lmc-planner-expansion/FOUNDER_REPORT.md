# LMC + Planner Expansion — Mayflower & San Diego

Generated: 2026-10-15T21:35:00.000Z

## Verdict

Fresh paid Parallel research executed for **Mayflower** and **Wyndham San Diego Bayside** only through the standard coordinator. **13 unique** LMC/planner prospects qualified; **12 published customer-facing**, **1 held internal** (WHF — form-only contact). Bethesda used as reference/regression only. Queued hotels not started. PE excluded records not restored.

## Budget (authoritative ledger)

| Item | Value |
|------|-------|
| Authorized | **$11** (existing — no new allowance) |
| Spent before this pass | $3.85 |
| This pass (4 × $0.35 est.) | **$1.40** |
| Spent after | **$5.25** |
| Remaining uncommitted | **$5.75** |

Tasks charged:

- `paid_ms:lmc_planners_social_may_v1` — `trun_567f3335d0c140ad84cb51c43c0b4baa`
- `paid_ms:lmc_corporate_assoc_may_v1` — `trun_567f3335d0c140ad8222efec4611b85a`
- `paid_ms:lmc_planners_social_sd_v1` — `trun_567f3335d0c140ad8b6191e9c2922d9f`
- `paid_ms:lmc_corporate_assoc_sd_v1` — `trun_567f3335d0c140ad8e3066a4795127f5`

Surfe: **$0** this pass (no named person established for enrichment). Unpaid postprocess (dedupe + planner reclassification) after Parallel.

## Research executed (by hotel × subsegment)

### Mayflower (`rec0eTEnCqlsGJJt9`)

| Task | Subsegments | Candidates returned | Status |
|------|-------------|---------------------|--------|
| `lmc_planners_social_may_v1` | Professional planners; Social catering | 3 | completed — fresh research |
| `lmc_corporate_assoc_may_v1` | Corporate meetings; Room rental; Association; Nonprofit | 6 | completed — fresh research |

### San Diego (`recnPB3Q02NO8Ma1o`)

| Task | Subsegments | Candidates returned | Status |
|------|-------------|---------------------|--------|
| `lmc_planners_social_sd_v1` | Professional planners; Social catering | 3 | completed — fresh research |
| `lmc_corporate_assoc_sd_v1` | Corporate meetings; Room rental; Association; Nonprofit | 4 | completed — fresh research |

This was **not** a reuse-only or query-plan-only pass. Four new Parallel discovery runs with distinct job keys (did not resume prior `lmc_discovery_*`).

## Reused vs genuinely new

| Bucket | Count | Notes |
|--------|-------|-------|
| Prior LMC inventory (unchanged) | May 4 + SD 5 = **9** | ACEC, MBA/MW, ASID, Aero Club; Urban League, Arc, CMAA, BOMA, Downtown Partnership |
| Parallel raw candidates | 16 | Before dedupe |
| After dedupe / reclassify | **13 unique new** | Dropped Evoke Design & Creative (→ Evoke), Cohera-SD (→ Cohera), duplicate Gaslamp |
| Duplicate-existing vs prior LMC | **0** | Explicitly excluded known orgs in prompts |

## Qualified / published / internal

| Hotel | Researched (unique) | Qualified | Published (facing) | Internal |
|-------|---------------------|-----------|--------------------|----------|
| Mayflower | 8 | 8 | **7** | **1** (WHF form-only) |
| San Diego | 5 | 5 | **5** | 0 |
| **Total** | **13** | **13** | **12** | **1** |

### Mayflower — newly published

| Org | Kind | Contact route | Hotel relationship |
|-----|------|---------------|--------------------|
| Stratus Firm | Planner | phone + form | not yet confirmed |
| Evoke | Planner | INFO@EVOKEDC.COM + phone | not yet confirmed |
| A. Dominick Events | Planner | info@adominick.com + phone | not yet confirmed |
| Events by Andre Wells | Planner | info@eventsbyandrewells.com | not yet confirmed |
| CSI DMC - Washington, DC | Planner (DMC) | contact form URL | not yet confirmed |
| N Street Village | Association / nonprofit catering | info@nstreetvillage.org + phone | **documented prior Mayflower host** (2015 Empowerment Luncheon) — new to GDI ≠ unknown to hotel |
| Women in Housing & Finance | Association / professional meeting | contact form only | **INTERNAL** — `no_usable_route` (form not customer-facing) |
| DCBIA | Association awards / banquet | info@dcbia.org + phone | not yet confirmed |

### San Diego — newly published

| Org | Kind | Contact route | Hotel relationship |
|-----|------|---------------|--------------------|
| Cohera | Planner / DMC-style intermediary | ContactSanDiego@meetcohera.com + phone | not yet confirmed |
| Thomas Bui Lifestyle | Planner (social / gala) | thomas@… + phone | not yet confirmed |
| Gaslamp Event Management | Planner / producer | gaslampevent@gmail.com + phone | not yet confirmed |
| Kiwanis Club of San Diego | Recurring luncheon buyer (currently Westgate) | phone | not yet confirmed — alternate hotel pitch |
| San Diego Police Foundation | Nonprofit luncheon / fundraiser | phone + contact page | not yet confirmed |

## Named contacts vs organization fallbacks

| Metric | Count |
|--------|-------|
| Named person contacts | **0** |
| Organization fallback routes (email/phone/form) | **13 / 13** |

No Surfe spend — policy requires a named person before enrichment. Org routes are usable for first outreach.

## Inventory vs publication visibility

| Hotel | LMC stored inventory | Customer-facing visible (all LMC) | Newly published facing |
|-------|----------------------|-----------------------------------|------------------------|
| Mayflower | **12** (4 prior + 8 new incl. 1 internal) | prior 4 + new 7 = **11** LMC facing | **7** |
| San Diego | **10** (5 prior + 5 new) | **10** | **5** |

Visibility proof: published rows have `prospectHotelFacing=true` + `customerVisible=true` + `prospectPublicationBasis=REVIEWED_PACKAGE_V1` and pass `filterCustomerFacingOpportunities`. WHF held `customerVisible=false` / `prospectHotelFacing=false`. Artifacts: `publication-package.json`, `publication-diff.json`, `api-visibility-proof.json`.

Production reads Airtable — data is live for list/detail without requiring a UI deploy for these opportunity rows. Local code synced LMC v1.2 builders for correct titles/subsegments on write.

## Distinctions preserved

- **Hotel-hosted catering/meetings:** N Street Village, WHF, DCBIA, Kiwanis, SD Police Foundation
- **Professional planner relationships:** Stratus, Evoke, A. Dominick, Andre Wells, CSI DMC, Cohera, Thomas Bui, Gaslamp Event Management (Demand Generator path — not PE venues)
- **Outside-venue lodging referrals:** unchanged on PE path; no PE bulk restore

Guestrooms: all new LMC cards use `guestroomStatus=NOT_REQUIRED` / catering-capable framing where appropriate. No invented dates, room blocks, buying authority, or kosher capability.

## Time, cost, depth, gaps

| Dimension | Result |
|-----------|--------|
| Wall time (4 Parallel tasks) | ~11 minutes |
| Estimated cost | $1.40 of $11 |
| Research depth | Fresh Parallel discovery + unpaid qualification/dedupe/persist |
| Named-buyer gap | All 13 are org-route; named enrichment still open |
| Corporate training / room-rental pure plays | Thin — Parallel returned more planners + associations than pure corporate training accounts |
| Social / mitzvah | Covered via professional planners (Evoke lists mitzvah category; Thomas Bui social) — no private family/children data |
| Queued hotels | Not started |
| Kosher / outside-catering hotel constraints | Still UNKNOWN |

## Artifacts

- `reports/gdi-lmc-planner-expansion/` — execution, packages, publication, backups, proofs
- `reports/gdi-multisegment-research-delivery/lmc-expansion-packages.json`
- `data/group-demand-intelligence/paid-multisegment-budget/authorization-ledger-v1.json` (updated)
- Backups under `reports/gdi-lmc-planner-expansion/backups/`

## What was not done (explicit)

- Renaissance Times Square / Falls Church Marriott Fairview Park / Renaissance Arlington Capital View — **queued, not started**
- Bethesda — reference only
- New $11 allowance / ledger reset — **not created**
- PE excluded-record bulk restore — **not done**
- Surfe named enrichment — deferred (no named persons)
- Calling this “market coverage complete” — **no**; task complete ≠ market saturated
