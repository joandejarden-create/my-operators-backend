# GDI release closeout

**Date:** 2026-10-15  
**Host:** `https://my-operators-backend-production.up.railway.app`  
**Verdict:** Mayflower and San Diego drawer/contact presentation fixes are live in production on the shared hotel-generic path. Future hotels inherit the same contracts without handwritten repair packages.

---

## Explicit answers

### Is Mayflower fixed in production?

**Yes.** Production assets are `?v=gdi-closeout-v2` on Railway deployment `e6265e1d-2ce0-49b2-a04f-da5982d46318` (merge SHA `c7deaaa5`).

Browser-verified Mayflower share drawer for **AHIMA Advocacy Summit 2027** (`gdi_opp_prospect_mv8fnmju_53cfad28`):

- One **Overall Hotel Fit** tile (provisional **61**); no duplicate “Hotel fit” meta row.
- Why This Hotel no longer leads with dimension-level assessment-status prose (shows explicit “not yet established” + dimension grid).
- Contactability and other dimension labels render; missing dimensions show **Needs verification** / incomplete evidence states.
- Evidence confidence shows scale (**38 / 100**); Sales posture and Sales action remain distinct.
- No Surfe/Webhound/provider metadata in drawer copy.
- Opportunity Type dropdown sits beside This Week; research history shows a recovered Completed run (not false Never).

After screenshot: `reports/gdi-quality-review/mayflower-ahima-drawer-after-closeout-v2.png`

### Do future hotels receive these checks automatically?

**Yes.** There is no Mayflower/San Diego-only renderer, CSS, or quality bypass. Contracts execute on the shared pipeline below for every hotel on the allowlist.

---

## Where the standard contracts execute

| Stage | Module / path |
| --- | --- |
| Hotel profile → research | `workflow/hotel-segment-research-*`, weekly refresh orchestrator |
| Hotel relevance / fit | `workflow/hotel-fit-assessment-v1.js` (+ narrative consistency) |
| Contact enrichment | `workflow/buyer-contact-enrichment-*`, `contact-display-surface-v1.js` |
| Card / drawer generation | Shared `public/js/group-demand-intelligence/dealality-gdi-ui.js` |
| Canonical persistence | Airtable opportunity payload JSON + contact store |
| List / detail DTO | `opportunity-list-dto.js` + `customer-presentation-sanitize-v1.js` via `projectOpportunitiesCommercialQuality` in `api/group-demand-intelligence.js` |
| Weekly refresh | `workflow/weekly-refresh-*` (must not restore stale flat scores / provider leakage) |

Assessment states, scores, rationales, evidence refs, assessment date, and rule version travel in structured hotel-fit fields on the opportunity payload. Presentation scrubbing is hotel-agnostic.

---

## Acceptance by hotel and opportunity type

### Production SHA / assets (verified)

| Claim in prior report | Actual at closeout start | Actual after closeout |
| --- | --- | --- |
| `gdi-drawer-contact-v3` / Railway `8963208d` | **Stale.** Live was `gdi-history-type-v1` on `8d3530f8` (commit `23ed5b8d`). `8963208d` was an older REMOVED deploy. Querystring `?v=gdi-drawer-contact-v3` still 200 (static ignore). | **`gdi-closeout-v2`** on **`e6265e1d`**, merge SHA **`c7deaaa5`** |

### Mayflower (`rec0eTEnCqlsGJJt9`) — 30 facing

| Type | Record checked | Result |
| --- | --- | --- |
| Association / group | `gdi_opp_prospect_mv8fnmju_53cfad28` AHIMA Advocacy Summit 2027 | PASS (browser drawer + API) |
| Scaling company | `gdi_sc_43a6be41ced811a0` Quorum | PASS (list/API; Surfe scrubbed) |
| Local Meetings & Catering | `gdi_lmc_6a144810692f8a23` Mortgage Bankers Association of Metropolitan Washington | PASS (list/API thesis hotel-specific) |
| Preferred Lodging / PE | `gdi_pe_6e20211edf688737` Decatur House | PASS (list/API) |

### San Diego (`recnPB3Q02NO8Ma1o`) — 38 facing

| Type | Record checked | Result |
| --- | --- | --- |
| Association / group | `gdi_opp_prospect_mv8p5ddj_c848701a` Advanced Therapies Week 2027 | PASS (API; fit null explicit where unwarranted) |
| Scaling company | `gdi_sc_4e26ed53c6b438e8` Everett Dorey LLP | PASS (detail API; no Surfe / no assessment-thesis leak) |
| Local Meetings & Catering | `gdi_lmc_e30f688f4646cf21` Urban League of San Diego County | PASS (list/API) |
| Preferred Lodging / PE | `gdi_opp_prospect_mv8p5ddk_c3ef825a` USS Midway Museum | PASS (list/API) |

### Spot hotels (shared-path regression)

| Hotel | Check | Result |
| --- | --- | --- |
| Bethesda (`recLuxvwwxID7U2B8`) | Share contract live resolve + opportunities API (deploy smoke `gdisht_47c25d74…`); third-hotel fixture boundary test | PASS |
| Waterstone (`recgMYovrrZDJMqzX`) | Share registry token present; no hotel-specific bypass in UI source | PASS (no Mayflower hardcode in `whyThisHotelOpeningHtml`) |
| Renaissance (`recG66DQJKP2c0UNh`) | Share registry token present; same shared renderer | PASS (spot) |

---

## Research history and Opportunity Type filter

**Both already implemented and deployed** (first on `gdi-history-type-v1` / `23ed5b8d`, retained through closeout).

Confirmed on Mayflower production share UI:

- This Week history filter + truthful Completed research run label (not false Never).
- Opportunity Type dropdown beside This Week with structured categories combining with priority/action filters.
- Reset View present; counts match filtered inventory (All 30 / type splits).

No eligibility or commercial assignment changes were made to force filter/count consistency.

---

## Regressions added

- `npm run test:gdi-drawer-hotel-fit-display-v1` — withheld/provisional, single overall tile, assessment-prose skip, no hotel-name hardcode.
- `npm run test:gdi-customer-presentation-boundary-v1` — Bethesda third-hotel fixture proves generic sanitize → DTO → drawer parity (no hotel exception).
- `npm run test:gdi-hotel-activity-history-and-type-filter-v1` — history + type filter + thesis scrub.

---

## Merge / deploy

| Item | Value |
| --- | --- |
| Closeout PRs | [#85](https://github.com/joandejarden-create/my-operators-backend/pull/85), [#86](https://github.com/joandejarden-create/my-operators-backend/pull/86) |
| Merge SHA (on `main`) | `c7deaaa5e303bbfdb99247773091d514a534c933` |
| Prior clean-main deploy via `npm run deploy:production` | SHA `10b6a3ef` → Railway `c09c3a31` (assets `gdi-closeout-v1`) |
| Final production deployment | Railway **`e6265e1d-2ce0-49b2-a04f-da5982d46318`** (GitHub auto-deploy of merge SHA `c7deaaa5`, assets **`gdi-closeout-v2`**) |
| Branch guard | Honored (deploys from `main` only; no hotfix bypass) |

Worktrees: `gdi-main-consolidate` used for clean-main release. `gdi-release-main` left on `gdi/parallel-provider-san-diego` with unrelated ADP/census/untracked data preserved; stashes untouched.

---

## Customer feedback / actions / outcomes

**Preserved.** Closeout changed presentation sanitization and shared UI only. No bulk reimport, no Airtable opportunity rewrite, no feedback/action/outcome table mutation, no allowlist or persistence-flag changes, no new paid research.

---

## Budget

Shared Surfe/Parallel budget **unchanged by this closeout** (no paid enrichment). Latest reported remaining balance **$7.15** stands unless another authorized task updated ledgers out-of-band.

---

## Remaining manual steps / blockers

1. Optional: commit durable share-registry rows for the temporary Mayflower/SD closeout verify tokens if those URLs should survive ephemeral disk (issued via `railway run` for verification only).
2. Optional: formal second `npm run deploy:production` after `c7deaaa5` — not required; production already serves that SHA as `e6265e1d`. Avoid concurrent `railway up` + GitHub auto-deploy (dual BUILDING congestion observed earlier).
3. Human review should continue to assess completed research quality and presentation — not hand-repair packages per hotel.

---

## Production browser evidence summary

- **Before claim invalidated:** report’s `8963208d` / `gdi-drawer-contact-v3` was not current.
- **After:** Mayflower AHIMA drawer on `gdi-closeout-v2` — single provisional overall fit, explicit missing rationale, dimension states, evidence scale, no provider brands, type filter + research history present (`mayflower-ahima-drawer-after-closeout-v2.png`).
