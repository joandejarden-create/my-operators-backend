# GDI operating baseline — runbook

**Status date:** 2026-10-14  
**Branch / workspace:** `gdi/parallel-provider-san-diego` (uncommitted consolidation on top of Parallel + drawer/workflow work)  
**Evidence root:** `reports/gdi-operational-baseline/`

Truthful statuses used below: **planned / executed / partially completed / blocked / not applicable**.

---

## 1. Required hotel configuration

| Step | Status |
|------|--------|
| `config/group-demand-intelligence/hotels/<hotelId>.json` with displayName, city, rooms, meeting space, demandTerritory, segments | **executed** (Beth / May / Ren / SD present) |
| GDI hotel registration / onboarding for customer visibility | **partially completed** (US three + SD registered; new hotels need config + allowlists) |

Without this config, independent fit, geo expansion, and Parallel identity lock degrade.

---

## 2. Reuse and research-path selection

```bash
node scripts/gdi-research-to-page-workflow-v1.mjs --hotel <hotelId> --dry-run --out-dir reports/gdi-operational-baseline/runs
```

| Capability | Status |
|------------|--------|
| Opportunity store reuse (Airtable canonical) | **executed** |
| Demand Programs + Signals into reuse ledger | **executed** |
| PE venues / fits / signals read + disposition | **executed** (live AT; fixtures only with `GDI_PE_FIXTURE_MODE=replay` or `allowFixtureFallback`) |
| Proposed Hotel Venue Fits when fit rows = 0 | **executed** |
| Programs→occasion draft cards when signals empty | **executed** |
| Feedback → nextResearchNeed + discovery hints | **executed** (code); hotel feedback corpus varies |
| Path selection CSV/JSON | **executed** |

Unset `GDI_DATA_ROOT` for operational runs (drawer-completeness local store polluted Parallel job paths when set).

---

## 3. Discovery / deepening and budget

| Mode | Flag | Status |
|------|------|--------|
| Reuse only | default | **executed** |
| Fixture replay | `--run-discovery` (default replay) | **executed** |
| Live mock | `--run-discovery --discovery-mode live-mock` | **executed** (prior) |
| Live Parallel ≤ $2 | `--run-discovery --discovery-mode live --authorize-paid-discovery --budget-usd 2` + `GDI_PARALLEL_ENABLED=1` | **executed** (real submit; see cost section) |
| Native/geo plan | automatic in `discover_gaps` | **executed** as plan/dry-run (0 Serp/OpenAI) |
| Surfe / Ampfy | authorize flags | **not applicable** / unwired |

**Budget:** `GDI_PARALLEL_BUDGET_USD=2`, ledger reserve/commit, job store resume (do not resubmit known `providerRunId`).

---

## 4. Independent hotel / venue fit

| Step | Status |
|------|--------|
| `assess_fit` independent hotel fit | **executed** |
| PE disposition vocabulary (referral / lodging / ancillary / background / reject) | **executed** |
| Proposed venue–hotel fits for review | **executed** |
| Cross-market auto-promote | **not applicable** (forbidden) |

---

## 5. Contact resolution

```text
enrich_contacts → execute-targeted-contact-resolution-v1 (Surfe off unless --authorize-surfe)
```

| Step | Status |
|------|--------|
| Targeted plan + execute | **executed** |
| Accept email / phone / form / intermediary | **executed** (policy) |
| Reject homepage-only / invented emails | **executed** (policy) |

---

## 6. Evidence-grounded card / drawer generation

| Step | Status |
|------|--------|
| Draft review package + human checkpoint | **executed** |
| Drawer content contract scoring | **executed** |
| Shared UI: Priority/Exploratory pills, Watchlist ≠ Action WATCH, hide null fit scores, auth→intelligenceDetailHtml | **executed** (code; deploy **blocked** without Railway) |
| Sales-review overlay → Airtable payload | **executed** (Beth/May apply) |

---

## 7. Review checkpoint

Human review is a **quality decision on completed drafts** (approve / rewrite / reject / hold).  
It is not a substitute for missing research or unwritten card copy.

| Hotel | Review posture |
|-------|----------------|
| Bethesda / Mayflower | Reviewed packages published |
| San Diego | Qualified package published (21 facing); incomplete candidates remain internal with reasons |
| Renaissance | Drafts only unless a reviewed package is authorized |

---

## 8. Airtable persistence and publication

```bash
# Beth / May drawer repair (example)
node scripts/gdi-import-reviewed-prospects.mjs --both --adapter airtable --apply \
  --content-overlay reports/gdi-drawer-completeness/sales-review-overlay.json \
  --package-dir reports/gdi-bethesda-process-replication/hotel-review \
  --out-dir reports/gdi-operational-baseline/drawer-repair-apply

# San Diego reviewed package (executed 2026-10-14)
node scripts/gdi-import-reviewed-prospects.mjs --hotel recnPB3Q02NO8Ma1o --adapter airtable --apply \
  --content-overlay reports/gdi-operational-baseline/san-diego-publication-package/sales-review-overlay.json \
  --package-dir reports/gdi-operational-baseline/san-diego-publication-package \
  --out-dir reports/gdi-operational-baseline/san-diego-publication-apply
```

**Allowlist:** code default includes Beth + May + SD; production must set  
`GDI_REVIEWED_PROSPECT_HOTEL_IDS=…,recnPB3Q02NO8Ma1o` and `GDI_REVIEWED_PROSPECT_PUBLICATION_V1=1`.

---

## 9. Browser verification

1. Local: `PORT=3080 GDI_OPPORTUNITIES_PERSISTENCE=airtable node server.js`
2. Open `/group-demand-intelligence.html` (auth as required)
3. Switch Beth → May → Ren; open drawers; reload; CSV export if toolbar present
4. Production: after deploy of `public/js/group-demand-intelligence/*`

Screenshots: `reports/gdi-operational-baseline/screenshots/` (when captured).

---

## 10. Feedback-driven refresh and resumable reruns

| Step | Status |
|------|--------|
| Read local `feedback.json` / share-validation | **executed** |
| Prefer production Decision / share-validation APIs when validating “no feedback” | **partially completed** — do not treat empty local file as proof |
| Parallel job resume by `providerRunId` | **executed** |
| Re-run reuse → gaps → contact → draft | **executed** via coordinator |

---

## Hotel proof matrix

| Hotel | Purpose | Result |
|-------|---------|--------|
| Bethesda | Published drawer + presentation | Airtable apply **executed**; production UI deploy **blocked** (Railway login) |
| Mayflower | Shared Bethesda presentation | Titles/drawers cleaned; deploy **blocked** |
| San Diego | Research → qualify → publish | **21 facing** published (19 Priority / 2 Exploratory PE referrals); deploy + Railway env allowlist required for customer UI |
| Renaissance | Reuse + drafts without bespoke package | Reuse/contact/draft path **executed**; publication **not** authorized |
