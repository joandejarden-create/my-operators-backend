# GDI operational baseline — A–E answers

**As of:** 2026-10-14  
**Git HEAD (local server):** `9acf5dc1` on branch `gdi/parallel-provider-san-diego`  
**Workspace:** large uncommitted consolidation (Parallel live transport, PE fixture lock, UI, programs→occasion, workflow wiring) on top of that commit.

---

## A. What is deployed and visible now?

| Surface | Status |
|---------|--------|
| **Production Railway deploy of this consolidation** | **blocked** — no `RAILWAY_TOKEN` / interactive Railway auth in this environment; cannot claim production UI deploy |
| **Local server (this machine)** | **executed** — `http://localhost:3080` serving current `public/js/group-demand-intelligence/*` |
| **Airtable opportunity content (Beth/May)** | **executed** — repaired drawer fields visible via local API read-back against Airtable persistence |
| **Customer production GDI page** | **partially completed** — still serving last deployed assets until Railway deploy; content may already reflect Airtable payload reads |

Browser evidence (local):  
`screenshots/01-bethesda-list-after.png`, `screenshots/02-mayflower-list-priority-pills-after.png`  
Mayflower shows **Priority / Exploratory** commercial-class pills distinct from Action **WATCH**; drawers use shared intelligence framework with Validation/Action/Outcome preserved; null fit scores do not render a fake score grid.

---

## B. What is saved in Airtable?

Authorized drawer repair apply (`drawer-repair-apply/`):

| Hotel | Updates | Facing | Enrichment | Drawer contract pass |
|-------|--------:|-------:|-----------:|---------------------:|
| Bethesda `recLuxvwwxID7U2B8` | 14 | 13 | 1 | 13 |
| Mayflower `rec0eTEnCqlsGJJt9` | 15 | 11 | 4 | 11 |
| **Total** | **29** | **24** | **5** | |

- Persistence: `opportunityPayloadJson` via canonical importer (`--adapter airtable --apply`)
- Backups under `drawer-repair-apply/backups/`
- Unmatched existing rows preserved (Beth 40 / May 22)
- Creates: 0 · Quarantined: 0
- San Diego / Renaissance **not** published from this pass

API read-back (local → Airtable): Bethesda NADO thesis present; Mayflower Priority cards return repaired hotel thesis.

---

## C. What real research ran and what did it cost?

### Parallel (live)

| Item | Value |
|------|--------|
| Provider run ID (research) | `trun_567f3335d0c140ad8e87120211c6f61f` |
| Probe run ID (connectivity) | `trun_567f3335d0c140ad9269500c06dc2b38` |
| Job key | `gdi-parallel-recnPB3Q02NO-bounded-live-v1` |
| Estimated ledger spend | **$1.00** of $2.00 cap (`estimatedSpentUsd: 1`; one spent reservation) |
| Actual billed from provider payload | **null** (not returned on result) |
| Candidates returned | **8** (after rematerialize parse fix) |
| In-territory for SD Bayside | **3** provisional; **5** wrong-market (Bethesda/DMV) — identity lock bug on first live prompt; **fixed in code for future runs** |
| Resubmit | **No** — rematerialized known run only |

Artifacts: `san-diego-parallel-rematerialized/`, budget ledger under drawer-completeness `GDI_DATA_ROOT` path (ops note: unset `GDI_DATA_ROOT` for clean stores).

### Other research this baseline

| Path | Status | Cost |
|------|--------|------|
| Native/geo coordinator stage | **executed** as plan/dry-run | $0 |
| Programs→occasion drafts (SD) | **executed** — 12 drafts | $0 |
| Targeted contact execute (prior hotels) | **executed** earlier | $0 Surfe |
| SerpAPI / OpenAI / Surfe | **not initiated** | — |

---

## D. Which cards are complete, pending review, or unresolved?

| Set | Complete (drawer content) | Pending human review | Unresolved / not published |
|-----|---------------------------|----------------------|----------------------------|
| Bethesda reviewed package (13 facing + 1 enrich) | Content applied; contract pass on facing | Sales validation/actions still operator-owned | — |
| Mayflower reviewed package (11 facing + 4 enrich) | Content applied; contract pass on facing | Same; titles still often research-note style (data debt) | — |
| San Diego Parallel live rows (8) | Rematerialized with per-row non-publication reasons | All | Wrong-market + contact gaps; **not published** |
| San Diego program drafts (12) | Draft register + checkpoint | All 12 pending | **not published** |
| Renaissance | Existing facing corpus in AT; draft path available | New drafts stay review-only | No publication apply this pass |

Per-row SD non-publication: `san-diego-parallel-rematerialized/san-diego-non-publication-reasons.json`  
(zero confirmed buying is **not** used as automatic exclusion).

---

## E. Can another hotel run without bespoke code or hand-built packages?

**Mostly yes for research→draft; not fully for production publish polish.**

| Dependency | Required? |
|------------|-----------|
| Hotel config JSON (`config/group-demand-intelligence/hotels/<id>.json`) | **Yes** |
| Airtable credentials + GDI base | **Yes** for live reuse |
| Coordinator script (existing) | **Yes** — no second pipeline |
| Hand-built CSV package | **No** for draft path (`draft-review-package-v1`) |
| Parallel allowlist + `GDI_PARALLEL_ENABLED` + key | **Yes** only for paid live discovery |
| Human review checkpoint | **Yes** before publication |
| Railway deploy of UI assets | **Yes** for production presentation parity |
| Sales-review overlay / hand titles | **Optional** — improves Mayflower-like title quality; not required to run process |

**Exact remaining dependencies before “any hotel → published page” is turnkey:**

1. Deploy shared UI/API changes to Railway (auth blocker here).  
2. Clear/avoid stale `GDI_DATA_ROOT` pointing at repair sandboxes.  
3. Operator human review of drafts (cannot be automated).  
4. Optional: title/narrative cleanup for reviewed packages still carrying research-note titles.  
5. Optional: second Parallel live run **only if** newly authorized — current $2 cap already has $1 estimated spent; identity-lock fix should be used before any new spend.

---

## Status legend applied

- Parallel live transport: **executed** (was plan-only; now submit/poll/map)  
- PE auto Bethesda fixture: **executed** removal (explicit replay/test only)  
- Drawer Airtable apply: **executed**  
- Production UI deploy: **blocked** (Railway auth)  
- SD publication: **blocked** (not authorized)  
- Query planning alone: recorded as **plan**, not completed research
