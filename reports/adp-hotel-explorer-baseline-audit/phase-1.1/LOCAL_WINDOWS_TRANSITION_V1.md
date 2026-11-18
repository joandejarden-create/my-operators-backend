# ADP Phase 1 / 1.1 — Windows local transition (V1)

**Status:** STOP FOR FOUNDER REVIEW — LOCAL HANDOFF READY  
**Do not** merge/deploy PR `#364`, alter production Airtable, change customer visibility, or run paid research.  
**Do not** begin Phase 2 implementation from this handoff.

---

## 0. Git identity (frozen for this handoff)

| Item | Value |
|------|--------|
| Remote | `https://github.com/joandejarden-create/my-operators-backend` |
| Branch | `cursor/adp-hotel-explorer-audit-1ed2` |
| PR | `#364` |
| **Head SHA** | `d4b2ae102df9551bc8e340da4f5b5a9472b94c9e` |
| Merge-base with `main` (at handoff) | `1e85541e79c11125df42bc01ba3600f20f95c35b` |
| `origin/main` at handoff | `123453323a2ca795589a4b1a2910383a7ddc02e9` (**2 commits ahead of merge-base**; branch is **behind** main) |
| Cloud ADP worktree | `/agent/repos/my-operators-backend-adp-audit` |
| Primary GDI checkout (do not modify for ADP) | `/agent/repos/my-operators-backend` |

### Commits on this PR branch (vs merge-base)

1. `6e241028` — docs: ADP/Hotel Explorer baseline audit  
2. `4fec5fb1` — merge origin/main before Phase 1  
3. `fd1c35da` — Phase 1 Radar HE progressive enrichment  
4. `d4b2ae10` — Phase 1.1 certification authority, offline permanence, persistence fixes  

### Working-tree check (cloud, at handoff)

- All Phase 1 / 1.1 source + reports committed and pushed.
- Clean `git status` (no untracked Phase 1.1 deliverables left uncommitted).
- **Gitignored / local-only (expected missing until you materialize locally):**
  - `.env` / `.env.local` (never commit)
  - `data/adp-durable-published/**` (Layer B durable volume)
  - `data/census-map/**` (census map snapshot cache)
  - `fixtures/.../longitudinal-synthetic/history-store/` (runtime mock residue)
  - `reports/.../PRODUCTION_DURABLE_PUBLISHED_VS_MEASURED.json` (Layer B measured-dates)

### Implementation dependencies

No missing npm packages for Phase 1/1.1 gates beyond `npm ci`. Runtime browser HE needs **read-only Airtable + Mapbox** (see §2). Offline CI gates do **not** need durable volume.

---

## 1. Preserve branch — do not create extra clones

Prefer a **git worktree** next to your existing Dealality checkout so you do not duplicate history.

Suggested Windows layout:

```text
C:\Dev\dealality\
  my-operators-backend\              ← existing GDI / main Dealality (leave alone for ADP)
  my-operators-backend-adp-audit\    ← NEW worktree on cursor/adp-hotel-explorer-audit-1ed2
```

---

## 2. Windows PowerShell setup guide

> No credentials in this document. Copy values from your existing local Dealality `.env` (read-only keys where possible).

### 2.1 Prerequisites

- Git for Windows  
- **Node.js 22** (matches CI `.github/workflows/adp-hotel-explorer-radar-phase1.yml`)  
- PowerShell 7+ recommended  

```powershell
node -v   # expect v22.x
npm -v
git --version
```

### 2.2 Fetch + create worktree (safe beside GDI)

```powershell
# Adjust if your existing checkout lives elsewhere
$Gdi = "C:\Dev\dealality\my-operators-backend"
$Adp = "C:\Dev\dealality\my-operators-backend-adp-audit"
$Branch = "cursor/adp-hotel-explorer-audit-1ed2"
$Sha = "d4b2ae102df9551bc8e340da4f5b5a9472b94c9e"

Set-Location $Gdi
git fetch origin $Branch
git fetch origin main

# Do NOT checkout the ADP branch in the GDI folder.
# Create an isolated worktree:
git worktree add $Adp $Branch

Set-Location $Adp
git rev-parse HEAD
# Confirm: d4b2ae102df9551bc8e340da4f5b5a9472b94c9e
```

If the worktree already exists:

```powershell
Set-Location $Adp
git fetch origin $Branch
git checkout $Branch
git pull --ff-only origin $Branch
git rev-parse HEAD
```

### 2.3 Dependencies

```powershell
Set-Location $Adp
npm ci
# If npm ci fails on Windows lockfile quirks:
# npm install
```

### 2.4 Environment variable **names** (no values)

Copy from existing Dealality `.env` into `$Adp\.env` (file is gitignored).

**Minimum for Radar + Hotel Explorer browser validation (read-heavy):**

| Name | Purpose |
|------|---------|
| `AIRTABLE_API_KEY` or `AIRTABLE_API_KEY_READONLY` | Read census / brand-presence / map points |
| `AIRTABLE_BASE_ID_ALT` | Platform base (Hotel Census, Radar map points) |
| `MAPBOX_ACCESS_TOKEN` | Map tiles / geocoding display |
| `PORT` | Use **8091** for ADP worktree (8080 often reserved for main checkout) |
| `MEMBERSTACK_APP_ID` | Auth surfaces if testing signed-in paths |
| `MEMBERSTACK_SECRET_KEY` or test secret | Server-side Memberstack (only if you exercise auth) |

**ADP-related (usually leave writes off):**

| Name | Safe local default |
|------|--------------------|
| `ADP_AIRTABLE_READ_LIVE` | unset / `0` unless intentionally reading ADP Airtable overlay |
| `ADP_AIRTABLE_PUBLISH_APPLY` | unset / not `true` |
| `ADP_HISTORY_WRITES_ENABLED` | `0` / unset |
| `ADP_HISTORY_AIRTABLE_WRITE_APPLY` | unset |
| `ADP_DURABLE_PUBLISHED_ROOT` | path to local durable volume if present |
| `ADP_DURABLE_PUBLISHED_MODE` | `off` unless validating Layer B |
| `OPERATOR_SETUP_USE_NEW_BASE_WRITER` | leave `0` unless you own that work |
| `CENSUS_MAP_SNAPSHOT_BOOT_BUILD` | `1` (default) builds snapshot on boot when Airtable readable |

**Read-only Airtable rule:** prefer a token scoped to `data.records:read` on `AIRTABLE_BASE_ID_ALT`. Do not enable publish/history apply flags for this validation.

### 2.5 Start local server (ADP worktree only)

```powershell
Set-Location $Adp
$env:PORT = "8091"
node server.js
# Expect: Server running at http://localhost:8091
# Expect: non-canonical worktree note if 8080 is reserved for main
```

Keep the existing GDI server on its usual port if already running — do not stop it unless you intend to.

### 2.6 Phase 1 / 1.1 test groups

```powershell
Set-Location $Adp

# A — Radar HE progressive enrichment + wiring + foundation (CI composite)
npm run test:adp-phase1-radar-he-gates

# B — Permanence Layer A (offline fixtures; not production proof)
npm run test:adp-permanent-full-e2e-certification-v1

# C — Persistence Layer A (synthetic mock). Layer B measured-dates may BLOCK.
npm run test:adp-e2e-persistence-contract-v1

# D — Customer-surface (expect FAIL partial until evidence/BPP gaps addressed)
npm run test:adp-customer-surface-contract

# Optional Layer B hard fail if durable volume must be present:
# $env:ADP_PERMANENCE_REQUIRE_LAYER_B = "1"
# npm run test:adp-permanent-full-e2e-certification-v1
```

### 2.7 Browser URLs (Hotel Explorer / Radar)

Base: `http://localhost:8091`

| Purpose | URL |
|---------|-----|
| Radar ranked-list host (HE wired) | `/deal-capture-radar-with-ranked-list.html` |
| Bethesda **internal preview only** | `/deal-capture-radar-with-ranked-list.html?hotelExplorer=recLuxvwwxID7U2B8&internalPreview=1` |
| Enriched example (Casas — has census id) | `/deal-capture-radar-with-ranked-list.html?hotelExplorer=recjDsNzu93CFfe87&internalPreview=1` |
| Hilton census pin (if in map) | `/deal-capture-radar-with-ranked-list.html?hotelExplorer=rec35fExUxCClpOP6&internalPreview=1` |

**Do not** open or create customer share packs for Bethesda. Share routes remain fail-closed under existing authorization.

### 2.8 Troubleshooting

| Symptom | Likely cause | Action |
|---------|--------------|--------|
| Map shows 0 hotels / brand-presence 500 | Missing Airtable env or census snapshot | Confirm `AIRTABLE_API_KEY` + `AIRTABLE_BASE_ID_ALT`; restart server; wait for `data/census-map` build |
| HE panel never opens | Census pin pool empty | Fix census first; deep-link `hotelExplorer=rec…` still needs hotel in `allHotels` or HE seed path |
| Permanence EXIT 2 with `ADP_PERMANENCE_REQUIRE_LAYER_B=1` | Durable volume missing | Expected; run without that env for Layer A |
| Persistence Layer B BLOCKED | Missing measured-dates JSON | See §5; do not fabricate |
| Port in use / “8080 reserved” | Main checkout owns 8080 | Use `PORT=8091` on ADP worktree |
| Accidentally on GDI folder | Wrong cwd | `git branch` should show ADP branch only in `$Adp` |

---

## 3. First local validation sequence (Radar ↔ Hotel Explorer)

Confirm in order:

1. **Census pins load** on Radar map/list (`window.allHotels` / brand-presence).  
2. **Click a pin** → unified Hotel Explorer opens (not legacy-only popup).  
3. **Baseline-only hotel** → HE shows existing Radar sections only (no invented HI).  
4. **Enriched hotel** → progressive strip / extra HE content where verified.  
5. **Five Radar sections work:** Hotel Overview · Submarket Snapshot · Area Hotels · Demand Drivers · Access & Connectivity.  
6. **Filter + map/list selection** still sync.  
7. **Bethesda** (`recLuxvwwxID7U2B8`) opens with `internalPreview=1` — **no** customer publication / share pack.  
8. **Share/customer** paths still require existing auth (fail-closed).

### Suggested test hotels

| Class | Example | Census `rec…` | Notes |
|-------|---------|---------------|-------|
| Internal-preview enriched | Bethesda Marriott | `recLuxvwwxID7U2B8` | Internal only; not customer-visible |
| Enriched / ADP-linked | Casas del XVI | `recjDsNzu93CFfe87` | Progressive enrichment candidate |
| Brand-linked | Hilton Times Square | `rec35fExUxCClpOP6` | Map pin if in census snapshot |
| Baseline-only | Any census pin **without** HI/ADP enrichment | (pick from live map) | Must still show five Radar tabs |

Identity rule: **census `rec…` only** — never name-only matching.

---

## 4. Blocker register — 33 customer-surface failures

Machine JSON: `CUSTOMER_SURFACE_ROOT_CAUSE_MATRIX.json`  
Source run: `reports/ai-demand-positioning/adp-customer-surface-contract-v1-latest.json`

**Summary:** fail-closed behavior retained. None of these 33 should block **code integration of Phase 1 Radar HE** if treated as **legacy evidence / measurement gaps**. They **do** block claiming “customer-surface production ready.”

### 4.1 `TERRITORY_SUBJECT_RATES` (5) — Bethesda

| Field | Value |
|-------|--------|
| Root cause | Historical compatibility vs locked `BETHESDA_EXPECTED_SUBJECT_RATES` |
| Affected | `adp_bethesda_marriott` (business/leisure/couples/family/celebration) |
| Pre–Phase 1? | **Yes** (period drift vs golden expectations) |
| New regression? | **No** |
| Needs historical evidence? | Yes (which period is certified / comparable) |
| Needs paid measurement? | Only if re-certifying a new period |
| Blocks safe code integration? | **No** for Radar HE Phase 1 |
| Next action | Local: compare manifest latest vs locked constants; founder decides pin vs re-cert — **do not flip constants** |

### 4.2 `BPP` (24) — eligibility / measurement

Paired assertions (`clientReady gate fail` + class) for 12 properties:

| Property | Class | Pre–P1? | Paid measure? | Blocks HE merge? | Next action |
|----------|-------|---------|---------------|------------------|-------------|
| `adp_ac_hotel_a_coruna` | `MEASUREMENT_NOT_ATTEMPTED` | Yes | Yes (BPP cycle) | No | Governed BPP measurement later |
| `adp_hilton_times_square` | `MEASUREMENT_NOT_ATTEMPTED` | Yes | Yes | No | Same |
| `adp_spice_island_beach_resort` | `MEASUREMENT_NOT_ATTEMPTED` | Yes | Yes | No | Same |
| `adp_w_rome` | `MEASUREMENT_NOT_ATTEMPTED` | Yes | Yes | No | Same |
| `adp_castillo_hotel_son_vida` | `PEER_SET_NOT_FROZEN` | Yes | Peer freeze + measure | No | Freeze peer set before READY |
| `adp_mayflower_hotel_washington_dc` | `PEER_SET_NOT_FROZEN` | Yes | Same | No | Same |
| `adp_sheraton_mallorca_arabella_golf` | `PEER_SET_NOT_FROZEN` | Yes | Same | No | Same |
| `adp_westin_grand_munchen` | `PEER_SET_NOT_FROZEN` | Yes | Same | No | Same |
| `adp_westin_tampa_waterside` | `PEER_SET_NOT_FROZEN` | Yes | Same | No | Same |
| `adp_wyndham_san_diego_bayside` | `PEER_SET_NOT_FROZEN` | Yes | Same | No | Same |
| `adp_yotel_geneva_lake` | `PEER_SET_NOT_FROZEN` | Yes* | Same | No | Same (*affiliation mapping added in 1.1; measurement still open) |
| `adp_smoke_test_synthetic` | `UNKNOWN` | Fixture noise | No | No | Exclude from commercial published universe |

Phase 1.1 cleared `AFFILIATION_MISSING` for eight hotels via static mappings — **not** a claim of BPP READY.

### 4.3 Evidence link gates (3) — shared 427 defects

| Gate | Root cause |
|------|------------|
| `ALL_EVIDENCE_LINKS_NONEMPTY` | Empty evidence cards / missing self-contained packs |
| `ALL_EVIDENCE_READABLE` | Same defect set |
| `ALL_EVIDENCE_LINKS_CLIENT_READY` | 427 / 1541 links |

| Field | Value |
|-------|--------|
| Affected | Cross-property published evidence audit |
| Pre–Phase 1? | **Yes** (fixture/runtime drift) |
| New regression? | **No** |
| Needs historical evidence? | **Yes** (runtime periods or baked present/missing packs) |
| Needs paid measurement? | **No** for restore-from-durable; **Yes** only if responses never captured |
| Blocks safe code integration? | **No** for Radar HE; **Yes** for ADP customer evidence claims |
| Next action | Local Layer B: restore runtimes from durable; never invent EMPTY clears |

### 4.4 `DISPLACEMENT_COUNT_EVIDENCE_EXACT_PARITY` (1)

| Field | Value |
|-------|--------|
| Root cause | Harness + fixture drift (`adp_smoke_test_synthetic` assertion); commercial rows need published DSS / runtime |
| Pre–Phase 1? | Mostly yes; harness null-period crash fixed in 1.1 |
| New regression? | **No** (residual synthetic noise) |
| Needs historical evidence? | For commercial hotels, yes |
| Needs paid measurement? | No |
| Blocks safe code integration? | **No** |
| Next action | Drop/ignore smoke synthetic in commercial gate; validate DSS on real hotels locally |

---

## 5. Durable production evidence (read-only, local)

Keep **synthetic CI** and **production validation** separate.

### 5.1 Where authoritative records live

| Artifact | Typical location | Git? |
|----------|------------------|------|
| Durable published current | `data/adp-durable-published/current/<propertyId>/manifest.json` | **Gitignored** |
| Durable snapshots | `data/adp-durable-published/snapshots/<propertyId>/` | **Gitignored** |
| Measured-dates export | `reports/dealality-adp-single-version-publication-contract-v1/admin-published-vs-measured-dates/PRODUCTION_DURABLE_PUBLISHED_VS_MEASURED.json` | Not in tree here — materialize from governed export |
| Filesystem published SoT | `data/ai-demand-positioning/published/` | In repo (publication ≠ certification) |

Set `ADP_DURABLE_PUBLISHED_ROOT` if your durable copy is outside the default path.

### 5.2 Local process (read-only)

1. Obtain durable volume / snapshot copy from the governed Railway mount or founder-held archive (**do not commit**).  
2. Place under `data/adp-durable-published/` (or point `ADP_DURABLE_PUBLISHED_ROOT`).  
3. Confirm Hilton (or target) `current/.../manifest.json` exists.  
4. Place measured-dates JSON only if it is the real export — **never fabricate**.  
5. Run:

```powershell
$env:ADP_PERMANENCE_REQUIRE_LAYER_B = "1"
npm run test:adp-permanent-full-e2e-certification-v1
npm run test:adp-e2e-persistence-contract-v1
```

6. Record pass/fail/blocked under a new dated folder under `reports/adp-hotel-explorer-baseline-audit/phase-1.1/` if needed.

Full write-up: `PHASE1_1_LAYER_B_DURABLE_EVIDENCE_PROCESS.md`.

---

## 6. Merge-readiness checklist (PR `#364`)

**Verdict at handoff: NOT READY to merge/deploy.**

| Check | Status | Notes |
|-------|--------|-------|
| ADP Phase 1 Radar HE functionality (code) | Ready for **local** validation | Progressive enrichment + ranked-list host wiring |
| Phase 1.1 reliability fixes | In branch | Persist demotion/lifecycle; scan attach; BPP affiliations; Layer A fixtures |
| Automated CI (5 checks) | **PASS** | Runs `test:adp-phase1-radar-he-gates` only — **not** full production readiness |
| Permanence Layer A | PASS locally/CI path | Offline fixtures |
| Permanence / persistence Layer B | BLOCKED without durable | Expected until local materialization |
| Customer-surface | FAIL partial (33) | Legacy evidence/BPP/Bethesda — see §4 |
| Browser HE validation | Required locally | Cloud was BLOCKED_ENV without Airtable |
| Customer publication safeguards | Intact | No new share packs; Bethesda internal-only; writes flags off |
| GDI regression risk | Low if isolated worktree | Do not run ADP publish/history apply against GDI workflows |
| Conflict with newer `origin/main` | **Behind by 2 commits** (`#366`, `#367`) | Rebase/merge main **locally after** HE validation; resolve carefully — do not auto-merge in cloud |

Five green CI checks ≠ production certified.

---

## 7. First recommended local Cursor prompt

Paste into Cursor against the **ADP worktree** (`my-operators-backend-adp-audit` on `d4b2ae10`):

```text
You are working ONLY in the ADP worktree on branch cursor/adp-hotel-explorer-audit-1ed2
(commit d4b2ae102df9551bc8e340da4f5b5a9472b94c9e). Do not modify the primary GDI checkout.

Read reports/adp-hotel-explorer-baseline-audit/phase-1.1/LOCAL_WINDOWS_TRANSITION_V1.md
and FOUNDER_STOP_PHASE1_1.md.

Task: run the first local Radar ↔ Hotel Explorer validation sequence (§3):
census pins load, HE opens on click, baseline-only vs enriched hotels, five Radar sections,
map/list sync, Bethesda internalPreview only, share stays fail-closed.

Use PORT=8091. Read-only Airtable. No merge, deploy, Airtable writes, paid research,
or Phase 2 implementation. Report PASS/FAIL/BLOCKED with screenshots.
```

---

## 8. Report index (already on branch)

| File | Role |
|------|------|
| `FOUNDER_STOP_PHASE1_1.md` | Phase 1.1 stop package |
| `CERTIFICATION_BASELINE_AUTHORITY_RECONCILIATION.md` | 27=27 provisional inventory |
| `DURABLE_FIXTURE_ARCHITECTURE.md` | Layer A/B |
| `E2E_PERSISTENCE_GATE_RESULTS.md` | Persistence |
| `CUSTOMER_SURFACE_ROOT_CAUSE_MATRIX.md` | 33 failures |
| `RADAR_HOTEL_EXPLORER_VALIDATION.md` | HE gates + cloud browser block |
| `PHASE1_1_GATE_MATRIX.md` | Pass/fail/blocked |
| `LOCAL_HANDOFF_PHASE1_1.md` | Cloud-oriented handoff |
| **`LOCAL_WINDOWS_TRANSITION_V1.md`** | **This document** |
| `PHASE2_FULL_E2E_PRIORITIES.md` | Deferred — do not start |

---

**STOP — LOCAL HANDOFF READY.** Await founder direction before merge, deploy, or Phase 2.
