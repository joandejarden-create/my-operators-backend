# ADP Phase 1 / 1.1 — Windows local transition (V1)

**Status:** STOP FOR FOUNDER REVIEW — LOCAL HANDOFF READY  
**Do not** merge/deploy PR `#364`, alter production Airtable, change customer visibility, or run paid research.  
**Do not** begin Phase 2 implementation from this handoff.

Phase 1.1 findings are **accepted with conditions**. Next ADP work continues in the **local Windows** Dealality environment (cost control + validation against the existing local setup).

---

## 0. Git identity (frozen for this handoff)

| Item | Value |
|------|--------|
| Remote | `https://github.com/joandejarden-create/my-operators-backend` |
| Branch | `cursor/adp-hotel-explorer-audit-1ed2` |
| PR | [#364](https://github.com/joandejarden-create/my-operators-backend/pull/364) |
| **Head SHA (handoff tip)** | `6b03f62906586bd2195de20d26d16951c2532cc7` |
| Phase 1 HE + Bethesda tip | `83b1ceb2af1a5e7d9a91bf2695197cfbf6a30eb8` |
| Main reconciliation merge | `a373fa579c8c83c911d1f1473108721df345a6c1` |
| **Base / merge-base with `main`** | `9632c51544fd5aa6f164acbfe1af6e1a3048c5b2` (branch is **synchronized** with `origin/main` via merge) |
| `origin/main` at handoff | `9632c51544fd5aa6f164acbfe1af6e1a3048c5b2` |
| ADP worktree (this machine) | `C:\Dev\dealality\my-operators-backend-adp-audit` |
| Primary GDI checkout (**do not modify for ADP**) | `C:\Dev\dealality\my-operators-backend` |
| Ports | GDI / primary → **8080** · ADP worktree → **8091** |

### Approved Phase 1 / 1.1 commits on this PR (high level)

1. Baseline audit docs → Phase 1 progressive enrichment (`fd1c35da9`)  
2. Phase 1.1 certification / offline permanence / persistence (`d4b2ae102`)  
3. Windows handoff docs (earlier revisions)  
4. Phase 1.2/1.3 HE closeout + Bethesda authorized internal-preview (`83b1ceb2a`)  
5. Merge `origin/main` GDI #366–#371 (`a373fa579`)  
6. Final reconciliation report (`6b03f6290`)  

### Working-tree check (Windows ADP worktree, at handoff)

**Committed + pushed on tip `6b03f6290`:** Phase 1 / 1.1 Radar HE, Bethesda bridge, closeout reports, permanence Layer A fixtures/gates.

**Untracked / local-only (do not put into PR #364):**

| Path | Notes |
|------|--------|
| `data/hotel-identity/` | Phase 1.3B identity ledger prototype |
| `lib/hotel-identity/` | Ledger / eligibility / shadow modules |
| `reports/.../phase-1.3a/`, `phase-1.3b/` | Census migration analysis (follow-on PR) |
| `scripts/*identity*`, `*shadow*`, `*eligibility*` | 1.3B scripts |
| `package.json` local delta | Optional 1.3B npm script overlay (not required for Phase 1 HE) |
| `.env` / `.env.local` | Never commit |
| `data/adp-durable-published/**` | Layer B durable volume (gitignored) |
| `data/census-map/**` | Census map snapshot cache (gitignored) |

### Implementation dependencies

No missing npm packages for Phase 1/1.1 gates beyond `npm ci` / `npm install`. Browser Radar+HE needs **read-only Airtable** (+ Mapbox for map). Offline CI / Layer A gates do **not** need durable volume.

---

## 1. Preserve branch — no extra clones / cloud worktrees

Prefer the **existing** ADP worktree. Do **not** modify the primary GDI checkout for ADP tasks. Do **not** create additional cloud worktrees.

```text
C:\Dev\dealality\
  my-operators-backend\              ← GDI / primary (leave alone)
  my-operators-backend-adp-audit\    ← ADP branch worktree (this handoff)
```

```powershell
$Adp = "C:\Dev\dealality\my-operators-backend-adp-audit"
Set-Location $Adp
git fetch origin
git checkout cursor/adp-hotel-explorer-audit-1ed2
git pull --ff-only origin cursor/adp-hotel-explorer-audit-1ed2
git rev-parse HEAD
# Expect: 6b03f62906586bd2195de20d26d16951c2532cc7 (or newer ff-only tip if founder pushes docs)
```

If creating the worktree for the first time from the GDI folder:

```powershell
$Gdi = "C:\Dev\dealality\my-operators-backend"
$Adp = "C:\Dev\dealality\my-operators-backend-adp-audit"
$Branch = "cursor/adp-hotel-explorer-audit-1ed2"
Set-Location $Gdi
git fetch origin $Branch
git fetch origin main
# Do NOT checkout the ADP branch inside $Gdi
git worktree add $Adp "origin/$Branch"
Set-Location $Adp
git checkout -B $Branch "origin/$Branch"
git rev-parse HEAD
```

---

## 2. Windows PowerShell setup guide

> No credentials in this document. Copy values from your existing local Dealality `.env` (prefer read-only Airtable scope).

### 2.1 Prerequisites

- Git for Windows  
- **Node.js 22** (matches CI `.github/workflows/adp-hotel-explorer-radar-phase1.yml`)  
- PowerShell 7+ recommended  

```powershell
node -v   # expect v22.x
npm -v
git --version
```

### 2.2 Dependencies

```powershell
Set-Location C:\Dev\dealality\my-operators-backend-adp-audit
npm ci
# If npm ci fails on Windows lockfile quirks:
# npm install
```

### 2.3 Environment variable **names** (no values)

Copy into `$Adp\.env` (gitignored).

**Minimum for Radar + Hotel Explorer browser validation (read-heavy):**

| Name | Purpose |
|------|---------|
| `AIRTABLE_API_KEY` | Read census / brand-presence |
| `AIRTABLE_BASE_ID_ALT` | Platform base (Hotel Census) |
| `MAPBOX_ACCESS_TOKEN` | Map tiles |
| `PORT` | **8091** for ADP worktree |
| `MEMBERSTACK_APP_ID` | Signed-in `/app` paths |
| `DEV_AUTH_BYPASS_EMAIL` | Local-only founder bypass (never production) |

**Keep writes off:**

| Name | Safe local default |
|------|--------------------|
| `ADP_AIRTABLE_READ_LIVE` | `0` / unset |
| `ADP_AIRTABLE_PUBLISH_APPLY` | unset / not `true` |
| `ADP_HISTORY_WRITES_ENABLED` | `0` |
| `ADP_HISTORY_AIRTABLE_WRITE_APPLY` | unset |
| `OPERATOR_SETUP_USE_NEW_BASE_WRITER` | `0` |
| `ENABLE_HBX_CENSUS_WRITES` | `0` |
| `ENABLE_DATAFORSEO_VALIDATED_WRITES` | `0` |

**Optional Layer B:**

| Name | Purpose |
|------|---------|
| `ADP_DURABLE_PUBLISHED_ROOT` | Path to durable published volume |
| `ADP_DURABLE_PUBLISHED_MODE` | `off` unless validating Layer B |
| `ADP_PERMANENCE_REQUIRE_LAYER_B` | `1` only when durable volume present |

Read-only Airtable rule: prefer a token with `data.records:read` on `AIRTABLE_BASE_ID_ALT`.

### 2.4 Start ADP server (do not stop GDI on 8080)

```powershell
Set-Location C:\Dev\dealality\my-operators-backend-adp-audit
$env:PORT = "8091"
$env:ADP_AIRTABLE_PUBLISH_APPLY = "0"
$env:ADP_HISTORY_WRITES_ENABLED = "0"
$env:ADP_AIRTABLE_READ_LIVE = "0"
node server.js
# Expect: http://localhost:8091
# Expect: non-canonical worktree note (8080 reserved for main)
```

### 2.5 Phase 1 / 1.1 test groups

```powershell
Set-Location C:\Dev\dealality\my-operators-backend-adp-audit

# A — Radar HE composite (CI-aligned)
npm run test:adp-phase1-radar-he-gates

# B — Permanence Layer A (offline; not production proof)
npm run test:adp-permanent-full-e2e-certification-v1

# C — Persistence contract (Layer B may BLOCK without durable volume)
npm run test:adp-e2e-persistence-contract-v1

# D — Customer-surface (expect FAIL ~33 until evidence/BPP gaps addressed)
npm run test:adp-customer-surface-contract

# E — GDI isolation smoke (post-main merge)
npm run test:gdi-airtable-purpose-integrity-audit-v1
```

### 2.6 Browser URLs

Base: `http://localhost:8091`

| Purpose | URL |
|---------|-----|
| Canonical app Opportunity Radar | `/app?inventory=staged&preview=staged#/opportunity-radar` |
| Ranked-list host (HE wired; embed equivalent) | `/deal-capture-radar-with-ranked-list.html` |
| App-shell embed pattern | `/deal-capture-radar-with-ranked-list.html?embed=1&appShell=1&inventory=staged&preview=staged` |
| Bethesda deep link | `...&hotelExplorer=recLuxvwwxID7U2B8&hexTab=hotel` |
| Casas (enriched candidate) | `...&hotelExplorer=recjDsNzu93CFfe87&hexTab=hotel` |

**Auth notes**

- Signed-in `/app` may show **Sign in to continue** until Memberstack Test Mode / token works → report **BLOCKED_AUTH**, do not claim full app PASS from embed alone.  
- Bethesda map inclusion uses authorized `/api/brand-presence/internal-preview-hotels` (admin / local owner-app). Query `internalPreview=1` alone does **not** authorize. Local `DEV_AUTH_BYPASS` requires `X-Dealality-Owner-App: 1`. Production (`NODE_ENV=production`) disables bypass.  
- **No** Bethesda customer share pack.

### 2.7 Troubleshooting

| Symptom | Likely cause | Action |
|---------|--------------|--------|
| Map 0 hotels / brand-presence fail | Missing Airtable env / snapshot | Confirm `AIRTABLE_API_KEY` + `AIRTABLE_BASE_ID_ALT`; wait for census-map boot |
| HE never opens | Empty pin pool or wrong host | Use ranked-list host; deep-link `hotelExplorer=rec…` |
| Bethesda missing from public list | Expected | Public = Hotel Census only; use authorized internal merge |
| Internal-preview 403 | Missing auth / owner-app header | Signed-in admin or local bypass **with** `X-Dealality-Owner-App: 1` |
| Permanence EXIT 2 with Layer B required | Durable volume missing | Expected; Layer A only without volume |
| Port conflict | 8080 owned by GDI | Keep ADP on **8091** |
| Wrong folder | GDI checkout | `git branch` must be ADP branch only under `$Adp` |

---

## 3. First local validation sequence

Confirm in order:

1. Census pins load (~15,692) from Hotel Census via brand-presence.  
2. Click a pin → **unified Hotel Explorer** opens.  
3. **Baseline-only** hotel → existing Radar data; no invented HI.  
4. **Enriched** hotel → verified progressive attributes when present.  
5. Five Radar sections: Hotel Overview · Submarket Snapshot · Area Hotels · Demand Drivers · Access & Connectivity.  
6. Filters + map/list selection stay synchronized.  
7. **Bethesda** (`recLuxvwwxID7U2B8`) selectable in **authorized internal** preview; excluded from public list / share packs.  
8. Customer/share authorization remains fail-closed.

### Suggested hotels

| Class | Example | Census `rec…` |
|-------|---------|---------------|
| Internal-only | Bethesda Marriott | `recLuxvwwxID7U2B8` |
| Enriched / ADP-linked | Casas del XVI | `recjDsNzu93CFfe87` |
| Brand census pin | Hilton Times Square | `rec35fExUxCClpOP6` |
| Baseline-only | Any map pin without HI | live pick |

Identity: **census `rec…` only** — never name-only matching.

---

## 4. Blocker register — 33 customer-surface failures

Machine JSON: `CUSTOMER_SURFACE_BLOCKER_REGISTER_V1.json`  
Detail matrix: `CUSTOMER_SURFACE_ROOT_CAUSE_MATRIX.md` / `.json`  
Revalidated failure count at tip `6b03f6290`: **33** (unchanged).

| Gate group | Count | Root cause | Pre–P1? | New regression? | Historical evidence? | Paid measure? | Blocks HE code integrate? | Next action |
|------------|------:|------------|---------|-----------------|----------------------|---------------|---------------------------|-------------|
| `TERRITORY_SUBJECT_RATES` | 5 | Locked Bethesda rates vs published period drift | Yes | No | Yes | Only if re-cert | **No** | Compare certified vs latest; founder pin — do not flip constants |
| `BPP` | 24 | Measurement not attempted / peer set not frozen (+ synthetic noise) | Yes | No | No* | Yes (BPP cycle) | **No** | Governed BPP / peer freeze; exclude `adp_smoke_test_synthetic` |
| `ALL_EVIDENCE_LINKS_*` | 3 | Empty evidence / missing durable runtimes (~427 defects) | Yes | No | Yes | No | **No** | Restore durable packs; never invent evidence |
| `DISPLACEMENT_COUNT_EVIDENCE_EXACT_PARITY` | 1 | Harness / synthetic fixture drift | Yes | No | Yes | No | **No** | Ignore smoke synthetic; validate real DSS locally |

\*BPP itself is a measurement/eligibility gap, not a missing historical period artifact.

**Interpretation:** these failures **do not** block safe Radar HE code integration; they **do** block claiming customer-surface / production ADP readiness. Do not run paid research or speculative fixes from this handoff.

---

## 5. Durable production evidence (read-only)

Full process: `PHASE1_1_LAYER_B_DURABLE_EVIDENCE_PROCESS.md`.

| Artifact | Location | Git |
|----------|----------|-----|
| Durable published `current/` | `data/adp-durable-published/current/<propertyId>/manifest.json` | **gitignored** |
| Durable snapshots | `data/adp-durable-published/snapshots/` | **gitignored** |
| Measured-dates audit | `reports/.../PRODUCTION_DURABLE_PUBLISHED_VS_MEASURED.json` | local/export only |
| Filesystem published SoT | `data/ai-demand-positioning/published/` | publication ≠ certification |

```powershell
# Layer A only (CI / local free)
npm run test:adp-permanent-full-e2e-certification-v1

# Layer B required when durable volume is mounted
$env:ADP_PERMANENCE_REQUIRE_LAYER_B = "1"
npm run test:adp-permanent-full-e2e-certification-v1
```

Rules: publication ≠ certification; do not invent packages; fail closed with `BLOCKED_MISSING_AUTHORITATIVE_EVIDENCE`; keep synthetic CI separate from real durable validation. **Never commit production secrets or unrestricted production dumps.**

---

## 6. Merge-readiness checklist (PR #364) — not merge authorization

| Area | Status |
|------|--------|
| ADP Phase 1 HE in canonical ranked-list Radar | Implemented + gated |
| Phase 1.1 reliability / permanence Layer A | Implemented + gated |
| Phase 1.2/1.3 Bethesda authorized internal preview | Implemented; temporary bridge |
| Automated CI on tip | Must be green on latest push (do not treat “5 checks” as production readiness) |
| Local Phase 1 composite | PASS at reconciliation |
| Outstanding customer-surface failures | **33** legacy evidence/measurement gaps |
| Browser signed-in `/app` | Often **BLOCKED_AUTH** until founder Memberstack login |
| Customer publication safeguards | Fail-closed; Bethesda not in share packs / public list |
| GDI compatibility | Merged through `origin/main` `9632c5154`; isolated GDI gates PASS |
| Conflicts with newer main | None at handoff tip (synchronized via merge) |
| Census migration 1.3a/1.3b | **Out of PR #364** — local-only follow-on |

**Founder stance:** Phase 1.1 accepted with conditions → **do not merge/deploy #364** until local validation + your explicit approval. Five CI checks ≠ complete production readiness.

---

## 7. First recommended local Cursor prompt

```text
Work only in C:\Dev\dealality\my-operators-backend-adp-audit on
cursor/adp-hotel-explorer-audit-1ed2 at tip 6b03f6290 (or latest ff-only).

Do not modify C:\Dev\dealality\my-operators-backend or port 8080.
Do not merge/deploy PR #364. Do not start Phase 2 or census cutover.

Follow reports/adp-hotel-explorer-baseline-audit/phase-1.1/LOCAL_WINDOWS_TRANSITION_V1.md:
1) Confirm HEAD SHA and start server on PORT=8091 with write flags off.
2) Run npm run test:adp-phase1-radar-he-gates and Layer A permanence.
3) Browser-validate Radar pins → Hotel Explorer (baseline + enriched) and
   Bethesda authorized internal preview; report BLOCKED_AUTH if /app sign-in fails.
4) Summarize results vs the §3 checklist. Stop for founder review.
```

---

## Related reports

- `CUSTOMER_SURFACE_BLOCKER_REGISTER_V1.json`  
- `CUSTOMER_SURFACE_ROOT_CAUSE_MATRIX.md`  
- `PHASE1_1_LAYER_B_DURABLE_EVIDENCE_PROCESS.md`  
- `../phase-1-closeout/PHASE1_FINAL_MAIN_RECONCILIATION.md`  
- `../phase-1-closeout/FOUNDER_DECISION_REPORT.md`  

**STOP — LOCAL HANDOFF READY. No merge, deploy, Airtable writes, customer visibility changes, or paid research.**
