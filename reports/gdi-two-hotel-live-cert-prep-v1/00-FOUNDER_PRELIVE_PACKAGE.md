# GDI Two-Hotel Live Full E2E Certification — Pre-Live Package

**Status:** `GDI_TWO_HOTEL_LIVE_CERTIFICATION_READY — INTEGRATED_ON_8080 — AWAITING_FOUNDER_GO`  
**Date:** 2026-11-28  
**Serving / workspace HEAD:** `91989efb16b766dda9c5291f1e6a9647aa97baf6` (= `origin/main`; includes PR #676 after #674)  
**Reported stale tip `7b774c134`:** superseded — do not use.  
**Workspace:** `C:\Dev\dealality\ws-gdi` · branch home after merge: `ws/gdi`  
**Canonical :8080:** LIVE_AUTHORIZED_GDI_SERVER_SCRIPT (assignment-preflight PASS)  
**Paid spend this prep:** `$0` · **Surfe:** none · **CURRENT:** unchanged · **UI:** unchanged

---

## 1. Design Hotels triad (existing HPC only)

| Hotel | Airtable ID | Market / Country | Affiliation | Rooms | Meetings posture | STAGED ops | Contact channels (email/phone/LI) | CURRENT | Suitability |
|---|---|---|---|---|---|---|---|---|---|
| **Memmo Príncipe Real** | `rec1t35e3sPerzxYu` | Lisbon / Portugal | Design Hotels | 41 | Boutique / limited meetings (not conference plant) | 11 (`gdi_dh_who_how_…_20261124`) | **0 / 0 / 0** (11 named + titled) | none (409) | **Recommended cert** |
| LaLuna Boutique Hotels & Villas | `recaokE8psIbuVeuH` | Grenada / Grenada | Design Hotels | unknown locally | Leisure / destination | 3 | 0 / 0 / 0 (3 named) | none (409) | Ready later — not initiated |
| Condesa DF | `rece0NdV9FCnaFlVS` | Mexico City (CALA) / Mexico | Design Hotels | unknown locally | Urban / meetings | 4 | 0 / 0 / 0 (4 named) | none (409) | Ready later — not initiated |

**Recommendation — Memmo Príncipe Real**

- European boutique Design Hotels property with clear official identity and public-source evidence.
- Least complete **contact** processing among the triad (zero verified channels despite WHO/HOW names).
- Useful contrast to Bethesda Marriott (large US full-service meetings hotel).
- Commercially relevant for Design Hotels pilot discussions (Lisbon lifestyle / creative / leisure / small-group).
- **Not greenfield:** disclose starting inventory — STAGED 11 with named WHO/HOW; no hotel-local discovery matrix under `reports/gdi-canonical-full-e2e-v3/rec1t35e3sPerzxYu/`. Live certification must **not** be described as historical replay alone; missing discovery/deep artifacts require `-AuthorizePaidDiscovery` (Exa canonical chain).

**LaLuna / Condesa readiness (no paid research initiated):** both have thin STAGED WHO/HOW inventories and zero contact channels. Suitable follow-on certification after Memmo + Bethesda. Do not start their paid runs in this assignment.

---

## 2. Bethesda Marriott — actual starting baseline

| Layer | Actual latest state |
|---|---|
| Hotel ID | `recLuxvwwxID7U2B8` |
| STAGED | **68** · `gdi_hotel_wide_fqo_recLuxvwwxID7U2B8_20261125_quality_v1` |
| CURRENT | **67** · `gdi_pub_recLuxvwwxID7U2B8_20261113_local_master_v2` (**preserve**) |
| Local bag vs publication | Weekly DryRun priorCanonicalCount **86** (bag) ≠ STAGED 68 ≠ CURRENT 67 — reconcile to publication layers; do not reset |
| Primary contact coverage (STAGED) | named 68 · titled 68 · **email 45** · **phone 38** · **LinkedIn 0** · unique names ~61 · alternative contacts in API payload **0** (drawer alts require research continuation) |
| Discovery / deep artifacts | Reuse path for EXISTING_HOTEL_CONTINUATION; no NEW_HOTEL paid discovery required unless founder expands scope |
| Weekly DryRun | PASS · SerpApi calls **0** · delta NEW0 / UPDATED8 / UNCHANGED78 (frozen discovery) |
| Findymail history | Reuse settled person-level results; do not re-bill |
| Competitive / PLP / LDI | Assess via canonical Full E2E spine on Live continuation (Leisure/CAD $0 classify; PLP from discovery matrix cells) |

Preserve all valid intelligence and canonical opportunity IDs. CURRENT never published from this run.

---

## 3. Fresh discovery blocker — resolved (operational path)

PR #674/#676 closeout: missing discovery artifacts used to HOLD Live Full E2E.

**Canonical fix (integrated on `91989efb`):**

- Missing discovery/deep + `-AuthorizePaidDiscovery` + `-AuthorizePaidResearch` + `-AuthorizationId` + `-HardCeilingUsd` + `EXA_API_KEY` + `GDI_EXA_AGENT_LIVE=1` → `executeOriginalFullE2eResearchChain` (**NEW_HOTEL_FULL_E2E**).
- Missing without paid-discovery auth → fail-closed HOLD `DISCOVERY_ARTIFACTS_MISSING_PAID_NOT_AUTHORIZED`.
- **SerpApi is not required for NEW HOTEL Full E2E** (Exa-backed fourteen-segment discovery).
- **SerpApi is required only for Live weekly discovery** (`npm run gdi:weekly-refresh` without `--dry-run` / without frozen discovery).
- DryRun never calls SerpApi or Exa paid research.
- Prelive check `new_hotel_routing`: **PASS**.

Evidence: `reports/gdi-unified-live-e2e-prelive-readiness-v1/00-PRELIVE_READINESS.json` · DryRuns under `reports/gdi-two-hotel-live-cert-prep-v1/{memmo,bethesda}-dryrun/`.

**Honest DryRun note:** Memmo DryRun PASS reused an existing discovery matrix path (`…wyndham…fourteen-segment-discovery-matrix.json`) — that is DryRun wiring reuse, **not** Live Memmo discovery certification. Live Memmo without hotel-local artifacts must use `-AuthorizePaidDiscovery`.

---

## 4. SerpApi configuration (no secrets)

| Item | Status |
|---|---|
| Account / key | Configured in authorized secret store (Integration / operator session). **Not** present in `ws-gdi` `.env` (ENOENT). Live weekly session must inject `SERPAPI_KEY` without committing secrets. |
| Required for Run A / Run B Full E2E | **No** (Exa path) |
| Required for Bethesda Live weekly discovery | **Yes** (unless `--skip-discovery` / frozen discovery) |
| DryRun | SerpApi calls = 0 |
| Estimated weekly Live searches (Bethesda) | ~20–80 searches depending on segment queries (order-of-magnitude); cost depends on plan (~$0.002–$0.015/search typical) → propose **≤ $5 SerpApi allowance** only if Live weekly is authorized |
| Hard controls | Missing key → exit 2 fail-closed; DryRun never spends |

---

## 5. Consolidated budget proposal (NOT authorization to spend)

### Run A — Memmo (`rec1t35e3sPerzxYu`) — NEW_HOTEL_FULL_E2E

| Provider | Proposed hard ceiling | Notes |
|---|---|---|
| Exa | **USD 25** | Discovery + deep + WHO/HOW / contact gap recovery within same ledger |
| Findymail | **15 credits** (request top-up if >15 eligible unresolved emails) | ~11 primary + alternative-entry research; hotel-independent Findymail |
| SerpApi | **$0** for Full E2E | N/A |
| Airtable writes | Explicit write-set auth only | Persistence separate from paid research |
| STAGED activate | **Authorized** for valid package | No `-NoActivateStagedPointer` on certification Live |
| CURRENT | **Prohibited** | |
| Surfe | **Prohibited** | |

### Run B — Bethesda (`recLuxvwwxID7U2B8`) — EXISTING_HOTEL_CONTINUATION

| Provider | Proposed hard ceiling | Notes |
|---|---|---|
| Exa | **USD 15** | Incremental WHO/HOW + contact gap; reuse settled jobs |
| Findymail | **15 credits** (top-up if needed) | Prefer reuse; fill email gaps (~23 primary without email) |
| SerpApi | **$0** Full E2E; **≤ $5** only if Live weekly separately authorized | Weekly DryRun already proven at $0 |
| Existing intelligence reuse | **Mandatory** | |
| Airtable / STAGED / CURRENT / Surfe | Same rules as Run A | CURRENT preserve |

### Tokens (proposed — inactive until founder GO)

- `FOUNDER_GO_GDI_TWO_HOTEL_MEMMO_LIVE_20261128`
- `FOUNDER_GO_GDI_TWO_HOTEL_BETHESDA_LIVE_20261128`

Previous authorizations do **not** apply.

---

## 6. Exact executable commands (after founder GO)

Windows without PowerShell 7: replace `pwsh` with `powershell -NoProfile -ExecutionPolicy Bypass`.

### Preflight (both)

```powershell
npm run dealality:assignment-preflight -- --stream gdi
npm run gdi:unified-live-e2e-prelive-readiness-v1
```

### Run A — Memmo Live Full E2E

```powershell
$env:GDI_EXA_AGENT_LIVE = '1'
# EXA_API_KEY + FINDYMAIL_API_KEY must be injected into the session (never commit)
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/gdi/Invoke-GdiFullE2E.ps1 `
  -HotelId rec1t35e3sPerzxYu -Mode Live `
  -AuthorizePaidResearch -AuthorizePaidDiscovery -AuthorizeFindymail `
  -AuthorizationId 'FOUNDER_GO_GDI_TWO_HOTEL_MEMMO_LIVE_20261128' `
  -HardCeilingUsd 25 -FindymailCreditHardCap 15 `
  -Manifest 'reports/gdi-two-hotel-live-cert-prep-v1/auth/memmo-01-TRANCHE_MANIFEST.json' `
  -OutDir 'reports/gdi-two-hotel-live-cert-prep-v1/memmo-live'
```

### Run B — Bethesda Live Full E2E (reuse discovery/deep)

```powershell
$env:GDI_EXA_AGENT_LIVE = '1'
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/gdi/Invoke-GdiFullE2E.ps1 `
  -HotelId recLuxvwwxID7U2B8 -Mode Live `
  -AuthorizePaidResearch -AuthorizeFindymail `
  -OperationalMode EXISTING_HOTEL_CONTINUATION `
  -AuthorizationId 'FOUNDER_GO_GDI_TWO_HOTEL_BETHESDA_LIVE_20261128' `
  -HardCeilingUsd 15 -FindymailCreditHardCap 15 `
  -Manifest 'reports/gdi-two-hotel-live-cert-prep-v1/auth/bethesda-01-TRANCHE_MANIFEST.json' `
  -OutDir 'reports/gdi-two-hotel-live-cert-prep-v1/bethesda-live'
```

### Weekly DryRun (Bethesda — already proven)

```powershell
npm run gdi:weekly-refresh -- --hotel-id recLuxvwwxID7U2B8 --dry-run
```

### Airtable persistence (separate auth after research)

```powershell
$env:GDI_RESEARCH_INVENTORY_PRODUCTION_WRITES = '1'
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/gdi/Invoke-GdiFullE2E.ps1 `
  -HotelId <rec...> -Mode Live -AuthorizePaidResearch -SkipWhoHow `
  -ContinuationOnly -Resume -AuthorizeResearchPersistence `
  -ExactWriteSetAuthorization '<founder exact write-set text>' -ApplyResearchPersistence
```

---

## 7. Pre-live operational readiness matrix

| Gate | Result | Classification |
|---|---|---|
| Assignment preflight / :8080 ownership | PASS | LIVE_AUTHORIZED |
| origin/main == HEAD `91989efb` | PASS | INTEGRATED |
| Template freeze (first 3 tabs) | PASS (prelive) | IMPLEMENTED |
| New-hotel AuthorizePaidDiscovery routing | PASS | FIXTURE/DRYRUN + wiring LIVE_EXECUTION_VALIDATED on Wyndham path historically; Memmo Live pending GO |
| Memmo / Bethesda Full E2E DryRun | PASS stages 1–3,5; Stage 4 PARTIAL | DRYRUN_VALIDATED |
| Weekly Bethesda DryRun | PASS · serpApi=0 | DRYRUN_VALIDATED |
| Findymail hotel-independent path | Wired + planning PASS | DRYRUN_VALIDATED |
| Airtable clean reconstruction | PARTIAL (`INVENTORY_MEMBERSHIP_NOT_PERSISTED`) | PARTIAL — known limitation; not a spend blocker |
| SerpApi Full E2E dependency | Removed (Exa) | RESOLVED |
| Provider keys in ws-gdi checkout | Absent (session inject) | Operational prerequisite for Live |
| Paid Live two-hotel cert | Not started | AWAITING_FOUNDER_GO |

---

## 8. Outstanding blockers before GO

1. **Founder consolidated Live authorization** (tokens + ceilings above) — required.
2. **Session injection** of `EXA_API_KEY`, `FINDYMAIL_API_KEY`, and (weekly Live only) `SERPAPI_KEY` — required at execute time; not a code blocker.
3. **Inventory membership persistence PARTIAL** — disclose; correct under separate Airtable write auth during/after Live; does not HOLD discovery path.
4. **No UI / no CURRENT / no Surfe / no unauthorized schema** — binding.

---

## 9. Integrated baseline on :8080

| Check | Result |
|---|---|
| Memmo STAGED API | 200 · count 11 · `gdi_dh_who_how_rec1t35e3sPerzxYu_20261124` |
| Bethesda STAGED API | 200 · count 68 · `…20261125_quality_v1` |
| Bethesda CURRENT API | 200 · count 67 · preserved |
| Bethesda ES staged | 200 · ok |
| Wyndham STAGED tip | `gdi_hotel_wide_fqo_recnPB3Q02NO8Ma1o_20261128_b3e83a27` |

---

## 10. Permanent certification honesty

Do **not** claim `GDI_TWO_HOTEL_FULL_E2E_OPERATIONALLY_CERTIFIED` until authorized Live runs A→B complete with persistence, reconstruction, STAGED activation, contact scorecards, and :8080 browser evidence.

This package substantiates only:

**`GDI_TWO_HOTEL_LIVE_CERTIFICATION_READY — INTEGRATED_ON_8080 — AWAITING_FOUNDER_GO`**
