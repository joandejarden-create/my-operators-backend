# Bethesda 14-Segment Live Full E2E — Verification Report

**Hotel:** Bethesda Marriott (`recLuxvwwxID7U2B8`)  
**Auth:** `FOUNDER_APPROVE_BETHESDA_14_SEGMENT_LIVE_RESEARCH_20261112` · ceiling **$6.09**  
**Verified at:** 2026-11-12T07:25Z (live process still in flight)  
**Repo HEAD (git):** `8268a208c` (+ uncommitted deep-auth / rehydrate / dedupe fixes in working tree)  
**No new paid launch during this verification.**

---

## 1. Verdict (direct answers)

| # | Question | Answer |
|---|----------|--------|
| 1 | Is the current Full E2E run operating correctly? | **PARTIALLY** |
| 2 | Repeated Exa titles — normal family reuse or duplicate research? | **Mostly normal family reuse** (12/14 segments route to Association/Events or Scaling). Not 14 unique dashboard titles. |
| 3 | Did each of 14 segments perform live discovery? | **YES** — 14 unique Exa discovery `agent_run_*` IDs, $0.10 each |
| 4 | Is Stage 2 live deep research executing? | **YES, now** (after flag-forward fix + ORS rehydrate resume). First pass was offline ORS only. |
| 5 | Spend discovery vs deep | Discovery **$1.40** · Deep **~$1.30+** (rising) · Remaining **~$3.29** with $0.10 reserved |
| 6 | Truly new unique Bethesda candidates | **~12 unique orgs** non-EXCLUDE after pass-1 (15 NEW + 1 existing-org AAN); many of 170 raw are duplicates/noise |
| 7 | Fully researched (publication-grade) | **0** publication-review-ready |
| 8 | Complete opportunities ready for founder publication review | **0** |
| 9 | Defects found | Deep-auth flags not forwarded (fixed); resume dropped candidates (fixed); cross-segment duplicate paid deep (AFCEA×2 — fix staged); deep ledger missing Exa run IDs (fix staged); PLP/PEP/LCM targeting quality weak |
| 10 | What requires controlled continuation | Let PID **4280** finish or safely resume same ledger; do **not** re-authorize spend; prioritize remaining budget on watchlist orgs not EXCLUDE |

---

## A. Run inventory — process

| Field | Value |
|-------|-------|
| Parent PowerShell PID | **8296** (Cursor temp `ps-script-…ps1`, started ~08:04 local) |
| Child Node PID | **4280** — **ALIVE** |
| Command | `node scripts/gdi-full-e2e-fourteen-segment-orchestrator-v1.mjs --hotels recLuxvwwxID7U2B8 --run-original-full-e2e-chain --authorize-paid-discovery --authorize-paid-research --budget-ceiling-usd 6.09 --out reports/gdi-bethesda-fourteen-segment-live-20261112` |
| Env (this resume) | `GDI_EXA_AGENT_LIVE=1`, `GDI_RESEARCH_AUTHORIZATION_ID=FOUNDER_APPROVE_BETHESDA_14_SEGMENT_LIVE_RESEARCH_20261112`, `GDI_PRIOR_LIVE_SPEND_USD=0` |
| Ledger | `reports/gdi-bethesda-fourteen-segment-live-20261112/original-full-e2e-chain/paid-budget-ledger.json` |
| Discovery state | `data/group-demand-intelligence/hotels/recLuxvwwxID7U2B8/full-e2e-fourteen-segment-state/*.json` |
| ORS dossiers | `…/opportunity-research-state/disc_*.json` (**170** files) |
| Canonical store SHA | `79be23c4…` **unchanged** (59 rows; no publication / no maturity writes) |

### Budget (live snapshot ~07:25Z)

| | USD |
|--|--:|
| Ceiling | 6.09 |
| Spent | **2.70** |
| Reserved / in-flight | **0.10** |
| Remaining | **3.29** |
| Discovery ops | 14 × $0.10 = **1.40** |
| Deep ops settled | 13 × $0.10 = **1.30** |
| Frozen | no |

---

## A2. Discovery Exa run IDs (14/14 live)

| Segment | Agent family (routing) | Support | Exa run ID | Cost |
|---------|------------------------|---------|------------|-----:|
| associations_conferences_events | ASSOCIATION_EVENTS | FULL | `agent_run_e233e4df450e45b2acfd567736e9c68b` | 0.10 |
| corporate_accounts_scaling | SCALING_COMPANIES | FULL | `agent_run_e60fde63e13e404bbae0e4f112ad117d` | 0.10 |
| corporate_meetings_incentives | ASSOCIATION_EVENTS | FULL | `agent_run_8e0f81dc58c048fb94536fca51a30cdd` | 0.10 |
| government_public_sector | ASSOCIATION_EVENTS | PARTIAL | `agent_run_c1cddb5b99ae458c874711b55300e01a` | 0.10 |
| sports_entertainment | ASSOCIATION_EVENTS | PARTIAL | `agent_run_53c3feb7988a4deaa3fdd10311b9b4a1` | 0.10 |
| education_scientific | ASSOCIATION_EVENTS | FULL | `agent_run_71da6b0b00794d809a42c46c21d730f4` | 0.10 |
| projects_infrastructure | SCALING_COMPANIES | PARTIAL | `agent_run_855075e30c0a43b6bcba44a7ab51e1c9` | 0.10 |
| social_special_events | ASSOCIATION_EVENTS | FULL | `agent_run_7afe3cfd149b483dae5abfde4759da41` | 0.10 |
| **local_catering_day_meetings** | ASSOCIATION_EVENTS | **PARTIAL** | `agent_run_f50fd5305c5a46e98b31f4c79cd0a75d` | 0.10 |
| corporate_celebrations_private_events | ASSOCIATION_EVENTS | FULL | `agent_run_b5f8193f82e348d9828e9924b33786d8` | 0.10 |
| travel_trade_group_intermediaries | ASSOCIATION_EVENTS | PARTIAL | `agent_run_20c0423bf34b4b03bf70dbad0a512e1a` | 0.10 |
| relocation_extended_stay | SCALING_COMPANIES | PARTIAL | `agent_run_0c042388d8d449b7885c1bad42a1ddce` | 0.10 |
| **preferred_lodging_partnerships** | **PREFERRED_LODGING** | FULL | `agent_run_d45fcb4a46b54d20a0e69dc5a6f486ad` | 0.10 |
| **professional_event_planners** | ASSOCIATION_EVENTS | FULL | `agent_run_b36c3484aa904747b7bba0c295f8f5f9` | 0.10 |

**Why dashboard shows Association / Scaling titles repeatedly:**  
`gdi-segment-aware-exa-agent-routing-v1.js` maps most of the 14 workflow IDs onto two shared agent families. Segment-specific **query packs** differ (proven in durable `querySpecs`); dashboard title alone is not the segment ID.

---

## B. Discovery outcomes

| Metric | Value |
|--------|------:|
| Segments with fresh retrieval | **14 / 14** |
| Raw extracted (chain retained queue) | **170** |
| Durable candidate IDs across segment state | ~210 cell refs (with cross-segment overlap) |
| Pass-1 non-EXCLUDE | 16 rows (15 WATCHLIST + 1 PRE_SOURCING) |
| Unique orgs among those | **12** |
| Existing-org overlap | American Academy of Neurology (already in inventory) |
| Cross-segment org duplicates | Data Center Americas ×4, SATShow ×2, NXL ×2, AFCEA ×2 |

Canonical 59 / commercial 67 / union 88: **not overwritten**.

---

## C. Deep research (Stage 2) — critical handoff

### Defect (confirmed)

CLI passed `--authorize-paid-research`, but `executeOriginalFullE2eResearchChain` originally did **not** forward:

- `authorizeExaAgentLive: true`
- `allowLiveExaDeepSeed: true`

So pass-1 processed all 170 with **offline ORS** (`LIVE_EXA_SEED_NOT_AUTHORIZED`) while still claiming deep stages complete. **No live Exa deep agent runs** in that pass.

### Repair (applied in working tree; used by PID 4280 resume)

1. Orchestrator now forwards both flags when paid research authorized.  
2. Discovery resume rehydrates candidates from ORS when live discovery is skipped.  
3. Cross-segment org paid-deep dedupe added (prevents AFCEA-style double spend on next cycles).  
4. Ledger settle will record `providerRunId` + clearer mode (`EXA_DEEP_SEED_PLUS_ORS`).  
5. Offline test: `npm run test:gdi-fourteen-segment-deep-auth-flag-propagation-v1` **PASS**.

### Live Stage 2 status (PID 4280)

- Fair queue of **170** rehydrated candidates.  
- Deep ledger rows show `exaSeeded: true` and $0.10 each; **Scaling** dossiers (e.g. X-energy) persist `exaAgentDeepResearch` evidence packs.  
- Many Association-family seeds still lack durable `exaAgentDeepResearch` blobs / run IDs on ORS (ledger provenance gap — fix staged, not yet in in-flight binary beyond current settle).  
- Snapshot file lags (`AFTER_10`) while ledger advances — treat **ledger** as spend truth; do not assume complete from console.

---

## D. Preferred Lodging / Professional Event Planners / Local Catering

### preferred_lodging_partnerships
- **Discovery run:** `agent_run_d45fcb4a46b54d20a0e69dc5a6f486ad` (Preferred Lodging family — distinct dashboard title)  
- **Candidates:** 11  
- **Samples:** Rock Creek Mansion, Woodend, Savoy, KID Museum — **venue-competitor / F&B space inventory**, not corporate preferred-hotel account partnerships  
- **Live deep paid on segment candidates so far:** 0  
- **Publication-ready:** 0  
- **Assessment:** Segment ran live, but outputs are poorly targeted for PLP commercial intent.

### professional_event_planners
- **Discovery run:** `agent_run_b36c3484aa904747b7bba0c295f8f5f9` (Association/Events family)  
- **Candidates:** 16  
- **Samples:** U.S. IFE Conference, Model Arab League, SAME Capital Week — **events**, not planner firms / DMCs as primary entities  
- **Live deep paid on segment candidates so far:** 0  
- **Publication-ready:** 0  
- **Assessment:** Live discovery executed; planner-specific identity weak.

### local_catering_day_meetings (PARTIAL routing)
- **Discovery run:** `agent_run_f50fd5305c5a46e98b31f4c79cd0a75d`  
- **Query intents (real):** corporate training/board workshops, recurring professional room rental, chapter awards dinners — **hotel-hostable local demand wording is present**  
- **Candidates:** 20  
- **Deep paid example:** `disc_8f08c440b223` PBOA 10th Annual Meeting — large conference, not day-meeting/catering grain  
- **Publication-ready:** 0  
- **Assessment:** Adapters ask the right questions; Association/Events agent returns conference-weighted hits under PARTIAL support. Not skipped; quality incomplete.

---

## E. Customer content / publication readiness

| Gate | Result |
|------|--------|
| Ten-section DTO present on processed rows | Often yes (pass-1) |
| `customerPublicationReady` | **false** on inspected dossiers |
| Named buyer verified | Generally **no** (noise contacts: “Administrative Law Journal”, “Accommodation Request Form”, etc.) |
| Sales Ready / AD from this live wave | **0** admitted to commercial inventory |
| Auto-publish / Airtable / share changes | **none** |

---

## F. Two-stage orchestration evidence

```
PowerShell 8296
  → Node orchestrator 4280 (fourteen-segment)
    → Stage 1: executeFourteenSegmentDiscoveryMatrix
         14× createExaLiveDiscoveryProvider → FRESH_RETRIEVAL (ledger + agent_run IDs)
    → Stage 2: executeOriginalFullE2eResearchChain
         Pass-1: ORS offline (flags missing) — COMPLETED 170 offline
         Resume: ORS rehydrate + allowLiveExaDeepSeed — IN FLIGHT (PID 4280)
    → Contact / commercial / ten-section — running per candidate
    → Staging: opportunity-research-state only (canonical store untouched)
```

Imported modules alone are insufficient; the **14 discovery agent_run IDs + ledger settlements** prove Stage 1. Stage 2 live Exa is proven only on the **resume** wave after the flag fix.

---

## G. Safe continuation (no new authorization)

**Do not** start a second orchestrator while PID **4280** is alive.

When 4280 exits (or if reserved $0.10 is stuck >30 minutes with no ledger mtime change):

```powershell
$env:GDI_EXA_AGENT_LIVE='1'
$env:GDI_RESEARCH_AUTHORIZATION_ID='FOUNDER_APPROVE_BETHESDA_14_SEGMENT_LIVE_RESEARCH_20261112'
$env:GDI_PRIOR_LIVE_SPEND_USD='0'
node scripts/gdi-full-e2e-fourteen-segment-orchestrator-v1.mjs `
  --hotels recLuxvwwxID7U2B8 `
  --run-original-full-e2e-chain `
  --authorize-paid-discovery `
  --authorize-paid-research `
  --budget-ceiling-usd 6.09 `
  --out reports/gdi-bethesda-fourteen-segment-live-20261112
```

Same `--out` resumes the ledger. Discovery live cells are skipped (`LIVE_PROVIDER_ALREADY_COMPLETED`). Remaining budget continues Stage 2 only. **Do not raise the ceiling.**

---

## H. Source fixes staged for PR (not interrupting live PID)

- `scripts/gdi-full-e2e-fourteen-segment-orchestrator-v1.mjs` — deep-auth flag forward  
- `lib/.../full-e2e-fourteen-segment-discovery-execution-v1.js` — ORS rehydrate on resume  
- `lib/.../original-full-e2e-research-chain-v1.js` — org-level paid deep dedupe + ledger runId  
- `lib/.../gdi-original-full-e2e-execution-restore-v1.js` — return `exaProviderRunId`  
- `scripts/test-gdi-fourteen-segment-deep-auth-flag-propagation-v1.mjs` + package.json script  

**No paid calls were launched for this verification.**
