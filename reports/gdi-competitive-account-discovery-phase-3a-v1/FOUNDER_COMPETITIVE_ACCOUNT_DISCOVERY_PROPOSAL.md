# GDI Phase 3A — Competitive Account Discovery  
## Founder audit + minimal implementation proposal

**Mode:** Audit + proposal only  
**No** Airtable mutations · **No** STAGED/CURRENT changes · **No** paid discovery · **No** new database · **No** parallel opportunity engine  

**Closed / do not restart:** Bethesda 68 · Wyndham CLEAN 11 · Three-Layer · Canonical Full E2E · lossless persistence · PR #599 / #601  

---

## 1. Existing capability audit and gap assessment

GDI already has most of the *vocabulary* and *gates* for competitive demand capture. What it lacks is a **governed discovery + enrichment stage** that systematically turns competitor-hosted demand into shared research intelligence and hotel-specific pursuit options — without weakening FQO.

| Capability | Status | Verdict |
|---|---|---|
| Competitor / incumbent fields | Partial | `likelyCompetitor`, `competitiveContext`, commitment mapping exist; sparse / inconsistent on Wyndham CLEAN 11 |
| Historical venues / meeting history | Exists in SoT | `meetingHistory` + lossless merge; under-populated |
| Recurrence / future pathway | Enforced | FQO `FUTURE_SOURCING_PATHWAY` + competitive-future-cycle presentation |
| Room-block evidence | Partial | FACT/INFERENCE/UNKNOWN helpers; commitment mapping rejects venue-only |
| Demand Generators / Fit | Exists | Layer 1 DG / Programs / Signals / Hotel Fit |
| WHO/ALT/HOW + outreach | Strong (post-Wyndham fixes) | Place/event titles and `tickets@`/`conventionhelp@` now fail closed |
| Cross-hotel reuse | Exists | `reuse-prior-intelligence-v1`, contact lineage, multi-hotel FQO independence |
| Historical provenance | Exists | Research SoT + HOLD preservation |
| **Competitive Account Discovery as a stage** | **Missing** | No canonical Full E2E step that harvests competitor relationships into SoT with attainability classes |

**Pilot signal (read-only):**
- Bethesda quality 68: **32** rows with competitor labels; only **~4** with verified/overflow/future pathways that look sales-actionable.
- Wyndham CLEAN 11 STAGED: **0** competitor fields populated.
- Wyndham paid-wave HOLD set: **4** competitive hints (Marquis / Bayfront) — valuable research, correctly not FQO.

Full map: `01-CAPABILITY_AUDIT.json` · samples: `02-SAMPLE_FINDINGS.json` · deep module map: `03-DEEP_AUDIT_ADDENDUM.json`.

**Deep-audit confirmations (no scope change):** CAD is a constellation, not a missing product stack. Prefer extending `competitor-commitment-mapping`, `recurring-meeting-intelligence`, `demand-generators/lodging-evidence`, and `comp-set-demand-mining` resolution — not `emergency-displacement-hunter` and not a second CMI/FQO engine. Keep **CMI ≠ FQO**: competitor known does not equal hotel-qualified. Extend `namedCompetitorHost` carefully (closed regex today misses markets like SD Marquis unless evidenced via commitment mapping).

---

## 2. Proposed minimum implementation (not a rebuild)

Add **one thin Competitive Account Discovery (CAD) module** inside the existing Full E2E research chain:

```
Existing inventory / DG identity
        ↓
CAD harvest (competitor host / housing / block evidence)   ← NEW thin stage
        ↓
Enrich Research SoT (meetingHistory, competitiveContext, provenance)
        ↓
Cross-hotel reuse ledger (org+program key) — no auto FQO for peers
        ↓
Hotel-specific FQO (unchanged six gates)
        ↓
VALIDATED_FQO_ONLY persistence / STAGED (existing gates)
```

**In scope (minimum):**
1. CAD data contract on Research SoT (fields below).
2. Deterministic classifier: Historical intel / Emerging opportunity / FQO-eligible candidate.
3. Competitor commitment extractor reuse (`gdi-research-competitor-commitment-mapping-v1`).
4. Attainability + sourcing-window confidence enums (no fabricated dates).
5. Dedupe: enrich existing opp by org+program+cycle before create.
6. Read-only pilot report against Bethesda + Wyndham inventories (no writes).
7. Tests only — no production Airtable / pointer mutation in first PR.

**Out of scope (defer):**
- New tables / engines / UI products  
- Auto Surfe / Agent  
- Broad paid Exa wave  
- Weakening FQO or inventing open RFPs  
- Touching Bethesda/Wyndham certified pointers  

---

## 3. Competitive Account Discovery data contract (proposed)

Attach under Research SoT (not a parallel store), e.g. `researchPayloadJson.competitiveAccountDiscovery`:

| Field | Values / notes |
|---|---|
| `organizationId` / `demandGeneratorId` | Canonical DG link |
| `programKey` | Stable org+program identity for dedupe |
| `historicalDemand[]` | `{ year, hostHotel, location, evidenceUrls, roomBlockEvidence, confidence }` |
| `incumbent` | `{ hotelName, relationship: VERIFIED\|INFERRED\|UNKNOWN, commitmentScope, evidenceUrls }` |
| `sourcingWindow` | `{ state: CONFIRMED\|INFERRED\|UNKNOWN, start?, end?, basis, evidenceUrls }` — dates only when evidenced |
| `attainability` | `WINNABLE \| OVERFLOW_ONLY \| MONITOR \| UNATTAINABLE` |
| `buyer` | Named WHO + HOW (reuse contact objects) |
| `intermediary` | Optional housing bureau / planner — labeled, not fake person |
| `cadClass` | `HISTORICAL_COMPETITIVE_INTEL \| EMERGING_COMPETITIVE_OPPORTUNITY \| FQO_CANDIDATE` |
| `recommendedAction` | Hotel-specific next step (Layer 2) |
| `risks[]` | Unresolved questions |

**Hard rules:** Never fabricate sourcing dates or incumbent contracts. Venue ≠ competitor commitment (`isEventVenueOnlyMention`).

---

## 4. Integration points with canonical Full E2E

| Layer | CAD behavior |
|---|---|
| **1 Research SoT** | Enrich `meetingHistory`, `competitiveContext`, CAD block; lossless merge only |
| **2 Hotel FQO** | Unchanged six gates; competitive gate only when competitive scope; historical competitor ≠ FQO |
| **3 Presentation** | Derive competitive pills via existing `competitive-future-cycle-presentation-v1` |
| Persistence | `VALIDATED_FQO_ONLY`; HOLD CAD intel preserved as research |
| Publication | Independent STAGED/CURRENT — CAD never auto-activates |
| Entry | Wire as optional stage in `gdi-canonical-full-e2e-entry-v1` after shared reuse, before hotel-wide finalization |

---

## 5. Research source and evidence strategy

**Reuse first (no spend):**
1. Existing Bethesda / Wyndham research payloads + HOLD continuation inventory.  
2. Provenance URLs already on opportunities (housing pages, competitor blocks).  
3. Prior Exa enrichment logs (Wyndham) — reclassify, do not re-scrape blindly.  
4. Competitor commitment mapping + demand-engine FACT/INFERENCE helpers.

**Paid Exa (founder GO only, later):**
- Targeted WHO/HOW for housing bureaus on EMERGING rows (Comic-Con, BIO, AUA, GBTA, Padres).  
- Competitor housing-page confirmation for named incumbents.  
- Ceiling proposal only after read-only pilot scores gaps — do **not** auto-spend remaining Wyndham $23.65.

**Surfe:** explicit-click only. **Agent:** OFF unless separately authorized.

---

## 6. Cross-hotel deduplication approach

1. **Identity key:** `normalizeOrgIdentityKey` + program/occasion key (`reuse-prior-intelligence-v1`).  
2. **Prefer enrich:** if Hotel A already has the account, patch competitive fields; do not mint a twin.  
3. **Multi-hotel FQO independence:** shared CAD intel may exist once; qualification remains per hotel.  
4. **Cycle separation:** 2027 overflow vs 2028 primary are related program rows, not duplicates — link via `programKey`.  
5. **HOLD preservation:** rejected FQO still keeps competitive history in SoT.

---

## 7. Contact and outreach verification strategy

Reuse post-Wyndham FQO contact rules:
- Reject place/event/org titles as persons.  
- Reject functional mailboxes (`tickets@`, `conventionhelp@`, `admin@`, `office@`, …).  
- Housing intermediaries (onPeak / FHT / Maritz) → `intermediary` object + labeled org route, **not** fake primary name.  
- Person gate still requires verified named commercially relevant WHO for FQO.  
- Public channel classification via existing verified-commercial-route modules.

---

## 8. Sample competitive account findings

### Bethesda (certified inventory — deepen, don’t duplicate)
| Account | Incumbent | Class | Ask |
|---|---|---|---|
| NADO/DDAA WashCon 2028 | Bethesda North Marriott | FQO competitive | Bid / overflow openness |
| NADO/DDAA WashCon 2027 | Bethesda North Marriott | FQO overflow | Overflow list if Crystal Gateway short |
| NIH SBPO VOS pattern | Ramada Rockville | Emerging / future cycle | Recurring lodging path |
| Many “Not Competitive” labels | Hyatt / North Marriott | Historical intel | Retain; don’t pursue as open |

### Wyndham (CLEAN 11 + HOLD research)
| Account | Competitor hint | Class | Gap |
|---|---|---|---|
| CLEAN 11 (prior 9 + 2 LMC) | mostly unlabeled | FQO non-CAD | Enrich competitive context where evidenced |
| Comic-Con 2027 | Marriott Marquis Marina | Emerging HOLD | Named housing bureau |
| BIO 2028 | Marriott Marquis Marina | Emerging HOLD | Correct housing contact (not wrong-domain) |
| AUA 2027 | Hilton Bayfront | Emerging HOLD | Named FHT/onPeak person |
| Padres weekends | Marquis Marina | Pattern HOLD | Group sales (not tickets@) |

---

## 9. Commercial-quality and qualification assessment

CAD must report **three distinct counts** (never collapse):

1. **Historical competitive intelligence** — competitor hosted/served org (evidence-backed).  
2. **Emerging competitive opportunity** — defensible future/recurring pathway; WHO/HOW or fit incomplete.  
3. **FQO-qualified competitive opportunity** — full six-gate pass **for this hotel**, including attainable action.

Wyndham lesson: competitive scrape ≠ FQO. Success = winnable revenue pursuits, not inflated PASS counts.

---

## 10. Cost and operational implications

| Item | Estimate |
|---|---|
| PR-A (contract + classifier + tests, $0 research) | Engineering only |
| Read-only Bethesda/Wyndham CAD pilot report | $0 |
| Optional WHO/HOW Exa continuation (later) | Propose ≤ $10–15 after gap score |
| Airtable / STAGED / CURRENT | **$0 / no mutations** in Phase 3A-A |
| Ops | Reuse existing Full E2E run; CAD stage adds CPU, not a new product home |

---

## 11. Proposed incremental implementation PRs

| PR | Content | Risk | Merge owner |
|---|---|---|---|
| **PR-A** | CAD data contract + classifier + competitor commitment wiring + tests + founder docs | Low | Integration Controller |
| **PR-B** | Read-only pilot reports (Bethesda + Wyndham) scoring Historical / Emerging / FQO-candidate; no writes | Low | IC |
| **PR-C** | Wire CAD stage into canonical Full E2E entry (opt-in flag); still dry-run default | Medium | IC |
| **PR-D** | Founder-authorized enrichment apply (VALIDATED_FQO_ONLY / research enrich) — only after A–C | High (gated) | IC |

No worktree sprawl. Short-lived feature branch from `ws/gdi` when implementation starts.

---

## 12. Recommended next development action

**Approve PR-A scope and start implementation of the CAD contract + classifier + tests only.**

Do **not** yet:
- Spend remaining Exa budget  
- Mutate Bethesda or Wyndham certified baselines  
- Broad rebuild of demand engine  
- Parallel database or opportunity product  

**Success criterion for Phase 3A:** GDI can explain, for a demand account: who currently captures it, when it may next be competitively available, who influences purchase, and what our hotel should do — using certified Full E2E architecture.

---

## Exact founder GO to proceed (implementation)

```
FOUNDER_GO AUTHORIZE_GDI_CAD_PHASE_3A_PR_A
scope=contract+classifier+tests+docs
airtableApply=OFF
stagedActivate=OFF
currentPublish=OFF
paidResearch=OFF
bethesdaFreeze=ON
wyndhamFreeze=ON
```

**STOP — awaiting founder authorization for PR-A.**
