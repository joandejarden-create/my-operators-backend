# GDI AI Agent — Exa-Powered Parity Experiment V1

**Status:** PROTOCOL DESIGN LOCKED (not authorized to spend)  
**Date:** 2026-11-19  
**Primary question:** Under matched Exa capabilities, models, budgets, and GDI qualification gates, does Dealality adaptive research planning add independently verified commercial value over the existing Exa-powered GDI workflow?  
**Non-goals:** Wave 2 execution in this PR; production/Airtable/publication writes; redesign of Exa Agent contracts; broad discovery productization.

---

## Binding clarification vs Wave 1 `$0` official-page run

| Prior result | What it tested | What it did **not** test |
|--------------|----------------|---------------------------|
| Wave 1 ITERATE (evidence-grounded) | Adaptive planner vs deterministic **official_page** sequence at `$0` | Whether an agent improves **Exa-powered** GDI research |

**Do not conclude that agents offer no commercial value based on official-page-only research.**  
Existing production GDI research already uses Exa (Search / Contents / Exa Agent API). Any ADOPT / ITERATE / REJECT about agent value on the real process requires an **Exa-parity** comparison.

Terminology (binding):

| Term | Meaning |
|------|---------|
| **Exa Search / Contents** | Exa HTTP `/search` and `/contents` (priced via `benchmark-provider-pricing-v1`) |
| **Exa Agent API** | Exa multi-step agent runs (`api.exa.ai/agent/runs`) already wired into GDI segment discovery + deep research |
| **Dealality adaptive research agent** | Local planning / next-action / stop / query-refinement layer (ORS adaptation, Phase 2G-style loops) — **not** the Exa vendor product |

This experiment isolates the **Dealality adaptive layer** while giving both arms the **same Exa access**.

---

## 1. Existing Exa-powered GDI workflow map

```text
Hotel / segment context
        │
        ▼
┌───────────────────────────────────────────────────────────┐
│ Segment discovery adapters                                 │
│  buildSegmentDiscoveryQueries (deterministic query packs) │
└────────────────────────────┬──────────────────────────────┘
                             │
                             ▼
┌───────────────────────────────────────────────────────────┐
│ Segment discovery executor                                 │
│  segment-discovery-executor-v1                             │
│  provider: fixture | historical | Exa live bridge          │
└────────────────────────────┬──────────────────────────────┘
                             │ authorizePaidDiscovery +
                             │ authorizeExaAgentLive +
                             │ GDI_EXA_AGENT_LIVE=1
                             ▼
┌───────────────────────────────────────────────────────────┐
│ Exa live discovery bridge                                  │
│  exa-live-discovery-provider-bridge-v1                     │
│  → segment-aware routing (Scaling / Events / Pref lodging) │
│  → runExaAgentResearch (discovery mode, effort quote)      │
└────────────────────────────┬──────────────────────────────┘
                             │
                             ▼
┌───────────────────────────────────────────────────────────┐
│ Candidate extract / dedup / yield labels                   │
│  Stage A candidates + DISCOVERY_YIELD_LABEL                │
│  (fresh vs HISTORICAL_REPLAY vs FIXTURE)                   │
└────────────────────────────┬──────────────────────────────┘
                             │
                             ▼
┌───────────────────────────────────────────────────────────┐
│ Deep research / ORS maturation                             │
│  opportunity-research-supervisor-v1                        │
│  adaptive-ors-exa-research-loop-v1 (selection record)      │
│  selectExaAgentStrategy — escalate complex gaps to Exa     │
│  Agent; prefer cheaper Search/Contents/direct_public first │
└────────────────────────────┬──────────────────────────────┘
                             │
                             ▼
┌───────────────────────────────────────────────────────────┐
│ Evidence validation → CIB / contact / eligibility          │
│  exa-agent-evidence-validation-v1                          │
│  evaluateSharedCommercialEligibility + challenger          │
│  intake disposition (OPEN / WATCH / COMPETITOR / …)        │
└───────────────────────────────────────────────────────────┘

Budgets: gdi-segment-stage-call-budget-v1 (discovery vs deep caps)
Entry: gdi-canonical-full-e2e-entry-v1 (dual auth + ceiling)
```

### How Exa is invoked

| Layer | Module | Invocation |
|-------|--------|------------|
| Canonical Full E2E | `workflow/gdi-canonical-full-e2e-entry-v1.js` | Preflight: `EXA_API_KEY`, `--authorize-exa-agent-live`, `GDI_EXA_AGENT_LIVE=1`, paid discovery/research flags, `budgetCeilingUsd` |
| Segment discovery | `exa-agent/exa-live-discovery-provider-bridge-v1.js` | `runExaAgentResearch({ mode: "discovery" })` with segment routing + query-family context |
| Provider adapter | `exa-agent/exa-agent-provider-adapter-v1.js` | HTTPS create + poll Agent runs; mock unless dual-authorized |
| ORS strategy | `exa-agent/exa-agent-ors-strategy-v1.js` | `selectExaAgentStrategy` — Agent only when complex + authorized + budget |
| Adaptive ORS↔Exa loop | `adaptive-ors-exa-research-loop-v1.js` | Builds selection record (does not auto-spend); classifies simple vs complex questions |
| Search / Contents | `exa-benchmark-adapter-v1.js`, live discovery/cert runners | `/search` lite + `/contents` under reservation ledgers |
| Stage budgets | `gdi-segment-stage-call-budget-v1.js` | Independent discovery vs deep call/USD caps (PR #382 5>4 fix) |

### Query generation and refinement (today)

- **Deterministic:** `buildSegmentDiscoveryQueries` / segment adapters / fourteen-segment mission packs emit fixed query families.
- **Prompt packing:** Hotel context + exclusions + query list are concatenated into Exa Agent `contextBlock` (bridge).
- **ORS adaptation:** `selectNextAdaptedResearchStep` + commercial priority scoring choose next question; complexity regex decides Search vs Exa Agent escalation.
- **Limited local refinement:** Exa Agent itself multi-hops inside a single paid run; Dealality rarely reformulates a second Exa query mid-case beyond adapter packs + `previousRunId` continuation hooks.

### Discovery → investigation → dedup

1. Discovery Agent run → structured candidates.  
2. Dedup against prior identities / exclusions (`deduplicatedCandidates` in Bethesda priority-8 traces).  
3. Deep research selection register chooses follow-ups.  
4. Yield labels prevent scoring historical replay as fresh discovery completion.

### Evidence / contact extraction and validation

- Structured validation: `exa-agent-evidence-validation-v1`.  
- Mapping into CIB / events / PLP packs.  
- Contact intelligence + shared commercial eligibility + independent challenger.  
- **Observed bottleneck (Bethesda Exa priority-8, spend $2.20):** 71 org identities / 38 dated signals → **0 sales-ready**; contact extraction after Exa deep seed remains primary commercial bottleneck (`DISCOVERY_GAPS_AND_NEXT_IMPROVEMENTS.md`).

### Budgets and stopping (today)

| Control | Behavior |
|---------|----------|
| Dual auth | `authorizeExaAgentLive` + `GDI_EXA_AGENT_LIVE=1` |
| Paid flags | `authorizePaidDiscovery` / `authorizePaidResearch` |
| USD ceiling | Canonical entry `budgetCeilingUsd`; Agent effort quotes (medium ≈ **$0.10**, high ≈ **$0.50**) |
| Call caps | Per-segment discovery vs deep caps |
| ORS stops | Agent attempt cap (≥2), insufficient budget, prefer cheaper Search first |
| Adaptive plan stop states | `adaptive-research-budget-v1` (`BUDGET_PAUSED`, low-information, etc.) |

### Underutilized / discarded / duplicated Exa results (from code + audits)

| Pattern | Evidence |
|---------|----------|
| Retrieval volume ≠ commercial yield | Bethesda priority-8: fresh Exa on 8/8 segments, **0 sales-ready** |
| Historical replay mistaken for completion | Full E2E matrix blocks “complete” on replay-only |
| Contact serialization / disposition hides ready rows | Post-Exa `$0` rescore recovered named contacts without new spend |
| Duplicate prevention | Priority-8: `duplicatesPrevented: 8`, resume duplicate live calls blocked |
| Shared discovery/deep cap leakage | Scaling 5>4 defect → stage-separated budgets |
| Official-page Wave 1 premature STOP | Lost verified pages vs deterministic crawl — planner stop ≠ Exa underuse, but warns against rewarding early stop |

### AI reasoning vs deterministic rules (today)

| Already AI / multi-step | Mostly deterministic |
|-------------------------|----------------------|
| Exa Agent API runs (vendor multi-hop inside one call) | Segment query packs, routing tables, stage budgets |
| Structured output schemas / validation heuristics | ORS complexity regex + strategy selection records |
| Some adaptive ORS next-step selection | Eligibility / disposition / challenger gates |

**Gap:** Dealality does not yet run a strong **evidence-driven query reformulation + multi-call investigation controller** on top of Exa with equal budget accounting. That is the candidate incremental role.

---

## 2. What the Dealality agent would actually add

Do **not** duplicate Exa Agent’s internal multi-hop or the existing supervisor’s eligibility gates.

| Potential incremental role | In scope for Arm B? | Notes |
|----------------------------|---------------------|-------|
| Adaptive query formulation across **multiple** Exa calls | **Yes** | Reformulate Search / Agent prompts from gaps left after prior Exa output |
| Evidence-driven search refinement | **Yes** | Choose next Exa Search vs Contents vs Agent mode from challenger gaps |
| Multi-step investigation **across** paid calls | **Yes** | Sequence discovery → contact → thesis verification under shared USD |
| Commercial contact identification prioritization | **Yes** | Prefer contact-bearing strategies when demand already confirmed |
| Cross-source verification planning | **Yes** | Plan corroboration (Search/Contents/official) after Agent claims |
| Dynamic research prioritization among frozen cases’ gaps | **Yes** | Re-rank unresolved tasks mid-run |
| Cost-aware stopping that preserves evidence yield | **Yes** | Fix premature `STOP_SALES_READY` / empty-seed stops; stop only when expected information gain < remaining budget |
| Replacing Exa Agent API | **No** | Both arms may use Exa Agent under same caps |
| Loosening ORS / eligibility / challenger | **No** | Identical gates |
| New discovery architecture / mission packs productization | **No** | Out of scope |
| Extra providers (SerpAPI, PDL, Surfe, Firecrawl metered, Parallel, JEV) | **No** | Denied in this design |

Attribution must still use: `REASONING_ONLY` | `RETRIEVAL_STRATEGY` | `PROVIDER_ACCESS` | `BUDGET_ASYMMETRY` (forbidden).

---

## 3. Fair paid comparison design (Conclusion A — eight cases)

### Arms

| Arm | Definition |
|-----|------------|
| **A — Existing Exa-powered GDI** | Current segment/ORS Exa path: deterministic query packs + existing ORS↔Exa selection + Exa Search/Contents/Agent as today’s code would invoke them under the shared ceiling |
| **B — Exa + Dealality adaptive agent** | **Same** Exa operations, effort models, call caps, USD ceiling, allowlists, and qualification gates; adaptive planner may change **order, query text, strategy family, and stop** within those caps |

### Shared constraints (binding)

- Frozen starting snapshots: existing focus-eight cohort (`fixtures/.../focus-eight-cases.json`).  
- Identical Exa capabilities (Search lite, Contents, Exa Agent discovery/deep as already routed).  
- Identical max USD and max Exa operations per case **and** paired total ceiling.  
- Identical ORS + `evaluateSharedCommercialEligibility` + evidence challenger + intake disposition.  
- Evidence-grounded `materialLift` evaluator (PR #420) — drawerReady / narrative / fewer calls alone cannot ADOPT.  
- No Airtable, GDO, customer publication, or production persistence writes.  
- No CMAA/DDAA deepen expansion.  
- **No Exa spending until separate founder authorization** (env + CLI flags).

### Wave structure

| Wave | Auth | Spend | Purpose |
|------|------|-------|---------|
| **Wave E0** | None | `$0` | Offline / mock-Exa parity smoke — selection records + gates only |
| **Wave E1** | Separate founder auth | Bounded paid Exa | Eight-case Conclusion A paired run |
| **Wave E2** | Separate | TBD | Optional paid follow-up after E1 ITERATE/ADOPT |

Auth env (proposed, not enabled by this PR):  
`GDI_AI_AGENT_EXA_PARITY_AUTH=1` **and** existing `GDI_EXA_AGENT_LIVE=1` + paid authorize flags.

### Estimated costs (bounded paired experiment)

Pricing basis (`exa-agent-provider-adapter-v1` / `benchmark-provider-pricing-v1`, as-of 2026-10):

| Unit | Quoted max |
|------|------------|
| Exa Agent effort=medium | **$0.10** / run |
| Exa Agent effort=high | **$0.50** / run |
| Exa lite search (≤10 results) | **$0.004** |
| Exa contents (per URL × type) | **$0.001** |

**Smallest controlled proposal (recommended):**

| Cap | Value |
|-----|-------|
| Per arm per case | **$1.25** hard ceiling (≈ up to 12 medium Agent-equivalent units, typically 2–4 Agent runs + Search/Contents) |
| Per case total (A+B) | **$2.50** |
| Eight-case experiment hard ceiling | **$20.00** |
| Effort | Prefer **medium**; high only if remaining case budget ≥ $0.50 and gap is contact/thesis critical |
| Historical reference | Bethesda priority-8 spent **$2.20** for 8 segment discoveries (no sales-ready) — E1 is opportunity-scoped, not 14-segment hotel matrix |

Reserve **$4** contingency only if founder raises ceiling in the auth token; default denied.

### Acceptance criteria (evidence-grounded)

Reuse locked metrics; ADOPT only if:

1. ≥2 cases with **evidence-grounded material commercial lift** (verified named contact gain and/or genuinely-new verified demand evidence).  
2. Zero false `SALES_READY` under challenger block.  
3. Speculative + competitor slots not falsely promoted.  
4. No `BUDGET_ASYMMETRY` / provider asymmetry.  
5. Independent evaluation concurs (not drawerReady-only).

**ITERATE** if efficiency/demotion quality improves but commercial lifts &lt;2.  
**REJECT** if no commercial lift and no systematic quality gain, or false sales-ready pressure.

ADOPT still means **further validation / selective integration only** — not production-wide deployment.  
Conclusion scope remains **A** (existing opportunities). Does **not** establish **B** (new opportunity discovery).

---

## 4. Conclusion B — genuinely new opportunity discovery (later)

Test separately after E1:

1. Freeze a **pre-discovery hotel/segment inventory** (known orgs excluded).  
2. Both arms receive the same Exa discovery budget and segment routing.  
3. Score only **net-new** org identities with dated hospitality-demand mechanisms + verified contacts that pass eligibility.  
4. Dedup against global intelligence retention / cross-hotel graph so replay cannot count.  
5. Larger budget than E1; still dual-auth; still no publication writes until founder review.

Do **not** claim B from an eight-case E1 ADOPT.

---

## 5. Risks and implementation requirements

| Risk | Mitigation |
|------|------------|
| Confusing Exa Agent API with Dealality agent | Explicit terminology + Arm definitions |
| Arm A artificially weakened | Arm A must call the **current** Exa GDI path, not official_page-only |
| Budget asymmetry | Shared ledger per case; abort if B spends more than A’s cap |
| Premature stop loses Exa value | Cost-aware stop rules; ban `STOP_SALES_READY` while challenger-blocked |
| Retrieval ≠ commercial value | Evidence-grounded evaluator + independent review |
| Paid spend without auth | Fail-closed; this PR does not enable live flags |
| Harness gap | Need thin Exa-parity arms wrapper (reuse canonical bridges + ORS selection) before E1 |

**Implementation before any spend (separate PR after founder go):**

1. Exa-parity arm runners (A = current path; B = adaptive controller over same provider adapters).  
2. Shared per-case ledger enforcing equal ceilings.  
3. Wire evidence-grounded rescore.  
4. Offline E0 mock tests.  
5. Founder auth packet with exact USD ceiling.

---

## 6. Recommendation

**Yes — a bounded paid Exa-parity experiment is commercially justified to run *after* separate founder authorization**, because:

1. Wave 1 `$0` official-page ITERATE does not answer the real product question.  
2. Existing Exa runs already show high retrieval / low commercial conversion — the plausible agent value is **better investigation sequencing and contact/thesis focus**, not “more Exa.”  
3. Cost can be capped at **~$20** for eight paired cases with equal arms.  
4. Evidence-grounded evaluator + frozen cohort already exist.

**Do not spend yet.** Ship this protocol lock only. Await founder authorization for Wave E1.

If founder declines spend: keep agent work focused on stop-gate quality and offline planning tests against recorded Exa artifacts (`HISTORICAL_REPLAY` labeled — never scored as fresh).

---

## Required conclusion checklist (this document)

1. Existing Exa-powered GDI workflow map — §1  
2. What the agent would add — §2  
3. Estimated costs — §3  
4. Exact experimental design + acceptance — §3  
5. Risks / implementation requirements — §5  
6. Recommendation — §6  

**Paid research executed by this PR:** none.
