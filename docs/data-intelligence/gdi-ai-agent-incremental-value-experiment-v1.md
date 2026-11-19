# GDI AI Agent Incremental Value Experiment V1

**Status:** Protocol LOCKED (founder refinements 2026-11-19)  
**Primary question:** Does agent-assisted research deliver meaningful incremental commercial value over the existing GDI research workflow under matched source access, fetch, budget, and execution limits?  
**Non-goal:** Broad GDI discovery architecture productization.  
**Wave 1 live research:** not authorized by this document — requires separate founder authorization.

---

## Clarification vs PR #414

PR #414 (`gdi-discovery-quality-pivot-plan-v1`) remains a **limitations memo**. It is **not** authorization to build mission packs, graph intake product gates, or multi-segment discovery providers.

| From #414 | Role in this experiment | Defer as product work |
|-----------|-------------------------|------------------------|
| Replay/fixture can look like discovery | **Use** yield labels so arms cannot score `HISTORICAL_REPLAY` / `FIXTURE` as new findings | Hardening Full E2E discovery completion policy |
| Open vs competitor-placed vs too-early | **Use** disposition labels in evaluation | Demand-mechanism mission pack productization |
| Drawer gaps (`whyActNow`, next step, named buyer) | **Use** as secondary actionability checklist | Certified drawer product enforcement |
| Graph / cross-hotel reuse | **Measure** duplicate + cross-hotel notes only | Market Opportunity Graph intake product gate |
| `$0` official public discovery providers | **Out of scope** as architecture work | New discovery-provider architecture |

**Binding rule:** Do not start discovery-architecture refactoring until this experiment answers whether **agent adaptive planning** creates incremental commercial value under **parity** of budget and source access.

---

## What this experiment does — and does not — conclude

| Conclusion type | Meaning | Supported by this 8-case design? |
|-----------------|---------|----------------------------------|
| **A. Incremental improvement researching existing opportunities** | Same frozen opportunity IDs; Arm B improves verified evidence, contacts, thesis, or actionability vs Arm A under parity | **Yes — primary claim surface** |
| **B. Incremental discovery of genuinely new opportunities** | Agent finds orgs/programs not already in the frozen cohort / inventory | **No — not established by this design.** Requires a separate discovery-specific comparison (out of scope here). |

Do **not** claim B from an ADOPT/ITERATE result on this protocol alone.

---

## What already exists (reuse — do not rebuild)

| Asset | Path / ID | Use |
|-------|-----------|-----|
| Frozen 20-case cohort + snapshots | `fixtures/gdi-experiments/gdi-20opp-adaptive-investigation-v1/` | Immutable starting inputs |
| Frozen 8-case focus lock | `fixtures/gdi-experiments/gdi-ai-agent-incremental-value-experiment-v1/focus-eight-cases.json` | Exact IDs + slots (immutable before execution) |
| Baseline vs agent harness | `gdi-20opp-adaptive-investigation-experiment-v1` + thin `gdi-ai-agent-incremental-value-experiment-v1` | Arms A/B scaffolding |
| Live public prep | `gdi-20opp-live-research-prep-v1` | Official-page `$0` retrieval patterns |
| Evidence challenger | embedded in 20-opp experiment | Independent verification before eligibility |
| ORS + commercial eligibility | `evaluateOpportunity`, `evaluateSharedCommercialEligibility` | Identical gates both arms |
| Yield / disposition helpers | `gdi-discovery-intake-disposition-v1` | Eval-only provenance + commercial state |
| Prior evidence | Offline 20-opp = ITERATE (rubric ≠ facts); live 5-case `$0` weak lift; Phase 2B = STOP | Inform design; no CMAA/DDAA deepen loops |

---

## Exact baseline-versus-agent protocol

### Fairness principle (binding)

**Arm A must execute the existing GDI research process** within the **same** source-access, fetch, budget, and comparable execution limits available to Arm B.

**Arm B** uses adaptive AI research planning and investigation inside those same limits.

Do **not** compare an active research agent against an artificially restricted, frozen-evidence-only baseline in Wave 1 and attribute the difference to agent reasoning.

| Wave | Fair comparison? | What differs |
|------|------------------|--------------|
| **Wave 0 — Offline reasoning** | Yes (identical frozen bag; no network) | Arm B may plan/recover/classify on the same bag; Arm A evaluates without adaptive planning |
| **Wave 1 — Live public parity** | Yes — **both arms actively research** under shared caps | Arm A = existing GDI retrieval/research sequence; Arm B = adaptive planning/source selection within the **same** allowlist, fetch cap, and `$0` paid rule |
| **Wave 2 — Paid** | Only if both arms get the same paid tools + budget | Separate founder authorization; default denied |

### Arms (same starting inputs)

**Shared for every case**

1. Load frozen opportunity snapshot (immutable fixture; never mutate).  
2. Clone to a working copy per arm.  
3. Apply identical ORS + `evaluateSharedCommercialEligibility` + evidence challenger standards (no relaxed gates for Arm B).  
4. Enforce identical caps: paid USD, max fetches, allowlist/origin policy, max steps / wall-time band.  
5. Emit packets with cost, fetch count, duration, provenance, attribution tags.

**Arm A — Existing GDI workflow (active baseline)**

- Wave 0: ORS + eligibility + challenger on frozen evidence only (no adaptive planner).  
- Wave 1: Run the **existing** GDI public/official research path on the working copy (deterministic seed-URL / official_page / established extractor sequence used by current Full E2E / live-prep tooling), consuming the **same** fetch budget and allowlist as Arm B. No adaptive next-action planner.  
- Does **not** withhold retrieval that Arm B is allowed to use.

**Arm B — Agent-assisted GDI**

- Wave 0: Adaptive preflight + next-action planning + local recovery on frozen bag only (no network).  
- Wave 1: Same caps as Arm A; adaptive planning chooses order/targets of allowed retrieval and investigation steps.  
- Never mutates the frozen fixture; never bypasses challenger/eligibility.

### Attribution (mandatory)

Every claimed Arm B − Arm A delta must be tagged exactly one of:

| Tag | Meaning |
|-----|---------|
| `REASONING_ONLY` | Same retrieved evidence (or Wave 0 identical bag); better planning/classification/demotion |
| `RETRIEVAL_STRATEGY` | Different pages/facts found **within the same allowlist/fetch cap** because Arm B chose differently than Arm A’s existing sequence |
| `PROVIDER_ACCESS` | Would require Exa/SerpAPI/PDL/etc. — blocked unless Wave 2; record as unmet |
| `BUDGET_ASYMMETRY` | Forbidden — experiment invalid if Arm B receives higher spend/fetch limits than Arm A |

`RETRIEVAL_STRATEGY` is **not** automatic proof of agent value unless Arm A had equal opportunity to fetch within the same cap (Wave 1 fairness). In Wave 0, `RETRIEVAL_STRATEGY` must not be claimed.

### Source-access & budget parity

| Mode | Authorization | Arm A | Arm B | Cap |
|------|---------------|-------|-------|-----|
| Wave 0 | Default / CI | Frozen eval (existing GDI) | Adaptive planning on frozen bag | `$0`, no network |
| Wave 1 | Separate founder flag `GDI_AI_AGENT_LIVE_PUBLIC_AUTH=1` | Active existing GDI public research under shared caps | Adaptive public research under **identical** caps | `$0` paid providers; ≤12 fetches/case; shared allowlist/same-origin policy |
| Wave 2 | Separate founder paid authorization | Same paid tools | Same paid tools | Explicit USD ledger |

**Denied without Wave 2:** Exa, SerpAPI, PDL, Surfe, Firecrawl metered, Parallel, JEV.  
**Writes denied:** Airtable, production persistence, customer publication.  
**No Wave 1 execution** until founder sets the live-public auth flag.

### Held-out labels

- `evaluationLabelsHeldOut` remain evaluator-only.  
- Arms must not receive known sales-ready / known-contact hints.  
- Independent review = challenger + Wave 1 provenance spot-check — not agent self-scores.

---

## Representative test cohort (locked)

**Primary cohort:** existing frozen **20-opportunity** adaptive investigation cohort.

**Focus subset (LOCKED before execution)** — exact IDs in  
`fixtures/gdi-experiments/gdi-ai-agent-incremental-value-experiment-v1/focus-eight-cases.json`:

| # | Opportunity ID | Slot | Hotel (from cohort) |
|---|----------------|------|---------------------|
| 1 | `disp_recLuxvwwxID7U2B8_aledade` | incomplete_demand | Bethesda Marriott |
| 2 | `disp_recLuxvwwxID7U2B8_cohera_formerly_csi_dmc_and_360dg_` | incomplete_demand | Bethesda Marriott |
| 3 | `rb_gdi_disc_dc428c1bd4c4e412` | missing_verified_contact | Radisson Blu Toronto |
| 4 | `gdi_opp_prospect_mv8p5ddh_7edb8e7c` | missing_verified_contact | Wyndham San Diego Bayside |
| 5 | `gdi_opp_governor_s_hurricane_conference_2027_26` | forward_looking | Waterstone Resort & Marina |
| 6 | `gdi_opp_ajas_hq_rockville_watch` | rejected_speculative | Bethesda Marriott |
| 7 | `gdi_opp_wrcamp_altaroma_2027_altaroma_organizer` | competitor_cross_hotel | W Rome |
| 8 | `camp_ivirma_2027_castillo` | competitor_cross_hotel | Castillo Hotel Son Vida |

**Excluded from focus and from deepen loops:**  
`gdi_lmc_00562bf81b1e1957` (CMAA), `gdi_opp_2027_ddaa_annual_conference_8` (DDAA).  
They may remain in the 20-case cohort for continuity only — **no** further official-domain deepen campaigns.

**Hotels:** cohort hotels only. No new hotel onboarding. No second research platform.

---

## Primary success metrics (commercial incremental value)

Score **Arm B − Arm A** only for challenger-confirmed items. Assess lift **against** additional execution cost (fetches, duration, `$`).

1. Strengthened verified demand evidence on an **existing** opportunity (Conclusion A).  
2. Newly verified commercially relevant named contacts (not previously known on the frozen snapshot).  
3. Material demand-thesis strengtheners (class change with provenance).  
4. Hotel-specific actionability (`whyActNow` + `recommendedNextStep` + non-generic hotel relevance).  
5. Efficiency — wall time, fetch count, `$` paid (must be `$0` unless Wave 2).  
6. Harm — false positives, unsupported claims, duplicate canonical opportunities.  
7. Correct rejections / demotions — **reported separately** from lift.

### Explicit non-rewards

- Extra URLs without new extractable commercial facts  
- Historical replay / fixture recovery as “new discovery”  
- Narrative polish without new facts  
- Known contacts presented as newly discovered  
- Rubric-total deltas alone  
- Claiming Conclusion B (new opportunity discovery) from this 8-case design

---

## Secondary metrics

- Disposition agreement (`OPEN` / `COMPETITOR_PLACED` / `TOO_EARLY_WATCH` / `SPECULATIVE`) — diagnostic  
- Attribution mix (`REASONING_ONLY` vs `RETRIEVAL_STRATEGY`)  
- Fetch→fact conversion (verified facts / fetches)  
- Cross-hotel reuse notes without double-counting sales-ready inventory  
- Cost-adjusted lift (commercial hits per fetch-minute)

---

## Independent evaluation & decision framework

### Independent evaluation

1. Evidence challenger on both arms.  
2. Yield/provenance gate: fixture/historical cannot count as new web evidence.  
3. Wave 1 spot-check: every claimed primary hit must show a provenance URL supporting the claim.  
4. Identical eligibility contract; intake disposition may block SALES_READY for speculative / competitor-placed / too-early.

### Decision (Wave 1 live public parity, `$0` paid) — 8-case focus

| Decision | Criteria | Interpretation |
|----------|----------|----------------|
| **ADOPT** | ≥2 cases with material commercial lift on metrics 1–4 **and** zero false SALES_READY **and** correct rejection retained on speculative/competitor slots **and** no `BUDGET_ASYMMETRY` **and** lift justifies added research effort vs Arm A | Evidence supporting **further validation or selective integration** — **not** automatic production-wide deployment |
| **ITERATE** | Some challenger-confirmed lift **or** clear reasoning-only quality gains **but** <2 material lifts **or** weak/URL theater **or** cost not justified | Refine agent planning; re-test under parity |
| **REJECT** | No confirmed commercial lift and no systematic quality gain, **or** increased false positives / unsupported sales-ready pressure | Do not integrate agentic research on this basis |

### Evidence-grounded `materialLift` (binding)

`materialLift` requires independently verified incremental commercial value:

- more **verified named commercial contacts**, and/or
- more **genuinely-new verified demand evidence**

These alone do **not** count as material commercial lift: `drawerReady` becoming true, formatting/narrative polish, recovery overlays, fewer retrievals, rubric totals, eligibility-only changes, or existing facts repackaged as new findings.

Efficiency (fetch/time savings) and correct demotions are tracked **separately**. The automated decision **must not return ADOPT** when evidence-grounded material commercial lifts are zero (aligns with independent evaluation).

Evaluator module: `lib/group-demand-intelligence/experiments/gdi-ai-agent-wave1-evidence-grounded-evaluator-v1.js`. Offline rescore: `npm run gdi:ai-agent-wave1-evidence-grounded-rescore-v1`.

### Wave 0 rule

Wave 0 may only support **ITERATE** or **REJECT** for reasoning-only hypotheses. It **cannot** support **ADOPT**.

### Wave 2 rule

Wave 2 is a provider-access question. Do not fold unpaid provider advantages into an agent-reasoning ADOPT.

---

## Proposed execution plan

1. **Protocol lock (this document + frozen 8 IDs)** — PR #416 → CI → merge → `ws/gdi` restore.  
2. **Minimal harness** — thin wrapper over existing 20-opp / live-prep runners: fair Wave 1 Arm A active baseline, attribution tags, Conclusion A metrics; **no** new platform.  
3. **Wave 0** — offline parity on locked 8 cases (CI-safe).  
4. **Stop for founder** — Wave 1 live public requires separate `GDI_AI_AGENT_LIVE_PUBLIC_AUTH=1`.  
5. **Wave 1 (only if authorized)** — both arms active under shared caps; challenger + spot-check; cost-adjusted decision.  
6. **Decision** — ADOPT (further validation only) / ITERATE / REJECT.  
7. Still do not authorize #414 product architecture by default.

**Paid spend default:** `$0`. **Wave 1:** not started by this PR.

---

## Clear stops

- No CMAA/DDAA Phase 2B deepen expansion.  
- No parallel research platform.  
- No Airtable / customer publication.  
- No paid providers without separate founder authorization.  
- No Wave 1 live research without separate founder authorization.  
- No claiming genuine new-opportunity discovery (Conclusion B) from this 8-case experiment.  
- No treating ADOPT as production-wide deployment.

---

## One-line answer this experiment must produce

> Under matched inputs, qualification rules, and **matched active research limits**, does adaptive agent planning improve verified commercial outcomes on existing opportunities versus the current GDI research process enough to justify further validation (ADOPT), iteration (ITERATE), or rejection (REJECT) — without claiming new-opportunity discovery or automatic production rollout?
