# GDI AI Agent Incremental Value Experiment V1

**Status:** Protocol (founder clarification 2026-11-19)  
**Primary question:** Does agent-assisted research outperform the current GDI workflow in producing genuinely new, verified, commercially actionable hotel demand intelligence?  
**Non-goal:** Broad GDI discovery architecture productization.

---

## Clarification vs PR #414

PR #414 (`gdi-discovery-quality-pivot-plan-v1`) remains useful as a **limitations memo**. It is **not** authorization to build mission packs, graph intake product gates, or multi-segment discovery providers.

| From #414 | Role in this experiment | Defer as product work |
|-----------|-------------------------|------------------------|
| Replay/fixture can look like discovery | **Use** yield labels so arms cannot score `HISTORICAL_REPLAY` / `FIXTURE` as new findings | Hardening Full E2E discovery completion policy |
| Open vs competitor-placed vs too-early | **Use** disposition labels in evaluation (park competitor-placed; score overflow separately) | Demand-mechanism mission pack productization |
| Drawer gaps (`whyActNow`, next step, named buyer) | **Use** as secondary actionability checklist | Certified drawer product enforcement across all hotels |
| Graph / cross-hotel reuse | **Measure** duplicate + cross-hotel notes only | Market Opportunity Graph intake product gate |
| `$0` official public discovery providers | **Out of scope** unless separately authorized as a provider-access experiment | New discovery provider architecture |

**Binding rule:** Do not start discovery-architecture refactoring until this experiment answers whether **agent reasoning** creates incremental commercial value under **parity** of budget and source access.

---

## What already exists (reuse — do not rebuild)

| Asset | Path / ID | Use |
|-------|-----------|-----|
| Frozen 20-case cohort + snapshots | `fixtures/gdi-experiments/gdi-20opp-adaptive-investigation-v1/` | Immutable baseline inputs |
| Baseline vs agent harness | `gdi-20opp-adaptive-investigation-experiment-v1` | Arms A/B scaffolding |
| Live public prep / 5-case gate | `gdi-20opp-live-research-prep-v1` | Official-page parity path (`$0`) |
| Evidence challenger | embedded in 20-opp experiment | Independent verification before eligibility |
| ORS + commercial eligibility | `evaluateOpportunity`, `evaluateSharedCommercialEligibility` | Identical gates both arms |
| Yield / disposition helpers | `gdi-discovery-intake-disposition-v1` | Label provenance + commercial state (eval only) |
| Prior evidence | Offline 20-opp = **ITERATE** (rubric ≠ new facts); live 5-case `$0` = weak commercial lift; Phase 2B deepen = **STOP** | Inform design; do not re-open CMAA/DDAA deepen loops |

---

## Exact baseline-versus-agent protocol

### Arms (same starting inputs)

**Arm A — Existing GDI workflow**

1. Load frozen opportunity snapshot (immutable).  
2. Run `evaluateOpportunity` (ORS).  
3. Run `evaluateSharedCommercialEligibility`.  
4. Run evidence challenger on **frozen evidence only** (no adaptive planning, no new fetches).  
5. Emit Arm A packet: eligibility, challenger, drawer readiness gaps, cost=`$0`, wall time.

**Arm B — Agent-assisted GDI (bounded)**

1. Same frozen snapshot as Arm A (byte-identical start).  
2. Same ORS + eligibility + challenger **standards** (no relaxed gates).  
3. Bounded adaptive research: `buildAdaptiveResearchPreflight` → `selectAdaptiveNextAction` → allowed retrieval only.  
4. Evidence recovery / public extractors may update a **working copy**; never mutate the frozen fixture.  
5. Re-run ORS + eligibility + challenger on the working copy.  
6. Emit Arm B packet + **attribution tag** on every claimed improvement (see below).

### Attribution (mandatory — separates reasoning from access)

Every claimed delta must be tagged exactly one of:

| Tag | Meaning |
|-----|---------|
| `REASONING_ONLY` | Same evidence bag; better planning/classification/demotion without new retrieval |
| `RETRIEVAL_COVERAGE` | New pages from **parity-allowed** sources (official_page / already-listed URLs) |
| `PROVIDER_ACCESS` | Would require Exa/SerpAPI/PDL/etc. — **blocked** unless separate founder auth; if blocked, record as unmet |
| `BUDGET_ASYMMETRY` | Forbidden — experiment invalid if Arm B spends more than Arm A under the active budget gate |

### Source-access & budget parity

| Mode | When | Arm A | Arm B | Cap |
|------|------|-------|-------|-----|
| **Wave 0 — Offline parity** | Default / CI | Frozen evidence only | Adaptive **planning** + local recovery on frozen bag only (no network) | `$0` |
| **Wave 1 — Live public parity** | Founder sets `GDI_AI_AGENT_LIVE_PUBLIC_AUTH=1` | Same frozen start; **optional** identical official_page allowlist fetch if needed for fair baseline re-check | Same allowlist + adaptive URL selection within allowlist / same-origin deepen | `$0` paid providers; max N fetches/case (reuse Phase 2 prep: ≤12) |
| **Wave 2 — Paid providers** | **Separate** founder authorization + budget | Same paid tools if any | Same paid tools | Explicit USD ledger; default **not authorized** |

**Denied without Wave 2 auth:** Exa, SerpAPI, PDL, Surfe, Firecrawl metered, Parallel, JEV.  
**Writes denied:** Airtable, production persistence, customer publication.

### Held-out labels

- Cohort `evaluationLabelsHeldOut` remain **evaluator-only**.  
- Arms must not receive “known sales-ready / known contact” hints.  
- Independent review uses challenger + (for Wave 1) human/spot-check of provenance URLs — not agent self-scores.

---

## Representative test cohort

**Primary cohort:** the existing frozen **20-opportunity** adaptive investigation cohort (already quota-balanced).

**Scoring focus subset (smallest defensible commercial answer):** 8 cases from that frozen set — keep fixtures immutable; do not invent a second platform.

| Slot | Count | Purpose |
|------|------:|---------|
| Incomplete demand / thesis gap | 2 | Can agent add verified demand evidence? |
| Missing verified named contact | 2 | Can agent verify a commercially relevant buyer/entry point? |
| Forward-looking open/unknown lodging | 1 | New timing/housing evidence without competitor collapse |
| Rejected / speculative | 1 | Correct non-promotion (credit as quality, not “lift”) |
| Competitor-placed / cross-hotel booked | 2 | Correct demotion + optional overflow only if evidenced |

**Exclude from “deepen until success” loops:** Phase 2B STOP cases as *research targets* for more official-domain iterations (`gdi_lmc_00562bf81b1e1957`, `gdi_opp_2027_ddaa_annual_conference_8`). They may remain in the 20-case cohort for measurement continuity, but Wave 1 must not expand into another CMAA/DDAA deepen campaign.

**Hotels:** reuse cohort hotels (Bethesda, Wyndham San Diego, YOTEL Geneva, Radisson Toronto, W Rome, + documented substitutes). No new hotel onboarding.

---

## Primary success metrics (commercial incremental value)

Score **Arm B − Arm A** only for items that pass independent challenger review.

1. **Δ New verified opportunities / cycles** — genuinely new program/cycle evidence not present in frozen bag (`LIVE_NEW` novelty or newly verified demand evidence). Replay/fixture ≠ count.  
2. **Δ Newly verified commercially relevant named contacts** — name + role + provenance; prior known contacts ≠ “newly discovered.”  
3. **Δ Material demand-thesis strengtheners** — new facts that change thesis class (e.g. speculative → reliable inferred / confirmed) with provenance.  
4. **Δ Hotel-specific actionability** — independent pass on `whyActNow` + `recommendedNextStep` **and** hotel relevance without generic boilerplate (drawer checklist).  
5. **Efficiency** — wall time, fetch count, `$` paid (must be `$0` unless Wave 2).  
6. **Harm / noise** — false positives, unsupported claims, duplicate canonical opportunities.  
7. **Correct rejections / demotions** — reported **separately** from discovery lift (AJAS-class, competitor-placed primary blocks).

### Explicit non-rewards

- Extra URLs without new extractable commercial facts  
- Historical replay or fixture recovery labeled as new discovery  
- Narrative polish without new facts  
- Eligibility flips blocked by challenger  
- Rubric-total deltas alone (offline 20-opp already showed this is misleading)

---

## Secondary metrics

- Disposition agreement with intake classifier (`OPEN` / `COMPETITOR_PLACED` / `TOO_EARLY_WATCH` / `SPECULATIVE`) — diagnostic only  
- Attribution mix (`REASONING_ONLY` vs `RETRIEVAL_COVERAGE`)  
- Fetch→fact conversion rate (verified facts / fetches)  
- Cross-hotel reuse notes without double-counting sales-ready inventory

---

## Independent evaluation & acceptance criteria

### Independent evaluation

1. Evidence challenger on both arms (existing).  
2. Yield label gate: Arm B claims of “new” require `claimsGenuineNewDiscovery` **or** documented `RETRIEVAL_COVERAGE` with URL+excerpt hash; fixture/historical cannot qualify.  
3. Spot-check: for every claimed primary metric hit, reviewer confirms provenance URL supports the claim (Wave 1).  
4. Same eligibility contract both arms; intake disposition may **block** SALES_READY for speculative / competitor-placed / too-early (no auto-promotion).

### Decision framework (Wave 1 live public parity, `$0` paid)

Compute on the **8-case focus subset** (challenger-confirmed):

| Decision | Criteria |
|----------|----------|
| **ADOPT** | ≥2 cases with material commercial lift on metrics 1–4 **and** zero false SALES_READY **and** correct rejection retained on speculative/competitor slots **and** attribution shows lift is not `BUDGET_ASYMMETRY` / unpaid provider smuggling |
| **ITERATE** | Some challenger-confirmed lift **or** clear reasoning-only quality gains (correct demotions) **but** <2 material commercial lifts **or** lift concentrated in URL theater / weak housing pages |
| **REJECT** | No challenger-confirmed commercial lift across focus subset **and** no systematic quality gain in correct rejections **or** agent increases false positives / unsupported sales-ready pressure |

### Wave 0 (offline) rule

Wave 0 may only support **ITERATE** or **REJECT** for *reasoning-only* hypotheses. It **cannot** support **ADOPT** (no new web evidence possible under network denial). Prior offline 20-opp result remains **ITERATE** under this rule.

### Wave 2 (paid) rule

Wave 2 answers a **different** question (provider access). Do not merge Wave 2 lift into an “agent reasoning” ADOPT without a paired Arm A that receives the **same** providers and budget.

---

## Proposed execution plan

1. **Protocol lock (this document)** — PR → CI → Integration Controller merge → `ws/gdi` restore.  
2. **Harness thin patch (only if needed)** — extend existing `gdi-20opp-*` runners to emit attribution tags + commercial primary metrics; **no** new research platform.  
3. **Wave 0 re-score** — offline parity on 8-case focus; confirm non-reward of rubric-only lift.  
4. **Founder gate** — authorize Wave 1 live public `$0` (flag) if Wave 0 instrumentation is clean.  
5. **Wave 1 execute** — 8 cases, fetch caps, ledger, challenger, independent spot-check.  
6. **Decision** — ADOPT / ITERATE / REJECT per framework; stop.  
7. **Only then** — if ADOPT or targeted ITERATE, consider *narrow* follow-ups; still do not authorize #414 product architecture by default.

**Estimated effort:** protocol + metric wiring days, not a multi-week architecture program.  
**Paid spend default:** `$0`.

---

## Clear stops

- No CMAA/DDAA Phase 2B deepen expansion.  
- No parallel research platform.  
- No Airtable / customer publication.  
- No paid providers without separate founder authorization.  
- No treating #414 bottleneck remediations as pre-approved build scope.

---

## One-line answer this experiment must produce

> Under matched inputs, qualification rules, and source/budget parity, does the agent arm produce challenger-confirmed commercial increments that the current GDI workflow does not — enough to ADOPT, or only enough to ITERATE/REJECT?
