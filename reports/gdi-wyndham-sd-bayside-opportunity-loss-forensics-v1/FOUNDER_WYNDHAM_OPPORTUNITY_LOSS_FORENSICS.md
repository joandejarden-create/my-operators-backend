# P0 — Wyndham GDI Opportunity Loss Forensics + Search vs Agent Evaluation

**Hotel:** Wyndham San Diego Bayside `recnPB3Q02NO8Ma1o`  
**Mode:** Forensic audit + benchmark **design** only  
**Paid spend this task:** **$0** · Agent OFF · Surfe OFF · Findymail OFF  
**Writes / STAGED / CURRENT / Bethesda:** unchanged · CLEAN 11 protected  

**Related:** Live $35 Search E2E (completed) · Commercial conversion PR [#604](https://github.com/joandejarden-create/my-operators-backend/pull/604) (OPEN — FQO quality enforcement; not a substitute for this forensics)

**STOP** after this deliverable. No architecture change. No paid Agent until separate founder GO.

---

## Executive answers

### What is suppressing conversion?

Not “FQO is too strict” as a single explanation. The **59 → 12** drop is a **stack of layers**:

1. **Machine PASS (59)** already over-counts (historical retain + soft stamps + functional-email/phone soft-pass — addressed in #604).  
2. **VALIDATED ~42–56** was inflated by a **persistence-filter bug** (historical soft-path applied to every row).  
3. **Strict commercial 12** (11 CLEAN + **1** net new) correctly refused scrapes, but **also treated incomplete WHO/HOW/pathway research as commercial death** instead of research maturity.  

**Largest avoidable loss drivers (of 47 excluded from 59):**

| Rank | Bottleneck | Count | Nature |
|---|---|---:|---|
| 1 | Future/recurring **pathway** incomplete or under-evidenced | **25** | Often *valid demand* waiting on cycle/recurring proof — **not** “fake demand” |
| 2 | **Research extraction defects** (non-person / role-glued WHO) | **14** | Search snippet regex pollution — demand may still be real |
| 3 | Other / residual commercial rejection | 6 | Mixed |
| 4 | Incomplete HOW (functional channel) | 1 | Undercounts HOW issues hidden inside extraction/pathway multi-fails |
| 5 | Hotel-fit unsupported | 1 | Rare in this machine-PASS slice |

**Primary bottleneck:** pathway/event assumptions **plus** poor contact extraction — **not** missing demand discovery (14-seg live Search already proved coverage).

### Would Agent beat Search-only for conversion?

**Hypothesis (unproven until matched benchmark):** Yes for **WHO/HOW + pathway verification** on already-discovered demand; **No** as a replacement for broad 14-seg discovery economics.

Exa Search already found demand cheaply (~$0.31 discovery / ~$2.98 total). Conversion failed in **post-discovery research quality**, which is exactly what Exa Agent (`api.exa.ai/agent/runs`) is designed for: multi-hop, structured JSON, page reading, contact enrichment — distinct from GDI’s Search-only loops.

**Do not claim Agent superiority without the $10 matched experiment below.**

---

## 1. Funnel reconciliation: 59 → 56 → 12

| Stage | Count | What it means |
|---|---:|---|
| Reconciled research universe | **303** | Inventory ∪ live discovery ∪ CLEAN 11 (pre–hotel-wide) |
| Machine `FINALIZATION_PASS` | **59** | Candidate `…_fb10174d` — includes soft-pass inflation |
| VALIDATED dry-run (buggy historical blanket) | **42*** | *Recomputed with current FQO; original live report cited ~56 under older gate* |
| VALIDATED after #604 filter fix | **18** | Historical soft-path only for CLEAN 11 IDs |
| Strict commercial (ad-hoc scrub at live close) | **12** | CLEAN 11 + ACG only |
| Net new beyond CLEAN 11 (strict 12) | **1** | ACG San Diego / Judy Susser |
| Excluded machine PASS − strict 12 | **47** | Full forensic set |

\*Original live summary said VALIDATED **56**; recomputation under current contract yields **42** buggy-blanket keeps. The **mechanism** (blanket `historicalApprovedInventory`) is confirmed either way.

**Do not write 59 or buggy-VALIDATED counts to production.**

Artifacts: `01-FULL_RECORD_RECONCILIATION.json`, `02-EXCLUDED_47_FORENSICS.json`.

---

## 2. Rule-level exclusion waterfall

| Rule | Affected (of 47) | Uniquely | Correct rejects (examples) | Potential false / research-incomplete |
|---|---:|---:|---|---|
| `NO_VALID_FUTURE_SOURCING_PATHWAY` | **~25+** | High | Past one-time events without next-cycle proof | Recurring accounts / partnerships lacking dated event fields |
| Non-person / junk contact name | 8–14* | Medium | `Nolita House`, `Sponsorship Opportunities`, `National` | Real orgs whose **WHO scrape** failed |
| `ROLE_GLUED_PERSON_NAME` | 5 | 4 | `Jennifer Klein Senior`, `… Regional` | Same person recoverable if title stripped |
| `PERSON_SPECIFIC_CHANNEL_REQUIRED` | 1+ | Low unique | Functional-only without LinkedIn/label | NCCN / housing desks with real named people |
| `MISSING_HOTEL_FIT_THESIS` | 1 | — | Negative fit language | Rare in this slice |
| `COMPETITIVE_INCUMBENT_OR_PATHWAY_UNVERIFIED` | low | — | Competitive without incumbent | Overflow rows needing SPARGO/housing proof |

\*Extraction defects span multiple reason codes; primary-cause tally = **14**.

Full stats: `03-RULE_WATERFALL.json`.

### Controlled counterfactuals (potential eligibility — **not** actual PASS)

| If missing evidence were supplied… | Would become FQO-eligible (of 47) |
|---|---:|
| Name extraction fixed to a plausible person | **13** |
| Person-direct email **or** LinkedIn supplied | **14** |
| Recurring pathway evidence populated | **32** |
| WHO + HOW + pathway all supplied | **47** |

Interpretation: **almost every machine-PASS row has a recoverable commercial core if research completeness is fixed** — but that is **not** permission to loosen FQO or stamp pathways. It measures **avoidable research/process loss**.

---

## 3. False positives vs false negatives

### False positives (machine / buggy VALIDATED) — correctly refused by commercial FQO

- Functional inbox presented as person-direct (`exhibits@`, `westhousing@`) + company switchboard phone  
- Non-person WHO: venues, FAQ titles, “Sponsorship Opportunities”  
- Role-glued names and SignalHire “ask to be directed” scrape blobs  
- Bare `futureDemandPathway` stamps on past cycles  

→ **Valid commercial rejection / extraction defect.** Keep strict FQO.

### False negatives (research maturity, not “bad demand”)

Rows with **credible lodging/meeting demand + hotel fit** blocked mainly because:

- Pathway fields empty / event-centric  
- WHO field polluted though org/program is real  
- HOW is functional until LinkedIn/org route labeled  

→ Classify as **`VERIFIED_DEMAND_*_REQUIRED`**, not as non-opportunities.

**Do not infer commercial unsuitability from contact failure alone.**

---

## 4. Duplicate / CLEAN 11 lineage

| Finding | Evidence |
|---|---|
| CLEAN 11 IDs sit inside candidate 59 | 11/11 present |
| Prior “near-miss” lists re-listed CLEAN 11 contacts | Duplicate presentation, not new defects |
| Material CLEAN 11 commercial defect under forensics | **None** — all remain in strict 12 |
| Silent downgrade | **Not done** — STAGED still `…_3283ee92` |

---

## 5. Demand credibility vs contact readiness (Three-Layer)

Proposed **research-maturity states** (Layer 1/2 metadata — **not** new tables / not auto-publish):

| State | Maps to existing concepts | Publication |
|---|---|---|
| **Verified Demand** | Evidence + hotel fit; WHO/HOW/pathway may be incomplete | Research SoT only |
| **Contact Research Required** | Verified demand; named person missing/unverified | Research SoT + continuation queue |
| **Identification Required** | Demand weak/ambiguous generator identity | Research SoT |
| **Sales Ready / FQO** | Full six-rule FQO | Only VALIDATED_FQO_ONLY → STAGED auth |
| **Historical / Rejected** | Past-only / wrong-fit / junk | Preserve; never active STAGED |

**Dimension counts on the 59 (post-fix FQO lenses):**

| Maturity | Count |
|---|---:|
| Sales-ready FQO candidate | 18 |
| Verified demand — pathway incomplete | 32 |
| Verified demand — contact research required | 8 |
| Demand present — hotel fit unsupported | 1 |

FQO remains the **only** sales-ready gate. Maturity states prevent **dropping Layer-1 research** when Layer-2 contact/pathway is incomplete.

---

## 6. Top 20 most promising blocked opportunities

See `04-TOP20_PROMISING_BLOCKED.json`. Highest-priority themes:

1. Named scientific/association 2027 programs with functional or incomplete HOW (e.g. NCCN)  
2. Live-discovered local/government/education housing pages needing pathway + WHO cleanup  
3. Workforce / relocation / project housing pages with lodging language but scrape WHO  
4. Competitive WEST / intermediary rows needing labeled housing routes + LinkedIn  

**Recommended resolution pattern:** preserve as Verified Demand → targeted Agent/Surfe WHO-HOW → requalify — **never** auto-promote HOLD→FQO.

---

## 7. Exa Search vs Exa Agent (capability comparison)

### Important distinction

| Surface | What it is in Dealality |
|---|---|
| **Exa Search API** (`/search` + optional `/contents`) | Used in Wyndham $35 live E2E |
| **Exa Agent API** (`/agent/runs`) | Separate Exa product; GDI adapter `exa-agent-provider-adapter-v1.js` |
| **GDI “Agent” path** | Dual-auth wrapper around Exa Agent (+ segment contracts) — **not** identical to raw Search |

### What Search actually did on Wyndham

| Behavior | Observed |
|---|---|
| Returns | Ranked URLs + optional **text snippets** (`contents.text`, ~1600 chars in live script) |
| Full page investigation | **No** systematic multi-hop page graph; mostly snippet regex |
| Contact extraction | Regex email/phone/LinkedIn + `Name + Title` line patterns |
| Why role-glued / non-person names | Snippet titles (“Housing & Travel”), role words glued to names, venue/org tokens scraped into `primaryContact.name` |
| Linked source follow-up | Weak — URLs stored; deep verification not Agent-orchestrated |
| Stop condition | Budget/coverage bands; often stopped before verified WHO/HOW |
| Cost (live) | **$2.982** / 426 calls; discovery alone **~$0.31** |

Public contact Search helper sometimes returns **URLs only** (no text) — even thinner than the live Wyndham path.

### What Exa Agent does differently (docs + existing GDI adapter)

| Capability | Agent |
|---|---|
| Multi-hop search + page read + verify | Yes (async research agent) |
| Structured `outputSchema` (name, title, LinkedIn, …) | Yes — reduces regex false persons |
| Grounding / citations | Field-level grounding when emitted |
| List-build + enrich decision-makers | Explicit product purpose |
| Latency | Higher (async poll) |
| Cost (GDI list prices) | Fixed effort: minimal **$0.012**, low **$0.025**, medium **$0.10**, high **$0.50**, xhigh **$1.00**; auto/max ceiling-quoted |
| Live gate | `authorizeExaAgentLive` + `GDI_EXA_AGENT_LIVE=1` |

### Hybrid Search + Agent

- **Search:** cheap broad discovery (keep 14-seg coverage pattern)  
- **Agent:** deep WHO/HOW/pathway on **Verified Demand** cohort only  
- Avoids “blind Agent for everything” and “Search regex for contacts”

---

## 8. Matched provider benchmark plan ($10 **proposal**)

**Cohort:** 30 opportunities from existing Wyndham universe (`05-BENCHMARK_COHORT_30.json`) — scaling, planners/intermediaries, venues/partnerships, associations/competitive, projects/extended stay.  
**Identical starting evidence** per arm (frozen snapshot of current SoT fields + source URLs).  
**No Surfe / Findymail.**  

### Arms

| Arm | Method | Proposed budget |
|---|---|---:|
| A | Search-only enrichment (search + contents, no Agent) | **$2.50** |
| B | Agent-led research (medium/low effort structured WHO-HOW-pathway) | **$5.00** |
| C | Hybrid (Search triage → Agent on top 15) | **$2.00** |
| Reserve | | **$0.50** |
| **Total** | | **$10.00** |

### Metrics (all arms)

Correct named stakeholders · verified role · usable outreach route · source accuracy · future/recurring pathway · hotel-specific action · false-person rate · unsupported sourcing claims · strict FQO conversion · **$/newly verified actionable opportunity** · wall time  

### Blind review

Founder or independent reviewer grades a **10-row sample** from each arm using source links only (hide which provider produced the row).

### Authorization

This is a **PROPOSAL**. **Zero Agent/Search spend** until:

`FOUNDER_GO AUTHORIZE_WYNDHAM_PROVIDER_BENCHMARK_10USD`

---

## 9. Ranked minimum remediations

| Priority | Action | Impact | Effort | Status |
|---|---|---|---|---|
| 1 | Preserve Verified Demand when WHO/HOW/pathway incomplete (maturity states in SoT) | Stops “losing” research | Low | Design herein — implement after GO |
| 2 | Contact extractor + lineage (strip role glue; reject non-persons; promote LinkedIn provenance) | Recovers ~13–14 candidates to FQO *evaluation* | Low–Med | Partially in **#604** |
| 3 | Recurring account/partnership pathway fields (no invented dates) | Addresses largest attrition bucket (25) | Med | Partially in **#604**; needs evidence population via research |
| 4 | Commercial quality inside canonical FQO | Removes false machine PASS | Med | **#604** |
| 5 | Matched Agent benchmark on 30-cohort | Answers Search vs Agent economics | Med + $10 | **Awaiting GO** |
| 6 | Presentation maturity chips (Verified Demand / Contact Required / FQO) | Sales clarity without weakening FQO | Low | After #604 merge |

**Explicitly out of scope:** loosening six-rule FQO · fabricating contacts/dates/blocks · auto-promoting HOLDs · new database/engine.

---

## 10. Exact next founder decisions

1. **Merge or revise PR #604** (FQO commercial-quality + validated historical blanket fix) via Integration Controller.  
2. **Authorize or defer** `FOUNDER_GO AUTHORIZE_WYNDHAM_PROVIDER_BENCHMARK_10USD` for the matched Search / Agent / Hybrid experiment.  
3. **Authorize or defer** Surfe explicit-click on the separate ranked queue (post-benchmark or in parallel — your call).  
4. **Do not** authorize Airtable write / Wyndham STAGED expansion from machine 59 or buggy VALIDATED counts.

---

## Governance confirmation

- No production Airtable writes  
- No STAGED/CURRENT changes  
- Bethesda frozen · CLEAN 11 protected  
- No paid research this task  
- No automatic Surfe / Findymail / Agent  
- No new database or parallel FQO engine  
- Research provenance preserved in forensic JSON packs  

**Persistence classification:** still `PROTOTYPE` for live Wyndham intelligence pending write/STAGED auth.

---

## Success criteria (this task)

We can now answer precisely:

1. **Which rules/limitations suppress conversion?** Pathway under-evidence (25) + contact extraction defects (14) + layered over-counts (machine/VALIDATED) — not “discovery failed.”  
2. **Would Agent help?** Likely on WHO/HOW/pathway for Verified Demand; **unproven** until the $10 matched benchmark. Search remains the right broad discovery tool.

**STOP.**
