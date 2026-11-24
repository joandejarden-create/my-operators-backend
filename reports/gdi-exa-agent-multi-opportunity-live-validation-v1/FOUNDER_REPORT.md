# Exa Agent Multi-Opportunity Live Contact Recovery — Founder Report

**Contract:** `gdi-exa-agent-multi-opportunity-live-validation-v1`  
**Canonical tip:** `b554a08aa4bbf4ea95ae029e962e6de54a2c0c08` (PR #587 merged)  
**New Exa Agent spend:** **USD $0.70 / $1.00** (isolated; not combined with prior balances)  
**Effort:** medium only · **Agent default:** OFF (dual auth required)  
**People Search / Findymail / Surfe / Airtable / STAGED pointer / CURRENT / publish:** not used  

**STOP** after this report. Integration Controller owns any merge. No rollout activation in this task.

---

## Verdict

Conditional Exa Agent escalation, run through the **canonical** CONTACT → ALT (9) → Agent → verify → rank → HOW path, recovered a verified commercially relevant named primary on **7/7** allowlisted opportunities at **$0.10** each.

**Recommendation: `SCALE_CAUTIOUSLY`**

WHO recovery is consistent and commercially useful (including two clear lodging/meetings DIRECT_BUYER hits). Public HOW remains thin (Sales Ready = 0). Do not enable Agent globally; expand only via allowlisted, dual-authorized Full E2E waves with separate HOW enrichment authorization.

---

## Phase 1 — Eligibility and allowlist

| Metric | Count |
|---|---:|
| Screened from Design Hotels V3 unresolved pool | 12 |
| Demand/fit eligible after live source inspection | 8 |
| Allowlisted for this paid wave | **7** |
| Excluded (fetch fail / ambiguous past / prior Agent bench) | 5 |

**Allowlist (order):**

| # | Opportunity ID | Hotel | Country | Organization | Demand |
|---|---|---|---|---|---|
| 1 | `disc_248756ebebe7` | Condesa DF | Mexico | Society for Phenomenology and Media | SPM27 association conference |
| 2 | `disc_541bab6495cf` | Memmo Príncipe Real | Portugal | SCRM / Proxima | SCRM-2027 stem-cell congress |
| 3 | `disc_586de760588b` | Condesa DF | Mexico | AMEG | Ecos Endoscópicos 2027 |
| 4 | `disc_ea2931224b86` | Condesa DF | Mexico | Mexico Business Events | Mexico Business Summit 2027 |
| 5 | `disc_9cd2c082c05d` | Memmo Príncipe Real | Portugal | Advantage Travel Partnership | Annual Conference 2027 |
| 6 | `disc_3ac4c29a1ce9` | Memmo Príncipe Real | Portugal | AFCONA Additives | European Sales Meeting 2026 |
| 7 | `disc_d78506e81cf4` | LaLuna | Grenada | IMA Grenada / Co-op Bank | IMRT 2026 round table |

**Excluded from spend:** GALA + BHC (already Agent-researched in benchmark v1); Grenada Sailing Week (HTTP 403); Film Festival (2026 ambiguous); Alpaca (not in final allowlist cut after eligibility ordering — eligibleCount was 8 before diversity/cap to 7).

Manifest: `ALLOWLIST.json`

---

## Phase 2 — Preflight

| Gate | Result |
|---|---|
| Tip = `b554a08…` / PR #587 | PASS |
| Mocked escalation cert `test:gdi-exa-agent-contact-escalation-v1` | **25/25 PASS** |
| Canonical ALT invokes Agent adapter | PASS |
| Full 9-strategy ladder (no maxStrategies truncation) | PASS (all runs `ladderExhausted=true`, strategies = 9 + Agent) |
| No qualifying named primary pre-Agent | PASS |
| No prior Agent persistence on these IDs | PASS |
| Dual auth + allowlist + medium + $1 / $0.15 ceilings | PASS |
| Agent default OFF without contact flag | PASS (`CONTACT_ESCALATION_FLAG_OFF`) |
| HOW / finalization standards retained | PASS |

Artifact: `PHASE2_PREFLIGHT.json`

---

## Phases 3–8 — Live canonical execution

**Invocation:** `processOpportunityThroughOriginalChain` with `skipDeepResearch`, saved-evidence ALT ladder, dual-authorized Agent escalation, public HOW only (`authorizePaidPublicContactSearch=false`).

**Not used:** standalone Agent benchmark script; person-name seeds; People Search; Findymail; Surfe.

### Provider ledger

| Opportunity | Run ID | Cost |
|---|---|---:|
| SPM27 | `agent_run_0f63f2addd304597aaa504b8952e83de` | $0.10 |
| SCRM-2027 | `agent_run_ed5cd0f8d4d64065bd9dc5feea375848` | $0.10 |
| AMEG Ecos | `agent_run_bd1f59d83a034be1a4f76b755052e5ad` | $0.10 |
| Mexico Business Summit | `agent_run_ff1a1800a1fc46348b44e0fe3de5eb01` | $0.10 |
| Advantage Travel | `agent_run_0f11022ad81f46e7adb0f8670c332951` | $0.10 |
| AFCONA Sales Meeting | `agent_run_326ff33fbed346d1b6bafcab8f2a93dd` | $0.10 |
| IMRT Grenada | `agent_run_36326b8e76b648b4b630beaa316474fc` | $0.10 |
| **Total** | **7 completed** | **$0.70** |

Ledger: `ledgers/agent-live-ledger.json` · Traces: `traces/*.json`

### Founder scorecard

| Opportunity | Hotel | Demand valid | Agent cost | Primary verified | Secondary verified | HOW found | Finalized eligible | Sales Ready |
|---|---|---|---:|---|---|---|---|---|
| SPM27 | Condesa DF | Y | 0.10 | Mariano E. Navarro Arroyo (VIABLE_ALT) | Luis Acebal | N | Y | N |
| SCRM-2027 | Memmo | Y | 0.10 | Sai Varma Namburi (**DIRECT_BUYER**) | Prakash Bandikinda | N | Y | N |
| AMEG Ecos 2027 | Condesa DF | Y | 0.10 | Mayra Membrillo Garduño (VIABLE_ALT) | Armando Hernández Cendejas | Org inbox `contacto@…` | Y | N |
| Mexico Business Summit | Condesa DF | Y | 0.10 | Jeroen Posma (VIABLE_ALT) | Alma González | N | Y | N |
| Advantage Travel 2027 | Memmo | Y | 0.10 | Pelin Dede (**DIRECT_BUYER** — Head of Supplier Partnerships, Lodging) | Sonia Michaels | N | Y | N |
| AFCONA Sales 2026 | Memmo | Y | 0.10 | Giovanni Conca (VIABLE_ALT) | — | LinkedIn `/in/` on stakeholder | Y | N |
| IMRT 2026 | LaLuna | Y | 0.10 | Larry Lawrence (VIABLE_ALT) | Willvorn Grainger | Person email `llawrence@grenadaco-opbank.com` | Y | N |

### Aggregate metrics

| Metric | Value |
|---|---:|
| Opportunities screened | 12 |
| Qualifying for live Agent | 7 |
| Actually researched | 7 |
| Verified relevant named people (primary) | **7** |
| DIRECT_BUYER primaries | **2** |
| VERIFIED_VIABLE_ALTERNATIVE primaries | **5** |
| Distinct verified secondary | **6** |
| HOW channel found (completeness) | 2 |
| Person-specific professional email | **1** |
| Org shared-inbox route (not person-specific) | 1 |
| Person LinkedIn `/in/` on primary stakeholder evidence | 3 |
| Newly finalization-eligible (named WHO coverage) | **7** |
| Sales Ready | **0** |
| Total Agent expenditure | **$0.70** |
| Cost per newly finalized (WHO) opportunity | **~$0.10** |
| False positives rejected at verification | **3** (AFCONA local partners — employer not verified) |
| Unresolved (no verified primary) | **0** |
| Research failures | **0** |

### Commercial relevance note

Organizational identity ≠ lodging buying responsibility. Ranking correctly:

- Promoted **Pelin Dede** (lodging supplier partnerships) and **Sai Varma Namburi** (conference operations manager) to DIRECT_BUYER.
- Kept association officers / MDs / admin directors as VIABLE_ALTERNATIVE where lodging decision evidence was weaker.
- Rejected three AFCONA venue-partner names for employer mismatch.

Missing direct email does **not** invalidate WHO_COMPLETE (independent gates preserved).

---

## Phase 9 — Local STAGED candidates

Versioned local packages under `staged-candidates/*.json` for all 7 opportunities.

| Control | Status |
|---|---|
| STAGED pointer flip | **Not performed** |
| CURRENT modified | **No** |
| Customer publication | **No** |
| Prior packages preserved | **Yes** |

---

## Phase 10 — Permanent Full E2E validation ($0)

| Check | Result |
|---|---|
| Same escalation module as live trial (`gdi-exa-agent-contact-escalation-v1`) | PASS |
| Reachable via `processOpportunityThroughOriginalChain` / ALT (new hotel, existing, cohort, portfolio, leisure, resume, replay entrypoints that already call ALT) | PASS — wired in PR #587; no trial-only path |
| Default Agent activation OFF | PASS |
| Resume/replay no duplicate paid | PASS (test 20) |
| High effort refused | PASS (test 23) |
| Mocked CI suite | **25/25** |
| Paid Search not repeated for these opps | PASS (`authorizePaidAlternativePersonResearch=false`; ladder used saved evidence) |

No new orchestration defects required a code fix in this wave. Residual HOW quality issue (org inbox labeled `DIRECT_WORK_EMAIL` for AMEG) is pre-existing verifier behavior — noted for a separate hardening task; not activated here.

---

## Phase 12 — Scale decision

### `SCALE_CAUTIOUSLY`

**Why not SCALE:** Sales Ready = 0; person-specific HOW coverage low under public-only rules; sample still Design Hotels–centric though market/org diversity is good.

**Why not HOLD/STOP:** 7/7 verified named recovery; 2 strong DIRECT_BUYER lodging/meetings hits; verification rejected wrong-employer noise; spend predictable at $0.10/opp; dual-auth/allowlist controls held.

### Proposed next wave (do **not** activate now)

| Control | Proposal |
|---|---|
| Budget | **USD $3.00** new Exa Agent only |
| Scope | ~25–30 unresolved allowlisted opps after $0 eligibility |
| Effort | medium · one run/opp |
| Path | Canonical Full E2E ALT escalation only |
| HOW | Separate founder auth (public + optional Surfe/Findymail) — not bundled into Agent cap |
| Activation | Dual auth + allowlist + ceilings · Agent remains OFF by default |
| Success bar | ≥70% WHO finalize-eligible · ≥20% DIRECT_BUYER or lodging-evidenced pathway · track Sales Ready after HOW wave |

---

## Governance confirmation

| Authorized | Done |
|---|---|
| ≤ $1.00 new Agent | $0.70 |
| Medium only | Yes |
| 5–8 qualifying existing opps | 7 |
| Scoped dual auth | Process-local `GDI_EXA_AGENT_LIVE=1` during apply only |
| Canonical WHO→ALT→Agent→HOW | Yes |
| Local STAGED candidates | Yes |
| $0 regression cert | 25/25 |

| Not authorized | Status |
|---|---|
| Global Agent on | Not enabled |
| Portfolio-wide Agent | Not run |
| Broad demand discovery | Skipped |
| People Search / Findymail / Surfe | Off |
| Other paid providers | Off |
| Airtable / STAGED flip / CURRENT / publish / deploy | Not done |
| Spend > $1.00 | Not done ($0.30 unused) |

---

## Artifacts

| Path | Contents |
|---|---|
| `ALLOWLIST.json` | Phase 1 eligibility + allowlist |
| `PHASE2_PREFLIGHT.json` | Preflight gates |
| `ledgers/agent-live-ledger.json` | Provider spend ledger |
| `traces/*.json` | Per-opp canonical stage traces |
| `staged-candidates/*.json` | Local STAGED candidate packages |
| `LIVE_RESULTS.json` / `SCORECARD_CORRECTED.json` | Scorecards |
| `APPLY_CONSOLE.log` | Apply console |

**Worktree:** `C:\Users\joand\AppData\Local\Temp\dealality-gdi-exa-agent-live-validation-v1` on tip `b554a08` · feature branch `feat/gdi-exa-agent-multi-opp-live-validation-v1` (report/scripts only; no production default changes).

---

## Final acceptance

Conditional Exa Agent escalation **does** consistently recover verified, commercially relevant named contacts across multiple genuine GDI opportunities when the permanent canonical Full E2E architecture is used with Agent **OFF** by default and dual authorization + allowlist + budget.

Success is WHO/commercial usefulness at ~$0.10/opportunity — **not** Sales Ready completeness. Scale the Agent WHO wave cautiously; fund HOW enrichment separately.
