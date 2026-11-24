# Exa Agent 30-Opportunity Live Validation — Founder Report

**Contract:** `gdi-exa-agent-30-opportunity-live-wave-v1`  
**Canonical tip:** `0956eeab8aa6d0ebab790b3535afca42e269d889` (PR #588 merged)  
**Agent integration tip lineage:** PR #587 + HOW hardening #588  
**New Exa Agent spend:** **USD $2.80 / $3.00** (isolated; no prior-balance carry-forward)  
**Effort:** medium only · **one run / opportunity** · **Agent default OFF**  
**People Search / Findymail / Surfe / Airtable / STAGED pointer / CURRENT / publish:** not used  

**STOP** after this report. Do not activate routine Full E2E Agent policy in this task. Integration Controller owns any follow-up merges.

---

## Verdict

Conditional Exa Agent, run through **canonical** CONTACT → ALT(9) → Agent → verify → HOW, recovered a gate-verified named primary on **~71–75%** of Agent-researched opportunities at **$0.10** each.

| Scale target | Result |
|---|---|
| **Target 1:** ≥70% verified commercially relevant named primary | **PASS (raw gate)** — **20/28 (71.4%)** Agent-researched with verified primary; **FAIL (quality-adjusted)** — **17/28 (60.7%)** after removing junk/UNVERIFIED (`Android Once`, `Member Select`, unverified persons) |
| **Target 2:** ≥20% DIRECT_BUYER or lodging-commercial pathway | **FAIL** — **3/28 (10.7%)** Agent DIRECT_BUYER; **4/29 (13.8%)** including Mundiconvenius saved-evidence |

**Recommendation: `SCALE_CAUTIOUSLY` — do not activate routine Full E2E Agent yet.**

Raw Target 1 clears, but quality-adjusted Target 1 and Target 2 both miss. Agent remains valuable as an **explicitly authorized** escalation only until junk-name gates harden and lodging-pathway precision improves.

---

## Phase 1 — Allowlist revalidation ($0)

Source plan: `reports/gdi-agent-recovery-hardening-v1/NEXT_WAVE_ALLOWLIST.json` (30).

| Metric | Count |
|---|---:|
| Screened | 30 |
| Paid-eligible after independent validation | **29** |
| Excluded | 1 (`UNNAMED_OR_INVALID_ORGANIZATION` — AJP title with date-as-org) |
| Weakened to fill 30? | **No** |

Diversity retained: Condesa DF, LaLuna, Memmo · Mexico / Grenada / Portugal.

Artifact: `ALLOWLIST.json` / `PHASE1_ELIGIBILITY.json`

---

## Phase 2 — Preflight

| Gate | Result |
|---|---|
| Tip = `0956eea…` / PR #588 | PASS |
| PR #587 Agent escalation in call graph | PASS |
| HOW hardening (`contacto@`, person `/in/`) | PASS |
| Mocked `test:gdi-exa-agent-contact-escalation-v1` | **25/25** |
| Mocked `test:gdi-agent-recovery-hardening-v1` | **8/8** |
| Dual auth + allowlist + medium + $3 / $0.15 | PASS |
| Truncation ≠ exhaustion without auth | PASS (full ladder) |
| Agent OFF without contact flag | PASS |

Artifact: `PHASE2_PREFLIGHT.json`

---

## Phases 3–6 — Live execution

**Path:** `processOpportunityThroughOriginalChain` → ALT → conditional Agent (dual auth, process-local `GDI_EXA_AGENT_LIVE=1`) → verify → public HOW.

| Control | Observed |
|---|---|
| Agent calls completed | **28** |
| Resolved without Agent (saved-evidence WHO) | **1** (SUSHP / Mundiconvenius — Mafalda Cassiano Neves) |
| Not researched / budget-blocked | **0** of allowlist |
| Aggregate spend | **$2.80** |
| Per-opp medium cost | **$0.10** (observed) |
| Ceiling breaches | None |
| Automatic retries | None |
| Seeded buyer names | None |

Ledger: `ledgers/agent-live-ledger.json` · Traces: `traces/*.json`

---

## Founder scorecard (aggregate)

| Metric | Value |
|---|---:|
| Opportunities screened | 30 |
| Paid-eligible | 29 |
| Agent-researched | 28 |
| Verified primary (Agent-researched) | **20** (**71.4%**) |
| Verified primary (any path) | 21 |
| DIRECT_BUYER (Agent) | **3** (**10.7%**) |
| DIRECT_BUYER (incl. saved-evidence) | 4 |
| VERIFIED_VIABLE_ALTERNATIVE | 14 |
| Distinct secondary | 19 |
| Person-specific email | 3 |
| Organization email route | 6 |
| Person LinkedIn `/in/` | 3 |
| Phone coverage | 2 |
| Finalization-eligible (gate) | 24 |
| Sales Ready | **0** |
| Wrong-employer / junk rejections (Agent) | **31** |
| Total Agent spend | **$2.80** |
| Avg Agent cost / researched opp | **$0.10** |
| Cost / newly finalizable (gate) | ~$0.12 |
| Remaining unresolved (no verified primary) | 8 Agent misses + quality holds |

### Strong DIRECT_BUYER / lodging pathway examples

| Person | Opportunity | Why commercial |
|---|---|---|
| Cecilia Gochi Salazar | IBTM Americas 2026 | Official travel-agency **accommodation** contact |
| Ricardo Plancarte | WIP Congress 2026 | Local Organizing Committee chair |
| Jason Gooding | Grenada Open Water Swim | Race Director / Events Manager (viable lodging pathway) |

### Quality holds (do not treat as success)

| Issue | Example | Note |
|---|---|---|
| Junk / chrome name | `Android Once` (Wobi Mexico) | Must not satisfy commercial WHO |
| Nav chrome | `Member Select` (CWWA) | Rejected for finalization |
| Weak DIRECT_BUYER | `Director General` / bare `Manager` without lodging evidence | Seniority ≠ hotel sourcing |
| UNVERIFIED_PERSON still finalize-eligible in coverage | Keyla Velazquez (RAI) | Gate hygiene follow-up |

HOW / Sales Ready reported **separately** — Sales Ready remains **0**; person email ≠ Sales Ready auto-pass.

---

## Per-opportunity scorecard (compact)

Full rows: `SCORECARD.json` / `LIVE_RESULTS.json`.

| Opportunity | Hotel | Agent $ | Primary | Type | Secondary | Public HOW | Finalize | Sales Ready |
|---|---|---:|---|---|---|---|---|---|
| WAPOR 2026 | Condesa | 0.10 | Kseniya Kizilova | VIABLE_ALT | Y | — | Y | N |
| Nestlé Convención 2026 | Condesa | 0.10 | — | — | — | — | N | N |
| Akamai Partner Summit 2026 | Condesa | 0.10 | — | — | — | — | N | N |
| IBTM Americas 2026 | Condesa | 0.10 | Cecilia Gochi Salazar | **DIRECT_BUYER** | Y | — | Y | N |
| WIP Congress 2026 | Condesa | 0.10 | Ricardo Plancarte | **DIRECT_BUYER** | Y | — | Y | N |
| RAI 2026 | Condesa | 0.10 | Keyla Velazquez | UNVERIFIED | Y | person email | Y* | N |
| DIA Latin America 2026 | Condesa | 0.10 | Jessica Culp | VIABLE_ALT | — | org email + LI | Y | N |
| Ortopedia Residentes 2027 | Condesa | 0.10 | — | — | Y | org email | Y* | N |
| INA PAACE 2027 | Condesa | 0.10 | Rafael Fernández | VIABLE_ALT | Y | — | Y | N |
| Comercio Exterior 2027 | Condesa | 0.10 | — | — | — | — | N | N |
| Wobi Mexico 2027 | Condesa | 0.10 | Android Once | UNVERIFIED/junk | — | org email | Y* | N |
| Corp Secretary Training | LaLuna | 0.10 | — | — | Y | — | Y* | N |
| OECS Procurement Fair 2026 | LaLuna | 0.10 | Naomi Akoy-Bouguenon | VIABLE_ALT | Y | — | Y | N |
| Compliance Risk Workshop | LaLuna | 0.10 | — | — | Y | — | Y* | N |
| Grenada Chocolate Festival | LaLuna | 0.10 | Renee Goodwin | VIABLE_ALT | Y | — | Y | N |
| Open Water Swim 2026 | LaLuna | 0.10 | Jason Gooding | VIABLE_ALT | — | — | Y | N |
| CWWA Conference | LaLuna | 0.10 | Member Select | junk | — | org email | N | N |
| Dance Grenada 2026 | LaLuna | 0.10 | Dr. Shola K. Roberts | VIABLE_ALT | Y | org email | Y | N |
| Pure Grenada Sailing Week | LaLuna | 0.10 | Lucas Sales | VIABLE_ALT | Y | — | Y | N |
| Masters T20 Cricket 2026 | LaLuna | 0.10 | Kirl Grant-Hoschtialek | VIABLE_ALT | Y | — | Y | N |
| CARPHA 2027 | LaLuna | 0.10 | Lisa Indar | VIABLE_ALT | Y | — | Y | N |
| Public Sector Directors | LaLuna | 0.10 | — | — | Y | — | Y* | N |
| GTA Marketplace 2026 | LaLuna | 0.10 | Meryem Bennouna | VIABLE_ALT | — | person email | Y | N |
| Workvivo Summit 2026 | Memmo | 0.10 | Laura Lozano Ortega | VIABLE_ALT | Y | LI `/in/` | Y | N |
| EUnetCCC 2026 | Memmo | 0.10 | Thomas Dubois | VIABLE_ALT | Y | — | Y | N |
| Europatat 2027 | Memmo | 0.10 | Berta Redondo Benito | DIRECT_BUYER† | — | org email | Y | N |
| FCI Annual Meeting 2026 | Memmo | 0.10 | — | — | — | — | N | N |
| EAIE 2026 | Memmo | 0.10 | Liza Ortegón | VIABLE_ALT | Y | LI `/in/` | Y | N |
| SUSHP 2027 | Memmo | **0.00** | Mafalda Cassiano Neves | DIRECT_BUYER† (saved evidence; Agent not called) | Y | person email | Y | N |

\* Finalize via coverage/secondary quirks — treat quality-adjusted WHO as stricter than raw gate.  
† Seniority-heavy DIRECT_BUYER — lodging pathway not independently strong.

---

## Phase 7 — Local STAGED candidates

| Item | Status |
|---|---|
| Packages written | **21** under `staged-candidates/` |
| Pointer flip | **Not performed** |
| CURRENT / publish | **No** |
| Historical packages | Preserved |

---

## Phase 8 — Permanent certification ($0)

| Suite | Result |
|---|---|
| `test:gdi-agent-recovery-hardening-v1` | **8/8 PASS** |
| `test:gdi-public-source-contact-discovery-permanent-v1` | **15/15 PASS** |
| `test:gdi-exa-agent-contact-escalation-v1` | **25/25 PASS** |
| `dealality:ci-precheck` | **PASS** |
| Agent default OFF | Confirmed |
| Dual auth / ceilings / resume skip | Confirmed in prior cert + live ledger |

No merge opened from this wave. Optional follow-up PR (IC): junk-name / chrome rejection (`Android Once`, `Member Select`) before any routine activation.

---

## Phase 10 — Routine Full E2E recommendation

### Proposed policy (NOT activated)

When a Full E2E run carries **explicit paid research authorization** and budget:

1. Run CONTACT → WHO → full 9-strategy ALT as today.  
2. If ladder **genuinely exhausted** and no qualifying named primary → conditional Agent (medium) may auto-fire **within the same authorized budget**.  
3. Hard controls that must ship first:
   - Dual auth still required (`authorizeExaAgentContactEscalation` + live env) **or** a single Full E2E paid envelope that sets both process-locally  
   - Portfolio / hotel / opportunity ceilings  
   - Allowlist **or** segment-safe eligibility (no blind portfolio-wide Agent)  
   - Resume/replay idempotency (already certified)  
   - **Junk-name / chrome rejection** before WHO finalize  
   - Lodging-evidence bar for DIRECT_BUYER (already partially present; enforce on live ranking)  
   - HOW hardening retained (`contacto@`, `/in/` only)  
   - No paid auth → Agent **OFF**  
   - Publication / STAGED pointer / CURRENT remain separately gated  

### Activation criteria (suggested)

- Target 1 ≥70% on **quality-adjusted** verified primaries (junk excluded)  
- Target 2 ≥20% true lodging/meetings pathway  
- Sales Ready tracked separately (not a bar for Agent WHO value)  
- Founder-approved Full E2E budget envelope  

**This wave alone does not authorize routine activation.**

---

## Governance confirmation

| Constraint | Status |
|---|---|
| ≤ $3.00 new Agent | **$2.80** |
| Medium only / one run / opp | Yes |
| ≤30 verified eligible | 29 eligible; 28 Agent calls |
| Canonical Full E2E only | Yes |
| Local STAGED only | Yes |
| No Exa Search / People / Findymail / Surfe | Yes |
| No Airtable / pointer / CURRENT / publish / deploy | Yes |
| No spend carry-forward | Yes |
| Dirty worktrees preserved | Yes (`ws-gdi` untouched) |

---

## Artifacts

| Path | Contents |
|---|---|
| `FOUNDER_REPORT.md` | This report |
| `ALLOWLIST.json` | Final paid-eligible 29 |
| `PHASE2_PREFLIGHT.json` | Preflight |
| `ledgers/agent-live-ledger.json` | Run IDs + costs |
| `traces/*.json` | Canonical stage traces |
| `staged-candidates/*.json` | Local STAGED packages |
| `LIVE_RESULTS.json` / `SCORECARD.json` | Scorecards |
| `APPLY_CONSOLE.log` | Apply console |

**Worktree:** `C:\Users\joand\AppData\Local\Temp\dealality-gdi-exa-agent-live-validation-v1` · branch `feat/gdi-exa-agent-30-opp-live-wave-v1` @ `0956eea`
