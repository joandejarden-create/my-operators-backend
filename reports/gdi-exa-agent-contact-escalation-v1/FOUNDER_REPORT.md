# Integrate Exa Agent into Canonical GDI Contact Intelligence — Founder Report

**Phase:** $0 implementation + certification only  
**Branch:** `feat/gdi-exa-agent-contact-escalation-v1`  
**Base tip:** `80058da285532d27e7af95546f6196e69fcb75d4`  
**Benchmark reference:** `reports/gdi-exa-people-vs-agent-contact-benchmark-v1/FOUNDER_REPORT.md`  
**New paid Exa calls:** **0**  
**Agent default:** **OFF**

STOP for founder review before merge or live activation. Integration Controller owns merge.

---

## 1. Code and provider adapters reused

| Asset | Role |
|---|---|
| `exa-agent-provider-adapter-v1.js` | Unchanged create/poll/dual-auth/medium quote |
| `exa-agent-contact-intelligence-contract-v1.js` | **New** contact schema + query builder + candidate map |
| `gdi-exa-agent-contact-escalation-v1.js` | **New** eligibility, verify/classify, run wrapper |
| `gdi-buyer-alternative-person-research-v1.js` | Post-ladder conditional hook |
| `original-full-e2e-research-chain-v1.js` | Pass-through of escalation opts (default false) |
| Existing WHO/HOW/ranking modules | Unchanged finalization path |

**Not built:** parallel contact engine, People Search Full E2E enablement, demand-discovery Agent changes.

---

## 2. Canonical Full E2E integration

Sequence (unchanged stages + new optional step inside ALT):

```
CONTACT_RESEARCH
→ BUYER_ALTERNATIVE_PERSON_RESEARCH
   → saved-evidence ranking
   → 9-strategy ladder
   → CONDITIONAL EXA AGENT ESCALATION   ← new, default OFF
→ existing person verification / commercial ranking / primary-secondary
→ HOW_CONTACT_CHANNEL_DISCOVERY
→ FINALIZATION GATES
```

Wired through `processOpportunityThroughOriginalChain` for all modes that already call ALT (new/existing hotel, portfolio/cohort, leisure, resume/replay). No hotel-specific or Design Hotels branch. No Agent call unless opts authorize it.

---

## 3. Escalation decision logic

Eligible only when **all** hold:

1. `authorizeExaAgentContactEscalation === true`  
2. Live dual-auth (`authorizeExaAgentLive` + `GDI_EXA_AGENT_LIVE=1`) **or** explicit mock-only test path  
3. Demand verified  
4. Organization identified  
5. Ladder exhausted (full sequence **or** maxStrategies-capped miss)  
6. No commercially relevant verified named person yet  
7. Opportunity ID on allowlist  
8. Aggregate/remaining budget covers medium ($0.10)  
9. Effort = **medium** only  
10. Not already escalated (resume/replay skip)

**Hard refuses:** missing-email-only when WHO complete; secondary-only without separate auth; high/auto/ultra; not allowlisted; budget missing/insufficient.

---

## 4. Authorization and spending controls

| Control | Behavior |
|---|---|
| Contact escalation flag | Default **false** |
| `GDI_EXA_AGENT_LIVE` alone | **Insufficient** |
| Effort | medium only (~$0.10) |
| Allowlist | Required non-empty |
| Ceilings | Aggregate + per-opp + remaining |
| Accounting | Attempt/cost/reason stamped on opportunity + ALT trace |
| Experiment balances | Not inherited |

---

## 5. Contact verification behavior

Agent outputs → candidates only → `verifyAndClassifyAgentCandidates`:

- Plausible person name (junk gate includes `Member Organizations`)  
- Employer affiliation  
- Non-homepage source URL  
- Classification: DIRECT_BUYER / VIABLE_ALTERNATIVE / RELEVANT_ORGANIZATIONAL_CONTACT / REJECTED  
- Seniority alone → **VIABLE_ALTERNATIVE**, not direct buyer  

Then existing `selectBestCommercialContacts` / projection.

---

## 6. Primary/secondary ranking

Unchanged commercial selection + `enforceDistinctPrimarySecondaryContacts`. Agent people merge into `namedStakeholders` before re-harvest/rank.

---

## 7. WHO / HOW finalization

Agent does not set HOW or Sales Ready. HOW remains `runRestoredHowContactChannelDiscovery`. Missing email does not erase WHO. Classifications independent.

---

## 8. Regression results ($0)

| Suite | Result |
|---|---|
| `test:gdi-exa-agent-contact-escalation-v1` | **23/23 PASS** |
| `test:gdi-buyer-alternative-person-research-v1` | **19/19 PASS** |
| `test:gdi-full-e2e-contact-architecture-call-graph-v1` | **5/5 PASS** |

Covers required checks 1–22 (+ high-effort refuse). Artifact: `CERTIFICATION.json`.

---

## 9. CI integration

- `package.json` → `test:gdi-exa-agent-contact-escalation-v1`  
- `.github/workflows/gdi-regression-recovery-baseline.yml` → runs with ALT suite  

---

## 10. PR status

See PR URL after open. **GDI does not merge** — Integration Controller.

---

## 11. Agent remains OFF by default

Confirmed: no opts → no eligibility → no provider call. Check 22 proves prior Full E2E behavior with flags false. Check 21: demand-discovery adapter path untouched.

---

## 12. Next live validation plan

Prepared in `NEXT_LIVE_VALIDATION_PLAN.md` — **not executed**. Propose separate ≤$1.00 medium-only allowlisted wave after merge + founder GO.

---

## Benchmark accounting reconciliation

See `BENCHMARK_ACCOUNTING_RECONCILIATION.json`.

| Source | Spend | Calls |
|---|---:|---:|
| live-ledger.json | **$0.248** | 14 (12 people_search + 2 agent_run) |
| EXECUTION_RESULTS totals | **$0.248** | 14 |
| SCORECARD_STRICT | **$0.248** | B $0.048 + C $0.200 |

Ledger matches execution. Documented only: narrative “16 lite calls” vs ledger **12** people_search — ledger is SoT; historical rows not rewritten.

---

## Governance

| Item | Status |
|---|---|
| Paid Exa this phase | **0** |
| Auto-activation | **OFF** |
| Findymail / Surfe | unused |
| Airtable / STAGED / CURRENT / publish | untouched |
| Dirty `ws-gdi` preserved | yes (implementation on tip worktree) |

**FINAL ACCEPTANCE (implementation):** Exa Agent is a permanently available, authorization-controlled fallback inside canonical WHO→ALT→HOW. Verification, ranking, budget, and finalization standards are not weakened.

**STOP — AWAITING_INTEGRATION_CONTROLLER**
