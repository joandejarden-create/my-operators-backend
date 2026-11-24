# Next live Exa Agent contact escalation — controlled plan (NOT EXECUTED)

**Status:** Prepared only. **No live paid calls under this authorization.**  
**Prerequisite:** Merge of `feat/gdi-exa-agent-contact-escalation-v1` + separate founder GO.

## Candidate pool criteria

Unresolved opportunities where:

1. Demand evidence verified and commercially relevant  
2. Hotel fit credible (primary or overflow/partner lodging)  
3. Demand-generating organization identified  
4. Existing CONTACT + 9-strategy ALT ladder failed to verify a named person  
5. **No named buyer pre-seeded** (including Ferch/Bucheli-class names)

### Seed shortlist (from prior unseeded / hardening inventories — validate again before spend)

| Priority | Opportunity | Hotel | Why |
|---:|---|---|---|
| 1 | Additional Design Hotels / association events with ladder-exhausted WHO | TBD from STAGED gaps | Highest commercial relevance |
| 2 | Post-hardening unseeded pool members still without verified primary | see `reports/gdi-contact-pilot-hardening-v1/UNSEEDED_OPPORTUNITIES.json` | Known ladder miss pattern |
| Exclude | GALA / BHC benchmark pair | Memmo / Condesa | Already Agent-researched in benchmark — use only for $0 replay/regression, not re-spend |

Final allowlist size: **3–5** opportunities max for first live wave.

## Budget proposal (separate founder GO)

| Control | Value |
|---|---|
| Aggregate ceiling | **USD $1.00** (new; no carry-forward) |
| Per-opportunity | 1× medium = **$0.10** |
| Optional second medium | Only if first returns zero plausible people + new hypothesis |
| Refuse | high / auto / ultra; Findymail; Surfe; People Search scale |
| Expected spend | **$0.30–$0.50** |

## Acceptance criteria

- ≥1 opportunity gains a **strict-verified** named primary with org-domain or strong provenance  
- Agent candidates still pass existing verification + commercial ranking  
- HOW runs independently; missing email does not erase WHO  
- Zero automatic Full E2E activation outside allowlist + dual auth  
- Ledger entries reconcile to execution summary  

## Authorization shape (when GO arrives)

```
authorizeExaAgentContactEscalation: true
authorizeExaAgentLive: true
GDI_EXA_AGENT_LIVE=1
exaAgentContactOpportunityAllowlist: [ … ]
exaAgentContactAggregateCeilingUsd: 1.0
exaAgentContactEffort: "medium"
```

**STOP — do not execute in this phase.**
