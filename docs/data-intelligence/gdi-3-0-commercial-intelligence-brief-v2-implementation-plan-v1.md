# GDI 3.0 — Commercial Intelligence Brief v2 implementation plan

**Track:** A — Research & Commercial Intelligence  
**Status:** Implementation PR (after PDI-2 #450 merged)  
**Shared contract:** Same brief must serve individual hotel GDI page **and** future Portfolio Demand Intelligence tab (PDI-3). Do **not** create a second drawer engine.

## Objective

Improve the **actual generated narrative and commercial reasoning**, not field-presence validation alone.

## Frozen quality fixtures (to lock when Brief v2 PR opens)

| Class | Sample | Role |
|---|---|---|
| Gold | PDAC | Narrative + demand mechanism + pursuit quality bar |
| Gold | NICE | Evidence-backed hotel relevance + contacts bar |
| Weak | NRC RIC | Boilerplate / truncated / speculative failure modes |
| Weak | IMC Construction | Weak buyer authority / generic relevance |
| Weak | OWWA | Timing / competitive-history / gap treatment failures |

Exact fixture paths and hashes to be frozen in the Brief v2 PR from audited PDAC/NICE / NRC RIC / IMC / OWWA packages already in the GDI research corpus.

## Required brief qualities

1. Clear demand mechanism and opportunity context  
2. Evidence-backed hotel relevance  
3. Realistic commercial timing  
4. Verified contacts and authority basis  
5. Competitive history only when evidenced  
6. Specific hotel pursuit recommendations  
7. Explicit unknowns and research gaps  
8. No speculative demand as confirmed  
9. No generic boilerplate, truncated prose, or internal methodology leakage  
10. Appropriate treatment of rejected, research-continuation, and previously booked opportunities  

## Implementation approach (next Track A PR after #450)

1. Reuse existing drawer generator + presentation controller.  
2. Add evidence-based quality scoring with before/after briefs on frozen fixtures.  
3. Gate Brief v2 output on scoring thresholds for gold samples; document weak-sample deltas.  
4. Export one presentation contract consumed by hotel GDI + PDI-3.  

## Non-goals for preparation

- No second implementation PR while #450 is open  
- No paid research, production writes, publication, or deployment  
