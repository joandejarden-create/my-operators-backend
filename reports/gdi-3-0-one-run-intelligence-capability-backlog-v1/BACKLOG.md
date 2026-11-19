# GDI 3.0 — One-Run Complete Intelligence Capability Backlog V1

**Generated for founder correction:** Complete intelligence in every Full E2E run  
**Mode:** Documentation only · no production writes · no paid research  
**PR scope:** Ammends Slice 2A contract surface only — does **not** implement all pillars

## Legend

| Status | Meaning |
| --- | --- |
| `WIRED_PARTIAL` | Present in canonical path but incomplete / opt-in / lossy |
| `EXISTS_OFFLINE` | Module/tests exist; not integrated as required one-run default |
| `NOT_YET_WIRED` | Required by contract; not yet integrated into canonical Full E2E |
| `GATED` | Intentionally blocked until founder authorization |

## Capability matrix (production execution target)

| # | Dimension | Current status | Gap / next slice | Blocks durable completion claim? |
| ---: | --- | --- | --- | --- |
| 1 | Discovery across 14 segments | `WIRED_PARTIAL` | Slice 1 honesty/coverage; fair rotation + Option 2 weights still eval-only | Yes until coverage ledger is authoritative |
| 2 | Reuse existing Airtable / durable intel | `WIRED_PARTIAL` | FS reuse default; Airtable DG reads **off by default** — enable after identity/confidentiality validation (post-2B) | Yes while lookup skipped |
| 3 | Demand evidence + qualification | `WIRED_PARTIAL` | ORS/qualify exist; evidence often lost before persistence until 2A | 2A addresses persistence loss |
| 4 | Verified named contacts | `WIRED_PARTIAL` | Contact pass exists; 2A slim payload retains; typed columns still JSON-only | Yes if contacts available but not persisted |
| 5 | Partners / planners / intermediaries | `EXISTS_OFFLINE` | DG/PE tables exist; Full E2E does not consistently upsert — **Slice 2B** | Yes when relevant & available |
| 6 | Placements / recurring / competitive | `EXISTS_OFFLINE` | CMI/competitor fields + PE host prose; no dedicated competitor pipeline (correct); wire into same run | Yes when verified host history exists |
| 7 | Forward-looking sourcing windows | `WIRED_PARTIAL` | Timing/need-date modules exist; not always on persistence/Weekly Change path | Partial |
| 8 | Hotel fit + pursuit recommendations | `WIRED_PARTIAL` | V41 qualify / fit exist; often dropped on thin processed→bridge path — 2A carries forward | 2A |
| 9 | Commercial Intelligence Brief | `WIRED_PARTIAL` | Brief/presentation generated in-chain; drawer regeneration from Airtable not certified | Partial |
| 10 | Durable create/update (org/opp/contact/partner) | `WIRED_PARTIAL` | 2A honesty for research inventory; GDO/DG/PE dual-write + partner links = **2B**; production apply still founder-gated | Yes if persistence fails or not attempted |
| 11 | Weekly Change vs prior durable state | `EXISTS_OFFLINE` | `gdi-weekly-change-durability-v1` exists; **not required default on every Full E2E** yet — wire into canonical run | Yes |
| 12 | Customer publication-readiness | `GATED` | Assessment may run; publication remains separately authorized (correct) | Assessment required; publish remains gated |

## Prioritized implementation backlog (development phases)

1. **Slice 2A (this PR)** — Persistence honesty: full commercial slim payload; fail closed on field loss; no durable-completion claim without successful persistence when persistence is in scope.  
2. **Slice 2B** — Relationship wiring: DG / Program / Signal / Hotel Fit / Venue Fit + partner reuse in the **same** Full E2E run (not a weekly partner pipeline).  
3. **Slice 2C** — Safe Airtable reuse on by authorized live runs; Research Targets continuation scheduling.  
4. **Weekly Change on canonical path** — Invoke durability classifier every Full E2E; baseline on first run; forbid NEW/UPDATED without material change.  
5. **Lifecycle dimension ledger** — Populate `buildEmptyIntelligenceLifecycleLedger` from chain stages; surface attempted vs verified outcomes.  
6. **Option 2 selection policy** — After founder chooses arm from offline eval, enable bounded fairness+commercial **inside** the same run (not a separate weekly selector).  
7. **Publication readiness certification** — Assess every run; keep publish authorization separate.

## Explicit non-goals of this backlog commit

- Do not create weekly competitive / contact / partner pipelines.  
- Do not expand PR #444 into Slices 2B/2C or paid research.  
- Do not enable production Airtable writes or customer publication.

## Contract reference

`lib/group-demand-intelligence/workflow/gdi-full-e2e-complete-intelligence-lifecycle-contract-v1.js`  
`docs/data-intelligence/gdi-3-0-complete-intelligence-one-run-founder-correction-v1.md`
