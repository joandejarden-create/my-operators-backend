# GDI 3.0 — Dual-track coordinated roadmap V1

**Status:** Founder-confirmed parallel tracks (not sequential projects)  
**Product end state:** One reliable GDI research engine feeding hotel-specific sales intelligence **and** market-level portfolio intelligence, with independent hotel qualification and durable reuse across customers.

| Track | Name | Focus |
|---|---|---|
| **A** | Research & Commercial Intelligence | Certified Full E2E research → persistence → partners → hotel sales brief / weekly change |
| **B** | Market & Portfolio Intelligence | Market opportunity identity → operator membership → multi-hotel fit → portfolio UI / market orchestration |

## Shared architecture (mandatory — do not duplicate)

Both tracks **reuse** these systems; neither invents a parallel stack:

| Shared asset | Role |
|---|---|
| Canonical market opportunity identity | One public market research object → many hotel assessments |
| Airtable research registry + persistence | Existing GDI Hotel Research Opportunities / ORS / bridge |
| Market Opportunity Graph (MOG) | Packet extract, geography, market-linked hotel opportunity ids |
| `evaluateCrossHotelFit` | Hotel-specific qualification engine |
| Verified Hotel Explorer / census attributes | Rooms, meetings, territory — never invent positives from UNKNOWN |
| Opportunity drawer + publication controls | Hotel-scoped publish authorization remains authoritative |

**Layer separation (always):**

1. Shared **public** market intelligence packet (no confidential operator/hotel fields)  
2. Operator ↔ market ↔ hotel **membership** (explicit provenance; never notes-only)  
3. Hotel-specific **fit / pursuit** evaluation  
4. Hotel-specific **publication** authorization  

## Parallel delivery rules

1. One coordinated roadmap (this doc); two track backlogs.  
2. Progress analysis → specs → fixtures → implementation readiness on **both** tracks concurrently.  
3. Work only in registered `C:\Dev\dealality\ws-gdi` (`ws/gdi`).  
4. **One active GDI implementation PR at a time** unless Integration Controller authorizes another registered worktree.  
5. Focused PRs; Integration Controller owns `main` merges.  
6. Post-merge home-sync GDI after every merge.  
7. Do not interrupt / overwrite completed Slice 2A honesty contracts.  
8. Coordinate changes to shared identity, persistence, or hotel-fit contracts **before** either track ships them.

## Safety gates

- No paid providers, production Airtable writes, deployment, or customer publication without founder authorization.  
- Do **not** wait for all 14 segment certifications before initial **read-only** portfolio UI (PDI-3).  
- Do **not** enable market-level paid research or multi-hotel production writes until shared persistence + authorization contracts are tested.

---

## Track A — Research & Commercial Intelligence

| Slice | Capability | State |
|---|---|---|
| Phase 0A / Slice 1 | Baseline drawer audit; research priority + depth amendment | Shipped (#438 / #440 lineage) |
| Slice 2 audit | Airtable persistence / partners / reuse audit | Shipped (#441) |
| **Slice 2A** | Persistence honesty + slim payload + one-run 12-dimension lifecycle | **MERGED** [#444](https://github.com/joandejarden-create/my-operators-backend/pull/444) |
| **Slice 2B** | DG / Partner relationship wiring (existing tables; dry-run first) | **MERGED** [#448](https://github.com/joandejarden-create/my-operators-backend/pull/448) |
| **Brief v2** | Commercial Intelligence Brief narrative quality (PDAC/NICE gold; NRC/IMC/OWWA weak) | **Plan prepared** — implement after #450 merges |
| Slice 2C | Read-only Airtable DG reuse hooks + continuation / Weekly Change prep | After 2B |
| Brief v2 | Commercial Intelligence Brief v2 drawer quality | Parallel readiness after 2B evidence |
| 14-segment Full E2E | Certified repeatable process + progressive depth | Ongoing; not a PDI UI blocker |
| Weekly Change (hotel) | Commercial feedback loop | After persistence + brief quality |

### Next focused PR (Track A)

**Commercial Intelligence Brief v2** (after PDI-2 merges)  
- Improve generated narrative/commercial reasoning (not field-presence only)  
- Gold: PDAC / NICE · Weak: NRC RIC, IMC Construction, OWWA  
- Reuse existing drawer generator + presentation controller  
- Before/after briefs with evidence-based quality scoring  
- No second drawer engine; PDI-3 must consume the same contract  

---

## Track B — Market & Portfolio Intelligence

| Slice | Capability | State |
|---|---|---|
| Assessment | Portfolio Demand Intelligence product assessment | **MERGED** [#445](https://github.com/joandejarden-create/my-operators-backend/pull/445) |
| **PDI-1** | Operator/market/hotel membership + fit classes + Aimbridge GDL fixtures | **MERGED** [#446](https://github.com/joandejarden-create/my-operators-backend/pull/446) |
| **PDI-2** | Read-only portfolio API + operator authorization | **IN FLIGHT** |
| **PDI-3** | GDI tab left of Commercial Priorities + fit matrix; **reuse Brief v2 + existing drawer** | After PDI-2 + Brief v2 |
| PDI-4 | Market-level Full E2E orchestration + same-run multi-hotel qualification | After shared persistence + auth tested |
| PDI-5 | Portfolio Weekly Change + hotel-isolated feedback | After PDI-3/4 foundations |

### Tab placement (binding)

`Group & Demand Opportunities` · **`Portfolio Demand Intelligence`** · `Commercial Priorities`

### Next focused PR (Track B)

**PDI-2** read-only portfolio API + authorization (this cycle). After founder review + Brief v2: **PDI-3** tab.

---

## Shared dependency matrix

| Dependency | Owner slice | Blocks |
|---|---|---|
| Slim persistence / ORS→bridge honesty | Slice 2A ✅ | Track A 2B; PDI-4 reuse of research rows |
| Canonical market opportunity id + MOG packets | Existing MOG + PDI-1 | Both tracks’ multi-hotel views |
| Portfolio membership provenance | PDI-1 | PDI-2/3 authorization |
| Cross-hotel fit → portfolio fit classes | PDI-1 | PDI-2/3 matrix |
| Operator auth allowlist | PDI-2 | PDI-3 customer UI |
| Publication controls (hotel-scoped) | Existing drawer | Both tracks — never bypass |
| Market paid Full E2E | Founder-gated | PDI-4 only after auth + persistence tested |

Coordinate before changing: market id computation, slim payload allowlist, `evaluateCrossHotelFit` semantics, membership provenance kinds, confidential strip keys.

---

## Immediate execution order (founder)

| Step | Item | Status |
|---|---|---|
| A | Finish PR #444 + integration lifecycle | **DONE** |
| B | Portfolio assessment docs PR | **DONE** (#445) |
| C | PDI-1 identity + fit fixtures | **DONE** (#446) |
| D | Track A Slice 2B relationship wiring | **DONE** (#448) |
| E | PDI-2 read-only portfolio API | **IN FLIGHT** |
| F | Track A Brief v2 → then PDI-3 (shared drawer) | **QUEUED** |

## Milestone report template

At each merge / milestone, report:

1. Track A progress  
2. Track B progress  
3. Shared dependencies  
4. Current PR and merge state  
5. Research quality improvements  
6. Portfolio capability improvements  
7. Remaining customer-delivery blockers  
8. Next focused PR for **each** track  

## Coordinated follow-on — Brief v2 + PDI-3

Sequence after PDI-2:

1. **Commercial Intelligence Brief v2** (Track A) — narrative quality on audited samples.  
2. **PDI-3 portfolio tab** (Track B) — reuses Brief v2 + existing hotel opportunity detail component.

- Do **not** create a second drawer renderer for portfolio.
- Tab placement: Opportunities · **Portfolio Demand Intelligence** · Commercial Priorities.
- Remaining deps: Airtable membership write/read-back certification; market-level Full E2E (PDI-4) founder-gated.

## Related docs

- Assessment: `docs/data-intelligence/gdi-3-0-portfolio-demand-intelligence-assessment-v1.md`  
- Slice 2A: `docs/data-intelligence/gdi-3-0-slice2a-persistence-honesty-v1.md`  
- Slice 2B: `docs/data-intelligence/gdi-3-0-slice2b-relationship-wiring-contract-v1.md`  
- One-run correction: `docs/data-intelligence/gdi-3-0-complete-intelligence-one-run-founder-correction-v1.md`  
- Slice 2 audit: `docs/data-intelligence/gdi-3-0-slice2-airtable-persistence-partners-reuse-audit-v1.md`  
