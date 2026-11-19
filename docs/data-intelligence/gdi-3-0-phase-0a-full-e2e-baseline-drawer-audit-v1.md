# GDI 3.0 — Phase 0A: Full E2E Certification Baseline + Opportunity Drawer Quality Audit

**Status:** Founder review gate (read-only / $0)  
**Date:** 2026-11-19  
**Workspace:** `C:\Dev\dealality\ws-gdi` · Stream `gdi` · Home `ws/gdi`  
**Baseline HEAD at audit branch tip:** current `origin/main` at PR open  
**Explicit non-goals:** No Phase 0B/1B build · no paid providers · no Airtable writes · no publication · no adaptive AI-agent experiment resume · no second research platform

---

## Phase A — Workspace verification

| Check | Result |
| --- | --- |
| Registered root | `C:/Dev/dealality/ws-gdi` |
| Stream ownership | `gdi` (preflight OK) |
| Home branch | `ws/gdi` (returned after merge) |
| Unresolved merge/rebase | None |
| Runtime evidence | Preserved under `data/group-demand-intelligence/` (ledgers, ORS, opportunities) — not discarded |
| Sync note | Product home may lag briefly after non-GDI merges; start task branch from `origin/main` tip |

**Verdict:** Safe to audit from GDI permanent workspace. Isolation respected (no ADP/HE checkout).

---

## Deliverable 1 — Fourteen-segment Full E2E readiness matrix

Canonical IDs from `listFourteenWorkflowSegmentIds()` / `FOURTEEN_SEGMENT_EXECUTOR_SPECS`  
(`lib/group-demand-intelligence/workflow/full-e2e-fourteen-segment-discovery-executor-registry-v1.js`).

**Status vocabulary (this audit):**

| Status | Meaning |
| --- | --- |
| `IMPLEMENTED_AND_VALIDATED` | Code path + offline wiring + at least one demonstrated live/paid cell with durable evidence; not claiming customer-delivery PASS |
| `IMPLEMENTED_NOT_E2E_VALIDATED` | Full executor/query/routing/contact blocks present; live Full E2E not certified end-to-end for this segment |
| `PARTIAL` | Shared executor present but strategy thin, wrong Exa family reuse, or fixture/replay-shaped without proven live yield |
| `BLOCKED` | Hard gate (provider auth, missing fixture, fail-closed) prevents validation |
| `MISSING` | No executable path |

| # | Segment ID | Discovery / strategy | Live vs replay | Exa / research | Dedupe | Evidence | Thesis | Contacts | Hotel qual | ORS / eligibility | Drawer | Persist | Publish auth | Weekly / rerun | Recovery / spend | **Readiness** |
| ---: | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | `associations_conferences_events` | Assoc strategies + specialized runner | Bethesda live Exa executed; high REJECT | ASSOCIATION_EVENTS family | Pre-deep + org keys | URL claims | Offline CHD | Segment prompt | Shared spine | Shared ORS | Assoc builder | Bridge/ORS local | Separate publish | Module exists; not in canonical | Stage budgets | **IMPLEMENTED_NOT_E2E_VALIDATED** |
| 2 | `corporate_accounts_scaling` | Scaling strategies + specialized runner | Best live NEW_UNIQUE (Amentum) | SCALING_COMPANIES | Stronger org reuse | Stronger | Stronger | Named recoveries (6-opp) | Fit often weak | Shared | Scaling / CIB gold | Bridge/ORS | Separate | Same | Stage budgets fixed | **IMPLEMENTED_AND_VALIDATED** *(discovery+contacts offline+bounded live; not full customer E2E)* |
| 3 | `corporate_meetings_incentives` | Destination corporate queries | Live executed; national EXCLUDE | Routes ASSOCIATION_EVENTS | Shared | Thin | Shared | Prompt only | Shared | Shared | Reuses scaling sections 2–10 | Bridge/ORS | Separate | Same | Cap bites | **PARTIAL** |
| 4 | `government_public_sector` | Gov queries + manuals | Live NEW_UNIQUE (APSA) | ASSOCIATION_EVENTS | Shared | Mixed | Shared | Prompt | Shared | Shared | Scaling-shaped drawer | Bridge/ORS | Separate | Same | Cap | **IMPLEMENTED_NOT_E2E_VALIDATED** |
| 5 | `sports_entertainment` | Base-of-demand sports | Live REJECT heavy | ASSOCIATION_EVENTS; Apify preferred but mock/off | Shared | Thin | Shared | Prompt | Shared | Shared | Scaling-shaped | Bridge/ORS | Separate | Same | Apify fail-closed | **PARTIAL** |
| 6 | `education_scientific` | Medical/pharma base | Live REJECT; NIH ecosystem under-hit | ASSOCIATION_EVENTS | Shared | Mixed | Shared | Prompt | Shared | Shared | Scaling-shaped | Bridge/ORS | Separate | Same | Cap | **IMPLEMENTED_NOT_E2E_VALIDATED** |
| 7 | `projects_infrastructure` | Project workforce | Live REJECT; forward-looking thin | SCALING_COMPANIES | Shared | Thin | Shared | Prompt | Shared | Shared | Scaling-shaped | Bridge/ORS | Separate | Same | Cap | **PARTIAL** |
| 8 | `social_special_events` | Venue discovery | Live NEW_UNIQUE (1) | ASSOCIATION_EVENTS; Apify family | Shared | Thin | Shared | Prompt | Shared | Shared | Scaling-shaped | Bridge/ORS | Separate | Same | Cap | **IMPLEMENTED_NOT_E2E_VALIDATED** |
| 9 | `local_catering_day_meetings` | Local meetings/catering | Live REJECT | ASSOCIATION_EVENTS | Shared | Thin | Shared | Prompt | Shared | Shared | Scaling-shaped | Bridge/ORS | Separate | Same | Cap | **PARTIAL** |
| 10 | `corporate_celebrations_private_events` | Recognition/celebration | Live REJECT; thin published mix | ASSOCIATION_EVENTS | Shared | Thin | Shared | Prompt | Shared | Shared | Scaling-shaped | Bridge/ORS | Separate | Same | Cap | **PARTIAL** |
| 11 | `travel_trade_group_intermediaries` | Planner queries | Live REJECT | ASSOCIATION_EVENTS | Alias overlap risk w/ planners | Thin | Shared | Prompt | Shared | Shared | Scaling-shaped | Bridge/ORS | Separate | Same | Cap | **PARTIAL** |
| 12 | `relocation_extended_stay` | Relocation templates | Live REJECT; missing some packages | SCALING_COMPANIES | Shared | Thin | Shared | Prompt | Shared | Shared | Scaling-shaped | Bridge/ORS | Separate | Same | Cap | **PARTIAL** |
| 13 | `preferred_lodging_partnerships` | Preferred lodging prompts | Live DUPLICATE (Rock Creek) | PREFERRED_LODGING | Pre-deep gap demonstrated | Thin | Shared | Prompt | Shared | Shared | Scaling-shaped | Bridge/ORS | Separate | Same | Cap | **PARTIAL** |
| 14 | `professional_event_planners` | Planner prompts | Live REJECT | ASSOCIATION_EVENTS | Alias overlap risk | Thin | Shared | Prompt | Shared | Shared | Scaling-shaped | Bridge/ORS | Separate | Same | Cap | **PARTIAL** |

### Implementation vs demonstrated readiness

| Layer | State |
| --- | --- |
| Offline wiring (queries, routing metadata, contact prompts, executor on disk) | **14/14 VERIFIED_WORKING** (`reports/gdi-full-e2e-powershell-deep-research-certification-v1/FOURTEEN_SEGMENT_FUNCTIONAL_CERTIFICATION.md`) — does **not** equal live E2E |
| Registry class | All 14 remain `PARTIAL_EXECUTOR` by design |
| Bounded Bethesda live Exa ($15 campaign) | All 14 discovery cells LIVE_EXECUTED; commercial yield sparse; most REJECT (`FOURTEEN_SEGMENT_EFFECTIVENESS.md`) |
| Historical execution ledgers | Often `UNKNOWN` per segment — must not be relabeled EXECUTED (`FOURTEEN_SEGMENT_EXECUTION_AUDIT.md`) |
| Full E2E certification (2026-11-11) | **FAIL** overall (`docs/gdi-full-e2e-certification-report-20261111.md`) |
| Fresh paid 14-seg deep + customer delivery | **BLOCKED** / founder-gated (`REMAINING_INTEGRATION_GAPS.md`) |

**Bottom line:** The platform **plans and can execute** all 14 segments. It cannot yet claim repeatable, commercially validated Full E2E across 14 segments with durable ledgered live yield and customer delivery.

---

## Deliverable 2 — Canonical pipeline architecture and failure map

### Call graph (authoritative)

```
npm run gdi:canonical-full-e2e
  → scripts/gdi-canonical-full-e2e-entry-v1.mjs
  → runCanonicalFullE2eResearch
       (gdi-canonical-full-e2e-entry-v1.js)
  → executeOriginalFullE2eResearchChain
       (original-full-e2e-research-chain-v1.js)
       → executeFourteenSegmentDiscoveryMatrix
       → pre-deep screen / dedupe / deep selection
       → processOpportunityThroughOriginalChain (per candidate)
       → Airtable persistence dry-run / optional inventory bridge
  ✗ does not publish (authorizePublish hard-refused)

Customer delivery / publish:
  → gdi-research-to-page-workflow-v1.mjs / universal operational workflow
  → separate approval gates (no automated handoff from canonical summary)
```

PowerShell `gdi-workflow.ps1` FullRun/Research maps to canonical Node entry with **forced dry-run**. `-Apply` does not unlock paid/live on that path.

### Stage failure map

| Stage | Works | Primary failure / inconsistency | Smallest fix |
| --- | --- | --- | --- |
| 1 Discovery | 14-cell matrix, budgets, resume skip | Replay/fixture shaped unless paid Exa auth; 10/14 share ASSOCIATION_EVENTS Exa family; Apify mock/off | Yield labels `LIVE_NEW`/`HISTORICAL_REPLAY`/`FIXTURE`/`INVENTORY_REOPEN`; equality test segment IDs ↔ executor specs |
| 2 Dedupe | Pre-deep org keys, composite key | Candidate ID minting differs by source; Airtable reuse off by default → re-research risk | Org-key-first persisted identity; surface skip counters in canonical summary |
| 3 Evidence | URL → acceptedClaims | Offline corpus pads lodging keywords → false CONFIRMED_DEMAND risk | Remove synthetic padding; mark SYNTHETIC evidence mode |
| 4 Thesis | Offline CHD / hypothesis | Sales Ready vs SPECULATIVE_DEMAND disagreements historically | Certify disagreement as FAIL |
| 5 Contacts | Restored investigation; 6/6 named on rescore sample | Surfe excluded; contacts in ORS JSON not bridge rows | Persist contact class + authorityBasis on bridge payload |
| 6 Hotel qual | V41 + disposition reconcile | Fit often not defensible for Sales Ready | Count dispositionConflict in summary |
| 7 ORS / eligibility | Offline ORS deep; need-date | Default `maxCandidates=8` < 14 segments; paid ledger under timestamped `outDir` resets ceiling; **no challenger on canonical path** | Preflight fail if maxCandidates < planned segments; stable ledger path |
| 8 Drawer | Ten-section DTO generated | 12/14 reuse Scaling section builder; contract checks presence not quality; next-action often false | Harden certified commercial content contract |
| 9 Persistence | Fail-closed inventory bridge | Thin processed rows lack full opp/evidence/drawer; ORS writes ignore `GDI_DATA_ROOT` | Pass full opportunities into bridge; isolate data root |
| 10 Publication | Separate gated workflows | No canonical→GDO handoff manifest | Explicit handoff artifact; never auto-set customerPublicationReady |
| 11 Weekly change | Durability module exists | Canonical entry does not annotate; unchanged case may falsely UPDATED | Fix snapshot flat contact fields; assert UNCHANGED |

---

## Deliverable 3 — Repeatability, deduplication, and persistence gap register

| ID | Gap | Impact | Priority |
| --- | --- | --- | --- |
| R1 | Discovery yield unlabeled → replay presented as completion | False confidence; wasted paid spend | P0 |
| R2 | `maxCandidates` default 8 vs 14 segments | Systematic deep-research starvation | P0 |
| R3 | Paid ledger path per timestamped outDir | Cross-run spend ceilings ineffective | P0 |
| R4 | Persistence bridge thin-row risk (processed without full opportunity) | Evidence/contacts/drawers lost to Airtable inventory | P0 |
| R5 | Alias `professional_planners_dmc` → two workflow IDs | Duplicate historical replay | P0 |
| R6 | Preferred lodging live duplicate without pre-deep inventory check | Paid waste (Rock Creek) | P0 |
| R7 | Offline corpus lodging padding | Inflated thesis confidence | P0 |
| R8 | ORS/report writes not always under `GDI_DATA_ROOT` | Local-only / contaminated trees | P0 |
| R9 | Weekly change not wired into canonical; false UPDATED | Opportunities look new / changed on rerun | P1 |
| R10 | No automated research→publish handoff | Operator error; delivery FAIL historically | P1 |
| R11 | Drawer contract presence ≠ commercial quality | Weak customer briefs pass gates | P1 |
| R12 | Market Opportunity Graph under-applied at intake | Repeat org research; weak cross-hotel reuse | P2/P3 |
| R13 | Challenger absent on canonical path | No systematic alternative disposition stress | P2 |
| R14 | Historical ledgers UNKNOWN | Cannot certify past “full” runs | P2 |

---

## Deliverable 4 — Representative opportunity drawer quality scorecard

Sources: frozen Bethesda / Radisson / YOTEL inventories + zero-cost commercial rescore reports.  
Scale 1–5 (evidence-grounded; length/polish not rewarded).  
**CC** clarity · **HR** hotel relevance · **BI** buyer · **CP** competitive/predictive · **PU** pursuit · **EV** evidence.

| Sample | Mechanism | Hotel | CC | HR | BI | CP | PU | EV | Critical defects |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| NICE Conference 2027 | Gov conference / RFP | Bethesda | 4 | 3 | 4 | 4 | 5 | 4 | Blank top-level primaryContactName; competitor wash |
| AMWA 112th 2027 | Medical association | Bethesda | 3 | 2 | 3 | 3 | 3 | 3 | READY while fit speculative; generic inbox action |
| ACTS TS27 | Medical/research | Bethesda | 4 | 3 | 2 | 4 | 3 | 4 | Org inbox as contact |
| ESA 112th | Association overflow | Radisson | 4 | 4 | 4 | 3 | 4 | 4 | Title missing org; hotel-spec boilerplate |
| PDAC 2027 | Convention partner | Radisson | 5 | 4 | 4 | 4 | 4 | 4 | Best sample; expiration gate uses cutoff as start |
| PDA TRI training | Corporate training | Bethesda | 2 | 3 | 3 | 1 | 1 | 1 | 0 evidence rows; research-continuation as commercial |
| NRC RIC | Training/conference | Bethesda | 2 | 3 | 3 | 1 | 2 | 2 | Past date; URL-only why; 0 evidence |
| IMC Construction | Scaling | Bethesda | 3 | 2 | 2 | 1 | 3 | 3 | Truncation at “D.C.”; methodology leakage |
| REGENXBIO | Scaling (stale) | Bethesda | 2 | 2 | 1 | 1 | 2 | 2 | 5-year-old expansion; missing contact fields |
| GuidePoint | Scaling | Bethesda | 3 | 2 | 4 | 1 | 3 | 3 | Strong buyers; Reston geography weak |
| True Anomaly | Scaling | Bethesda | 4 | 1 | 3 | 1 | 2 | 4 | Crystal City mismatch; “(the market)” residue |
| M.C. Dean | Projects | Bethesda | 3 | 1 | 4 | 1 | 2 | 3 | Named buyers; wrong geography |
| Whiting-Turner | Project/workforce | Bethesda | 3 | 3 | 2 | 1 | 3 | 3 | Local GC signal; alt entry point |
| NIH OITE Fellows | Medical relocation | Bethesda | 2 | 3 | 3 | 1 | 1 | 2 | Best insight buried in contact note |
| Virtual Colonography | Medical education | Radisson | 3 | 1 | 2 | 1 | 2 | 3 | Convention boilerplate for 12 students |
| NIH SBPO VOS | Recurring gov | Bethesda | 3 | 2 | 4 | 2 | 3 | 3 | Honest past session; no room-night proof |
| MSYSA State Cup | Sports overflow | Bethesda | 3 | 2 | 2 | 3 | 2 | 3 | Self-admits better Gaithersburg fit |
| Toronto Tempo/WNBA | Incumbent sports | Radisson | 3 | 3 | 4 | 4 | 4 | 3 | Truncated whyThisHotel |
| OWWA 2027 | Competitor-placed CMI | Radisson | 3 | 2 | 3 | 3 | 2 | 2 | Empty cmiEvidenceUrls; template next step |
| SFPE Europe 2027 | Competitor-placed CMI | YOTEL | 3 | 2 | 4 | 3 | 2 | 2 | Internal enum leakage |
| AAN 2027 | Rejected competitor | Bethesda | 3 | 3 | 1 | 2 | 2 | 2 | Disqualified yet pursuit copy |
| AMWA 2028 | Forward / no lodging | Bethesda | 3 | 2 | 3 | 3 | 3 | 2 | Location Bethesda vs destination TBD |
| ASAE 2029 | Forward TOO_EARLY | Bethesda | 4 | 3 | 2 | 3 | 3 | 3 | Honest monitor; boilerplate competitors |
| AAO 2028 | Rejected Chicago | Bethesda | 4 | 4 | 1 | 1 | 2 | 1 | Disqualified + relationship-touch action |
| Corporate corridor watch | Speculative | Bethesda | 4 | 2 | 1 | 1 | 2 | 2 | Vendor/$ leakage (gated invisible today) |
| NIDA virtual junk | Artifact | Bethesda | 1 | 1 | 1 | 1 | 1 | 1 | Invalid title; still commercial class internally |
| City of Rockville | Local catering | Bethesda | 1 | 2 | 3 | 1 | 1 | 1 | Missing research body |

**Pattern:** Presence-based certified drawer contract can PASS while commercial usefulness fails. Top drawers pair dated demand + open sourcing + named buyer. Bottom drawers are research-continuation or junk still typed as commercial opportunities.

---

## Deliverable 5 — GDI 3.0 Commercial Intelligence Brief specification

**Purpose:** Replace generic ten-section prose with an evidence-gated brief that a Director of Sales can act on in <60 seconds.

### Field contract (v2)

| Block | Required fields | Rules |
| --- | --- | --- |
| Header | `identityLine`, `demandMechanism`, `maturity`, `stage` (`PURSUE`/`QUALIFY`/`MONITOR`/`DO_NOT_PURSUE`), `asOf`, activity/expiry dates | One canonical enum set; past events cannot be PURSUE |
| 1 What is happening | `fact` (≤2 complete sentences), `location`, `claimKind`, `sourceCount` | Ban methodology / vendor / cost strings |
| 2 Hotel business | `revenueStreams[]`, `volume` or `UNQUANTIFIED`, `hotelRole` (`HOST`/`OVERFLOW`/`PREFERRED`/`PARTNER`/`NONE`) | Unknown ≠ zero; no invented room nights |
| 3 Why this hotel | ≤3 `reasons` with numbers where known + one `limitation` | No shared hotel-spec boilerplate as the only content |
| 4 Competitive / predictive | `incumbents[]` + evidence URLs, prior edition hotels, rotation, next decision window, win path | Competitor-placed cannot carry host RFP action; empty evidence ⇒ cannot claim “confirmed commitment” |
| 5 Buyer intelligence | Named primary + class + `authorityBasis`, alt route, lodging owner function, housing agency, channels | Generic inbox must be labeled routing-only; never invent names |
| 6 Pursuit | `actionType`, complete `nextStep`, 2–4 qualification questions when pursuing, trigger/deadline | Maturity-appropriate; disqualified ⇒ DO_NOT_PURSUE copy only |
| 7 Evidence | Claim rows with kind/url/accessedOn; known/estimated/inferred/unknown summary; top uncertainties | Null sourceUrl fails gate |
| 8 Gate result | Eligibility, blockers, customerVisible | Render status line; research-continuation ≠ COMMERCIAL_OPPORTUNITY surface |

### Before → after examples (frozen)

**NRC RIC (past event):** stage → `DO_NOT_PURSUE`; overflow role; named contact retained; monitor next housing announcement; drop URL-only why and “lodging path open”.

**OWWA 2027 (CMI):** per-hotel incumbents with evidence URLs; `OVERFLOW_QUALIFY` questions; forbid host RFP language; stop identical 13-row templates.

**IMC Construction:** complete sentence (no “D.C.” hard cut); strip research-verification methodology; surface gold-brief qualification questions in the drawer.

### Gate upgrades (P1)

Extend `gdi-certified-drawer-commercial-content-contract-v1.js` to fail on:

- Methodology leakage (`Research verification date`, `activity dates, not`, Webhound/$ , internal enums)
- URL-only commercial fields
- Truncation / mid-abbreviation cuts
- Past date + pursue language
- Disqualified/archive + pursuit action
- Expanded generic prose patterns (CMI templates, Marriott-area boilerplate)

---

## Deliverable 6 — Five-pillar architecture compatibility (no implementation)

| Pillar | Reuse now | Missing contracts / relationships | Persistence gap |
| --- | --- | --- | --- |
| 1 Predictive Demand Discovery | Segment missions, Exa agent, intake disposition experiment (`gdi-discovery-intake-disposition-v1`), demand-mechanism research | OPEN vs TOO_EARLY vs SPECULATIVE enforced at intake; $0 official-calendar providers | Durable yield labels + revisit triggers |
| 2 Competitive Demand Intelligence | `recurring-meeting-intelligence-v1`, CMI presentation, competitor fields | Prior-edition hotel history; commitmentType + evidence URLs required; challenger on canonical path | Empty `cmiEvidenceUrls` today |
| 3 Buyer & Account Intelligence | Universal contact contract, restored contact passes, Surfe excluded by default | authorityBasis, lodgingOwnerFunction, housing agency; pairing name+role+mailto only | Contacts often ORS-local only |
| 4 Hotel-Specific Pursuit Intelligence | V41 qualify, honest sales brief, certified drawer, HI profile hooks | Market opp → N hotel qualifications; confidential client feedback isolation | Shared public packet vs private hotel overlay split |
| 5 Continuous Intelligence & Weekly Change | Weekly durability, weekly refresh orchestrator, execution ledgers | Canonical entry annotation; false-new prevention; org/program identity stability | Stable ledger paths; graph intake cache |

### Critical multi-hotel rule (binding for later phases)

- A **market opportunity** may apply to many hotels.
- Each hotel must have **separate** qualification, fit, and pursuit recommendation.
- Shared public intelligence must **never** expose confidential client-specific research or sales feedback to another hotel.

Existing building blocks: `market-opportunity-graph/*` (`cross-hotel-fit-v1`, `build-hotel-opportunity-from-market-v1`), `universal-opportunity-identity-v1`, publication registry isolation. Gap: not the default intake path for Full E2E.

---

## Deliverable 7 — Prioritized implementation backlog

### P0 — Full E2E correctness

1. Discovery yield labeling + intake disposition (no new providers).  
2. Preflight: `maxCandidates >= plannedSegmentCount` (or fair deep coverage policy).  
3. Stable paid-budget ledger path (not timestamped outDir).  
4. Persistence bridge must carry full opportunity (evidence, contacts, drawer pointers).  
5. Remove offline lodging-keyword corpus padding.  
6. Harden alias/dedupe for planner workflow double-replay + preferred-lodging inventory precheck.  
7. Isolate ORS/report writes via `GDI_DATA_ROOT` / `--out`.

### P1 — Commercial drawer quality

1. Certified drawer quality gates (leakage, truncation, past-date, disposition conflicts).  
2. Strip methodology strings from drawer path (tile already strips some).  
3. Sentence-aware truncation; event-specific `whyThisHotel`.  
4. Evidence-bearing CMI templates.  
5. Surface qualification questions on pursuit drawers.  
6. Disposition/stage copy consistency for rejected / TOO_EARLY / research-continuation.

### P2 — Predictive and competitive expansion

1. Demand-mechanism mission packs (projects, scaling, rotational overflow) — query diffs $0 first.  
2. Prior-edition / incumbent evidence model.  
3. Bounded paid Exa only after P0 yield honesty + founder auth.

### P3 — Shared intelligence and continuity

1. Market Opportunity Graph as default intake gate.  
2. Org/program global identity + per-hotel overlays.  
3. Weekly change on canonical path + false-new prevention.  
4. Canonical→review/publish handoff manifest.

---

## Deliverable 8 — First focused implementation slices (acceptance)

### Slice 1 — `gdi-full-e2e-yield-and-coverage-honesty-v1` (P0)

**Scope ($0):**

1. Label every discovery candidate: `LIVE_NEW` | `HISTORICAL_REPLAY` | `FIXTURE` | `INVENTORY_REOPEN`.  
2. Wire/extend intake disposition: `OPEN_OR_UNKNOWN` | `COMPETITOR_PLACED` | `TOO_EARLY_WATCH` | `SPECULATIVE_WATCH` | `INVENTORY_REPLAY` (reuse existing disposition modules).  
3. Fail preflight when `maxCandidates < plannedSegmentIds.length` unless explicit `--allow-partial-segment-deep`.  
4. Unit + offline harness on frozen Bethesda (+ optional YOTEL) inventories.

**Acceptance:**

- 0 fixture/historical candidates labeled `LIVE_NEW`.  
- ≥80% known competitor-placed / banquet-style rows classified non-pursuit appropriately on frozen sample.  
- Preflight fails on default 8-vs-14 without override flag.  
- No paid calls; no Airtable writes.

### Slice 2 — `gdi-persistence-bridge-full-opportunity-v1` (P0)

**Scope ($0 dry-run):**

1. Attach full opportunity (or ORS reload) into persistence bridge extract.  
2. Assert bridge payload includes evidence refs + primary contact fields when present in ORS.  
3. Dry-run only; production writes remain fail-closed.

**Acceptance:**

- Test fails if bridge row drops named contact / evidence URLs present in ORS.  
- `outputsIngested` reflects real processed/ORS artifacts, not merely chain version string.

### Slice 3 — `gdi-certified-drawer-commercial-quality-gate-v1` (P1)

**Scope ($0):**

1. Extend certified drawer contract with leakage/truncation/past-date/disposition checks.  
2. Fixtures: NRC RIC, IMC Construction, OWWA CMI as failing cases until remediated.  
3. Strip verification-date methodology from drawer composition path.

**Acceptance:**

- Failing fixtures fail the gate.  
- PDAC-class gold sample still passes.  
- No customerVisible=true row may contain vendor/cost/`TITLE_SOURCE_INVALID` text (publication guard test).

---

## Founder review gate

**STOP here.** Do not start Phase 0B or Phase 1B until this audit and the slice order are approved.

Recommended founder decisions:

1. Approve Slice 1 → 2 → 3 sequence (or reorder).  
2. Confirm $0-only until Slice 1 gates pass.  
3. Defer all five GDI 3.0 pillar builds.  
4. Keep rejected adaptive AI-agent experiment closed.

**Success criteria for Phase 0A:** We know what must be corrected for Full E2E repeatability, and how to make customer-facing intelligence more valuable — without building the future platform yet.

---

## Evidence index (reused; not re-researched)

- `docs/gdi-full-e2e-certification-standard-v1.md`
- `docs/gdi-full-e2e-certification-report-20261111.md`
- `docs/data-intelligence/gdi-discovery-quality-pivot-plan-v1.md`
- `reports/gdi-full-e2e-powershell-deep-research-certification-v1/*`
- `reports/gdi-bethesda-exa-live-discovery-effectiveness-v1/FOURTEEN_SEGMENT_EFFECTIVENESS.md`
- `reports/gdi-full-e2e-foundation-commercial-quality-scale-audit-v2/FOURTEEN_SEGMENT_EXECUTION_AUDIT.md`
- `lib/group-demand-intelligence/workflow/gdi-canonical-full-e2e-entry-v1.js`
- `lib/group-demand-intelligence/workflow/original-full-e2e-research-chain-v1.js`
- `lib/group-demand-intelligence/workflow/full-e2e-fourteen-segment-discovery-executor-registry-v1.js`
- `lib/group-demand-intelligence/presentation/gdi-certified-drawer-commercial-content-contract-v1.js`
- Frozen hotel inventories under `data/group-demand-intelligence/hotels/{recLuxvwwxID7U2B8,recH9h3McgMNFQEla,recrPQcZg7SFARRb2}/`
