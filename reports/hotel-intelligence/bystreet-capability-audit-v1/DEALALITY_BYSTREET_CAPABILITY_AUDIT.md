# DEALALITY — BYSTREET CAPABILITY AUDIT

**Marker:** `DEALALITY_BYSTREET_CAPABILITY_AUDIT_V1`  
**Date:** 2026-08-11  
**Scope:** Audit only — no schema changes, no Airtable writes, no new MCP, no Sprint execution.  
**Governing question:** What has Dealality already built toward Hotel → Owner → Portfolio → Principal → Contact → Evidence → Opportunity?

---

## 1. Executive conclusion

Dealality already has **strong hotel-identity infrastructure** and a **parallel, GTM-internal owner/opportunity stack**, plus an **external Webhound research MCP** that has been used for Mexico owner-intel experiments and Census/Brand hard-case learning.

What it does **not** have is a connected product path that starts from **Hotel Property Census** and automatically returns ByStreet-style ownership intelligence through Hotel Intelligence MCP.

In plain English:

> Roughly **~35–40% of the building blocks exist** (hotel census + identity + evidence patterns + GTM owner/opportunity schemas + Webhound research capability), but they live in **three disconnected layers**. The ByStreet end-to-end loop is **not executable as one system today**.

---

## 2. Current architecture (what actually exists)

### Layer A — Hotel Intelligence (product / Census-adjacent)

| Piece | Path | Role today |
| --- | --- | --- |
| Hotel Property Census | Airtable `tbl9aY5ijiuIzzWam` via `MAP_HOTEL_PROPERTY_CENSUS` | ~6k CALA hotels; identity + geo + brand family + rooms/status |
| HI MCP | `mcp/hotel-intelligence/server.js` | Hotel search/resolve/enrich/nearby/sources/ingest/room-count |
| Core lib | `lib/hotel-intelligence/**` | Canonical `dhl_*`, identity resolve, local evidence store, providers |
| Discovery Factory | `lib/hotel-intelligence/discovery-factory/**` | Candidate → READY/REVIEW staging (census expansion) |
| “Portfolio Coverage OS” | `lib/hotel-intelligence/portfolio-coverage-os/**` | **Geographic** census coverage planner — **not** owner portfolios |

### Layer B — GTM Owner / Decision Radar (internal only)

| Piece | Path | Role today |
| --- | --- | --- |
| Owner Targets | `lib/gtm-owner-target/**`, `docs/gtm-owner-target-list.md` | CoStar-derived owner rollups, contacts, ICP, portfolio audit |
| Decision Opportunities | `docs/gtm-decision-radar.md`, `decision-opportunity-*.js` | Live owner-decision opportunities + evidence table contract |
| Branding signals | `branding-decision-signals.js` | Heuristic brand/operator/reflag/development triggers |
| Corporate web seeds | `adapters/*corporate-web*` | Seed URLs for contact/entity research |
| Registry adapters | MX SIGER/RNT, CR/DR/CALA enrichment research adapters | Country-specific enrichment — **not** HI MCP tools |

**Hard firewall:** CoStar / GTM Owner Targets must **never** sync to Hotel Census, Scout, or public APIs (`field-map.js`, `AGENTS.md`).

### Layer C — Webhound (external research harness)

| Piece | What it is |
| --- | --- |
| Cursor MCP `user-webhound` | Live MCP tools: `webhound_start_report`, `webhound_start_dataset`, claims/sources/evidence pack, etc. |
| Dealality policy | Hard-case pattern discovery only; never production census writes (`AGENTS.md`, batch learning docs) |
| Repo artifacts | Prompts/payloads (e.g. `data/webhound-mexico-owner-intel-test1-*`), learning scripts, Brand Explorer claim patches |
| Executable Dealality “Webhound engine” | **None** — no in-repo crawler that replaces Webhound |

### Layer D — Partner / Operator / Brand Intelligence

Source Library + Extracted Facts models (`docs/partner-*-airtable-fields.md`), Operator Explorer, Brand Explorer — strong **brand/operator presentation** evidence, not property-owner graphs from Census hotels.

---

## 3. Capability matrix

| Capability | Existing? | Location | Status | Inputs | Outputs | Persistence | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Canonical hotel identity | Yes | `canonical-hotel.js`, census map | `BUILT_ACTIVE` | Census / providers | `dhl_*` + MVP fields | Census + local stage | Strong |
| Alternate/former names | Partial | Canonical shell | `PARTIAL` | — | arrays on shell | Rarely populated | Slots exist; not systematically filled |
| Address / coords / market | Yes | Census + HI | `BUILT_ACTIVE` | Census, GIATA, SerpAPI | location fields | Census | Market/submarket Dealality-defined |
| Brand / parent company | Yes | Census `Current Brand` / `Brand Family` | `BUILT_ACTIVE` | Census, GIATA, HBX | brand fields | Census | Brand ≠ owner |
| Operator | Slots only | Canonical `operator.*` | `PARTIAL` | — | mostly null | Not in `MAP_CENSUS_FIELDS` | Not HI MVP |
| Room count research | Yes | `hotel_room_count_research` | `BUILT_ACTIVE` | Website + SerpApi search | evidence-backed keys | Local evidence; no census write by default | Best pattern for field research |
| Website / phone | Yes | Census + providers | `BUILT_ACTIVE` | Official URL, SerpAPI, GIATA | digital fields | Census / stage | Property contact ≠ owner contact |
| Owner name on HI | Slots only | `canonical-hotel.owner` | `DEFINED_NOT_IMPLEMENTED` | — | null | Not mapped to Census SoT | `owner_get` listed as future tool |
| Owner in Census SoT map | No | `map_hotel_intelligence_fields.js` | `MISSING` | — | — | — | No owner keys in `MAP_CENSUS_FIELDS` |
| Legal SPV / % ownership | No in HI | GTM/Webhound prompts only | `MISSING` / `PARTIAL` | Public filings via Webhound designs | unstructured | Session exports | Not product pipeline |
| Parent ownership group | GTM rollup | Owner Targets | `BUILT_NOT_CONNECTED` | CoStar True Owner | Owner Target record | GTM base | Disconnected from Census `dhl_*` |
| Portfolio (owner hotels) | GTM | `owner-portfolio-audit.js`, Properties link | `BUILT_NOT_CONNECTED` | CoStar properties | portfolio confidence | GTM | Not Census-keyed |
| Geographic “portfolio OS” | Yes | `portfolio-coverage-os` | `BUILT_ACTIVE` | Coverage dashboard | sprint plans | reports/data | **Wrong sense of portfolio** for ByStreet |
| People / principals | GTM + Webhound | contacts, LinkedIn import, MX prompt | `PARTIAL` | CoStar contacts / web | contact fields | GTM / Webhound sessions | Not HI tools |
| Contact enrichment | GTM | `owner-contact-sync`, registry verification | `BUILT_NOT_CONNECTED` | Corporate web seeds, CoStar | email/phone tiers | GTM | Firewall from product |
| Field evidence store | Yes | `evidence-store.js` | `BUILT_ACTIVE` | Provider observations | field+source+confidence | Local JSON | Hotel fields, not ownership edges |
| Partner Source Library | Proposed/partial | docs + PI governance | `PARTIAL` | Brand/operator sources | source records | Airtable (when ensured) | Brand/operator, not hotel-owner |
| Confidence scoring | Yes | HI confidence + discovery tiers + GTM ICP | `BUILT_ACTIVE` / `PARTIAL` | identity/source | tiers | Various | No ownership-edge confidence model in HI |
| Opportunity signals | GTM Decision Radar | `decision-opportunity-*`, branding signals | `PARTIAL` | Heuristics + research | Decision Opportunity rows | GTM schema Stage 1 | Not wired to HI MCP |
| Entity resolution (non-hotel) | Partial | hotel identity only in HI; GTM name normalize | `PARTIAL` | names | owner keys | GTM | No org/person graph engine |
| Ownership graph model | No unified graph | Canonical slots + GTM links | `MISSING` | — | — | — | Relationships exist as Airtable links in GTM only |
| End-to-end Census→Owner MCP | No | Future contracts only | `DEFINED_NOT_IMPLEMENTED` | — | — | — | `owner_get`, `owner_portfolio`, `opportunity_search` |
| Webhound owner discovery | External MCP | Cursor `user-webhound` + prompts | `BUILT_NOT_CONNECTED` | Research budget | reports/datasets/claims | Webhound cloud + local exports | Not called by HI service code |
| Batch hotel owner research | No HI path | — | `MISSING` | — | — | — | Batch jobs exist for HI staging, not owners |

---

## 4. Hotel Intelligence MCP audit

**Server:** `mcp/hotel-intelligence/server.js` (`dealality-hotel-intelligence`)  
**Service:** `lib/hotel-intelligence/orchestration/service.js`

### Tools (implemented)

| Tool | What it does | Providers | Owner intel? |
| --- | --- | --- | --- |
| `hotel_search` | Search census (+ optional providers) | census, hotelbeds, giata_drive, serpapi, stayingapi (flagged) | No |
| `hotel_get` | Canonical hotel + evidence summary | local store / census | Owner slots empty |
| `hotel_resolve` | Identity vs Census; no auto-merge ambiguous | census | Hotel-only |
| `hotel_enrich` | Fill missing hotel fields from providers; stage evidence | HBX, StayingAPI, SerpAPI, GIATA | Does not enrich ownership |
| `hotel_nearby` | Haversine census slice | census | No |
| `hotel_sources` | Field-level evidence/conflicts | evidence store | Hotel fields |
| `hotel_review_queue` | Ambiguous/conflict/missing queue | local | No owner issues |
| `hotel_census_ingest` | Normalize→resolve→enrich→stage | pipeline | Stage-only default |
| `hotel_room_count_research` | Targeted keys research + evidence | official site + optional SerpApi | Pattern reusable; not owners |
| `hotel_intelligence_meta` | Provider availability + future contracts | registry | Lists future owner tools |

### Future tool contracts (NOT implemented)

From `FUTURE_TOOL_CONTRACTS` in `lib/hotel-intelligence/index.js`:

- `owner_get`, `owner_portfolio`
- `operator_get`, `operator_portfolio`
- `hotel_history`, `hotel_reflags`
- `pipeline_search`, `opportunity_search`
- (+ market/brand whitespace tools)

### Providers wired into registry

| Provider | Flag / auth | Role | Ownership data? |
| --- | --- | --- | --- |
| `dealality_census` | Airtable read | Primary identity SoT | No owner fields in HI map |
| `hotelbeds` | HBX creds + flags | Identity/geo/rooms | No |
| `giata_drive` | `HOTEL_INTELLIGENCE_GIATA_DRIVE` + API key | Identity/geo/brand | No ownership graph |
| `serpapi` | `HOTEL_INTELLIGENCE_SERPAPI` + key | Google Hotels / search assist | Indirect public hints only |
| `stayingapi` | flag + key | Listing identity | No |
| google_places / OSM / brand_directory | IDs reserved | Not production-wired as full adapters | — |

### Persistence

- Default: **no Airtable writes** (`ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES=0`)
- Local: evidence JSON, external ID registry, review queue, staged discovery hotels
- Census writes: gated, separate enrichment lanes

### Can MCP research one Census hotel end-to-end for ByStreet?

**Hotel identity / rooms / website: yes (partial).**  
**Owner → principal → portfolio → contacts → opportunity: no** — tools do not exist; enrich does not populate `owner.*`.

### Batch safety

Discovery Factory + census ingest support staged batches. Owner research batch orchestration **does not exist** in HI.

---

## 5. Webhound audit

### Instructions vs implementation

| Question | Answer |
| --- | --- |
| Only a Cursor rule? | **No** — live external MCP (`user-webhound`) with many tools |
| Executable Webhound code in repo? | **No crawler** — prompts, payloads, learning/reconciliation scripts, claim patches |
| Callable programmatically from Dealality Node? | **Not via HI service** — agent/Cursor MCP calls only (unless separately scripted against Webhound API outside this audit’s verified HI path) |
| Owner/developer discovery? | **Yes as research product** (see Mexico owner-intel prompt); **not** a Dealality production writer |
| Public web search? | Yes (Webhound harness) |
| Multi-query / recursive / structured facts / claims / sources? | Yes in Webhound product (claims, sources, evidence pack) |
| Entity resolution / portfolios / % ownership / contacts? | Prompted for in owner-intel tests; quality varies; not persisted into Census graph |
| Results stored? | Webhound sessions + local `data/`/`reports/` exports; **not** HI ownership graph |
| Dealality policy | Hard-case learning only; never production census writes |

**Mexico owner intel test** (`data/webhound-mexico-owner-intel-test1-prompt.txt`) is a **ByStreet-adjacent research brief** (owner, SPV, parent, principal, contacts, evidence, early signals). That proves **research capability exists**, not that Dealality has an automated Census→Owner product pipeline.

---

## 6. Ownership graph audit

### Desired edges vs what exists

| Desired relationship | Exists as? | Where |
| --- | --- | --- |
| HOTEL → OWNED_BY → ENTITY | Text field / CoStar True Owner | Legacy census inventory docs; GTM Properties — **not** HI census map |
| HOTEL → OPERATED_BY → OPERATOR | Partial | Brand Explorer / Operator Explorer / Management Company legacy; not HI MVP |
| HOTEL → BRANDED_AS → BRAND | Yes | Census Current Brand / Affiliation pipelines |
| HOTEL → DEVELOPED_BY → DEVELOPER | Research/GTM only | Webhound prompts, Decision Opportunities |
| ENTITY → OWNED_BY → PARENT | GTM rollup / research | Owner Targets parent notions; not graph DB |
| ENTITY → CONTROLLED_BY → PERSON | Contacts link | GTM Contacts |
| PERSON → WORKS_FOR → ENTITY | Contacts | GTM |
| ENTITY → OWNS → HOTEL | Inverse of CoStar property link | GTM |
| ENTITY → DEVELOPS → PROJECT | Decision Opportunity | GTM Stage 1 schema |
| HOTEL → SUBJECT_TO → SIGNAL | Decision Opportunity + branding signals | GTM |

**Conclusion:** A usable **GTM relational model** (Owner Target ↔ Properties ↔ Contacts ↔ Decision Opportunities ↔ Evidence) exists for internal acquisition. A **product ownership graph keyed by Census `dhl_*` / Property Identity Key** does **not**.

Canonical hotel already reserves:

```text
owner: { owner_id, owner_name }
operator: { operator_id, operator_name, operating_structure }
```

…but Census HI field map omits owner/operator, and no resolver fills them.

---

## 7. Provider / source matrix

| Provider | Hotel Identity | Ownership | People | Contacts | Projects | Signals | Currently Wired? |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| Dealality Census | High | None (HI map) | None | Property phone only | Limited status | None | Yes (HI read) |
| Hotelbeds/HBX | High | None | None | Limited | None | None | Yes (flagged) |
| GIATA Drive | High (name/geo/brand) | None | None | Website/phone sometimes | None | None | Yes (flagged) |
| SerpAPI Google Hotels | Medium–High | None direct | None | Sometimes | None | Weak | Yes (flagged) |
| StayingAPI | Medium | None | None | None | None | None | Yes (flagged) |
| Cvent (discovery stock) | Medium | None | None | Venue URLs | None | None | Discovery Factory files |
| CoStar (GTM) | High (GTM props) | **High (True/Recorded Owner)** | Medium | Medium | Weak | Heuristic | GTM only — firewall |
| Corporate web seeds | Low | Low | Medium | Medium | Low | Low | GTM adapters |
| MX SIGER/RNT etc. | Low | Medium (registry) | Low | Low–Med | Medium filings | Early signals | GTM adapters |
| Webhound | High research | **High potential** | High potential | High potential | High potential | High potential | Cursor MCP; not HI |
| Partner Source Library | N/A | Low | Low | Low | Brand/op decks | Low | PI / Brand-Operator |
| News APIs (dedicated) | — | — | — | — | — | — | `MISSING` as first-class HI provider |

---

## 8. Current pipeline vs desired pipeline

### CURRENT (verified executable today)

```text
Hotel Property Census
        ↓
Hotel Intelligence MCP (search / resolve / get)
        ↓
Provider enrichment (HBX / GIATA / SerpAPI / StayingAPI)  → identity/geo/brand/rooms
        ↓
Local evidence store + review queue
        ↓
[BREAK] ── no owner discovery tool ──
        ↓
[PARALLEL GTM TRACK — not Census-linked]
CoStar import → Owner Targets → Properties rollup → Contacts
        ↓
Branding decision heuristics / Decision Opportunity schema
        ↓
[PARALLEL RESEARCH TRACK]
Webhound MCP (manual/agent-started sessions) → claims/sources export
        ↓
Learning scripts (Census/Brand hard cases) — no ownership graph writeback
```

### DESIRED (ByStreet-style)

```text
Hotel Census
 ↓ Identity resolution
 ↓ Public-source research
 ↓ Owner / entity discovery
 ↓ Entity resolution
 ↓ Ownership graph
 ↓ Portfolio discovery
 ↓ People discovery
 ↓ Contact enrichment
 ↓ Evidence verification
 ↓ Confidence scoring
 ↓ Opportunity monitoring
 ↓ Dealality Owner Intelligence
```

**Gap:** Everything from “Public-source research” downward is either **GTM-isolated**, **Webhound-session-isolated**, or **unimplemented** relative to Census hotels.

---

## 9. Readiness score

| Component | Weight | Score | Rationale |
| --- | ---: | ---: | --- |
| Hotel census | 10 | **9** | Live SoT ~6k; expansion factory working |
| Hotel identity resolution | 10 | **8** | `resolveHotelIdentity` + discovery tiers; strong |
| Public-web research | 15 | **6** | Webhound MCP + room-count research pattern; not owner-orchestrated |
| Owner/entity discovery | 15 | **4** | GTM CoStar + Webhound prompts; not Census→HI |
| Entity resolution | 10 | **3** | Hotel matching strong; org/person graph weak |
| Ownership graph | 10 | **2** | Canonical slots + GTM links; no product graph |
| Portfolio discovery | 5 | **3** | GTM portfolio audit; HI “portfolio” = geography |
| People discovery | 5 | **3** | GTM contacts + Webhound; not automated from Census |
| Contact discovery | 5 | **3** | GTM verification stack exists; firewalled |
| Evidence/provenance | 5 | **4** | HI field evidence + Webhound claims + PI sources; no ownership-edge SoT |
| Confidence scoring | 5 | **3** | Hotel/field/ICP scores exist; ownership-edge model missing |
| Opportunity monitoring | 5 | **3** | Decision Radar Stage 1 + branding heuristics; not live monitor on Census |
| **TOTAL** | **100** | **51** | |

**TOTAL READINESS = 51 / 100**

Interpretation: **foundation present, product loop incomplete.** Not a greenfield; not near-parity with ByStreet automation.

### A+B vs C gap share (rough)

| Class | Share of remaining work |
| --- | ---: |
| **A — Connect existing** | ~25% (GTM patterns, evidence store, room-count research pattern, Webhound MCP, future tool stubs) |
| **B — Small extension** | ~30% (Census↔owner link keys, owner evidence schema, thin orchestration) |
| **C — New capability** | ~35% (ownership graph SoT, org/person resolution, continuous opportunity monitor, safe non-CoStar owner discovery at scale) |
| **D — External limits** | ~10% (opaque SPVs, private % ownership, CoStar license wall for product) |

---

## 10. Top gaps (by business impact)

1. **No Census-keyed ownership graph / HI owner tools** — blocks ByStreet loop in product stack.  
2. **GTM owner intelligence firewalled** — richest owner/portfolio/contact data cannot be reused product-facing as-is (CoStar).  
3. **Webhound not orchestrated from HI** — research works, but is agent-manual and non-persistent to graph.  
4. **Entity resolution stops at hotels** — SPV↔parent↔person unresolved systematically.  
5. **Opportunity monitoring not Census-driven** — Decision Radar is GTM Stage 1, not live hotel watch.  
6. **Naming collision: “portfolio”** — geographic coverage OS ≠ owner hotel portfolio (confusion risk).

---

## 11. What NOT to rebuild

Preserve and reuse:

- Hotel Property Census + `dhl_*` identity model  
- `resolveHotelIdentity` / Discovery Factory / evidence store / confidence tiers  
- `hotel_room_count_research` as the **template** for bounded field research  
- Provider registry (census, HBX, GIATA, SerpAPI)  
- GTM Owner Target / Decision Opportunity **schemas and validation** (as internal reference + acquisition — not as CoStar product leak)  
- Webhound MCP for hard-case / deep research  
- Partner Source Library / Extracted Facts governance patterns  
- Batch learning loop (code learns; Webhound for hard cases only)

Do **not** rebuild a second census, second identity resolver, or a second CoStar CRM inside Hotel Intelligence.

---

## 12. 25-hotel proof-of-concept recommendation

### Goal

Prove whether Dealality can produce, for **25 CALA Census hotels**:

`Hotel → Owner → Ownership Group → Principal → Portfolio → Contact → Evidence → Confidence`

**without** production writes and **without** exposing CoStar in product outputs.

### Reuse

- Census read + `hotel_resolve` / `hotel_get`  
- Local evidence store pattern  
- SerpAPI / official site fetch patterns from room-count research  
- Webhound for a **bounded** subset (e.g. 5–8 hard cases)  
- GTM field vocabulary as **schema inspiration** only (owner type, contact tiers, evidence rows) — no CoStar values in product artifacts  
- Existing branding-signal taxonomy as optional opportunity labels

### Connect (minimal)

- Thin orchestrator script (not new MCP yet): for each hotel_id → gather identity → research owner hypotheses → stage JSON  
- Write results to `data/hotel-intelligence/owner-intel-poc-25/` (stage-only)

### Minimal new code

- Owner evidence record shape (edge: hotel_id → entity claim + sources + confidence)  
- Prompt/query pack for owner/principal/portfolio (derive from Mexico owner-intel Tier 1)  
- Manual review checklist + metrics rollup  

### Do NOT build yet

- New Airtable ownership graph tables  
- `owner_get` MCP productionization  
- Continuous opportunity monitor  
- Contact-enrichment SaaS  
- Merging CoStar into Census  

### Success metrics

| Metric | Target (directional) |
| --- | --- |
| Hotel identities resolved | ≥95% |
| Probable owner | ≥60% |
| High-confidence owner | ≥30% |
| Ownership group | ≥40% of those with owner |
| Principal identified | ≥35% |
| Useful contact | ≥25% |
| Portfolio ≥1 other hotel | ≥40% of owners |
| Avg sources / owner relationship | ≥2 |
| Primary/official source share | ≥40% |
| False-positive rate (manual) | ≤20% high-conf set |
| Cost / hotel | Track Webhound+SerpAPI $ |
| Time / hotel | Track wall-clock |

### Safe test already run (this audit)

No-cost inspection confirmed:

- Future contracts include `owner_get` / `owner_portfolio` / `opportunity_search` but are unimplemented  
- `MAP_CENSUS_FIELDS` has **zero** owner keys  
- Canonical hotel has empty `owner` / `operator` slots  
- MVP fields exclude owner  

Did **not** burn SerpAPI/Webhound budget for live owner pulls.

---

## 13. Founder recommendation

```text
RECOMMEND_EXTEND_EXISTING_STACK
```

**Why not CONNECT only?** GTM owner stack is real but CoStar-firewalled and not Census/`dhl_*`-keyed; HI owner tools are stubs. Connecting alone cannot produce product-safe ByStreet outputs.

**Why not NEW LAYER?** The expensive pieces (census, identity, evidence, providers, Webhound, opportunity schemas, contact verification patterns) already exist. A greenfield “owner agent” would duplicate them.

**Why EXTEND?** Smallest proof is an **owner-intel staging layer** on top of HI identity + evidence patterns + selective Webhound/SerpAPI, learning from GTM Decision Radar vocabulary — then decide whether to promote `owner_get` into the MCP.

---

## Data Contract Snapshot (audit touchpoints)

| Area | Tables / stores |
| --- | --- |
| Product census | Hotel Property Census (`MAP_HOTEL_PROPERTY_CENSUS`) |
| HI local | evidence store, review queue, staged discovery |
| GTM | Owner Targets, Properties, Contacts, Decision Opportunities, Decision Opportunity Evidence |
| PI | Source Library / Extracted Facts (brand/operator) |

**Change Impact Classification:** Low (documentation/audit only).

**Regression checklist:** N/A — no code/schema changes in this task beyond this report file.

---

## Manual QA (if validating audit claims)

1. Open `mcp/hotel-intelligence/server.js` — confirm tool list matches §4.  
2. Open `lib/hotel-intelligence/index.js` — confirm `FUTURE_TOOL_CONTRACTS` owner tools.  
3. Open `map_hotel_intelligence_fields.js` — confirm no owner census fields.  
4. Open `docs/gtm-owner-target-list.md` + `docs/gtm-decision-radar.md` — confirm GTM parallel stack.  
5. Confirm Webhound tools available in Cursor MCP (external).  
6. Skim `data/webhound-mexico-owner-intel-test1-prompt.txt` — ByStreet-adjacent research design.

---

*End of audit.*
