# AH Hospitality Advisors × AO — Collaboration Framework

**Version:** 0.5 (draft for Dean Auburn & Osama)  
**Date:** 16 July 2026  
**Status:** For discussion — not a binding agreement  
**Prepared by:** Joan / AO (A-O entity, US)  
**Updated after:** AH–AO alignment calls (Jul 7 + Jul 16, 2026)

---

## 1. Purpose

This document describes how **AH Hospitality Advisors** (UK, client-facing) and **AO** (US, technology subcontractor) propose to work together to design, build, and operate a **tech-enabled commercial performance hub** for AH hotel clients — starting with one **incubation property** (current owner-operated Courtyard mandate).

**Goal:** Give owners and AH advisors a single, trusted view of the KPIs that matter for commercial decisions — without AH becoming a software company.

**Dashboard #1 (agreed direction, Jul 16):** **Actuals · Budget · Forecast by segment** — with **segmentation + channel** as the foundation. This drives everything else (including pace later). Pace is intentionally **not** the first dashboard: a single pace snapshot is weak until history accumulates; ABF by segment is more static and usable sooner.

**Explicitly out of scope for this engagement:** Dealality (deal-flow platform). AO and Dealality are separate entities. This commercial hub must be able to **stand alone** (clear costs, portable data/config) — even if, later, it could optionally plug into a broader AO lifecycle vision. Client-facing deliverable remains under the **AH** brand unless AH decides otherwise.

---

## 2. Guiding principles

| Principle | What it means in practice |
|-----------|---------------------------|
| **AH owns the client** | AH leads discovery, commercial strategy, client relationship, invoicing, and ongoing advisory. The client sees AH only. |
| **AO builds under AH flag** | AO subcontracts to AH; works on AH email/tools where agreed; does not market AO to the end client without AH consent. |
| **One dashboard, phased** | MVP = one signed dashboard on one hotel. Enhancements are quoted per phase — no open-ended scope. |
| **Foundation before pace** | Segmentation (and channel) first → ABF by segment → then pace / overlays. Stairway-class snapshot history is a later horizon. |
| **Under-promise, over-deliver** | Fixed phase boundaries, signed wireframes, and change-control before new widgets or data sources. |
| **Data factory first** | The hard work is ingest → clean → standardize → store → visualize. Short term: **standardized upload**, not live PMS integrations. |
| **Client-defined rules** | Segmentation, fiscal calendar, and KPI definitions are configured per client — not hard-coded to one brand’s reporting pack. Mapping library (codes/keywords → segments) with an **Other** catchall. |
| **Stand-alone platform** | Commercial hub is its own stack (hosting, storage, costs visible). Not mixed into Dealality. Portable enough that someone else could operate it later without a forced migration. |
| **EU-first compliance** | Design for **EU GDPR** (AH’s current commercial clients include EU). UK/GDPR alignment as needed. **EU AI Act** only when AI features are introduced (MVP is rules/code-based). |
| **Commentary is first-class** | Users can add context (events, comp set notes, operational colour) that feeds owner narrative and any future LLM prompts. |
| **Dashboards connect** | Pace, forecast, budget, and comp context should overlay over time — not sit as isolated silos (after foundation is live). |

---

## 3. Parties & roles

### AH Hospitality Advisors (prime)

- Owns end-client contract and relationship start-to-finish  
- Defines **what** the dashboard must answer (KPIs, segmentation, cadence, owner narrative tone)  
- Runs client workshops, owner meetings, and sign-off on wireframes  
- Manages weekly/monthly refresh responsibility (AH staff, client staff, or AO via retainer)  
- Invoices client; pays AO per subcontractor agreement  

### AO / Joan (subcontractor)

- **Technical lead & builder:** architecture, data factory, hosting, UI, refresh automation  
- Translates AH commercial expertise into wireframes, data models, and working dashboards  
- Documents source map, validation rules, and runbooks  
- Does **not** provide revenue-management or sales strategy advice to the end client unless AH explicitly requests and brands it as AH  

### End client (hotel / owner)

- Provides data exports (or grants access for AH/AO to pull agreed extracts)  
- Validates segmentation, fiscal calendar, and KPI definitions  
- Uses dashboard for decisions; provides commentary where context is needed  

---

## 4. Commercial & legal structure

### Recommended structure (aligned on call)

**AH = prime contractor to the hotel owner**  
**AO = subcontractor to AH**, working under the AH flag for this engagement.

| Topic | Proposed approach |
|-------|-------------------|
| **Contracting** | AH ↔ Client (MSA/SOW). AH ↔ AO (subcontractor agreement + SOW per phase). |
| **Insurance & liability** | AH carries client-facing professional liability as prime; AO carries dev/E&O as subcontractor. Details to be confirmed with each party’s broker. |
| **Branding** | **Default:** client-facing product is **AH Hospitality Advisors** (commercial / performance). No Dealality branding on this deliverable. Co-brand “Powered by AO” only if AH wants it. Branding for a future dual-use (commercial hub vs deal-flow) is a separate decision — see §4e. |
| **Invoicing** | AH invoices client for discovery + build + optional retainer. AH pays AO per subcontractor rates and milestones. |
| **Entity** | AO = A-O entity (US). AH = UK entity. Cross-border payments via AH’s existing banking. |

### Ownership summary (read this first)

**There is no single owner of "the dashboard." Ownership splits across three layers — by design:**

- **AH owns the client** — the relationship, the advisory, the invoicing. The hotel owner contracts with and sees AH only.
- **The hotel owner owns their data** — pace, PMS exports, forecasts, actuals. AH and AO only process it.
- **AO owns the platform** — the data factory, parsers, reusable UI shell, and code. AH receives a **non-exclusive license** to deploy it for AH clients during the partnership.
- **The configured dashboard instance** (this hotel's KPIs, layout, segmentation) is a **licensed deliverable** the client may use for the duration of the AH mandate — not a software product the owner buys outright.

In one line: **AH owns the client · the owner owns the data · AO owns the technology · the client licenses the running instance.**

### IP & licensing (to agree in subcontract)

| Asset | Default proposal |
|-------|------------------|
| **Client-specific data** | Owned by client; AH/AO are processors under client engagement terms. |
| **Dashboard configuration** (KPI layout, segmentation maps, commentary templates for *this* client) | Licensed to client for the term of AH mandate; AH retains advisory methodology. |
| **AO platform components** (ingest parsers, factory patterns, reusable UI shell) | Remain AO IP; AH receives a **non-exclusive license** to deploy for AH clients during the partnership and for properties under active AH mandates. |
| **New parsers / integrations** funded by AH or client | AH gets use rights for AH clients; AO may reuse generic patterns in other work (no client confidential data). |
| **Third-party data** (STR, Lighthouse, PMS exports, etc.) | Subject to each vendor’s license; AH/client responsible for entitlements. |

### What happens if AH and AO part ways (post-termination license)

| Question | Default position |
|----------|------------------|
| **Do existing dashboards keep running?** | Yes for **active AH mandates at termination** — AH retains a license to keep running those specific deployments, subject to a maintenance arrangement (AO retainer or agreed handover). New properties after termination are not covered. |
| **Can AH hire another developer to maintain AO's code?** | No by default. AO platform code is not handed over. AH either keeps an AO maintenance license/retainer, or the deployment is frozen (runs as-is, no new development). |
| **Can AH take over / own the platform outright?** | Only via a separate **buyout** — priced independently, not included in build fees. AO is not obliged to sell. |
| **What does AH get on exit regardless?** | Handover package for active clients: runbook, data/source map, and an **export of the client's own data and configuration values** (not AO source code). |
| **Client data on exit** | Returned or deleted per Section 8; the owner's data is always theirs. |

**Rollout to additional properties:** Each new property = incremental SOW (data onboarding + config), not automatic unlimited use unless a master license fee is agreed.

### 4b. Pricing model — fixed vs percentage (recommended hybrid)

**Short answer (confirmed Jul 16):** Prefer a **clean first step** — **fixed fee + fixed timeline + signed deliverable** for Phase 0/1. Revisit % or deeper alignment **after** the first deliverable works. Use **percentage only as an optional alternative** later if both sides want it.

| Model | Best for | Pros | Cons |
|-------|----------|------|------|
| **Fixed fee (per phase)** | Phase 0, Phase 1, per-property rollout | Predictable for AH and AO; easy to budget; protects AO when data is messier than expected (if priced *after* Phase 0) | AH must agree a number; wide ranges ($15k–$40k) are not real quotes |
| **% of AH client fee** | Optional alternative when AH sets client price and wants one simple rule | Aligns when AH charges premium; AO participates in upside | AH may not disclose client fee; AO margin unknown until AH prices; encourages disputes if scope grows |
| **Fixed retainer (monthly)** | Phase 2 ongoing refresh/support | Clear SLA; no accounting gymnastics | Must define what's in vs out of scope |
| **Day rate / T&M** | Change requests only | Fair for unknown scope | Hard to sell to client; feels open-ended |

**Recommended structure (AO subcontract to AH):**

| Phase | AO fee model | Notes |
|-------|--------------|-------|
| **0 — Discovery** | **Fixed** | Always fixed; small; paid before deep technical work |
| **1 — MVP build** | **Fixed quote after Phase 0** | Not a range in the SOW — one number, tied to signed source map + wireframe |
| **2 — Retainer** | **Fixed monthly** | e.g. $2k–$6k/mo AO subcontract; AH marks up to client as they choose |
| **3 — New property** | **Fixed per property** (rate card) | Lower than Phase 1 because factory exists |

**Optional % alternative (if AH prefers later):** AO subcontract = **25–35% of AH's client-facing implementation fee** for Phase 1, with a **floor** (minimum AO fee) and **ceiling** (maximum unless change order). AH keeps advisory margin separately. Only use this if AH is willing to share client pricing transparently.

**What AH charges the hotel owner** is entirely AH's decision. This framework governs **AH ↔ AO**, not AH's client markup.

**Ongoing infra (order of magnitude):** Hosting/database for a single-property MVP is typically **low hundreds–low thousands USD per year** (not tens of thousands), subject to EU hosting choice and usage. Exact line items and transparency in Phase 0.

### 4c. Payment terms & milestones

| Item | Proposed default |
|------|------------------|
| **Currency** | USD (AO) invoiced to AH; AH may invoice client in GBP/EUR or USD per client contract |
| **Phase 0** | 50% on SOW signature · 50% on delivery of signed wireframe + source map + Phase 1 quote |
| **Phase 1** | 30% on SOW signature · 40% on data factory UAT (clean ingest on real files) · 30% on dashboard acceptance |
| **Retainer** | Monthly in advance; 30-day notice to pause or cancel |
| **Per-property rollout** | 50% on SOW · 50% on UAT sign-off |
| **Payment terms** | Net 15 from invoice (negotiable to Net 30) |
| **Late payment** | Work pauses after 15 days overdue; AH responsible for re-mobilisation fee if restart needed |
| **Expenses** | No travel/expenses unless pre-approved in writing |

### 4d. Exit, pause & dependency clauses

| Scenario | Treatment |
|----------|-----------|
| **Phase 0 kill** | If source inventory shows data is not viable for MVP (no reliable exports, no owner access), Phase 0 is still due. No obligation to proceed to Phase 1. Joint written summary of findings. |
| **Phase 1 pause** | Either party may request pause; AO invoices work completed to date per milestone schedule. |
| **AH loses client mandate mid-build** | Phase 1 fees due for work completed; AO delivers handover package (runbook, data map, export of config). No refund of completed milestones. |
| **Client / AH late on data** | Timeline extends day-for-day; if delay exceeds 30 days, AO may re-quote Phase 1 or pause at AH's cost to hold capacity. |
| **Change orders** | Written CR → impact on time/cost → AH approval before AO proceeds. Billable unless caused by AO error. |

### 4e. Client-facing branding, hosting & stand-alone stack

**Branding (Jul 16 discussion)**

- **Near-term default:** A&H product for the **commercial / performance** hub (AH brand, AH client trust).  
- **Dealality** stays separate for **deal-flow / development** — not mixed into this client deliverable.  
- Longer-term, both parties may want the commercial hub to **also** serve AO’s lifecycle vision — but only if the stack remains **stand-alone and portable** so costs and IP stay clear. Branding for that dual use is a future decision, not a Phase 1 requirement.

**Hosting (must be explicit — Dean raised this Jul 16)**

- Storage and platform must have a **clear owner** (default: AO-managed infra under AH branding; AH has operational access as agreed).  
- Hub is a **stand-alone** environment — not shared tables/cost pools with Dealality — so hosting cost is understandable and divisible.  
- Login URL, page title, and email notifications = **AH Hospitality Advisors** (unless AH requests co-brand).  
- AH approves any owner-facing demo or URL before first client meeting.  
- Design for **portability**: export of client data + config; ability for a future operator to continue without rewriting the whole product.

### 4f. Operating models after MVP (flexibility)

AH chooses per client (can change over time):

| Model | Description |
|-------|-------------|
| **A — Handoff** | Build complete; hotel/AH uploads to template; AO on call for break/fix or enhancements (change order). |
| **B — Managed refresh** | AH or AO pulls/processes weekly feeds into the factory; dashboard stays current. |
| **C — AO maintain & manage** | AO builds + ongoing ops; retainer covers refresh SLA + light support; enhancements quoted separately. |

No single “right” answer — **flexibility** matters. First engagement should prove Model A or B before assuming C.

### 4g. Reference & partnership (soft terms)

- AO may request **anonymized case study** rights after MVP go-live (AH approves wording).
- **First negotiation right:** AO gets first opportunity to quote Phase 1-equivalent work for the next AH property using the same factory (not exclusive forever; e.g. 90-day window per new mandate).

---

## 5. Phased delivery model

### Phase 0 — Discovery & signed wireframe *(fixed fee)*

**Objective:** Lock **Dashboard #1 = Actuals · Budget · Forecast by segment**, plus source map, segmentation/channel rules, and success criteria before build.

| Deliverable | Owner |
|-------------|-------|
| Data & source inventory (what exists, format, cadence, who pulls it) — **include 2–3 real sample files** | AH leads; AO documents |
| Segmentation dictionary + **channel** dimension (direct / indirect / other) + YoY mapping rules | AH |
| Code/keyword → segment **mapping library** (with **Other** catchall) | AH defines; AO documents |
| Fiscal calendar & budget/forecast input rules (entered once, used everywhere) | AH + client |
| Clickable wireframe / mockup sign-off for ABF-by-segment | AO builds; AH signs |
| Hosting region + estimated annual infra cost (EU-first) | AO |
| Phase 1 scope, timeline, and **single fixed price** | Joint |

**Exit criteria:** Written sign-off on wireframe + source map + mapping library outline + Phase 1 SOW.

**Pricing note:** Phase 0 is always **fixed fee**. The output of Phase 0 is a **single fixed Phase 1 quote** — not a range.

**Indicative range (USD, planning only — not a quote):** $3k–$8k

---

### Phase 1 — MVP: data factory + Dashboard #1 *(fixed fee — quoted after Phase 0)*

**Objective:** One working **Actuals · Budget · Forecast by segment** dashboard on the incubation hotel, with channel visibility where data allows, fed by standardized upload (and/or agreed extracts), hosted EU-compliant with secure logins.

**Prerequisite:** Phase 0 complete; real sample export files reviewed; Phase 1 SOW signed with **one fixed price**.

| Workstream | Scope |
|------------|--------|
| **Data factory** | Ingest for **agreed sources only**; validation; segment + channel mapping; clean tables; refresh log; missed-upload alerts |
| **Dashboard #1** | **Actuals · Budget · Forecast by segment** (+ channel cut / direct vs indirect where available) |
| **Core inputs** | Budget once; forecast once (transient vs group as required); segmentation config |
| **Commentary** | User notes on periods/events; stored with audit trail |
| **Owner narrative** | Rules-based summary for MVP (optional LLM in Phase 2+) |
| **Hosting** | Stand-alone stack; EU-region where required; AH + client logins; AH-branded entry |
| **Handover** | Runbook: who uploads what, when; how to refresh |

**Out of scope for MVP unless explicitly added:** Full **booking pace** workspace; Stairway-class multi-year daily snapshot history; live API to PMS/RMS; displacement calculator; portfolio roll-up; AI/LLM; additional dashboards; **new data sources not in Phase 0 source map**; replacing Stairway as RMS.

**Acceptance criteria (all required for sign-off):**

- [ ] Agreed sources ingest on **real client files** without manual rework each refresh  
- [ ] Segments + channel roll up to the same totals (reconciliation check)  
- [ ] Dashboard #1 matches signed wireframe (ABF by segment)  
- [ ] Budget and forecast inputs work as defined in Phase 0  
- [ ] Commentary saves with audit trail  
- [ ] Owner narrative generates from live data (rules-based)  
- [ ] AH + client logins work; AH branding on owner-facing URL  
- [ ] Runbook delivered and walkthrough completed  

**Exit criteria:** All acceptance criteria met; AH written sign-off; client demo complete (AH-led).

**Indicative range (USD, planning only — replaced by fixed quote after Phase 0):** $15k–$40k  
*Range depends on: number of sources, export messiness, segmentation/channel complexity, refresh cadence.*

---

### Phase 2 — Enhancements & overlays *(quoted per item)*

Examples (each scoped separately) — **after** ABF foundation is live:

- **Booking pace** (group / transient) built on agreed segmentation  
- Forecast accuracy / bias views  
- Pace + forecast + comp **overlay** for rate decisions  
- Market/comp overlay (e.g. Lighthouse)  
- LLM-assisted owner narrative (standardized prompts) — with EU AI Act review before go-live  
- Portfolio view across multiple AH mandates  
- Alerting / exception inbox  
- Closer-to-live integrations where licenses allow  

**Indicative retainer (ongoing refresh / light enhancements):** $2k–$6k/month *(fixed AO subcontract; AH sets client price independently)*

**Retainer includes (default):**

- Monitoring refresh log; fixing broken ingest on **existing** sources  
- Hosting + uptime for agreed environment  
- Up to **4 hours/month** minor UI or copy tweaks  
- Monthly check-in call (30 min)

**Retainer excludes (billable change order or new SOW):**

- New data source or parser  
- New dashboard tab or major widget (including first pace module)  
- Segmentation methodology change  
- Live API integration  
- Training beyond initial handover  

---

### Phase 3 — Rollout framework *(per property)*

When MVP is stable on the incubation hotel, **“ready to push”** means:

- [ ] Source map and upload templates documented and tested  
- [ ] Segmentation and fiscal rules configured for the new property  
- [ ] AH training complete (refresh + interpretation)  
- [ ] Client acceptance on UAT  
- [ ] Support model agreed (AH-only, or AH + AO retainer)  

**Per-property onboarding fee:** Quoted from a standard rate card after Phase 1 (typically lower than MVP because factory exists).

---

### Longer horizon — Stairway-class capability *(not Phase 1)*

**Context (Jul 16):** Incubation owner currently uses **Stairway** (~**€5,000/month**). Contract renews **September 2026** for another year; AH has told the owner they are **not ready** to replace Stairway now. Realistic replace window: **~September 2027**, and preferably with **more than one client** sharing the platform cost. Replacing Stairway also implies AH operational RMS capacity (e.g. revenue manager for group decisions / rapid response) — **advisory/ops**, not only software.

**What Stairway-class implies technically (later SOW):**

- Daily (or frequent) **snapshots** retained over time (pickup last 14 days, etc.)  
- Multi-year history + future OTB held in the factory  
- Decision workflows beyond ABF reporting  

**MVP does not commit** to Stairway replacement. Phase 1 builds the **scalable kit of parts** so that history/pace layers can be added without throwing away the foundation. Brand PMS/system changes (e.g. Marriott platform moves) may also change feed formats — factory must be adaptable.

---

## 6. Data architecture (agreed direction)

```
┌─────────────┐   ┌─────────────┐   ┌─────────────┐
│ Email / CSV │   │ Spreadsheets │   │  Future APIs │
│  extracts   │   │  (upload)    │   │  (Phase 2+)  │
└──────┬──────┘   └──────┬──────┘   └──────┬──────┘
       │                 │                 │
       └────────────────┼─────────────────┘
                        ▼
              ┌──────────────────┐
              │   DATA FACTORY    │
              │ validate · clean  │
              │ segment map lib   │
              │ channel map       │
              │ version / history │
              └────────┬─────────┘
                       ▼
              ┌──────────────────┐
              │  Standard DB      │
              │  actuals · bud    │
              │  fcst · segment   │
              │  channel · (pace) │
              └────────┬─────────┘
                       ▼
              ┌──────────────────┐
              │  Dashboard layer  │
              │  ABF by segment   │
              │  + commentary     │
              └──────────────────┘
```

**Short-term:** Properties upload to agreed templates (or AH/AO pulls weekly extracts).  
**Long-term:** Add parsers per system without changing the dashboard contract; add snapshot history for pace.

**Segmentation + channel (Jul 16):**

- **Segment** = selling/marketing pattern (e.g. group corp vs group other, retail, negotiated corporate) — client-defined; brands/RMS vendors often disagree (e.g. Stairway vs Marriott).  
- **Channel** = how the booking arrived (**direct vs indirect**); owner-critical for “value of the deal.” Often available in major brands; confirm hotel-level access in Phase 0.  
- Both dimensions must **reconcile to the same totals**; include an **Other** catchall.  
- Factory holds a **mapping library** (codes/keywords → segment/channel). YoY methodology shifts keep historic maps so comparisons remain valid.

---

## 7. Governance & change control

| Activity | Cadence | Participants |
|----------|---------|--------------|
| **Steering / scope check** | Biweekly during build; monthly in retainer | AH + AO |
| **Client demo / UAT** | End of each phase | AH leads; AO demos tech |
| **Change requests** | Written request → impact (time/cost) → AH approval → client if billable | AH gatekeeps client scope |
| **Scope creep rule** | No new widgets or data sources without amended SOW | All |

**Single points of contact:**

| Party | Role |
|-------|------|
| **AH** | Commercial lead + client gatekeeper (Dean / Osama) |
| **AO** | Technical lead + delivery (Joan) |

**Refresh ownership (to decide per client):**

| Model | When to use |
|-------|-------------|
| **Client uploads** | Hotel has capable revenue/sales admin |
| **AH uploads** | AH owns mandate; client is thin on property |
| **AO retainer** | High cadence, automation, or multiple properties |

---

## 8. Security, confidentiality & data protection

### Confidentiality

- Client data hosted in agreed region; access limited to named users  
- No client data used to train public models without written consent  
- AO does not contact end client directly without AH introduction  
- Subcontractor NDA + confidentiality terms in AH↔AO agreement  
- Dealality codebase, customer list, and product roadmap remain separate from this engagement  

### Data protection (EU-first)

AH’s near-term commercial clients include **EU** properties — design to the stricter EU bar.

| Topic | Proposed approach |
|-------|-------------------|
| **Roles** | Client = data controller (or AH on client's behalf per mandate). AH = processor or sub-processor per mandate. AO = processor to AH under subcontract. |
| **Hosting region** | **EU hosting default** for EU client data (confirm provider in Phase 0). |
| **DPA** | Reference AH↔client DPA; AH↔AO subcontract includes processor terms (Art. 28-style / UK equivalent). |
| **Retention** | Client operational data retained for mandate duration + **90 days** after termination unless client requests earlier deletion. |
| **Deletion** | On written request or contract end: delete or return client data; AO confirms in writing. |
| **Subprocessors** | AO lists hosting providers; AH approves material changes. |
| **EU AI Act** | MVP is **rules/code-based** (not AI). Before any LLM narrative or AI feature, jointly review EU AI Act obligations and document the approach. |

### Data accuracy & liability

- AO **cleans, validates, and presents** data; AO is **not** the system of record and does not guarantee accuracy of source exports.  
- Owner and AH **remain responsible for business decisions** made using the dashboard.  
- Dashboard outputs are **decision support**, not audited financial statements.  
- **AO liability cap:** total fees paid by AH to AO under the applicable SOW (standard subcontract cap — confirm with counsel).  
- Neither party liable for indirect or consequential loss (lost profit, lost mandate) except where law requires otherwise.

### Backups & incident response

- AO maintains reasonable backups of configuration and ingested data.  
- Security incident affecting client data: AO notifies AH within **48 hours**; AH owns client communication.  
- Planned maintenance: **48 hours** notice where possible.

---

## 9. Success metrics & acceptance (MVP)

Agreed in Phase 0; **at least two** become formal Phase 1 acceptance gates:

- Owner/advisor can answer **actual vs budget vs forecast by segment** without rebuilding spreadsheets  
- Segment + channel totals **reconcile** (no unexplained leakage outside Other)  
- Time to produce weekly commercial pack reduced (hours → minutes)  
- Client confirms KPIs match how they already think about the business  

---

## 10. Mutual homework (next steps)

### AH / Osama / Dean

- [ ] Confirm **Dashboard #1 = ABF by segment** (and any “performance test” KPIs to include in v1)  
- [ ] Draft segmentation + channel dictionary for the incubation hotel (and how Stairway/brand maps today)  
- [ ] Confirm **direct/indirect** availability at hotel level for this property  
- [ ] React to this framework (ownership, hosting stand-alone, branding, fixed-fee Phase 1)  
- [ ] Brief Osama; schedule 3-way regroup  
- [ ] Owner context: Stairway stay through ~Sep 2027 — keep MVP expectations clear  

### AO / Joan

- [x] External mockup link (GitHub Pages)  
- [ ] Send this **v0.4 framework** to Dean for reaction  
- [ ] Confirm AO entity, insurance, and subcontractor template  
- [ ] Phase 0 proposal + timeline once framework comments land  
- [ ] EU hosting options + rough annual cost line items  
- [ ] Upload template sketch for ABF + segment/channel once sample files arrive  

---

## 11. Delivery risks & assumptions

*Shared openly so A&H and AO plan Phase 0/1 with eyes open. These are management risks, not reasons to pause — they define what must be true for the fixed-fee path to work.*

### Assumptions (Phase 1 depends on these)

| # | Assumption |
|---|------------|
| A1 | Phase 1 starts only after Phase 0 delivers **2–3 real sample export files** and a signed source map. |
| A2 | Dashboard #1 remains **Actuals · Budget · Forecast by segment** (+ channel only where data is confirmed available). |
| A3 | Short-term ingest is **standardized upload / agreed extracts**, not live PMS/RMS APIs. |
| A4 | A&H owns the **segmentation + channel dictionary**; AO implements the signed mapping library (including Other). |
| A5 | The hub is a **stand-alone** stack (not mixed into Dealality), with EU hosting where required. |
| A6 | MVP is **rules/code-based** (no LLM/AI until a separate EU AI Act review). |
| A7 | Replacing Stairway / daily multi-year snapshot history is **out of Phase 1** (~Sep 2027 horizon). |
| A8 | A&H remains client-facing; AO does not provide on-property revenue-management staffing. |

### Delivery risks & mitigations

| Risk | Why it matters | Mitigation |
|------|----------------|------------|
| **Data quality / messiness unknown** | Until real files are seen, effort and the Phase 1 fixed price are estimates. Dirty multi-system exports can expand scope. | Phase 0 includes sample files; **Phase 1 is one fixed quote only after file review**. Late or unusable files → timeline flex or Phase 0 kill (no Phase 1 obligation). |
| **Scope creep into pace / Stairway-class** | Pace and daily snapshot history are a different product class and can consume the MVP. | Written SOW boundaries; pace/overlays = Phase 2+ change orders; Stairway replacement = separate later SOW. |
| **Segmentation / channel disagreement** | Brand, Stairway, and owner definitions often conflict; Cursor cannot resolve “whose map is truth.” | A&H signs the dictionary; AO implements only the signed map; reconciliation + Other catchall required. |
| **Direct/indirect not available at hotel level** | Channel views are owner-critical but access is unconfirmed for this property. | Confirm in Phase 0; if unavailable, ship ABF-by-segment without channel (or limited proxy) and quote channel as a change order. |
| **Client / A&H data latency** | Waiting on extracts looks like AO delay. | Day-for-day timeline extension; pause rights after extended delay (see §4d). |
| **EU hosting / GDPR / future AI** | Wrong region or premature AI creates compliance risk. | EU host chosen in Phase 0; DPA-style terms in subcontract; AI only after joint EU AI Act review. |
| **Operating-model mismatch** | If A&H expects AO to run Stairway-like RMS ops, that is staffing + advisory — not Phase 1 software. | Phase 1 = build + handoff or light refresh (Models A/B); Model C only under explicit retainer; RMS ops capacity remains A&H. |
| **Planning ranges read as quotes** | $15k–$40k can be treated as a firm offer before discovery. | All Phase 1 ranges are **planning anchors only**; the binding number is the post–Phase 0 fixed quote. |
| **Solo delivery / calendar risk** | AO is a lean delivery team; coding speed (including AI-assisted) does not remove QA, demos, or dependency waits. | Buffer calendar time; under-promise weeks; change control for new widgets/sources. |
| **Brand / PMS system changes** | Marriott (or other) platform moves can change feed formats mid-engagement. | Factory designed for adaptable parsers; format changes after Phase 0 = change order. |

### What “done” for Phase 1 does *not* mean

- Not a Stairway replacement  
- Not full booking-pace history  
- Not live PMS integration  
- Not AI-generated commercial advice  
- Not AO owning the client relationship or owner RM decisions  

---

## 12. Illustrative commercial summary

| Phase | Scope | Fee model (AO ↔ AH) | Planning anchor (USD) |
|-------|--------|---------------------|------------------------|
| 0 | Discovery + signed ABF wireframe | **Fixed** | $3k–$8k |
| 1 | Factory + ABF-by-segment dashboard (1 hotel) | **Fixed quote after Phase 0** | $15k–$40k (planning range only) |
| 2 | Pace / overlays / enhancements | **Fixed monthly retainer** + per-SOW | $2k–$6k/mo |
| 3 | Additional properties | **Fixed per property** (rate card) | TBD after Phase 1 |
| Later | Stairway-class snapshot platform | Separate SOW | TBD; multi-client preferred |

*Planning anchors are not binding quotes. Phase 1 becomes a single fixed price in the Phase 0 deliverable. AH sets client-facing prices independently. Infra hosting is typically low annual cost — itemize in Phase 0.*

**Optional % model (alternative to fixed Phase 1 subcontract):** 25–35% of AH's client implementation fee, with agreed floor and ceiling — only if both parties prefer it over fixed subcontract (preferred approach after first successful fixed deliverable).

---

## 13. Build effort reality (internal AO planning note)

*This section helps AO plan; may be removed from client-facing version.*

| Workstream | Relative effort | Notes |
|------------|-----------------|-------|
| Dashboard UI (ABF by segment) | ~20–25% | Mockup exists; retarget away from pace-first |
| Data factory + segment/channel maps | ~50–60% | Mapping library + reconciliation; messiness unknown until Phase 0 files seen |
| Hosting, auth, branding (EU stand-alone) | ~10–15% | Separate from Dealality; EU region |
| Commentary + owner narrative | ~5–10% | Rules-based MVP |
| Handover + runbook | ~5% | |

**Risk:** Fixed-price Phase 1 before seeing real export files. **Mitigation:** Phase 0 must include 2–3 actual client files; Phase 1 quote issued only after review.  
**Risk:** Scope creep into Stairway/pace too early. **Mitigation:** keep Phase 1 = ABF foundation only.

---

## 14. Document control

| Version | Date | Changes |
|---------|------|---------|
| 0.1 | 2026-07-07 | Initial draft post alignment call |
| 0.2 | 2026-07-07 | Pricing model (fixed vs %), payment terms, exit clauses, DPA/liability, retainer SLA, acceptance criteria, build effort note |
| 0.3 | 2026-07-07 | Ownership summary box + post-termination license terms |
| 0.4 | 2026-07-16 | Jul 16 call: Dashboard #1 = ABF by segment (not pace); segment+channel foundation; stand-alone hosting; EU-first GDPR; EU AI Act later; operating models A/B/C; Stairway ~Sep 2027 horizon; branding AH commercial vs Dealality; fixed-fee first step confirmed |
| 0.5 | 2026-07-16 | Delivery risks & assumptions section (for Word proposal / Dean review) |

**Next step:** Dean reviews with Osama → 3-way call to mark up this draft → Phase 0 kickoff with sample files.

---

*Questions or markups: reply to Joan or add comments inline before the alignment call.*
