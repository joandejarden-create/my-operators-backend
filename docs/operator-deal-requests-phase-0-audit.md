# My Operator Deals — Phase 0 audit & schema proposal

**Status:** Phase 0 complete · Phase 1 shell in progress  
**Date:** 2026-05-29  
**Mirror reference:** Brand Deal Requests (BDR) → proposed Operator Deal Requests (ODR)

---

## 1. Brand Deal Requests architecture audit

### 1.1 Airtable tables & env

| Table | Env var | Default name |
|-------|---------|--------------|
| Brand Deal Requests | `AIRTABLE_TABLE_BRAND_DEAL_REQUESTS` | `Brand Deal Requests` |
| Deal Activity Log | `AIRTABLE_TABLE_DEAL_ACTIVITY_LOG` | `Deal Activity Log` |
| Communication Log | `AIRTABLE_TABLE_COMMUNICATION_LOG` | `Communication Log` |
| Threads / Messages | `AIRTABLE_TABLE_THREADS`, `AIRTABLE_TABLE_MESSAGES` | Outreach Hub |
| Deals | `AIRTABLE_TABLE_DEALS` | `Deals` |
| Proposal Submissions | `AIRTABLE_TABLE_PROPOSAL_SUBMISSIONS` | `Proposal Submissions` |

**Source:** `api/brand-deal-requests.js` constants.

### 1.2 BDR field mapping (`mapBdrToResponse`)

| Airtable field | API response key | Notes |
|----------------|------------------|-------|
| Deal (link) | `dealId` | First linked record id |
| Brand Name | `brandName` | Scopes list/activity to brand org |
| Status | `status` | Pipeline single-select |
| Request Sent At | `requestSentAt` | KPI “requests sent (7d)” |
| Response Date | `responseDate` | Set on accept/decline |
| Response Notes | `responseNotes` | Brand-facing response |
| Match Score | `matchScore` | Brand fit score at outreach |
| Created At / Last Updated | `createdAt`, `lastUpdated` | Activity sorting |
| Owner Notes | `ownerNotes` | Owner-side notes on row |
| Next Follow-up Notes (Internal) | `brandInternalNotes` | Team-only (legacy: Brand Internal Notes) |
| Next Follow-up Date / Header | `nextFollowupDate`, `nextFollowupHeader` | Follow-up scheduling |
| Next Follow-up Notes (External) | `nextFollowupNotes` | Owner-visible follow-up |
| NDA Required? / NDA Status / timestamps / files | `ndaRequired`, `ndaStatus`, … | NDA flow |
| Deal Room Access / Granted / Revoked | `dealRoomAccess`, … | Deal room gating |
| Proposal Status + ~40 proposal fields | `proposal` object | **Out of scope for ODR v1** |
| External CRM * (optional) | `externalCrmId`, … | Optional sync readiness |

### 1.3 API routes (BDR)

| Method | Path | Handler | Purpose |
|--------|------|---------|---------|
| POST | `/api/brand-deal-requests` | `createRequest` | Owner creates outreach row |
| GET | `/api/brand-deal-requests?brand=` | `listForBrand` | Brand workspace list |
| GET | `/api/brand-deal-requests?all=1` | `listAll` | All contacted deals (BDD bootstrap) |
| GET | `/api/brand-deal-requests?dealIds=` | `listForDeals` | Batch by deal ids |
| POST | `/api/brand-deal-requests/by-deals` | `listForDealsPost` | POST variant |
| GET | `/api/brand-deal-requests?dealRoom=1` | `listForDealRoom` | Deal Room (Brand) |
| GET | `/api/brand-deal-requests/deal-meta?ids=` | `getDealMetaBatch` | Deal titles for rows |
| GET | `/api/brand-deal-requests/activity` | `getActivityLog` | `?brand=` or `?dealIds=` |
| GET | `/api/brand-deal-requests/:requestId` | `getById` | Single request |
| PATCH | `/api/brand-deal-requests/:requestId` | `updateStatus` | Status, notes, NDA actions |
| POST | `/api/brand-deal-requests/bulk-update` | `bulkUpdateStatus` | Bulk status |
| GET/POST | `/api/brand-workspace/kpi-history` | KPI weekly snapshots | Brand workspace KPI trends |
| GET | `…/:requestId/proposal-draft` | `getProposalDraft` | **Defer for ODR** |
| POST | `…/:requestId/submit-proposal` | `submitProposal` | **Defer for ODR** |

### 1.4 Status transitions (PATCH / bulk)

**Pipeline statuses** (allowed on update):

`New`, `Viewed`, `Brand Viewed`, `Sent / Awaiting Response`, `Accepted`, `Declined`, `Archived`, `Responded - Accepted`, `Responded - Declined`, `More Info Requested`, `Revisit Later`, `Pre-LOI`, `Pre-LOI / Term Comparison`, `Finalist`, `Deal Room Active`, `Feasibility`, `Feasibility In Progress`, `LOI Signed`, `LOI Signed / Platform Exit`

**NDA actions** (PATCH `action`):

`sendNda`, `markSigned`, `grantAccess`, `revokeAccess`

**Activity logged on transition** (examples):

| Status / action | Activity Action label | Stakeholder |
|-----------------|----------------------|-------------|
| Create | Request Sent | Owner |
| Viewed / Brand Viewed | Opportunity reviewed | Brand |
| Accepted | Marked interested | Brand |
| More Info Requested | Information requested | Brand |
| Declined | Declined | Brand |
| NDA Sent | NDA Sent | Owner |
| Deal Room Access Granted | Deal Room Access Granted | Owner |
| Follow-up | Follow-up scheduled | Owner or Brand |
| Proposal * | Proposal Updated / Submitted | Brand |

### 1.5 Activity log shape

**Table:** Deal Activity Log  
**Write fields:** `Deal`, `Brand Name`, `Stakeholder`, `Action`, `Details`, `Created At`, optional `Subject`, `Message_Summary`  
**Read filter:** by `Brand Name` and/or linked `Deal` record ids  
**BDD tab:** Deal Activity Log — up to 250 entries, stage-scoped or all-deals scope

### 1.6 KPI & pipeline logic

**Module:** `lib/deal-workspace-pipeline.js`

| Function | Role |
|----------|------|
| `deriveWorkspaceBucket(row)` | Maps status → tab bucket: `new`, `active-review`, `awaiting-info`, `nda-room`, `terms-proposal`, `advanced`, `archived` |
| `deriveBrandNextAction` / `deriveOwnerNextAction` | Persona “next step” labels |
| `computeWorkspaceKpiSnapshot(rows, persona)` | Needs action, awaiting counterparty, at risk, new (7d), pipeline counts |
| `isStalledRow` | Overdue follow-up or 14+ days no activity |
| Mirror rule | Owner “Awaiting brand” = Brand “Brand action”; Owner “Owner action” = Brand “Awaiting owner” |

**UI:** `public/js/deal-workspace-insights.js` — renders KPI strip; syncs to `/api/brand-workspace/kpi-history`.

### 1.7 Frontend tabs & filters (My Brand Deals)

**Route:** `/brand-development-dashboard`  
**Files:** `public/brand-development-dashboard.html`, `public/brand-development-dashboard.js`

| Tab id | Label |
|--------|-------|
| `bdd-new` | New opportunities |
| `bdd-active-review` | Active brand review |
| `bdd-awaiting-info` | Awaiting owner info |
| `bdd-nda-room` | NDA & deal room |
| `bdd-terms-proposal` | Terms & proposal |
| `bdd-advanced` | Finalist & advanced |
| `bdd-archived` | Declined & archived |
| `bdd-deal-log` | Deal Activity Log |

**Filters:** Brand, Status, Match Score band, Property Type, Country, Reset View  
**Row actions:** Interested / Request Info / Decline (new tab); NDA, deal room, proposal (later tabs)  
**Data load:** `GET /api/brand-deal-requests?all=1` + activity + optional `/api/my-deals` (403 for brand role today — known gap)

---

## 2. Proposed Operator Deal Requests schema

> **All rows below marked `[NEW TABLE]` or `[NEW FIELD]` until created in Airtable.**  
> Do not write to these fields until schema is confirmed.

### 2.1 Table: Operator Deal Requests `[NEW TABLE]`

**Proposed env:** `AIRTABLE_TABLE_OPERATOR_DEAL_REQUESTS=Operator Deal Requests`

| Field | Type | Status | BDR equivalent | Notes |
|-------|------|--------|----------------|-------|
| Deal | Link → Deals | `[NEW FIELD]` | Deal | Required |
| Operating Company Name | Single line text | `[NEW FIELD]` | Brand Name | Scopes requests to operator org (display name) |
| Operator Setup | Link → Operator Setup | `[NEW FIELD]` | — | Optional stable link to operator profile record |
| Status | Single select | `[NEW FIELD]` | Status | Same option set as BDR (see §2.2) |
| Alignment Score | Number | `[NEW FIELD]` | Match Score | OAS total at request time — **not** a recommendation |
| Alignment Band | Single select | `[NEW FIELD]` | — | Strong / Moderate / Conditional / Limited / Insufficient |
| Data Confidence | Single select | `[NEW FIELD]` | — | From OAS meta — highlights **data gaps** |
| Request Sent At | Date/time | `[NEW FIELD]` | Request Sent At | |
| Response Date | Date/time | `[NEW FIELD]` | Response Date | |
| Response Notes | Long text | `[NEW FIELD]` | Response Notes | Operator response to owner |
| Created At | Date/time | `[NEW FIELD]` | Created At | |
| Last Updated | Date/time | `[NEW FIELD]` | Last Updated | |
| Owner Notes | Long text | `[NEW FIELD]` | Owner Notes | |
| Next Follow-up Notes (Internal) | Long text | `[NEW FIELD]` | Next Follow-up Notes (Internal) | Operator team-only |
| Next Follow-up Notes (External) | Long text | `[NEW FIELD]` | Next Follow-up Notes (External) | Owner-visible |
| Next Follow-up Date | Date | `[NEW FIELD]` | Next Follow-up Date | |
| Next Follow-up Header | Single line text | `[NEW FIELD]` | Next Follow-up Header | |
| NDA Required? | Checkbox | `[NEW FIELD]` | NDA Required? | Phase 3+ |
| NDA Status | Single select | `[NEW FIELD]` | NDA Status | Same options as BDR |
| NDA Sent At / Signed At | Date/time | `[NEW FIELD]` | same | Phase 3+ |
| NDA Sent File / Signed File | Attachment | `[NEW FIELD]` | same | Phase 3+ |
| Deal Room Access | Single select | `[NEW FIELD]` | Deal Room Access | Blocked / Granted / Revoked — **Operator Deal Room deferred** |
| Access Granted At / Revoked At | Date/time | `[NEW FIELD]` | same | Deferred |
| Stage | Formula or select | `[NEW FIELD]` | Stage | Optional derived stage label |
| External CRM ID / sync fields | Text/select | `[NEW FIELD]` | optional CRM fields | Optional |

**Explicitly excluded from ODR v1:** Proposal Status, proposal economics fields, submit-proposal flow.

### 2.2 Status options (proposed — mirror BDR)

Use the **same lifecycle labels** as BDR for shared pipeline logic, with operator-facing copy in UI only:

- Intake: `New`, `Sent / Awaiting Response`, `Operator Viewed` `[NEW OPTION]` (parallel to Brand Viewed), `Viewed`
- Review: `More Info Requested`, `Accepted`, `Responded - Accepted`, `Revisit Later`
- Advance: `Pre-LOI`, `Pre-LOI / Term Comparison`, `Finalist`, `Deal Room Active`, `Feasibility`, `Feasibility In Progress`, `LOI Signed`, `LOI Signed / Platform Exit`
- Close: `Declined`, `Responded - Declined`, `Archived`

> **Dependency:** Confirm whether Airtable should reuse `Brand Viewed` verbatim or add `Operator Viewed`. Pipeline code should accept both during transition.

### 2.3 Activity log extension

**Option A (recommended):** Reuse **Deal Activity Log** with new fields:

| Field | Status | Purpose |
|-------|--------|---------|
| Operating Company Name | `[NEW FIELD]` | Filter activity by operator (parallel to Brand Name) |
| Stakeholder | existing | Add `Operator` as allowed option |

**Option B:** Separate Operator Deal Activity Log table — higher isolation, more duplication.

### 2.4 Conceptual reuse vs operator-specific naming

| Concept | BDR | ODR |
|---------|-----|-----|
| Counterparty scope key | Brand Name | Operating Company Name |
| Fit score at intake | Match Score | Alignment Score |
| Fit detail surface | Brand match breakdown | Operator Alignment Snapshot |
| Capability context | Brand library / standards | Operator Capability Snapshot |
| Persona “needs action” | Brand action | Operator action |
| Owner waiting state | Awaiting brand | Awaiting operator |
| Internal team notes | Brand internal notes | Operator internal notes |
| Proposal / LOI economics | Full proposal draft | **Out of scope v1** |

---

## 3. API design: `/api/operator-deal-requests`

**Module:** `api/operator-deal-requests.js`  
**Mapping object:** `MAP_ODR_AIRTABLE` (central field names — no scattered strings)

### Phase 1 (stub — implemented)

| Method | Path | Behavior |
|--------|------|----------|
| GET | `/api/operator-deal-requests` | Returns `{ success, requests: [], meta: { phase: "1", tableConfigured, message } }` |
| GET | `/api/operator-deal-requests/activity` | Returns `{ success, entries: [], meta }` |

Auth: `memberstackAuth`, `requireDealalityUser`, `requireOperatorDealsAccess` (operator + admin).

### Phase 2 (planned)

| Method | Path | Body / query |
|--------|------|--------------|
| POST | `/api/operator-deal-requests` | `{ dealId, operatingCompanyName, operatorSetupId?, alignmentScore?, alignmentBand? }` |
| GET | `?operator=` | List for logged-in operating company |
| GET | `?all=1` | Admin / bootstrap |
| GET | `?dealIds=` | Batch |
| GET | `/deal-meta?ids=` | Deal project names |
| GET | `/activity?operator=` or `?dealIds=` | Activity log |
| GET | `/:requestId` | Single row |
| PATCH | `/:requestId` | `{ status?, responseNotes?, …, action? }` — **no proposal actions in v1** |
| POST | `/bulk-update` | `{ updates: [{ requestId, status }] }` |

### Phase 3+ (deferred)

- NDA actions, deal room access, operator workspace KPI history endpoint
- Operator Deal Room document APIs

---

## 4. Routing & nav (Phase 1)

| Item | Value |
|------|-------|
| Route | `/operator-development-dashboard` |
| HTML | `/operator-development-dashboard.html` |
| Nav label | My Operator Deals |
| Roles | `operator`, `admin` |
| Default home (operator role) | `/operator-development-dashboard` (mirrors brand) |
| Aliases | `/operator-development-dashboard.html`, `/my-operator-deals` → route (optional) |

---

## 5. Step-by-step implementation plan

| Phase | Scope | Deliverables |
|-------|--------|--------------|
| **0** | Audit + schema | This document; stakeholder sign-off on ODR table |
| **1** | Shell | Route, nav, HTML/JS shell, stub GET API, empty KPI/tabs, honest empty states |
| **2** | ODR API + Airtable | Create table, full CRUD/list, activity log writes, field mapping + validation |
| **2b** | Pipeline persona | Extend `deal-workspace-pipeline.js` + insights for `operator` persona; `Operator Viewed` status |
| **3** | Workspace table | Row rendering, filters, bulk status, alignment snapshot CTAs |
| **3b** | Owner write path | Owner My Deals → create ODR on operator contact/shortlist |
| **4** | NDA / shared workspace | NDA actions only (no full Operator Deal Room until separate spec) |
| **5** | KPI history | `/api/operator-workspace/kpi-history` or generalized scope |

---

## 6. Files to change (by phase)

### Phase 1 (this sprint)

| File | Change |
|------|--------|
| `docs/operator-deal-requests-phase-0-audit.md` | Phase 0 deliverable |
| `public/operator-development-dashboard.html` | New page shell |
| `public/operator-development-dashboard.js` | Tab shell, empty states, stub fetch |
| `public/css/operator-development-dashboard.css` | Layout styles (shared visual language) |
| `public/app.js` | ROUTES, NAV_SECTIONS, ROUTE_ALIASES, `getDefaultRoute` |
| `public/js/deal-workspace-insights.js` | `operator` KPI_CONFIG |
| `lib/deal-workspace-pipeline.js` | `operator` persona in snapshot |
| `middleware/requireOperatorDealsAccess.js` | New gate |
| `api/operator-deal-requests.js` | Stub list + activity |
| `server.js` | Static route + API mount |
| `.env.example` | `AIRTABLE_TABLE_OPERATOR_DEAL_REQUESTS` |

### Phase 2+

| File | Change |
|------|--------|
| `api/operator-deal-requests.js` | Full parity with BDR (minus proposal) |
| `public/operator-development-dashboard.js` | Table, actions, modals |
| `lib/deal-workspace-pipeline.js` | Operator-specific next-action labels |
| `api/operator-workspace-kpi-history.js` | KPI snapshots |
| Owner outreach modules | Create ODR on operator contact |
| `public/dealality-webflow-nav.js` | Navigate helpers |

---

## 7. Risks & dependencies

| Risk | Severity | Dependency / mitigation |
|------|----------|-------------------------|
| ODR table not in Airtable | **High** | Phase 0 sign-off; Phase 1 uses stub API with `tableConfigured: false` |
| Operator user ↔ company mapping | **High** | Company Settings / Operator Setup link — define before Phase 2 list filter |
| No owner → operator request creation | **High** | Phase 2b; empty state explains expected flow |
| Activity log `Brand Name` field model | **Medium** | Add `Operating Company Name` column or generic “Counterparty Name” |
| Status option `Operator Viewed` vs `Brand Viewed` | **Medium** | Pipeline accepts both; document in Airtable |
| BDD dependency on `/api/my-deals` (403) | **Low** for ODR | ODR uses deal-meta batch only |
| Forking 11k lines of BDD | **Medium** | Phase 1 shell only; extract shared modules in Phase 3 |
| Proposal / deal room scope creep | **Medium** | Explicitly out of Phase 1–2 per product brief |

---

## 8. Phase 1 manual QA checklist

- [ ] `/app#/operator-development-dashboard` loads in app shell (dev workspace: operator role)
- [ ] Nav shows **My Operator Deals** for operator + admin; hidden for owner + brand
- [ ] KPI strip shows zeros; no fake sample counts
- [ ] Banner/empty state explains owner-initiated flow; uses operator product language
- [ ] All 8 tabs switch; Deal Activity Log shows placeholder empty state
- [ ] `GET /api/operator-deal-requests` returns empty list + `meta.phase`
- [ ] Operator role cannot access stub via owner session without admin/operator role (403)
