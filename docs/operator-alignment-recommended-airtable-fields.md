# Operator Alignment — Recommended Airtable Fields (Phase 5B+)

**Date:** 2026-05-25  
**Companion:** [operator-alignment-scoring-data-quality-audit.md](./operator-alignment-scoring-data-quality-audit.md)  
**Scope:** Planning only — fields listed here are **not** created until Phase 5B is approved.

This document prioritizes schema additions and normalizations that fix **score credibility** (Priority 1), **narrative differentiation** (Priority 2), and **Explorer / shortlist** utility (Priority 3).

---

## Priority 1 — Needed to make scoring credible

These fields address the largest suppressors observed on deal `recIeGRZP21udmTnt`: franchise-only structure vs operator management profiles, must-have / service token mismatch, and geography scored as weak when data is missing.

| Table | Field name | Field type | Options (suggested) | Audience | Required? | Scoring? | Narrative? | Explorer? | Notes |
|-------|------------|------------|---------------------|----------|-----------|----------|------------|-----------|-------|
| **Market - Performance** (deal) | `Preferred Operator Management Structure` | Multiple select | Third-Party Management; Franchise + Operator; Franchise Only; Owner-Operated; Lease; Asset Management; To Be Confirmed | Owner-facing | **Required** when operator in scope | **Yes** — replaces conflation with brand franchise | **Yes** | Split from brand `Preferred Deal Structure`; map to operator `bf_selected_deal_structures` |
| **Strategic Intent** (deal) | `Operator Structure Intent` | Single select | Same option set as above | Owner-facing | Optional | **Yes** | **Yes** | Clarifies owner path when MP says Franchise Only |
| **Strategic Intent** (deal) | `Required Operator Services` | Multiple select | Align to `OPERATOR_SERVICE_GRANULAR` labels (Revenue Management; Sales & Marketing; Accounting & Reporting; HR & Training; Procurement; Technology; Pre-Opening; F&B Operations; …) | Owner-facing | **Required** when third-party in scope | **Yes** | **Yes** | Canonical token set for overlap |
| **Strategic Intent** (deal) | `Must-Have Operator Services` | Multiple select | Same vocabulary as Required Operator Services | Owner-facing | Optional | **Yes** | **Yes** | Migrate from free-text must-haves where possible |
| **Operator Setup - Platform & Markets** | `Active Countries` | Multiple select | ISO country list or curated CALA + US + EU subset | Operator-facing | **Required** for publish | **Yes** | **Yes** | **Yes** — country match without substring on city strings |
| **Operator Setup - Platform & Markets** | `Active Markets` | Multiple select | City/region tokens (Cancún, Mexico City, …) | Operator-facing | **Required** for publish | **Yes** | **Yes** | **Yes** | Replaces long-text-only `specificMarkets` for scoring |
| **Operator Setup - Platform & Markets** | `Market Presence Type` | Single select | Active Operations; Pipeline Only; Active + Pipeline | Operator-facing | Optional | **Yes** | **Yes** | **Yes** | Supports "active ops vs pipeline" copy |
| **Operator Setup - Commercial Fit** | `Management Structures Supported` | Multiple select | Same as deal structure options | Operator-facing | **Required** for publish | **Yes** | **Yes** | Partial | Explicit mirror of `bf_selected_deal_structures` |
| **Operator Setup - Master** (admin) | `Data Confidence Level` | Single select | High; Medium; Low; Unverified | Admin-only | Optional | **Yes** (cap/hide score) | **Yes** | Staleness / manual entry flag |
| **Operator Setup - Master** (admin) | `Profile Last Reviewed` | Date | — | Admin-only | Optional | **Yes** | Optional | No | Drives "data as of" |

**Normalization (no new columns, Phase 5B deliverable):**

| Artifact | Purpose |
|----------|---------|
| `lib/operator-alignment-normalize.js` (proposed) | Map `Franchise Only` vs `Third-Party Management` to compatibility matrix (neutral / mismatch / not applicable) |
| Service synonym table | Map legacy must-have strings → granular service ids |
| Country parser | Map "Mexico City, Cancún…" long text → `Active Countries` + `Active Markets` on import |

---

## Priority 2 — Needed to make narrative differentiated

| Table | Field name | Field type | Options (suggested) | Audience | Required? | Scoring? | Narrative? | Explorer? | Notes |
|-------|------------|------------|---------------------|----------|-----------|----------|------------|-----------|-------|
| **Strategic Intent** (deal) | `Market Presence Requirement` | Single select | Active Operations Required; Pipeline Acceptable; Either | Owner-facing | Optional | Partial | **Yes** | No | Drives geo narrative |
| **Strategic Intent** (deal) | `Brand Affiliation Path` | Single select | Unbranded; Soft Brand; Hard Brand; Franchise; Brand-Managed | Owner-facing | Optional | Partial | **Yes** | No | Pathway table |
| **Strategic Intent** (deal) | `Brand Operator Responsibility Split` | Multiple select | F&B; Revenue Management; Sales; HR; Procurement; Pre-Opening; … | Owner-facing | Optional | Future | **Yes** | No | Owner vs operator duties |
| **Strategic Intent** (deal) | `Owner Control Preference` | Single select | Hands-Off; Collaborative; Retain Control; To Be Confirmed | Owner-facing | Optional | **Yes** | **Yes** | Pairs with operator collaboration |
| **Strategic Intent** (deal) | `Owner Reporting Expectations` | Single select | Daily; Weekly; Monthly; Quarterly; Custom | Owner-facing | Optional | **Yes** | **Yes** | Single canonical field (retire duplicate SI fields in UI) |
| **Strategic Intent** (deal) | `Pre-Opening Support Needed` | Single select | Yes – Operator-Led; Yes – Shared; No; Unknown | Owner-facing | Optional | Partial | **Yes** | No | |
| **Deals** or **Location** | `Opening Timeline` | Single select | ≤12 mo; 12–24 mo; 24+ mo; TBD | Owner-facing | Optional | Partial | **Yes** | No | |
| **Strategic Intent** (deal) | `Commercial Priority` | Single select | Revenue Management; Distribution; Cost Control; Balanced | Owner-facing | Optional | Partial | **Yes** | No | |
| **Strategic Intent** (deal) | `F&B Complexity Level` | Single select | None; Limited; Full-Service; Complex Multi-Outlet | Owner-facing | Optional | Future | **Yes** | No | From `F&B Outlets?` + program |
| **Operator Setup - Profile** | `Brand Families Operated` | Multiple select | Hilton; Marriott; Hyatt; IHG; Choice; Independent; … | Operator-facing | Optional | **Yes** | **Yes** | **Yes** | Denormalized from brand links for display |
| **Operator Setup - Profile** | `Soft Brand Lifestyle Experience` | Single select | None; Limited; Core Capability | Operator-facing | Optional | Partial | **Yes** | **Yes** | |
| **Operator Setup - Commercial** | `New Build Opening Experience` | Single select | None; Limited; Extensive | Operator-facing | Optional | **Yes** | **Yes** | Partial | |
| **Operator Setup - Commercial** | `Conversion Reflag Experience` | Single select | None; Limited; Extensive | Operator-facing | Optional | **Yes** | **Yes** | Partial | |
| **Operator Setup - Commercial** | `Pre-Opening Support Capability` | Single select | None; Advisory; Full-Service | Operator-facing | Optional | **Yes** | **Yes** | Partial | |
| **Operator Setup - Governance** | `F&B Capability Level` | Single select | None; Limited; Full; Complex | Operator-facing | Optional | Future | **Yes** | Partial | |
| **Operator Setup - Governance** | `Revenue Management Capability` | Single select | None; Outsourced; In-House; Hybrid | Operator-facing | Optional | Partial | **Yes** | **Yes** | Aggregate granular RM |
| **Operator Setup - Governance** | `Sales Distribution Platform` | Single select | Centralized; Property-Led; Hybrid | Operator-facing | Optional | Partial | **Yes** | **Yes** | |
| **Operator Setup - Governance** | `Owner Reporting Level` | Single select | Daily; Weekly; Monthly; Quarterly; Custom | Operator-facing | Optional | **Yes** | **Yes** | **Yes** | Match deal reporting |
| **Operator Setup - Governance** | `Governance Cadence` | Single select | Board-Style; Investor Reporting; Operational Only | Operator-facing | Optional | Partial | **Yes** | Partial | |
| **Operator Setup - Commercial** | `Owner Collaboration Model` | Single select | Advisory; Collaborative; Operator-Led | Operator-facing | Optional | **Yes** | **Yes** | No | Replaces keyword guess on narrative |

**Existing fields to normalize (no rename — map in code):**

| Existing field | Action |
|----------------|--------|
| `Preferred Deal Structure` (MP) | Label in UI as brand/franchise economics; do not use alone for operator structure score |
| `Must-Haves From Brand/Operator` | Keep; add structured `Must-Have Operator Services`; migrate over time |
| `specificMarkets` (long text) | Backfill into `Active Markets` multi |
| `Preferred Third-Party Operator Profile` | Map to archetype ids in `fixtures/operator-profile-archetypes.json` |

---

## Priority 3 — Useful later for Operator Explorer / shortlist

| Table | Field name | Field type | Options (suggested) | Audience | Required? | Scoring? | Narrative? | Explorer? | Notes |
|-------|------------|------------|---------------------|----------|-----------|----------|------------|-----------|-------|
| **Strategic Intent** (deal) | `Operator Review Status` | Single select | Not Started; In Review; Shortlist; On Hold | Owner-facing | Optional | No | No | No | Workflow only |
| **Strategic Intent** (deal) | `Local Labor HR Support Needed` | Checkbox | — | Owner-facing | Optional | Future | Partial | No | |
| **Strategic Intent** (deal) | `Procurement Support Needed` | Checkbox | — | Owner-facing | Optional | Future | Partial | No | |
| **Strategic Intent** (deal) | `Owner Internal Ops Capability` | Single select | None; Limited; Strong | Owner-facing | Optional | Partial | Partial | No | Owner-operated pathway |
| **Operator Setup - Platform** | `Hotels in Market Count` | Number | — | Operator-facing | Optional | Low | Partial | **Yes** | Per primary market |
| **Operator Setup - Commercial** | `Minimum Key Count` | Number | — | Operator-facing | Optional | Filter | Partial | **Yes** | Shortlist filter |
| **Operator Setup - Commercial** | `Typical Fee Structure` | Single select | Base + Incentive; Base Only; Other; Not Disclosed | Operator-facing | Optional | Partial | Neutral mention | Partial | Not "fit" language |
| **Operator Setup - Commercial** | `Termination Flexibility` | Single select | Standard; Negotiable; Restrictive; Not Disclosed | Operator-facing | Optional | Review | Partial | No | Admin summary OK |
| **Operator Setup - Master** | `Profile Completeness Pct` | Formula or rollup | 0–100 | Admin-only | Auto | Gating | Partial | **Yes** | From required keys |
| **Case Studies** (child) | `Experience Tags` | Multiple select | New Build; Conversion; Airport; Select-Service; … | Operator-facing | Optional | Inference | **Yes** | **Yes** | Tag rows for narrative |

---

## Deal fields — quick reference (requested audit list)

| Requested field | Status | Existing name | Priority |
|-----------------|--------|---------------|----------|
| Operator Review Status | Partial | `Operator Strategy Status` | P3 (new workflow field optional) |
| Preferred Operator Type | Partial | `Preferred Third-Party Operator Profile` | P2 (normalize) |
| Required Operator Services | Partial → **Add** | `Services Required From Operator` + new structured field | **P1** |
| Must-Have Services | Partial → **Add** | `Must-Haves From Brand/Operator` + `Must-Have Operator Services` | **P1** |
| Preferred Management Structure | Partial → **Add** | `Preferred Operator Management Structure` | **P1** |
| Owner Control Preference | Partial → **Add** | `Owner Control Priorities` + new select | P2 |
| Owner Reporting Expectations | Partial → **Add** | consolidate to `Owner Reporting Expectations` | P2 |
| Pre-Opening Support Needed | Partial | priorities + `Opening / Transition Phase` | P2 |
| Opening Timeline | Partial → **Add** | `Opening Timeline` | P2 |
| Brand Affiliation Path | Partial → **Add** | — | P2 |
| Brand / Operator Responsibility Split | **Add** | — | P2 |
| F&B Complexity | Partial | `F&B Outlets?` + program | P2 |
| Commercial Priority | Partial → **Add** | importance fields | P2 |
| Market Presence Requirement | **Add** | — | P2 |
| Local Labor / HR Support Needed | Partial → **Add** | — | P3 |
| Procurement Support Needed | Partial → **Add** | — | P3 |
| Owner Internal Ops Capability | **Add** | — | P3 |

---

## Operator fields — quick reference (requested audit list)

| Requested field | Status | Existing name | Priority |
|-----------------|--------|---------------|----------|
| Active Countries | Partial → **Add** | footprint-derived | **P1** |
| Active Markets / Cities | Partial | `specificMarkets` | **P1** |
| Market Presence Type | **Add** | — | **P1** |
| Number of Hotels in Market | Partial → **Add** | footprint totals | P3 |
| Service Models Supported | Partial | `primaryServiceModel`, `bf_selected_deal_structures` | P1 normalize |
| Chain Scales Supported | Strong | `chainScalesSupported` | Keep |
| Preferred Asset Types | Strong | `bf_selected_asset_types` | Keep |
| New-Build Opening Experience | Partial → **Add** | situations / case studies | P2 |
| Conversion / Reflag Experience | Partial → **Add** | brand signals | P2 |
| Pre-Opening Support Capability | Partial → **Add** | granular / cap_kpi | P2 |
| Brand Families Operated | Strong | `brands` link | P2 denormalize |
| Soft Brand / Lifestyle Experience | Partial → **Add** | brand signals | P2 |
| F&B Capability Level | Partial → **Add** | granular | P2 |
| Revenue Management Capability | Partial → **Add** | `revenueManagementServices` | P2 |
| Sales Platform | Partial → **Add** | `salesMarketingSupport` | P2 |
| Owner Reporting Level | Partial → **Add** | cadence fields | P2 |
| Governance Cadence | Partial → **Add** | — | P2 |
| Management Structure Preference | Partial → **Add** | `Management Structures Supported` | **P1** |
| Minimum Key Count | **Add** | — | P3 |
| Typical Fee Structure | Partial → **Add** | narratives | P3 |
| Termination Flexibility | Partial | legacy Deal Terms | P3 |
| Similar Project Case Studies | Strong | Case Studies child | Keep |
| Data Confidence / Source Type | **Add** | — | **P1** (admin) |
| Last Updated Date | Partial | Airtable modified | **P1** (admin) |

---

## Risk summary by change type

| Change | Risk | Mitigation |
|--------|------|------------|
| New MP/SI fields | Deal intake PATCH drift | Update `deal-setup-fields.js`, aliases, readiness |
| Split franchise vs operator structure | Historical deals wrong band | Backfill script + "To Be Confirmed" |
| Service option vocabulary | Breaks must-have overlap | Synonym map + dual-read period |
| Operator multis required on publish | Blocks Explorer list | Gradual required + completeness % |
| Rename existing Airtable columns | High | **Do not rename** in 5B — add parallel fields |

---

## Phase 5B acceptance checklist

- [ ] Priority 1 fields created in Airtable (document field ids in repo env example or internal sheet)
- [ ] Operator Setup forms expose Active Countries, Active Markets, Management Structures Supported
- [ ] Deal intake exposes Preferred Operator Management Structure + structured services
- [ ] Normalization module exists but is **not** imported by `scoreOperatorMatchForDeal` yet
- [ ] Backfill guide for 10 active operators (markets + structure multis)
- [ ] `node scripts/audit-operator-alignment-scoring.mjs recIeGRZP21udmTnt` still runs (scores unchanged until 5E)

---

## Related implementation files (Phase 5B touch list)

| Area | Files |
|------|--------|
| Deal intake | `api/schemas/deal-setup-fields.js`, deal setup routes |
| Operator Setup read/write | `api/lib/operator-setup-new-base-read.js`, `third-party-operator-new-two-field-bindings.json`, `third-party-operator-basics-to-prefill.js` |
| Field catalog | `lib/third-party-operator-airtable-fields-used.js` |
| Docs | `docs/operator-alignment-field-matrix.md` (append changelog) |
