# Gate 1 — Founder Review (Revised: KGPV Golden Demo Clarification)

**Status:** AWAITING FOUNDER APPROVAL — substantive paid research **blocked**
**Audited:** 2026-11-19T14:18:15.354Z
**Runtime:** http://127.0.0.1:8091

## Architecture principle

**KGPV defines the desired experience. HPC and the appropriate established Airtable tables define the canonical intelligence.**

- KGPV = golden demonstration of UI structure, field coverage, content depth, and intended end-user experience.
- KGPV fixtures / showcase / local Contact Intelligence are **not** verified canonical Airtable data.
- Bethesda Marriott = Airtable-first research, mapping, integration, and persistence validation pilot.
- Do **not** migrate golden-demo fixtures into production Airtable without independent verification.

## Identity

| Hotel | Role | Owning Airtable table |
|---|---|---|
| KGPV (`recUNycnMwOVFX0hc`) | Golden demo UX | **Hotel Census** (`tblgj2qEwxjTcg6q0`) — not on HPC |
| Bethesda (`recLuxvwwxID7U2B8`) | Airtable-first pilot | **Hotel Property Census** (`tbl9aY5ijiuIzzWam`) |

Base: `appCCUsuGsE1ifoLk` (`AIRTABLE_BASE_ID_ALT`).

## Gate 1 scorecard (separated metrics)

| Metric | Value | Meaning |
|---|---:|---|
| **KGPV golden-demo field coverage** | **77.6%** (45/58 gold-required) | Demonstrated UI depth — **not** Airtable proof |
| **Bethesda existing verified Airtable coverage** | **63.5%** (40/63) | Facts already in HPC/HI (independent of HE wiring) |
| **Bethesda Airtable not yet connected to HE** | **15.9%** (10) | Exists in Airtable; HE not showing / not wired |
| **Bethesda live Airtable→UI integration** | **46%** (29/63) | HE actually serving verified HPC/HI values |
| **Genuine research gaps (after reuse)** | **23.8%** (15) | Missing in Airtable and HE — candidates for research post-integration |
| **UI populated without verified HPC field** | **0%** (0) | Overview shows values not present on primary HPC fields (enrichment/fallback) |
| **Integration work items before paid** | **13** fields | Must wire/fix provenance before enrichment spend |

## What already exists in Airtable (Bethesda)

HI relationships: Brand, Operator

HI orgs: Marriott International, Inc.

HI Org Contacts: 0 · Channels: 0 · Evidence: 0 · Runs: 0

### Existing Airtable not yet connected

- **portfolio_preview** (ownership) — Derived read-model — requires underlying HI facts + HE wiring
- **quality_summary** (contacts) — Derived read-model — requires underlying HI facts + HE wiring
- **ownershipSummary** (contacts) — Derived read-model — requires underlying HI facts + HE wiring
- **fallback_paths** (contacts) — HPC.Phone
- **ticker** (organization) — HI Organizations (Marriott International, Inc.)
- **portfolio_kpis** (organization) — Derived read-model — requires underlying HI facts + HE wiring
- **owned_controlled_hotels** (organization) — Derived read-model — requires underlying HI facts + HE wiring
- **operated_managed_hotels** (organization) — Derived read-model — requires underlying HI facts + HE wiring
- **why_this_matters** (organization) — HI Organizations (Marriott International, Inc.)
- **org_network** (relationships) — Derived read-model — requires underlying HI facts + HE wiring

### Genuine research gaps (after source reuse)

- **projectPhase** (hotel) — Not in Airtable and not in HE — research or steward fill after reuse
- **starRating** (hotel) — Not in Airtable and not in HE — research or steward fill after reuse
- **what_remains_uncertain** (ownership) — Not in Airtable and not in HE — research or steward fill after reuse
- **source_coverage** (ownership) — Not in Airtable and not in HE — research or steward fill after reuse
- **what_we_know[]** (ownership) — Not in Airtable and not in HE — research or steward fill after reuse
- **ubo_group** (ownership) — Not in Airtable and not in HE — research or steward fill after reuse
- **decision_authority.people** (ownership) — Not in Airtable and not in HE — research or steward fill after reuse
- **qualified_signatories** (ownership) — Not in Airtable and not in HE — research or steward fill after reuse
- **asset_agreement_profile** (ownership) — Not in Airtable and not in HE — research or steward fill after reuse
- **property_history** (ownership) — Not in Airtable and not in HE — research or steward fill after reuse
- **development_pursuit** (ownership) — Not in Airtable and not in HE — research or steward fill after reuse
- **sources** (ownership) — Not in Airtable and not in HE — research or steward fill after reuse
- **people_groups** (contacts) — Not in Airtable and not in HE — research or steward fill after reuse
- **emails_phones** (contacts) — Not in Airtable and not in HE — research or steward fill after reuse
- **key_people** (organization) — Not in Airtable and not in HE — research or steward fill after reuse

## Required integration work before paid enrichment

1. **fix_brand_presence_source_label** (P0) — Stop mislabeling HPC reads as Hotel Census; resolve owning table via RECORD_ID() and set censusSourceTable correctly.
1. **wire_overview_from_hpc_formatter** (P0) — For HPC-owned hotels (Bethesda), use formatHotelPropertyCensusAsBrandPresenceHotel (or equivalent) so Overview provenance is true HPC — and backfill empty HPC fields that UI currently invents via Brand Family / enrichment.
1. **wire_org_relationships_tabs_from_hi_graph** (P0) — Connect Organization + Relationships tabs to he-airtable-graph reader so Operator/Brand orgs and edges reach gold-depth UI without fixtures.
1. **derive_portfolio_views** (P1) — Build portfolio_kpis / operated_managed_hotels / structure_chart from HI Rel queries once org is linked.
1. **contacts_fallback_from_hpc** (P1) — Wire Contacts fallback_paths from HPC Phone + Official Property URL before paid person enrichment.
1. **kgpv_identity_unification** (P1) — Unify KGPV onto HPC (or approved link strategy) before any golden-demo → Airtable migration. Fixtures stay UX reference only until independently verified.

## Research priorities (only after integration reuse)

- P1: Confirm CURRENT Bethesda economic owner (free SDAT/filings first; paid only if blocked). Do not use 2013 Northview/Oaktree as current claim. _(est. $8–15)_
- P2: After economic owner — PropCo/UBO if still missing. _(est. $0–5)_
- P3: Decision-maker contacts only after owner known; public paths before paid. _(est. $5–10)_
- P4: No paid research / no fixture migration until HPC identity unification + independent fact verification. _(est. $0)_

## Field appendix

| Field | Tab | KGPV demo UI | Bethesda Airtable | Bethesda live→UI | Action |
|---|---|---|---|---|---|
| name | hotel | YES | EXISTS_HPC | LIVE_HPC_TO_UI | REUSE_OK |
| brand | hotel | YES | EXISTS_HPC | LIVE_HPC_TO_UI | REUSE_OK |
| status | hotel | YES | EXISTS_HPC | LIVE_HPC_TO_UI | REUSE_OK |
| website | hotel | YES | EXISTS_HPC | LIVE_HPC_TO_UI | REUSE_OK |
| telephone | hotel | YES | EXISTS_HPC | LIVE_HPC_TO_UI | REUSE_OK |
| address1 | hotel | YES | EXISTS_HPC | LIVE_HPC_TO_UI | REUSE_OK |
| city | hotel | YES | EXISTS_HPC | LIVE_HPC_TO_UI | REUSE_OK |
| country | hotel | YES | EXISTS_HPC | LIVE_HPC_TO_UI | REUSE_OK |
| hotelHeadline | hotel |  | EXISTS_HPC | LIVE_HPC_TO_UI | REUSE_OK |
| hotelDescription | hotel |  | EXISTS_HPC | LIVE_HPC_TO_UI | REUSE_OK |
| parentCompany | hotel | YES | EXISTS_HPC | LIVE_HPC_TO_UI | REUSE_OK |
| chainScale | hotel | YES | EXISTS_HPC | LIVE_HPC_TO_UI | REUSE_OK |
| censusPropertyType | hotel |  | EXISTS_HPC | LIVE_HPC_TO_UI | REUSE_OK |
| rooms | hotel | YES | EXISTS_HPC | LIVE_HPC_TO_UI | REUSE_OK |
| totalMeetingSpace | hotel | YES | MISSING_HPC | LIVE_VERIFIED_DERIVED | REUSE_OK |
| operationType | hotel | YES | EXISTS_HPC | LIVE_HPC_TO_UI | REUSE_OK |
| managementCompany | hotel | YES | EXISTS_HPC | LIVE_HPC_TO_UI | REUSE_OK |
| market | hotel | YES | EXISTS_HPC | LIVE_HPC_TO_UI | REUSE_OK |
| submarket | hotel | YES | EXISTS_HPC | LIVE_HPC_TO_UI | REUSE_OK |
| locationType | hotel | YES | EXISTS_HPC | LIVE_HPC_TO_UI | REUSE_OK |
| projectPhase | hotel | YES | MISSING_HPC | GENUINE_GAP | RESEARCH_OR_STEWARD_AFTER_REUSE |
| projectedOpenDate | hotel |  | N_A_OPEN_ASSET | LIVE_N_A | REUSE_OK |
| starRating | hotel |  | MISSING_HPC | GENUINE_GAP | RESEARCH_OR_STEWARD_AFTER_REUSE |
| dataConfidence | hotel |  | EXISTS_HPC | LIVE_HPC_TO_UI | REUSE_OK |
| amenitiesDisplay | hotel | YES | EXISTS_HPC | LIVE_HPC_TO_UI | REUSE_OK |
| executive_summary.text | ownership | YES | DERIVABLE_FROM_HI | UI_FROM_PARTIAL_OVERLAY | INTEGRATION_DEEPEN_FROM_HI |
| what_remains_uncertain | ownership | YES | MISSING_HI_EVIDENCE | GENUINE_GAP | RESEARCH_OR_STEWARD_AFTER_REUSE |
| economic_owner | ownership | YES | MISSING_HI_REL | UI_POPULATED_UNVERIFIED_SOURCE | NONE |
| property_company | ownership | YES | MISSING_HI_REL | UI_POPULATED_UNVERIFIED_SOURCE | NONE |
| operator | ownership | YES | EXISTS_HI_REL | LIVE_HI_GRAPH_TO_UI | REUSE_OK |
| control_status | ownership | YES | DERIVABLE_FROM_HI | UI_FROM_PARTIAL_OVERLAY | INTEGRATION_DEEPEN_FROM_HI |
| source_coverage | ownership | YES | MISSING_HI_EVIDENCE | GENUINE_GAP | RESEARCH_OR_STEWARD_AFTER_REUSE |
| what_we_know[] | ownership | YES | MISSING_HI_EVIDENCE | GENUINE_GAP | RESEARCH_OR_STEWARD_AFTER_REUSE |
| control_rows | ownership | YES | EXISTS_HI_REL | LIVE_HI_GRAPH_TO_UI | REUSE_OK |
| ubo_group | ownership | YES | MISSING_HI_ORG | GENUINE_GAP | RESEARCH_OR_STEWARD_AFTER_REUSE |
| portfolio_preview | ownership |  | DERIVABLE_FROM_HI | EXISTS_IN_AIRTABLE_NOT_CONNECTED | INTEGRATION_WIRE |
| brand_status | ownership | YES | EXISTS_HI_REL | LIVE_HI_GRAPH_TO_UI | REUSE_OK |
| decision_authority.people | ownership | YES | MISSING_HI_CONTACTS | GENUINE_GAP | RESEARCH_OR_STEWARD_AFTER_REUSE |
| qualified_signatories | ownership |  | MISSING_HI_CONTACTS | GENUINE_GAP | RESEARCH_OR_STEWARD_AFTER_REUSE |
| corporate_contact_channels | ownership | YES | MISSING_HI_CHANNELS | UI_POPULATED_UNVERIFIED_SOURCE | NONE |
| asset_agreement_profile | ownership | YES | MISSING_HPC | GENUINE_GAP | RESEARCH_OR_STEWARD_AFTER_REUSE |
| property_history | ownership | YES | MISSING_HI_EVIDENCE | GENUINE_GAP | RESEARCH_OR_STEWARD_AFTER_REUSE |
| development_pursuit | ownership | YES | MISSING_HI_RUNS | GENUINE_GAP | RESEARCH_OR_STEWARD_AFTER_REUSE |
| research_gaps | ownership | YES | MISSING_HI_RUNS | UI_POPULATED_UNVERIFIED_SOURCE | NONE |
| sources | ownership | YES | MISSING_HI_EVIDENCE | GENUINE_GAP | RESEARCH_OR_STEWARD_AFTER_REUSE |
| quality_summary | contacts |  | DERIVABLE_FROM_HI | EXISTS_IN_AIRTABLE_NOT_CONNECTED | INTEGRATION_WIRE |
| ownershipSummary | contacts |  | DERIVABLE_FROM_HI | EXISTS_IN_AIRTABLE_NOT_CONNECTED | INTEGRATION_WIRE |
| organization_contacts | contacts | YES | MISSING_HI_CHANNELS | UI_POPULATED_UNVERIFIED_SOURCE | NONE |
| people_groups | contacts | YES | MISSING_HI_CONTACTS | GENUINE_GAP | RESEARCH_OR_STEWARD_AFTER_REUSE |
| emails_phones | contacts | YES | MISSING_HI_CHANNELS | GENUINE_GAP | RESEARCH_OR_STEWARD_AFTER_REUSE |
| fallback_paths | contacts |  | EXISTS_HPC | EXISTS_IN_AIRTABLE_NOT_CONNECTED | INTEGRATION_WIRE |
| research_unresolved | contacts |  | MISSING_HI_RUNS | UI_POPULATED_UNVERIFIED_SOURCE | NONE |
| display_name | organization | YES | EXISTS_HI_ORG | LIVE_HI_GRAPH_TO_UI | REUSE_OK |
| legal_name | organization | YES | EXISTS_HI_ORG | LIVE_HI_GRAPH_TO_UI | REUSE_OK |
| ticker | organization |  | EXISTS_HI_ORG | EXISTS_IN_AIRTABLE_NOT_CONNECTED | INTEGRATION_WIRE |
| portfolio_kpis | organization |  | DERIVABLE_FROM_HI | EXISTS_IN_AIRTABLE_NOT_CONNECTED | INTEGRATION_WIRE |
| owned_controlled_hotels | organization |  | DERIVABLE_FROM_HI | EXISTS_IN_AIRTABLE_NOT_CONNECTED | INTEGRATION_WIRE |
| operated_managed_hotels | organization |  | DERIVABLE_FROM_HI | EXISTS_IN_AIRTABLE_NOT_CONNECTED | INTEGRATION_WIRE |
| key_people | organization | YES | MISSING_HI_CONTACTS | GENUINE_GAP | RESEARCH_OR_STEWARD_AFTER_REUSE |
| why_this_matters | organization | YES | EXISTS_HI_ORG | EXISTS_IN_AIRTABLE_NOT_CONNECTED | INTEGRATION_WIRE |
| structure_chart | relationships | YES | DERIVABLE_FROM_HI | UI_FROM_PARTIAL_OVERLAY | INTEGRATION_DEEPEN_FROM_HI |
| relationship_edges | relationships | YES | EXISTS_HI_REL | LIVE_HI_GRAPH_TO_UI | REUSE_OK |
| org_network | relationships | YES | DERIVABLE_FROM_HI | EXISTS_IN_AIRTABLE_NOT_CONNECTED | INTEGRATION_WIRE |

---

Machine-readable: `HE-TABS-1-5-GATE1-KGPV-GOLD-BETH-AIRTABLE-METRICS-V1.json`

**Founder decision:** Approve revised Gate 1 (KGPV=gold UX; Bethesda=Airtable-first), then authorize integration work. Substantive paid research stays blocked until approval.