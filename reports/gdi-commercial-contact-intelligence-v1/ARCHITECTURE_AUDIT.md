# Commercial Contact Intelligence — architecture audit

## Integration baseline (Phase 0)

| Item | Status |
|------|--------|
| PR #474 (Surfe auth states) | **MERGED** `38f539021` — kept separate; Surfe remains on-demand only |
| PR #475 (Public Contact Discovery 2.0) | **OPEN** — CCI built as sequential extension on same branch |
| Competing engine | **None** — reuses `discoverPublicContactChannels`, search providers, channel hierarchy, inventory store |
| Drawer redesign | **None** — projects `whyRelevant` / classification onto existing primary/secondary slots |

## Current stack (extended, not replaced)

1. `gdi-public-contact-search-providers-v1` — Exa (+ optional Serper)
2. `gdi-public-contact-discovery-v1` — person-scoped public channel recovery
3. `gdi-public-contact-channel-hierarchy-v1` — display + Surfe-last CTAs
4. **NEW** `gdi-commercial-contact-classification-v1` — evidence-gated classes
5. **NEW** `gdi-commercial-contact-scoring-v1` — dual independent scores
6. **NEW** `gdi-commercial-contact-buying-ecosystem-v1` — stakeholder targets + queries
7. **NEW** `gdi-commercial-contact-selection-v1` — relevance-first selection
8. **NEW** `gdi-commercial-contact-intelligence-v1` — orchestrator + contact-level weekly changes

## Design rules (binding)

- Title alone ≠ DIRECT_HOTEL_BUYER / VERIFIED_MEETINGS_OR_TRAVEL_BUYER
- Channel score cannot promote authority
- Verified buyer without email is kept; reachable referral shown separately
- New named person ≠ automatic commercial improvement
- Surfe never in automated research; never persisted
- Contact-level changes do not create duplicate demand opportunities

## Airtable

Reuses existing Hotel Research Opportunities inventory (`researchPayloadJson` primary/secondary + commercialContactIntelligence annotation). Idempotent upsert + read-back via existing store (same path as PCD 2.0 persist).
