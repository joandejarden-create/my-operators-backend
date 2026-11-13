# Commercial recommendation data model review

## Origins today

| Output | Typical origin | Durable in Airtable? |
|--------|----------------|----------------------|
| Priority | Research + remediation | Typed `priority` column + payload |
| Commercial action / sales pill | Deterministic resolvers + frozen package DTO | **Mostly payload JSON** (`salesActionKey`, `salesActionPillLabel`) — not typed columns |
| Recommended next step | Research / remediation | Typed `recommendedAction` + payload |
| Why now / why hotel | Research | Typed `whyNow` / `whyThisMatters` / `hotelWinThesis` |
| Evidence confidence | Research | Typed `evidenceConfidence` |
| Revenue stream SVG inputs | Classification rules + freeze | Payload `revenueStreamClass` / icons |
| Sourcing status | Research | Typed `venueStatus` |

## Gap

External customer renderer must **not** re-run independent commercial-action resolvers. Approved package freezes outputs. For Airtable-backed regeneration, the same inputs + **versioned derivation rules** must reproduce pills deterministically — today many approved pill strings exist only in publication DTOs / payload, and historical upserts may lack full salesAction fields.

## Recommendation

- Store: `salesActionKey`, `commercialStageKey`, `revenueStreamClass`, derivation rule version, evidence refs as typed or guaranteed payload fields.
- Keep approved publication immutable; regenerate only under founder publish workflow.
