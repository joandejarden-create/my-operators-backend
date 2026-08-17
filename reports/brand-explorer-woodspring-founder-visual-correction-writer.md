# Brand Explorer WoodSpring Founder Visual QA Correction v33G

- Generated: 2026-07-13T22:04:19.149Z
- Mode: **dry-run**
- Dry-run clean: **yes**
- Image fields untouched: **no**
- Company Validated untouched: **yes**

## Scenario rendering audit
- `recrnaRxigUSoDDTJ` **overview.scenario.3** — title: Extended-Portfolio Fee Discipline; display: Do Not Display; API: no; UI winner: no; image: attachment_present

## Root cause — Boutique Resort Adjacency / IMAGE placeholder
- atelier_hardcoded_title_fallback — no winning API block supplies title for overview.scenario.3
- atelier_image_placeholder — explorerFirstBlock(overview.scenario.3).imageUrl is empty
- api_slot_absent — no overview.scenario.3 rows pass Brand Library API filters (Active + External Display Status)
- clean_scenario3_row_missing — only quarantined or hidden overview.scenario.3 rows exist in Airtable

## Scenario 3 decision
- Action: **create_and_materialize_scenario3_image**
- Image decision: **materialize_from_approved_registry_source_url**
- Rationale: Preserve existing approved scenario image; patch title/body only.

## overview.bestAt before/after
### overview.bestAt.1
- Before title: Markets that support extended-stay ADR and required amenity stack.
- After title: Weekly & Longer-Stay Demand
- Before body: Markets that support extended-stay ADR and required amenity stack.
- After body: Markets with recurring weekly and longer-stay demand from workforce, relocation, project-based, or other extended-stay use cases.
### overview.bestAt.2
- Before title: Owners who model net contribution after fees, loyalty, and channel mix.
- After title: Extended-Stay Brand Comparison
- Before body: Owners who model net contribution after fees, loyalty, and channel mix.
- After body: Owners comparing extended-stay brand fit, local supply, operating model simplicity, and Choice platform participation.
### overview.bestAt.3
- Before title: Operators with tier-appropriate QA and opening discipline.
- After title: Practical Extended-Stay Operations
- Before body: Operators with tier-appropriate QA and opening discipline.
- After body: Operators prepared for a practical extended-stay service model, kitchen-equipped room expectations, and disciplined property-level execution.

## Registry traceability before/after
- `recI3cbO8mOhEpo1W` Orlando opening — before: (none); after: rec9Z0MraEaBj797S; property-specific: yes
- `recpNB0KoPq6y3Mhs` Charlotte opening — before: (none); after: rec0bZAeIQ8le0tna; property-specific: yes
- `rec4Eqp9lwXSP7UQE` Raleigh opening — before: (none); after: recFo5RWZn3FPmCNp; property-specific: yes
- `rechUn7nwlxjW1jyV` Gallery 1 — before: (none); after: recXQEgBfOD5Uim3b; property-specific: yes
- `recXfIGZUrwap6AIK` Gallery 2 — before: (none); after: recdK5ZpOBSQMWAZQ; property-specific: yes
- `recJokIWQxU64gVsl` Gallery 3 — before: (none); after: recJn99B9SyNbg3qd; property-specific: yes

## Patches — copy: 3, registry: 6, hide: 0

## Projected UI confirmation
- Scenario 3 title: Extended-Stay Competitive Positioning
- Scenario 3 IMAGE placeholder: no
- Risky bestAt language remains: no
- Property-specific opening registry links: yes

```bash
npm run brand-explorer-woodspring-founder-visual-correction-writer -- --brand woodspring-suites --apply --approve-brand-explorer-v33G-woodspring-founder-visual-correction --confirm-no-company-validation-claim --confirm-no-source-library-changes --confirm-no-summary-url-field --confirm-no-momentum-proof-standard-changes --confirm-woodspring-only
```
