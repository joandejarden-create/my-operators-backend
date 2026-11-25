# Dealality Three-Layer Intelligence Contract V1

**Permanent architectural standard** for GDI Full E2E + Airtable persistence.  
Designed for eventual reuse across Dealality intelligence products.

**Code:** `lib/group-demand-intelligence/workflow/gdi-three-layer-intelligence-contract-v1.js`  
**Ownership:** `gdi-research-sot-field-ownership-v1.js`  
**Lossless merge:** `gdi-research-sot-lossless-merge-v1.js`

---

## Founder principles (enforceable)

1. **Discover broadly. Research thoroughly. Qualify rigorously. Persist validated opportunities. Preserve rejected research as supporting intelligence, not as active hotel opportunities.**
2. **Research intelligence is a cumulative asset** — enrich, never silently degrade.
3. **Multi-hotel FQO is independent** — Hotel A approval does not approve Hotel B.
4. **Presentation is derived** — cards/drawers/summaries are not authoritative research.
5. **Publication is independent** — research persistence ≠ STAGED ≠ CURRENT.

---

## Three layers (one architecture)

| Layer | Responsibility | Authority |
|---|---|---|
| **1 — Research SoT** | What do we know, and what evidence supports it? | Airtable research inventory + related demand tables |
| **2 — Hotel-Specific Qualification** | Is this actionable for *this* hotel? | FQO six-rule contract + `hotelSpecificQualification` |
| **3 — Commercial Presentation** | What must sales know and do? | STAGED/CURRENT packages, cards, drawers (derived) |

**Do not** create three databases, three opportunity engines, or competing sources of truth.

---

## Existing architecture mapping

### Layer 1 surfaces (reuse)

- GDI Hotel Research Opportunities (`researchPayloadJson` SoT)
- Demand Generators / Programs / Signals
- Hotel Demand Generator Fit
- Group Demand Opportunities (separate write-routing)

### Layer 2 surfaces (reuse)

- Canonical FQO six-rule contract
- `researchPayloadJson.hotelSpecificQualification`
- Production write policy `VALIDATED_FQO_ONLY`
- `commercialEligibilityStatus` = org/GDO routing (**not** hotel FQO failure)

### Layer 3 surfaces (derived)

- STAGED (founder review) / CURRENT (customer publication)
- Slim / drawer / card builders
- Sales-action pills and display titles

---

## Field ownership (persistence boundary)

| Field group | Primary authority | Permitted modification |
|---|---|---|
| Historical research and evidence | Research SoT | Verified enrichment; corrections with provenance |
| Organization identities / relationships | Research SoT | Canonical identity reconciliation |
| Contacts and verification audits | Research SoT | Verified additions; preserve history |
| Demand and revenue classifications | Research SoT | Supported reclassification only |
| Hotel fit and FQO | Hotel qualification | Certified qualification updates |
| Sales priority / recommended actions | Hotel qualification | Commercial reevaluation |
| Titles, cards, drawer summaries | Commercial presentation | Regenerate display without rewriting SoT |
| STAGED package | Founder review | Explicit activation |
| CURRENT package | Customer publication | Explicit publication approval |

Unknown existing SoT fields are preserved by default.

---

## Persistence rule

Wholesale slim-over-fat `researchPayloadJson` replacement is **prohibited**.

Use evidence-aware lossless semantic merge (`mergeResearchSotLossless` / `mergeProposedResearchInventoryFields`).

Fail closed on information loss or storage-limit overflow.

---

## Future Weekly Change (metadata only)

Stable change categories (no UI in this PR):

- **Research:** new evidence, updated facts, contacts, relationships
- **Qualification:** newly qualified / maintained / lost / research required / timing
- **Commercial:** priority, outreach, sales recommendation, competitive pathway

Emitted as `weeklyChangeMetadata` from the persistence merge path.

---

## Tests

```bash
npm run test:gdi-three-layer-intelligence-contract-v1
npm run test:gdi-research-sot-lossless-merge-v1
npm run test:gdi-research-inventory-lossless-merge-v1
npm run test:gdi-validated-fqo-write-set-policy-v1
```

---

**THREE DISTINCT INTELLIGENCE LAYERS. ONE COHERENT ARCHITECTURE. ZERO SILENT INFORMATION LOSS.**
