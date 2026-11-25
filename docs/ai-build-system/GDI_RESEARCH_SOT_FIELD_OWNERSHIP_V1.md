# GDI Research Airtable Source-of-Truth field ownership V1

**Contract modules**

- `lib/group-demand-intelligence/workflow/gdi-research-sot-field-ownership-v1.js`
- `lib/group-demand-intelligence/workflow/gdi-research-sot-lossless-merge-v1.js`

## Permanent principle

**Enrich the Source of Truth. Never silently degrade it.**

Discover broadly. Research thoroughly. Qualify rigorously. Persist validated opportunities. Preserve rejected research as supporting intelligence, not as active hotel opportunities.

Existing authoritative research intelligence must never be silently degraded, deleted, or replaced by a thinner downstream representation (slim STAGED / drawer / package payload).

A successful FQO run may enrich Airtable SoT but must **not** automatically replace the full research payload.

## Ownership classes

### A. Authoritative research intelligence

Preserve unless independently supported by stronger evidence:

- Organization identity and relationships
- Demand history and recurring events (`meetingHistory`)
- Hotel and competitor relationships
- Source evidence, FACT statements, confidence, provenance
- Contact identification, grades, verification, research audits
- Room demand and revenue classifications
- Commercial demand mechanisms
- Previous research findings and unknown historical fields (default preserve)

### B. Hotel-specific certification state

Controlled updates from certified FQO / Full E2E:

- `hotelSpecificQualification`
- FQO result and reasons
- Named primary/alternative contacts when verified (merged, not replaced)
- Outreach validation / sales-action classifications
- Certification schema version / qualification provenance

### C. Derived presentation values

Never treat slim STAGED or drawer representations as authoritative over richer SoT facts merely because they were generated more recently.

## Merge behavior

1. Load complete live Airtable research payload.
2. Apply only permitted, verified improvements from certification.
3. Preserve all other existing fields (including unknown keys).
4. Merge contacts / evidence / meetings by identity + provenance.
5. Reject revenue / commercialDemandType downgrades and unsupported narrative replacements (auto-keep live).
6. Fail closed on information loss or storage-limit overflow (no truncate).
7. Compare before allowing mutation.

## Gates

```bash
npm run test:gdi-research-sot-lossless-merge-v1
npm run test:gdi-research-inventory-lossless-merge-v1
npm run gdi:nice-sot-preservation-audit-v1
npm run gdi:bethesda-68-lossless-write-manifest-v1
```
