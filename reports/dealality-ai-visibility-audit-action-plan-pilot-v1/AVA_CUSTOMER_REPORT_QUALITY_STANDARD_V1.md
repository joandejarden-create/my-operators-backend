# AVA Customer Report Quality Standard V1

Contract: `AVA_CUSTOMER_REPORT_QUALITY_STANDARD_V1`

## Purpose

One reusable quality standard for every hotel processed through Dealality AVA Full E2E — independent of brand, location, size, independence, finding count, provider, or PDF length.

Hotel-specific conclusions may vary. The quality bar must not.

## Governance scope

- Research → finding transformation (evidence-backed only)
- Finding classification (correction / improvement / opportunity / consistent / partial / follow-up)
- Canonical Action Register (shared records across report sections)
- Semantic duplicate detection
- Priority, commercial relevance, evidence, owners, steps
- Customer-facing language (no `[object Object]`, no truncated narratives, no internal enums)
- PDF rendering + visual QA
- Customer Quality Assurance gates
- Full E2E enforcement (stage 11.5 + CQA)

## Permanent rules

1. **No truncated customer narratives** from rendering or transformation.
2. **No serialization artifacts** (`[object Object]`, raw JSON, internal IDs).
3. **One canonical action** may appear in Priority Findings and Roadmap — never contradictory rewrites.
4. **Platform rows** must use assessment classes A–F; never blanket “confirm facts match…”.
5. **Digital Authority** opportunities require a verified useful source URL (or stay internal follow-up).
6. **Listing ≠ content.** Access restrictions are not content defects. Google Travel ≠ GBP/Maps place.
7. **No speculative revenue / AI ranking promises.**

## Regression fixtures

- TRUNCATED_EXECUTIVE_TAKEAWAY
- OBJECT_OBJECT_PAGE_LABEL
- DUPLICATE_MEETING_SPACE_ACTIONS
- GENERIC_REPEATED_PLATFORM_ACTIONS
- MISSING_DIGITAL_AUTHORITY_EVIDENCE
- INCORRECT_HOTEL_SOURCE_URL
- ACCESS_RESTRICTED_LABELED_VERIFIED_CONTENT
- HIGH_PRIORITY_FINDING_OMITTED

## Implementation modules

- `customer-report-quality-standard-v1.js`
- `canonical-action-register-v1.js`
- `customer-facing-serialize-v1.js`
- `narrative-completeness-v1.js`
- `build-audit-pdf-html-v4.js`
- CQA extensions in `customer-quality-assurance-v1.js`
- Full E2E hook in `ava-full-e2e-pipeline-v1.js`
