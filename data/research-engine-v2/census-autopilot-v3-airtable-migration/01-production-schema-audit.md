# Production Census Schema Audit (V3 Phase 1)

**Inspected:** live Airtable meta API (not documentation assumption)

## Production master
- **Base:** Deal Capture Platform (`AIRTABLE_BASE_ID_ALT` → `appCCUsuGsE1ifoLk`)
- **Table:** **Hotel Property Census** (`tbl9aY5ijiuIzzWam`)
- **Field count:** **116**
- **Primary field:** Property Name (primaryFieldId `flde2wQnc3Hxbqdmr`)

## Related tables (do not write in this pilot)
- Hotel Property Brand Affiliations (linked)
- Hotel Property Source Evidence (linked)
- Hotel Property Steward Review (linked)

## Explicitly NOT the write target
- Legacy `Hotel Census`
- VIC / Brand Setup / Brand Explorer

## Identity mechanism
- **Property Identity Key** (singleLineText) — durable production identity
- Official brand codes embedded as `ind_{family}_{cc}_{code}`

## Provenance
- Field-level provenance retained in Research Engine evidence store + pilot transaction design
- Supporting Source Evidence table exists but is **not** auto-written in Phase 1 pilot

## Brand Explorer / Operator Explorer
- No linked writes; no activation
- Census growth calculated in staging impact only after Phase 2

Live field dump: `_schema-live.json`
