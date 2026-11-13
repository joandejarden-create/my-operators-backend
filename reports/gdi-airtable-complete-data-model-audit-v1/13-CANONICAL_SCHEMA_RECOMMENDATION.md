# Canonical GDI schema recommendation

## Target flow

**Airtable-backed canonical research/commercial data → versioned deterministic presentation rules → immutable founder-approved publication (+ feedback merge).**

## Keep separate

1. Mutable research registry (Group Demand Opportunities + graph tables)
2. Durable evidence/history (FS research-state/runs + Airtable signals/target-runs; stop relying on reconstructed bags for production hotels)
3. Deterministic derived commercial values (versioned rules; persist keys + rule version)
4. Immutable publication packages
5. Feedback (FS + Decisions)

## Minimum schema additions (justified by matrix)

On Group Demand Opportunities (typed columns):

- `salesActionKey`, `commercialStageKey`, `revenueStreamClass`
- `cardContactSubtitle`, `primaryContactProfessionalTitle`
- `displayTitle` (if distinct from opportunityName)
- `presentationRuleVersion`

Ensure write path does **not** compact away customer-required payload keys.

Optional later: Contacts + OpportunityContacts; Evidence links table.

## Do not

- Replace publication packages with live Airtable customer reads
- Duplicate entire payload into hundreds of columns
- Invent need dates or contact titles
