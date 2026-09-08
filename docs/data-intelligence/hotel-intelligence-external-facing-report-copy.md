# Hotel Intelligence — external-facing report copy (binding)

**Status:** Binding for all Full Hotel Intelligence Investigations, Research Addenda, PDF downloads, Research Center archive cards, and any customer-visible Hotel Intelligence report.

## Rule

Customer-facing reports are **external diligence documents**. Readers are owners, operators, advisors, and counterparties. They are **not** aware of Dealality internals and must never see them.

Do **not** mention Dealality infrastructure, product mechanics, or implementation codes in customer prose, including:

| Forbidden (examples) | Use instead |
| --- | --- |
| Census / Hotel Property Census / Census affiliation / “Same asset as Census…” | Prior / recorded / verified property records; “same property previously recorded as…” |
| Airtable, `rec…` ids | Omit, or “internal records” only if unavoidable in non-customer surfaces |
| Research Center, golden demo, fixture, Packet 2.x, FULL_HI_* | Investigation archive / compiled research / omit |
| Webhound, provider budgets ($5), session UUIDs, auto_promote, claim_handoff | Reviewed research / Full Hotel Intelligence Investigation / “findings are research-grade and not auto-applied…” |
| Ontology codes (`OWNED_BY`, `propco` snake_case as a bare code) | Human labels (“Owned by”, “Property company”) |

Internal metadata may remain on non-rendered keys (`claim_handoff`, `raw_artifact_reference`, `hotel_airtable_record_id`, etc.).

## Enforcement

1. Adapters must emit customer-safe prose at compile time.
2. `prepareCustomerProse` / `rewriteResearchInstructionLeaks` rewrites known leaks.
3. `CLIENT_SAFE_BLOCK_PATTERNS` + `validateClientSafeReport` fail unsafe customer text.
4. `enforceClientSafeCustomerSurfaces` runs on dossier **serve** (registry) so legacy fixtures are sanitized.
5. Ownership-chain roles render via customer labels (`customerOwnershipRoleLabel`), never raw snake_case alone.

## QA gate

Before shipping any dossier / addendum / PDF:

- [ ] No “Census”, Airtable, Webhound, Research Center, fixture, auto_promote, or Packet codes in customer-visible text
- [ ] Ownership-chain titles use human role labels and readable dark ink (not washed-out muted titles)
- [ ] Confidence shown as High / Probable / Unverified / Former (not only raw internal codes as the only label)
- [ ] Cover **location** is a real place string (e.g. `Cancun, Mexico`) — never `Location not available` / `Location pending` when city+country are known; register in `DISPLAY_LOCATION_BY_HOTEL` or set `hotel_location` / `property_profile` at compile

See also: `lib/hotel-intelligence/dossier/client-safe/`, `lib/hotel-intelligence/dossier/report-hotel-identity.js`, [CONTENT_QA_CHECKLIST.md](./CONTENT_QA_CHECKLIST.md).
