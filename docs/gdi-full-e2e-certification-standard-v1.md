# GDI Full E2E certification standard

This standard certifies the existing Group & Demand Intelligence engine. It does not create a second research system. A passing readiness check means the execution path is present. It does not mean research quality or customer delivery has been signed off.

Engine baseline: `be179b441` on `main`, plus the stabilization commits that add this standard.

## Representative hotels

- Bethesda Marriott, `recLuxvwwxID7U2B8`
- Renaissance Times Square, `recG66DQJKP2c0UNh`
- Wyndham San Diego Bayside, `recnPB3Q02NO8Ma1o`
- Spice Island Beach Resort, the existing international published portfolio property

## Criteria

Each row is scored PASS, FAIL, or NOT VERIFIED. NOT VERIFIED is required when the run was not executed or the evidence is absent. Membership in a hotel list is not a pass.

| Area | What must be shown |
| --- | --- |
| Research engine | The 14 workflow catalog ids and 11 supervisor segments load from the existing modules. Discovery, investigation, and continuation use the research-to-page workflow. Evidence keeps its source and freshness. Zero-paid dry-run stays unpaid. Paid execution happens only with a separate authorization and writes a provider-call ledger. |
| Commercial intelligence | Buyer contacts and intermediaries are named from evidence. Sales Ready and Account Development come from the publication registry, not from an unapproved research row. Recommendations stay tied to the hotel's market. |
| Operational execution | Budget and provider controls fail closed. Weekly refresh, continuation, and stop rules are the existing supervisors. Need-Date V4 and sales feedback use the existing stores. |
| Customer delivery | List and detail match the approved source. Unpublished opportunities stay off the strict customer registry. Share links use the existing tokens and are not rotated by certification. |
| Artifacts | Record the commit, inputs, hotel and category matrix, regression command, budget ledger, and exceptions. The signed report uses only PASS, FAIL, and NOT VERIFIED. |

## Execution record

The 2026-11-11 measurement is `docs/gdi-full-e2e-certification-report-20261111.md`. Full E2E certification was not awarded.

## Current readiness

`npm run test:gdi-full-e2e-certification-readiness-v1` checks the catalog and entry points. Paid research is not part of that command. The events zero-paid orchestrator remains blocked because `gdi-events-mock-provider-responses-v1.js` is not in the repository and cannot be reconstructed from a saved fixture.
