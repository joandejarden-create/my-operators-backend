# Dealality Product Constitution

Permanent product principles for Dealality. Future features, AI behavior, and build decisions should align with this document.

## Purpose

Dealality helps hotel owners, brands, operators, advisors, and capital partners make better hotel deal decisions through structured context, alignment signals, readiness logic, and controlled opportunity intelligence.

## What Dealality Is

- A hotel deal intelligence platform.
- A structured decision-support system.
- A platform for evaluating opportunity readiness, brand alignment, operator alignment, and strategic paths.
- A controlled environment for improving the quality of hotel deal conversations.
- A recurring intelligence layer around hotel opportunities.

## What Dealality Is Not

- Not a listing marketplace.
- Not a generic lead list.
- Not a consultant replacement.
- Not a broker replacement.
- Not a legal advisor.
- Not a black-box recommendation engine.
- Not a platform that should make unsupported claims of fit, validation, or guaranteed outcomes.

## Core Product Principles

1. **Better context before conversation.** Users should understand opportunity context, alignment signals, and readiness before engaging counterparties.
2. **Owner-controlled opportunity activation.** Owners control when and how opportunities become visible and active.
3. **Structured options, not forced recommendations.** Present considerations and paths; avoid pushing a single definitive answer.
4. **Alignment signals over absolute answers.** Fit is expressed as signals, not guarantees.
5. **Evidence, interpretation, and next action should be separated.** Facts, Dealality's reading of them, and suggested follow-ups must be distinguishable.
6. **AI should show confidence, source status, missing data, and limitations where relevant.** Transparency builds trust.
7. **Dealality should create recurring value, not one-time list extraction.** Features should reward return visits as data and signals evolve.
8. **Brand, operator, owner, advisor, and capital workflows should remain distinct.** Do not collapse role-specific journeys into a generic flow.
9. **The platform should avoid advisory, legal, or overly definitive language.** Stay evidence-aware and neutral.
10. **Airtable is the operational source of truth** unless a future migration changes this.
11. **Memberstack is identity/auth only**, not business-data source of truth.
12. **Build for future workspace, region, and deal-level access layers.** Access assumptions should not be hardcoded against future needs.
13. **UI should feel premium, calm, structured, and credible.** Hospitality-grade presentation matters.
14. **Methodology is governed; quality controls learn.** For Existing Hotel ADP (and analogous measurement products), audits may strengthen implementation, entity resolution, reconciliation, regression tests, and release gates — they must not autonomously redefine approved measurement methodology, metric semantics, or comparability rules. Methodology changes require explicit founder governance.
15. **Approved customer output is a versioned production contract.** A later change may not silently regress an earlier approved customer surface. Report snapshot version, renderer version, and data contract version must move together.
16. **No fix is complete until it is in the customer-surface regression baseline.** New work may extend the product; it may not silently replace a verified customer output contract.
17. **Dealality Research Receipt is a permanent brand and design principle.** Make research accountability, customer-attributable intelligence, shared vs product-specific attribution, reuse, commercial relevance, Dealality branding, and no fabricated metrics visible to customers. **Implementation is product-specific** — GDI is the first reference implementation; do not build ADP / Hotel Explorer / Deal Opportunities receipts or a cross-product reporting engine until those products have dedicated cycles. Principles: [DEALALITY_RESEARCH_RECEIPT_PRINCIPLES.md](./DEALALITY_RESEARCH_RECEIPT_PRINCIPLES.md).
18. **Additive development and non-regression are permanent.** New functionality must be additive and must not silently regress previously accepted UI layouts, opportunity inventory, data accessibility, or customer-facing functionality. Preserve accepted design language; reconcile inventories by record ID; compare before/after baselines; resolve unintended regressions before certification. Integration Controller must block acceptance on unintentional regression; intentional material change requires explicit founder approval. An enhancement is complete only when the new capability works **and** previously accepted functionality continues to work. Principles: [DEALALITY_ADDITIVE_NON_REGRESSION_PRINCIPLES.md](./DEALALITY_ADDITIVE_NON_REGRESSION_PRINCIPLES.md).
19. **Port 8080 is the cumulative integrated founder review environment.** It must serve the integrated product surface (not an isolated feature branch that omits previously accepted capabilities). Isolated feature development may use temporary alternate ports. The Integration Controller coordinates the cumulative staging build; product chats must not silently replace `:8080` with an incomplete stream checkout.
20. **Canonical GDI publication lifecycle is FULL E2E → STAGED → FOUNDER APPROVAL → CURRENT.** Binding operational statuses: RESEARCH IN PROGRESS (E2E incomplete), HOLD (more evidence/qualification required), STAGED (E2E complete, commercially qualified, durably persisted, visible on port 8080), CURRENT (founder-approved for authorized customers). Every authorized completed Full E2E research cycle must automatically progress successfully qualified opportunities into the hotel’s complete, deduplicated STAGED inventory (no separate Proposed Qualified / Write-Ready founder gate; those remain internal technical classifications only). HOLD / continuation / incomplete / speculative / rejected results must not stage as qualified. STAGED never auto-publishes CURRENT. Only explicit founder approval of the exact reviewed package version promotes STAGED → CURRENT. Production customers remain CURRENT-only with hotel ACL and auth intact. Authorized local founder/admin review on `:8080` defaults to STAGED when present (CURRENT when STAGED is absent). Local server access alone does not grant founder privileges. Doctrine: [GDI_FULL_E2E_COMPLETION_AND_PUBLICATION_LIFECYCLE_V1.md](./GDI_FULL_E2E_COMPLETION_AND_PUBLICATION_LIFECYCLE_V1.md).
21. **Integrated non-regression validation is mandatory before GDI completion.** Feature-branch tests alone are not product acceptance. Before accepting a GDI enhancement, verify the actual cumulative integrated build retains previously accepted tabs, List/Tiles parity, drawers, filters, Executive Summary inventory parity, and opportunity IDs (`test:gdi-cumulative-integrated-staging-non-regression-v1`, `test:gdi-canonical-e2e-to-staged-lifecycle-v1`, `Invoke-GdiValidateFullE2E.ps1`, and the PowerShell staging validator). **A GDI Full E2E run cannot be declared complete until its qualified opportunities are durably persisted, STAGED, and visible in the actual integrated local application on port 8080.** Research reports, qualification unit tests, or proposed write-sets alone are not operational completion. **Inter-prompt:** every assignment must pass `npm run dealality:assignment-preflight` (`DEALALITY_INTER_PROMPT_NON_REGRESSION_V1`) before implementation; `:8080` ownership requires cwd **and** `serverScriptDir` match; live guard `dealality:canonical-8080-guard`; cumulative manifest must grow.
22. **End-to-end integrated delivery is the default assignment scope.** Every implementation assignment automatically includes architecture audit, baseline, implementation, tests, PR, Integration Controller coordination, CI, merge to `main`, post-merge sync, canonical `:8080` refresh, cumulative browser validation, and founder handoff. Do not routinely stop at PR OPEN / AWAITING_INTEGRATION_CONTROLLER / feature-branch-only validation. Normal successful product status: **`INTEGRATED_ON_8080 — AWAITING_FOUNDER_REVIEW`**. IC retains exclusive merge authority; the development stream initiates the established remote-merge handoff. Code integration ≠ CURRENT publication ≠ production deploy. Contract: [DEALALITY_INTEGRATED_DELIVERY_V1.md](./DEALALITY_INTEGRATED_DELIVERY_V1.md).

## AI Behavior Principles

AI-generated outputs should:

- Be clearly labeled when AI-assisted, source-informed, platform-derived, or company-validated.
- Avoid overstating certainty.
- Identify missing information.
- Support decision-making without pretending to replace human judgment.
- Use "options," "signals," "considerations," "questions to clarify," and "data gaps" rather than overly strong recommendations.
- Preserve confidentiality and owner control.
- Update as underlying deal, brand, operator, market, or response data changes.

## Recurring Value Principle

Every major feature should ask:

- Why would the user come back?
- What changes over time?
- What new signal can Dealality surface later?
- What decision does this help improve?
- What data does this help collect or validate?

## Related Documentation

- Build process: [AI_BUILD_PROTOCOL.md](./AI_BUILD_PROTOCOL.md)
- Naming and copy: [NAMING_AND_COPY_GUIDE.md](./NAMING_AND_COPY_GUIDE.md)
- Research Receipt principles: [DEALALITY_RESEARCH_RECEIPT_PRINCIPLES.md](./DEALALITY_RESEARCH_RECEIPT_PRINCIPLES.md)
- Additive development & non-regression: [DEALALITY_ADDITIVE_NON_REGRESSION_PRINCIPLES.md](./DEALALITY_ADDITIVE_NON_REGRESSION_PRINCIPLES.md)
- Integrated delivery (merge → :8080 → founder review): [DEALALITY_INTEGRATED_DELIVERY_V1.md](./DEALALITY_INTEGRATED_DELIVERY_V1.md)
- Data governance: [../data-intelligence/INTELLIGENCE_GOVERNANCE.md](../data-intelligence/INTELLIGENCE_GOVERNANCE.md)
- Project memory: [../../AGENTS.md](../../AGENTS.md)
