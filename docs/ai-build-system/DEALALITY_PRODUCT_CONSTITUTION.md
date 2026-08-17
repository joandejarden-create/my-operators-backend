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
- Data governance: [../data-intelligence/INTELLIGENCE_GOVERNANCE.md](../data-intelligence/INTELLIGENCE_GOVERNANCE.md)
- Project memory: [../../AGENTS.md](../../AGENTS.md)
