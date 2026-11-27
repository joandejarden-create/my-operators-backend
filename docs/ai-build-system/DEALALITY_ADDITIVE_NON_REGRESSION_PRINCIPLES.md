# Dealality Additive Development & Non-Regression Principles

**Contract:** `DEALALITY_ADDITIVE_NON_REGRESSION_V1`  
**Status:** Permanent product principle (platform-wide)  
**Applies to:** GDI, ADP, Hotel Explorer, Deal Opportunities, Brand Explorer, Operator Explorer, Census/HI, and all future capabilities

## Core doctrine

**New functionality must be additive and must not silently regress previously accepted UI layouts, opportunity inventory, data accessibility, or customer-facing functionality.**

An enhancement is **not** complete merely because the new capability works. It is complete only when the new capability works **and** previously accepted functionality continues to work.

## Permanent requirements

### 1. UI & design preservation

- Preserve previously accepted layouts, navigation, styling, spacing, icons, and interaction patterns.
- Introduce new functionality within the established design language.
- Do not redesign existing components unless explicitly requested.
- Preserve responsive behavior.

### 2. Data & intelligence preservation

- Never silently remove, overwrite, downgrade, or hide previously accepted intelligence.
- Preserve canonical record identities, evidence, research history, and commercial classifications.
- Maintain access to previously available intelligence within existing authorization rules.
- Reconcile inventory using **actual record IDs**, not counts alone.

### 3. Functional preservation

- Existing features, filters, selectors, drawers, deep links, exports, and workflows must continue working.
- New capabilities must not introduce unintended dependencies or competing implementations.
- Preserve previously certified customer-facing outputs.

### 4. Before/after validation (required before certification)

1. Establish the relevant pre-change baseline.
2. Compare affected UI surfaces.
3. Compare data inventories and record identities.
4. Validate existing workflows.
5. Identify intentional versus unintended changes.
6. Resolve unintended regressions before certification.

### 5. Integration Controller enforcement

Every relevant PR must include a **non-regression assessment**.

Integration Controller **must block acceptance** when previously approved functionality, data accessibility, or presentation has **unintentionally** regressed.

Any intentional removal, replacement, redesign, or material change to previously accepted functionality requires **explicit founder approval**.

### 6. Implementation posture

- Incorporate controls incrementally into existing development and validation workflows.
- Do **not** create a separate infrastructure project solely for this principle.
- When a defect is identified, add appropriate automated regression tests to the permanent product validation workflow.

## Related

- Product Constitution: [DEALALITY_PRODUCT_CONSTITUTION.md](./DEALALITY_PRODUCT_CONSTITUTION.md)
- Build decisions: [BUILD_DECISIONS.md](./BUILD_DECISIONS.md)
- PR / IC acceptance: [PR_COMPLETION_AND_WORKSPACE_OWNERSHIP.md](./PR_COMPLETION_AND_WORKSPACE_OWNERSHIP.md)
- QA checklist: [DEALALITY_QA_CHECKLIST.md](./DEALALITY_QA_CHECKLIST.md)
- Build protocol: [AI_BUILD_PROTOCOL.md](./AI_BUILD_PROTOCOL.md)
