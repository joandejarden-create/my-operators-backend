# ADR — Operator Fit Engine Phase 1–2 Founder Decisions

**Status:** Accepted  
**Date:** 2026-08-03  
**Scope:** Phase 1–2 only (`operatorFitEngineV2`)  
**Related:** `docs/architecture/operator-fit-engine-proposed-architecture.md`, `docs/architecture/operator-fit-engine-build-plan.md`

This record captures founder decisions **exactly** as approved for controlled implementation beside legacy OAS. Implementation must not reinterpret or silently override them.

---

## 1.1 Product terminology

The owner-facing result is called **Operator Alignment**.

Do not call it:

- Operator Performance Score
- Best Operator
- Recommended Operator
- Guaranteed Fit
- Predicted Performance

The MVP evaluates project-specific alignment based on available evidence. It does not claim that future operating performance has been predicted.

## 1.2 Existing operating-structure values

Preserve these owner-facing values exactly:

- Third-Party Management
- Franchise + Operator
- Franchise Only
- Owner-Operated
- Lease
- Asset Management
- To Be Confirmed

Do not rename these Airtable values, intake values, or persisted values.

Create an **internal canonical mapping only** where needed for deterministic logic. Document the mapping clearly.

The application must remain backward-compatible with existing records.

## 1.3 Brand-managed option

Include brand management in the v1 eligibility model as a distinct candidate type when:

- The selected or evaluated brand offers brand management
- Available data supports that conclusion
- The owner has not excluded brand management
- The project is within the brand’s applicable market and asset criteria

Do not build the complete brand-and-operator pathway matrix in this phase.

Brand-managed candidates may appear in the same Top-5 Operator Alignment experience, but must be visibly identified as **Brand Managed**.

Do not manufacture a normal third-party operator profile for the brand.

## 1.4 Layered model

Do not replace the legacy OAS with another single blended average.

The new model must keep these layers separate:

1. Eligibility  
2. Operator–Project Alignment  
3. Brand–Operator Compatibility  
4. Operating Structure Alignment  
5. Evidence Confidence  
6. Data Coverage  
7. Execution Risk  
8. Explanation and Validation  

The owner-facing UI may show one primary Operator Alignment score, but it must also show the separate layers needed to interpret that score.

Eligibility, confidence, coverage, and execution risk must not be hidden inside one unexplained number.

## 1.5 Table-stakes capabilities

Do not award positive differentiation points merely because an operator claims to provide:

- Revenue management  
- Sales  
- Marketing  
- Digital distribution  
- Procurement  
- Accounting  
- Financial reporting  
- Human resources  
- Generic owner relations  
- Generic pre-opening support  

These capabilities may be recorded and displayed.

Their confirmed absence may fail eligibility, create a warning, reduce alignment where specifically required, or produce a validation question.

Their generic presence does not create positive differentiation points.

Positive differentiation requires relevant depth, project similarity, evidence, specialization, demonstrated resources, or a project-specific operating advantage.

## 1.6 Missing information

Unknown must remain distinct from No.

Never:

- Convert unknown into yes  
- Convert unknown into a positive match  
- Remove an unknown factor and redistribute its weight  
- Recalculate the remaining factors to a new 100% denominator  
- Describe unknown information as a confirmed weakness  
- Describe unknown information as confirmed capability  

For every applicable scoring factor:

- The factor remains in the applicable denominator  
- Unknown produces no positive contribution  
- Unknown reduces data coverage  
- Unknown appears in the validation output  
- Unknown may constrain the displayed confidence band  

This specifically prevents sparse operator profiles from receiving inflated scores.

## 1.7 Evidence confidence

Use at least these evidence classes:

1. Verified project-level evidence  
2. Independently referenced or owner-referenced evidence  
3. Detailed operator-provided evidence  
4. Portfolio-level operator evidence  
5. General operator claim  
6. Unknown  

Operator-reported evidence alone may not produce Strong evidence confidence.

Strong confidence requires at least one independently supported, referenced, or verified source covering material alignment factors.

Do not describe marketing copy as verified evidence.

## 1.8 Evidence-based score ceilings

Configurable evidence ceilings for the displayed Operator Alignment result:

- Limited evidence: maximum displayed score of **69**  
- Moderate evidence: maximum displayed score of **84**  
- Strong evidence: **no** evidence-based ceiling  

The raw calculation may be retained internally for diagnostics. The owner-facing score must respect the confidence ceiling.

Classification rules live in **one centralized configuration module**.

## 1.9 Fee and economics treatment

Remove or bypass the legacy automatic flat fee score of 75 in the new engine.

Unknown operator economics must be represented as unknown.

Do not assume unknown fees are competitive, expensive, favorable, or unfavorable.

Until comparable economics exist, fees and contract economics primarily produce available facts, unknowns, validation questions, or eligibility conflicts where the owner specified a hard requirement.

Do not create false numerical precision from incomplete commercial terms.

## 1.10 Shortlisting

Do not create a new Airtable shortlist object in Phase 1–2.

The initial Top-5 result is a calculated result.

Continue using the existing Target List workflow for an owner’s affirmative selection of an operator where technically compatible.

Do not automatically add Top-5 operators to the Target List.

If the existing Target List cannot safely support operators, document the limitation and leave the save action disabled behind the feature flag rather than creating an unapproved persistence model.

**Phase 1–2 finding:** `api/target-list.js` is **brand-only** (`Brand Name`, deal-linked Target List rows). It cannot safely persist operators. Save-to-Target-List for operators remains **disabled** while `operatorFitEngineV2` is used.

## 1.11 Enrichment responsibility

Expected future ownership of operator information:

- **Dealality:** normalized baseline research and source control  
- **Operator:** structured claims, comparable hotels, resources, proposed team and commercial information  
- **Brand:** operator approval or brand-management availability where obtainable  
- **Owner or advisor:** optional references and project-specific experience  
- **System:** derived compatibility, coverage and scoring outputs  

Do not add owner intake questions asking owners to research operator capabilities.

## 1.12 Legacy OAS

Keep `scoreOperatorMatchForDeal` and the current OAS experience intact.

The new engine must operate beside it under a feature flag.

Do not:

- Change the legacy formula  
- Change legacy outputs  
- Replace existing saved scores  
- Alter Brand Match v2  
- Change owner intake mappings  
- Remove legacy UI components  
- Reinterpret historical OAS results  
