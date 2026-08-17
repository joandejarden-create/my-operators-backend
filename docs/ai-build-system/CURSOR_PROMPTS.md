# Dealality Cursor Prompts

Reusable prompts for feature planning, implementation, QA, and learning capture.

Copy a prompt below into Cursor or ChatGPT. Replace bracketed placeholders.

---

## Prompt 1: Feature Brief Prompt

```text
Help me turn the following Dealality idea into a build-ready feature brief.

Use:
- docs/ai-build-system/DEALALITY_PRODUCT_CONSTITUTION.md
- docs/ai-build-system/FEATURE_BRIEF_TEMPLATE.md
- docs/ai-build-system/NAMING_AND_COPY_GUIDE.md
- docs/data-intelligence/INTELLIGENCE_GOVERNANCE.md if data/content is involved.

Feature idea:
[PASTE FEATURE IDEA]

Produce:
1. Feature brief.
2. Key assumptions.
3. Data required.
4. AI behavior.
5. Edge cases.
6. QA checklist.
7. Cursor implementation prompt.
```

---

## Prompt 2: Cursor Build Prompt

```text
You are working inside the Dealality repo.

Before editing, read:
- docs/ai-build-system/DEALALITY_PRODUCT_CONSTITUTION.md
- docs/ai-build-system/AI_BUILD_PROTOCOL.md
- docs/ai-build-system/CURSOR_IMPLEMENTATION_PROTOCOL.md
- docs/ai-build-system/BUILD_DECISIONS.md
- docs/ai-build-system/NAMING_AND_COPY_GUIDE.md
- docs/ai-build-system/DEALALITY_QA_CHECKLIST.md

If this task touches data/content/intelligence, also read:
- docs/data-intelligence/DATA_VALIDATION_PROTOCOL.md
- docs/data-intelligence/INTELLIGENCE_GOVERNANCE.md
- docs/data-intelligence/CONTENT_QA_CHECKLIST.md

Task:
[PASTE TASK]

Required process:
1. Inspect relevant files first.
2. Summarize current implementation.
3. Identify assumptions and risks.
4. Provide a short implementation plan.
5. Implement in small, safe steps.
6. Avoid unrelated changes.
7. Do not invent Airtable fields.
8. Do not overwrite company-validated data.
9. Run available tests/checks.
10. Update relevant docs only if durable decisions or reusable patterns were created.
11. Final response must include changed files, tests run, risks, and suggested next step.
```

---

## Prompt 3: Cursor QA Prompt

```text
Review the completed changes against Dealality standards.

Read:
- docs/ai-build-system/DEALALITY_PRODUCT_CONSTITUTION.md
- docs/ai-build-system/DEALALITY_QA_CHECKLIST.md
- docs/ai-build-system/NAMING_AND_COPY_GUIDE.md
- docs/ai-build-system/BUILD_DECISIONS.md

If data/content/intelligence is involved, also read:
- docs/data-intelligence/DATA_VALIDATION_PROTOCOL.md
- docs/data-intelligence/INTELLIGENCE_GOVERNANCE.md
- docs/data-intelligence/CONTENT_QA_CHECKLIST.md

Check for:
1. Product strategy drift.
2. Wrong terminology.
3. Overclaiming or unsupported AI language.
4. Missing data handling.
5. Validation/status issues.
6. Airtable field risks.
7. Access/security risks.
8. Broken or fragile implementation.
9. Missing tests.
10. Documentation updates needed.

Provide:
- Issues found.
- Severity.
- Recommended fixes.
- Whether this is ready to test manually.
```

---

## Prompt 4: Learning Capture Prompt

```text
Review this completed task and identify any durable Dealality learnings.

Capture only items that will help future builds.

Classify each as:
- Product decision
- Naming/copy rule
- Data validation rule
- Intelligence governance rule
- Technical pattern
- Testing requirement
- Known risk

Then update the appropriate file:
- docs/ai-build-system/BUILD_DECISIONS.md
- docs/ai-build-system/NAMING_AND_COPY_GUIDE.md
- docs/data-intelligence/INTELLIGENCE_GOVERNANCE.md
- docs/data-intelligence/DATA_VALIDATION_PROTOCOL.md
- docs/platform-reference/TESTING_PROTOCOL.md

Do not add noise. Do not duplicate existing decisions.
```

---

## Prompt 5: Operator Explorer Tab Factory Build

```text
You are building or remediating an Operator Explorer profile in the Dealality repo.

Before editing, read:
- docs/ai-build-system/DEALALITY_PRODUCT_CONSTITUTION.md
- docs/ai-build-system/AI_BUILD_PROTOCOL.md
- docs/ai-build-system/CURSOR_IMPLEMENTATION_PROTOCOL.md
- docs/ai-build-system/BUILD_DECISIONS.md
- docs/data-intelligence/operator-explorer-protected-baseline-rules.md
- docs/data-intelligence/operator-explorer-arbor-hotel-equities-quality-baseline.md
- docs/data-intelligence/operator-explorer-tab-factory-build-operation.md
- docs/data-intelligence/operator-explorer-mandatory-release-gates.md
- docs/data-intelligence/INTELLIGENCE_GOVERNANCE.md
- docs/data-intelligence/CONTENT_QA_CHECKLIST.md

Quality bar (non-negotiable): Arbor Lodging (CALA) + Hotel Equities (CALA) — tab by tab and field by field.

Task:
[PASTE OPERATOR NAME + RECORD ID + GOAL]

Required process:
1. Inspect live Explorer + fixtures for Arbor and Hotel Equities as benchmarks.
2. Summarize current operator state (tabs thick/thin, empty shells, PI package status).
3. Identify risks (schema, Company Validated, golden baseline protection).
4. Follow Tab Factory sequence; dry-run before any apply.
5. Do not invent Airtable fields or overwrite company-validated data.
6. Do not modify Arbor/HE goldens unless this task is an explicit baseline revision.
7. Run: npm run test:operator-explorer-quality-baseline and npm run test:operator-explorer-mandatory-release-gates
8. Final response: changed files, audits, remaining failFindings, suggested next step.
```
