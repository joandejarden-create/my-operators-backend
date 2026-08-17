# Operator Explorer — Research Pipeline

**Date:** 2026-08-09  
**Status:** Process design (not implemented as single CLI yet)  
**Principle:** Founder approves **policy**; routine objective facts auto-publish under policy; exceptions escalate.

---

## Scalable wave flow

```text
Operator Wave Input (company list)
→ Entity Resolution (match Master / create Research Stage draft plan)
→ Existing Record Audit (fields, presence, claims, case studies, dummies)
→ Research Plan (gaps by MVOP + Fit readiness)
→ Source Discovery (queries + PI Source Library)
→ Structured Assignment Discovery
→ Company Claims
→ Market Presence
→ Brand Relationships (typed edges — when table exists)
→ Evidence attach / classify
→ Conflict Detection
→ Publication Resolver (lib/operator-intelligence/publication-policy.js)
→ Airtable Dry Run (write plan + sanitized preview)
→ Apply (explicit flag)
→ Validation (post-write)
→ Explorer Readiness gate
→ Fit Readiness gate
→ Exception Report (founder / researcher)
```

---

## Command shape (target)

```bash
# conceptual
npm run operator-explorer-research-wave -- --companies list.csv --dry-run
npm run operator-explorer-research-wave -- --companies list.csv --apply  # only after dry-run review
```

Reuse Brand Explorer **shell** patterns (dry-run, backup manifest, gates) via adapter — do not call brand writers.

---

## Readiness gates (separate)

| Gate | Meaning |
| ---- | ------- |
| Research Complete Enough | Structured evidence for internal use |
| Explorer Publishable | Owner-useful profile per minimum profile |
| Fit Data Ready | Production Fit ranking inputs sufficient |
| High Confidence | Deeper intelligence / Company Validated path |

No single percentage controls all gates.

---

## Exception classes requiring founder/human review

- Hard conflicts  
- Sensitive/negative claims  
- Performance / fees / capacity  
- Project-specific brand approval or interest  
- Graduation Research Stage → Active  
- Dummy/Test reclassification  
- Baseline golden remediations (Arbor/HE)

## Auto under policy (examples)

- Official identity/website/HQ  
- Current hotel assignments with primary sources  
- Typed market presence with evidence  
- Brands operated (experience) with sources  
