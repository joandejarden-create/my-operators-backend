# GDI Contextual How to Read — Help Content Accuracy

**Contract:** guides live in `public/js/group-demand-intelligence/gdi-guided-report-tour.js` (reuse `DealalityGuidedTourEngine`). No separate help CMS.

## When to update

Whenever a GDI feature, filter, column, metric, or interaction changes, validate whether the matching How to Read guide needs an update:

| Active tab | Guide pack |
|---|---|
| Executive Summary | `EXECUTIVE_SUMMARY_STEPS` |
| Group & Demand Opportunities | `OPPORTUNITIES_STEPS` |
| Commercial Priorities | `PRIORITIES_STEPS` |
| Portfolio Demand Intelligence | Reuses Opportunities guide (unchanged) |

## Validation

```bash
npm run test:gdi-guided-report-tour-v1
```

The suite forbids obsolete UI phrases (Contact Readiness, Opportunity Status, Sales Opportunity Type) and asserts live `data-gdi-tour-target` anchors still exist in ES / Opportunities / Commercial Priorities modules.
