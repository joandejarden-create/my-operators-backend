# GDI Canonical Hotel Template V1

**Contract:** `GDI_CANONICAL_HOTEL_TEMPLATE_V1`  
**Module:** `lib/group-demand-intelligence/presentation/gdi-canonical-hotel-template-v1.js`  
**Gate:** `npm run test:gdi-canonical-hotel-template-v1`

## Purpose

The integrated GDI UI for the first three tabs is the **approved product-wide hotel template**.

Different hotels receive different intelligence. They must **not** receive different templates.

## In scope (frozen)

1. **Executive Summary** — Commercial Snapshot, Executive Commercial Assessment, Demand Composition, Commercial Demand Outlook, Dealality Research Receipt, Research Productivity metrics, Priority Commercial Opportunities, Commercial Outcomes  
2. **Group & Demand Opportunities** — List View, Tiles View, columns configuration, Contact Details, Actions SVGs, opportunity drawers, filters, compact Market Scope  
3. **Commercial Priorities**

Also preserve: contextual How to Read This Report guides, DealalityWaveLoader, navigation, backgrounds, typography, spacing, and styling.

## Out of scope

- Portfolio Demand Intelligence redesign  
- Visual changes of any kind  
- Hotel-specific UI forks  

## Rules

- Use shared canonical components and data contracts.  
- No hotel-specific UI implementations.  
- Only research, persistence, and functional hydration defects may be corrected under this contract.  
- Cumulative non-regression + this freeze gate protect the baseline.

## Related

- `DEALALITY_INTEGRATED_DELIVERY_V1`
- `DEALALITY_ADDITIVE_NON_REGRESSION_V1`
- `GDI_CONTACT_RESEARCH_BEST_EFFORT_V1`
- `GDI_FULL_E2E_COMPLETION_AND_PUBLICATION_LIFECYCLE_V1`
