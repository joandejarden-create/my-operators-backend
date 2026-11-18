# Root cause analysis

## Verdict

Opportunities were **not deleted**. They were **present in the hotel research store**, then **silently suppressed** by customer-publication visibility filters on the founder Research Preview path.

## Sequence

1. PR #355 restored localhost **Research Preview** (`inventory=research` + `live=1`) instead of auto-STAGED.
2. API `getGdiOpportunities` correctly skipped publication packages when `live=1` and loaded `opportunities.json`.
3. For non-Bethesda hotels it then called `filterCustomerFacingOpportunities` — a **customer publication** gate.
4. Founder UI showed the filtered subset (Guadalajara **6**, Castillo **0**, etc.) with filters set to All.
5. Before #355, localhost often auto-loaded STAGED (Guadalajara STAGED = **38**), masking the live-path bug.

## Why multi-hotel / not Guadalajara-specific

Same live-path filter applies to every non-Bethesda hotel. Bethesda had a separate union path that still narrowed commercial surfaces; Research Preview now forces FULL for Bethesda as well.

## Fix

- `forceLive` → skip `filterCustomerFacingOpportunities`
- Bethesda `forceLive` → `GDI_LIST_SURFACE_VIEW.FULL`
- `listScope = FOUNDER_RESEARCH_INVENTORY`
- `researchInventoryIntegrity` diagnostic on responses
- STAGED / Published / share paths unchanged
