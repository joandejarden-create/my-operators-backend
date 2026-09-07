# 14 — Marketing OS Alignment Gaps

Audit vs Operating Law v1 — **recommend only; no broad schema rewrite applied.**

## Present (usable)

- Marketing Decisions table with D1–D5 + Phase 3A locks  
- Evidence Class / Decision Maker / provenance-ish Source fields  
- Approval Rules decision exists  

## Gaps (recommended small additions later)

| Need | Suggested field / structure | Priority |
|---|---|---|
| ICP label on content/growth rows | `ICP` select: ICP-O1/O2/B1/OP1/A1 | High for PREPARE join |
| GTM track | `GTM Track` select: ADP / OWNER_DEALMAKING | High |
| Product maturity | `Product Maturity` select | High |
| Claim class | `Claim Class` GREEN/YELLOW/RED | High |
| Pricing class | `Pricing Class` taxonomy | Medium (ADP offers) |
| Confidence | `Confidence` HIGH/MEDIUM/LOW | Medium |
| Epistemic type | `Epistemic` on insights/learnings | Medium |
| Prepare metadata completeness | checklist or JSON attachment | Medium |

## Do not do in this phase

- Full Marketing OS redesign  
- Automatic enum migration across all tables  
- Enabling joins that invent APPROVED content  

**Alignment score:** ~5.5/10 — law exists; OS not yet fully field-aligned.
