# Founder report — GDI multi-hotel inventory integrity recovery

## Direct answers

| Question | Answer |
|---|---|
| Were opportunities actually lost / deleted? | **No.** Research stores and packages are intact. |
| Wrong inventory / wrong filter? | **Yes.** Research Preview (`live=1`) applied **customer-facing publication filters**, hiding most research rows. |
| Sheraton Guadalajara’s previous ~40? | Research store still has **43** IDs. Broken preview showed **6**. STAGED package still has **38**. |
| Other hotels affected? | Systemic for non-Bethesda hotels on Research Preview (e.g. San Diego 82→47, Castillo 20→0, YOTEL research 42→17, Radisson research 66→17). |
| Can every missing ID be accounted for? | **Yes** — listed in `MISSING_OPPORTUNITY_IDS.md` / `.json` as suppressed, still in store. |
| What was fixed? | Shared API fix: Research Preview no longer applies customer-facing filters; Bethesda research uses FULL surface; integrity diagnostic added. |
| Original research preserved? | **Yes.** No store writes, no STAGED/CURRENT pointer changes, no Airtable, no paid research. |

## Root cause (one sentence)

PR #355 correctly defaulted localhost to Research Preview, which exposed an older bug: `getGdiOpportunities(?live=1)` still ran `filterCustomerFacingOpportunities`, so founder research looked like a collapsed published subset.

## Published baselines (unchanged)

- Bethesda CURRENT: **67**
- YOTEL CURRENT: **42**
- Radisson CURRENT: **66**

## Safety

Paid research off · Airtable writes off · No package pointer updates · No deploy · Unrelated WIP preserved.
