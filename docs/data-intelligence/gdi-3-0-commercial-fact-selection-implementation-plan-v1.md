# GDI 3.0 — Commercial Fact Selection implementation plan V1

**Track:** A — Research & Commercial Intelligence  
**Status:** Implementation PR (after PDI-3 #460 merged)  
**Contract:** `docs/data-intelligence/gdi-commercial-fact-selection-contract-v1.md` (shipped in #452)  
**Catalogs:** `fixtures/gdi-brief-v2-narrative-quality-v1/evidence-catalogs/`

## Objective

Automatically decide which verified research findings deserve Level 1 Brief prominence vs Level 2 Supporting Research — without arbitrary paragraph expansion or inventing demand.

## Workflow (per opportunity)

1. Load existing Exa / Webhound / ORS / reusable Airtable intelligence (no new paid calls without founder auth).  
2. Extract evidence-backed commercial claims.  
3. Preserve source URL, dates, certainty, provenance.  
4. Deduplicate overlapping facts without dropping independent sources.  
5. Score opportunity + hotel relevance.  
6. Detect whether the fact changes demand credibility, timing, hotel fit, buyer access, competitive position, or pursuit.  
7. Promote material facts to Level 1.  
8. Retain other verified useful findings in Level 2.  
9. Persist the complete catalog in durable research structure (Slice 2A/2B compatible).  
10. Report omissions, unsupported claims, and persistence failures.

## Critical reevaluation (do not trust prior L1/L2 labels blindly)

| Fixture | Must reevaluate |
|---|---|
| PDAC | Direct hotel booking vs housing bureau |
| NICE | FIU/New America logistics; private customer-sourcing feedback (hotel-private — never shared market packet) |
| OWWA | Sourcing timing, historical venue, verified buyer route |
| NRC RIC | Past cycle → no current open pursuit |
| IMC | Expansion signal without inventing room demand |

Historical dates must never become future deadlines. Hotel-private outcomes must never enter shared market packets.

## Quality measures (separate scores)

- Level 1 commercial fact selection accuracy  
- Complete verified fact retention across L1+L2  
- Source-to-claim fidelity  
- Unsupported assertion count  
- Redundancy  
- Hotel-specific pursuit usefulness  

Do **not** award perfect overall fidelity merely because predesignated Level 1 needles appear.

## Persistence

- Compatible with Slice 2A/2B graph persistence  
- No new Airtable tables unless evidenced schema gap + founder review  
- Never discard non-brief facts; never silently truncate source evidence for payload limits  
- If catalog lives outside Airtable, durable cross-environment reference required  

## Non-goals for the first fact-selection PR

- Paid Exa refresh  
- Production Airtable writes / schema migrations  
- Second drawer / AI composition platform  
- Customer publication  

## Suggested PR title

`feat(gdi): commercial fact selection Level 1/2 promotion v1`
