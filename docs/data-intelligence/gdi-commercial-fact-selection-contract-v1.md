# GDI Commercial Fact Selection Contract V1

**Track:** A — Research & Commercial Intelligence  
**Status:** Design contract (Brief v2 amendment on PR #452). Full automated selection pipeline = **next Track A slice**.  
**Shared with:** PDI-3 Portfolio Demand Intelligence (same drawer + supporting research; no second engine).

## Objective

Raise fidelity between archived source research (Exa / Webhound / ORS / contacts) and the customer opportunity drawer **without** equating length with quality.

## Fact record (required fields)

| Field | Description |
|---|---|
| `fact` / `claim` | Single commercially readable statement |
| `sourceUrl` | Evidence URL |
| `evidenceLocation` | Artifact path + JSON pointer / quote locus when available |
| `verificationDate` | When research verified the claim |
| `activityDate` | When the commercial activity occurred (if distinct) |
| `status` | `CONFIRMED` · `RELIABLY_INFERRED` · `UNKNOWN` |
| `opportunityRelevance` | Why it matters to this opportunity |
| `hotelRelevance` | Why it matters to this hotel (or `NONE`) |
| `commercialImportance` | `HIGH` · `MEDIUM` · `LOW` |
| `changesPursuit` | boolean — alters posture, buyer path, or next step |
| `destination` | `BRIEF` (Level 1) or `SUPPORTING` (Level 2) |

Never invent source information when archived research is incomplete. Mark absences as `UNKNOWN` / `ABSENT_FROM_RESEARCH`.

## Selection priority (into Level 1 Brief)

Promote only facts that materially affect:

1. Demand existence and timing  
2. Hotel fit / competitive placement  
3. Buyer accessibility  
4. Pursuit recommendation  

All other verified useful facts remain in durable research structure and Level 2 Supporting Research.

## Two-level presentation (existing drawer)

### Level 1 — Commercial Intelligence Brief

Concise fields already on the opportunity drawer:

- What is happening  
- Why this hotel  
- Potential hotel business  
- Why act now  
- Recommended next step  
- Uncertainties  
- Commercial posture  

**Rules:** evidence-backed; no unsupported certainty; no methodology leakage; no repeated paragraphs from Level 2.

### Level 2 — Supporting Research

Reuse existing opportunity detail sections (do not fork a portfolio drawer):

- Lodging evidence / claims / evidence[]  
- Contacts (primary + secondary + org routes)  
- Historical venue intelligence  
- Partners / competitor hotel detail  
- Source references  
- Research gaps / qualification questions  

**Rules:** no copy-paste of Level 1 paragraphs; Level 2 holds depth Level 1 intentionally omits.

## Honesty constraints (composer)

- Do not infer competitor placement from generic brand mentions or from **this hotel’s** partner listing.  
- Do not claim a hotel is a credible host merely because the venue is unannounced.  
- Do not append generic demand statements to repair truncated sentences.  
- Do not turn missing evidence into positive hotel suitability.  
- Do not replace a verified specific fact with vague boilerplate.  
- Incomplete sentences: reconstruct from preserved source fields or remove/close — never invent filler.

## Evaluation

Retain the 30-point Brief v2 narrative score **and** run evidence-fidelity evaluation:

- Material verified facts retained vs omitted  
- Unsupported statements introduced  
- Incorrect certainty / posture  
- Duplication / verbosity  
- Source-to-claim traceability  

A brief cannot be judged excellent solely for length, keyword presence, or URL count.

## Out of scope for PR #452 amendment

- Automated Exa→fact graph ingestion  
- Paid research refresh  
- Second drawer / AI composition platform  
- Production Airtable persistence of fact records  

**Next Track A slice:** implement reusable fact-selection into Level 1/2 destinations using this contract + the five-opportunity evidence catalogs under `fixtures/gdi-brief-v2-narrative-quality-v1/evidence-catalogs/`.
