# Contradiction-First V1 — Final Report

## Did Dealality learn enough from Webhound?

**Yes — for this experiment.** High-confidence material rediscovery rate **87.5%** (threshold 70%). Native checker independently caught the core Indigo Pipeline→Open freshness class, Casa Nizuc / Tribute Mexico directory gaps, Kimpton Tres Ríos gap, and Avani Brand Explorer absence — without Webhound, credits, or Airtable writes.

Production readiness: **Promising** — experiment-only code path; not production-hardened; match-confidence gates required before any apply path.

## What was built

- `lib/research-engine-v2/*` — claim model, source hierarchy, query generator, adapters (IHG/Marriott/Choice/generic), `checkHotelFreshness`, cross-table checks
- `scripts/research-engine-v2-contradiction-first-v1.mjs` — blind benchmark runner
- Artifacts under `data/research-engine-v2/contradiction-first-v1/`

## How it works

1. Snapshot Dealality census values (local CSV; no SoT write)
2. Route hotel → brand-family adapter
3. Match official directory → fetch live page → parse brand/status
4. Emit claims + proposed corrections (support + disproof query lists attached)
5. Light cross-table + directory-gap checks
6. Freeze native results → then compare to Test 6

## Existing infrastructure reused

Hilton status-audit pattern; IHG directory extract + hoteldetail parsers; Marriott URL helpers; Choice sitemap loader; census field constants; local census + directory reports.

## Native results (before Webhound)

- Hotels checked: 49
- Material proposed corrections: 14
- Runtime: 18339 ms (~18.3s)
- External cost: $0

## Webhound comparison (after freeze)

- High-confidence rediscovery: **87.5%** (found 7, partial 0, missed 1 / 8)
- All material incl. medium: **70.8%**
- Threshold ≥70%: **true**; stretch ≥80%: **true**

### Per finding

- **Found It Independently** · `indigo-playa-pipeline-open` · Pipeline → Operating · Hotel Indigo Playa del Carmen
- **Found It Independently** · `indigo-tijuana-pipeline-open` · Pipeline → Operating · Hotel Indigo Tijuana Downtown
- **Found It Independently** · `indigo-lima-pipeline-open` · Pipeline → Operating · Hotel Indigo Lima Miraflores
- **Found It Independently** · `indigo-barbados-pipeline-open` · Pipeline → Operating · Hotel Indigo Bridgetown Barbados
- **Found It Independently** · `tribute-casa-nizuc-missing-pipeline` · Missing pipeline hotels · Casa Nizuc
- **Missed It** · `barbados-autograph-to-tribute` · Reflags · Crystal Cove / Turtle Beach Barbados
- **Found It Independently** · `avani-missing-brand-explorer` · Missing brand census / BE profile · Avani (brand)
- **Found It Independently** · `kimpton-tres-rios-missing` · Missing pipeline / census hotels · Kimpton Tres Rios
- **Partially Found It** · `kimpton-aluna-identity` · Cross-table / identity conflict · Kimpton Aluna Resort Tulum
- **Found It Independently** · `tribute-mexico-gaps-alameda-merida-holbox` · Missing brand census records · Alameda / Merida / Mystique Holbox
- **Missed It** · `tulum-tribute-vs-design-hotels` · Reflags / brand page conflict · Tulum Tribute / Design Hotels
- **Missed It** · `choice-faranda-extra-hotels` · Missing census records / operator spine · V Grand Medellin / Faranda Collection Cartagena

## False positives

- Status FPs (Test 6 keep-Pipeline hotels marked Open): 3
- Likely bad reflags (excluding Casa Francia review candidate): 3

## Misses

- **barbados-autograph-to-tribute** (Crystal Cove / Turtle Beach Barbados): Barbados Crystal Cove / Turtle Beach not in amenities-blank census snapshot; no Autograph Barbados rows to reflag
- **tulum-tribute-vs-design-hotels** (Tulum Tribute / Design Hotels): No Design Hotels Tulum / Tribute page conflict check in V1 adapters
- **choice-faranda-extra-hotels** (V Grand Medellin / Faranda Collection Cartagena): Choice adapter checked existing census rows only; no Choice directory-gap pass in V1

## Top 3 next improvements

1. **Strict directory match gates** — require high name+geo confidence before status/brand corrections (kills Indigo false Opens / Holiday Inn reflags).
2. **Expand Marriott + Choice gap scans** — full Autograph/Tribute/Design CALA catalogs + Choice sitemap minus census (Barbados reflags + Faranda +2).
3. **Opening-announcement / bookability corroboration** — second source before Pipeline→Open proposals.
