# Hotel-relevance research — acceptance

**Date:** 2026-10-14  
**Hotel:** Mayflower `rec0eTEnCqlsGJJt9`

## Process (repeatable)

1. Coordinator stage `synthesize_hotel_relevance` runs after contacts, before eligibility/cards.
2. Module synthesizes the 7-field brief; labels FACT / INFERENCE / MIXED; never infers open buying from TBA/unnamed hotel.
3. Gaps → `INTERNAL_GAP` / `INSUFFICIENT` (internal retention; no generic facing filler).
4. Importer overlay is **generated from research** (`sales-review-overlay-from-research.json`).
5. Airtable `opportunityPayloadJson` stores `hotelRelevanceResearch` + version + disposition + researchedAt.
6. List DTO and detail API preserve those fields; drawer contract requires `READY_FOR_CARD` for facing.

## Mayflower backfill

| Metric | Result |
|--------|--------|
| Pool | 11 facing |
| `readyForCard` | 11 |
| Airtable apply | ok (`import-result-apply-2026-10-14T23-11-26-050Z.json`) |
| `drawerCompletenessPass` | 11 |
| Hand overlay | **not used** — research-stage overlay only |

Sample (AHIMA): Airtable stamp present; list DTO carries research; drawer contract `ok` with allowed unknown rooms / unconfirmed sourcing warnings.

## Next hotel

```bash
node scripts/gdi-research-to-page-workflow-v1.mjs --hotel <hotelId> --dry-run
```

Publication path consumes research-generated overlay; no separate research package required from an editor.
