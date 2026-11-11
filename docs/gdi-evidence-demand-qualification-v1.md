# GDI evidence-to-demand mapping

Shadow evaluation of the 649 canonical opportunity records after correcting one mapping defect. No opportunity store, publication registry, or Airtable record was written.

## Why 624 rows looked unsupported

The classifier read `quote`, `excerpt`, and `evidenceQuote`. The persisted research stores the source fact elsewhere:

- `evidence.supportsFact` is a short fact code such as `hotel_block`, `event_dates`, `venue`, or `source`
- `evidence.sourceTitle` and `evidence.extractedText` hold the source wording
- `sources[].title`, `claim`, and `establishes` hold the same class of text

`supportsFact: "source"` is the majority code (405 evidence rows). That records a source, not a demand claim. Those rows stay unsupported. The defect was that the minority of rows with a real fact code or a source title were invisible to the classifier, so generated thesis text was the only wording it could see, and that wording is not independent evidence.

## Corrected rule

Independent fields and fact codes are read. Generated thesis text is not.

| Result | Rule |
| --- | --- |
| CONFIRMED_DEMAND | The source text or fact code directly records accommodation, a room block, a host hotel, housing, overnight lodging, a meeting room, catering, or a banquet. |
| RELIABLE_INFERRED_DEMAND | Independent evidence shows a group activity, a date, a venue or city, and a scale figure or scale fact code. A bare organization page or the word "meeting" is not enough. |
| SPECULATIVE_DEMAND | A source exists but does not meet either rule, or no source artifact exists. |
| CONTRADICTORY_OR_NEGATIVE | The stored fact is `no_housing_partner`, or the source text cancels lodging, and no direct demand fact is present. |

## Portfolio result

| Class | Before this mapping | After |
| --- | --- | --- |
| CONFIRMED_DEMAND | 0 | 29 |
| RELIABLE_INFERRED_DEMAND | 0 | 2 |
| SPECULATIVE_DEMAND | 649 | 617 |
| CONTRADICTORY_OR_NEGATIVE | 0 | 1 |
| Sales Ready | 1, on W Rome | 3 |
| Account Development | 1, on W Rome | 7 |
| Research Continuation | 647 | 639 |
| Verified direct buyers | counted inside contact classes | 42 |
| Verified commercial intermediaries | counted inside contact classes | 17 |

Substantiated opportunities: 31. Unsupported or missing the required evidence combination: 617. One contradictory row.

### Per hotel

| Hotel | Opportunities | Confirmed | Reliable inferred | Speculative | Sales Ready | Account Development |
| --- | --- | --- | --- | --- | --- | --- |
| Bethesda Marriott | 59 | 12 | 2 | 44 | 2 | 4 |
| Waterstone Resort & Marina | 55 | 7 | 0 | 48 | 1 | 3 |
| Hilton New York Times Square | 45 | 5 | 0 | 40 | 0 | 0 |
| Wyndham San Diego Bayside | 82 | 3 | 0 | 79 | 0 | 0 |
| Renaissance Times Square | 72 | 2 | 0 | 70 | 0 | 0 |
| AC Hotel A Coruña | 39 | 0 | 0 | 39 | 0 | 0 |
| YOTEL Geneva Lake | 99 | 0 | 0 | 99 | 0 | 0 |
| Spice Island Beach Resort | 77 | 0 | 0 | 77 | 0 | 0 |
| Sheraton Mallorca Arabella | 30 | 0 | 0 | 30 | 0 | 0 |
| Westin Grand München | 26 | 0 | 0 | 26 | 0 | 0 |
| Castillo Son Vida | 20 | 0 | 0 | 20 | 0 | 0 |
| Radisson Santo Domingo | 16 | 0 | 0 | 16 | 0 | 0 |
| W Rome | 13 | 0 | 0 | 13 | 0 | 0 |
| NOW NOW NOHO | 8 | 0 | 0 | 8 | 0 | 0 |
| Cambridge Beaches | 8 | 0 | 0 | 8 | 0 | 0 |

### Examples

- Promotion: Bethesda rows whose stored fact code is a housing or room-block code move from speculative to confirmed. Two of those, with a verified named contact, are Sales Ready in the shadow replay. Four more are Account Development.
- Demotion: W Rome's previous shadow Sales Ready row returns to research continuation. Its stored sources do not directly record accommodation or the activity, timing, location, and scale combination.
- Unresolved: Spice Island's 77 research rows stay speculative and have no verified named person. The seven approved published records were not reclassified or rewritten.

## Code change

`classifyDemandMechanismEvidence` now reads the persisted source fields and fact codes listed above. `rehydratePersistedExaEvidence` copies `extractedText` or `sourceTitle` into the evidence quote when that quote is empty, so an existing evidence row is not treated as having no text. No second evidence store was added.

## Aimbridge research-only plan

`npm run gdi:research-only-import-dry-run` prints the copy plan and does not write. `--apply` exits 2 with `import_not_approved`.

Sheraton Guadalajara Expo, voco Cancún, and Wyndham Alltra remain `RESEARCH_ONLY`. They are not added to the customer hotel selector and do not receive an `opportunities.json` or a Sales Ready status. The dry run confirmed 14 segment files and 434, 381, and 227 research-state files.

## Customer inventory

The canonical selector still returns 18 hotels, 15 of which have an opportunity file. Mayflower, Nobu London Portman Square, and Westin Tampa Waterside remain selectable and empty. Bethesda's research store stays separate from its empty published inventory. Spice Island's seven published records were not modified.

## Scorecard

| Layer | Result |
| --- | --- |
| Engine execution | PASS for the zero-write shadow replay |
| Research quality | NOT VERIFIED. 617 opportunities still lack an independent demand case. |
| Commercial qualification | NOT VERIFIED. Three shadow Sales Ready rows are not a portfolio certification. |
| Customer delivery | NOT VERIFIED beyond the local selector |
| Publication safety | PASS. No store or registry was written. |
| Full E2E | NOT VERIFIED |
