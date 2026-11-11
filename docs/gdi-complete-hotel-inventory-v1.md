# Complete GDI hotel inventory

Read-only account of hotels with GDI configuration, a canonical opportunity store, or historical research state. No hotel store, publication registry, or Airtable record was changed.

Run: `npm run gdi:complete-hotel-inventory-audit`

The audit follows `GDI_DATA_ROOT` for the live selector. The canonical repository data root is used when that variable is unset. A certification isolation root must not be left on the application server.

## Inventory

33 canonical hotel identities. 18 are in the current selector when the server reads the canonical data root. 15 of those have a local `opportunities.json`.

| Hotel | Market | Canonical opportunities | Selector | Research location |
| --- | --- | --- | --- | --- |
| Bethesda Marriott | United States | 59 | Yes | Canonical store. Historical copies are smaller. Do not overwrite. |
| Renaissance New York Times Square | United States | 72 | Yes | Canonical store is larger than the historical copies. |
| Wyndham San Diego Bayside | United States | 82 | Yes | Canonical store matches the historical copies. |
| Waterstone Resort & Marina | United States, Boca Raton | 55 | Yes | Canonical store is larger than the historical copies. |
| The Mayflower Hotel | United States, Washington | 0 | Yes, empty | Profile only in the Aimbridge worktree. No persisted opportunity file. Earlier Airtable readback was not written locally. |
| Nobu Hotel London Portman Square | United Kingdom | 0 | Yes, empty | Report folders exist. No persisted opportunity file. |
| The Westin Tampa Waterside | United States, Tampa | 0 | Yes, empty | Config only. No persisted opportunity file. |
| YOTEL Geneva Lake | Switzerland | 99 | Yes | Canonical store. |
| W Rome | Italy | 13 | Yes | Canonical store. |
| The Westin Grand München | Germany | 26 | Yes | Canonical store. |
| Sheraton Mallorca Arabella | Spain | 30 | Yes | Canonical store. |
| AC Hotel A Coruña | Spain | 39 | Yes | Canonical store. |
| Castillo Hotel Son Vida | Spain | 20 | Yes | Canonical store. |
| Hilton New York Times Square | Market not recorded on the GDI config | 45 | Yes | Canonical store. |
| NOW NOW NOHO | Market not recorded on the GDI config | 8 | Yes | Canonical store. |
| Cambridge Beaches | Market not recorded on the GDI config | 8 | Yes | Canonical store. |
| Radisson Hotel Santo Domingo | Dominican Republic | 16 | Yes | Canonical store. |
| Spice Island Beach Resort | Grenada | 77 research / 7 published | Yes | Published registry remains the certified seven. Research rows are not the published inventory. |
| Sheraton Guadalajara Expo | Mexico | 0 opportunities | No | Aimbridge worktree: 14 segment files, 680 candidate ids, 434 research-state files. |
| voco Cancún Zona Hotelera | Mexico | 0 opportunities | No | Aimbridge worktree: 14 segment files, 646 candidate ids, 381 research-state files. |
| Wyndham Alltra Punta Cana | Dominican Republic | 0 opportunities | No | Aimbridge worktree: 14 segment files, 553 candidate ids, 227 research-state files. |
| Fiesta Americana Condesa Cancún | Mexico | 0 | No | Need-period mirror only. No opportunity store. |
| Grand Fiesta Americana Chapultepec | Mexico | 0 | No | Need-period mirror only. No opportunity store. |
| Falls Church Marriott Fairview Park | United States | 0 | No | Need-period mirror only. No opportunity store. |

These configured hotels have no recoverable opportunity store: Hotel Phillips Kansas City, Faranda Collection Bogotá, Hotel Caribe by Faranda Grand, The Westin Monterrey Valle, JW Marriott Hotel Santo Domingo, The St. Regis Cap Cana, The St. Regis Mexico City, Casas del XVI, and JW Marriott Hotel Monterrey Valle.

Tampa Airport Marriott and Charlotte Marriott City Center were not found as persisted GDI opportunity stores in the canonical tree, the Aimbridge worktree, or the Tampa scaling worktree.

## Why hotels were missing

Hotels with a canonical `opportunities.json` are already in the selector. They were hidden only while the local server had `GDI_DATA_ROOT` pointed at the four-hotel certification isolation copy. That was a process environment, not the repository default.

Sheraton Guadalajara, voco Cancún, and Wyndham Alltra are absent because their identity config and research state live in the Aimbridge worktree, not in the canonical data root. Their research is segment state and research-state dossiers, not `opportunities.json`. Those dossiers were not converted into customer opportunities.

Mayflower, Nobu, and Westin Tampa are selectable and empty because no opportunity file was persisted. Condesa, Chapultepec, and Falls Church have a need-period mirror only.

## Evidence

Across the canonical opportunity stores, independent-evidence classification is:

| Class | Count | Meaning |
| --- | --- | --- |
| A | 0 | Independent evidence already connected to a confirmed or reliable-inferred demand claim |
| B | 4 | Persisted Exa evidence can be connected by the existing rehydrate module. All four are on W Rome. |
| C | 624 | A source, URL, or evidence object exists and does not support the demand claim |
| D | 21 | No persisted evidence artifact |

Generated thesis text was not counted as independent evidence. Stores were not rewritten.

## Shadow qualification

The existing commercial eligibility contract was applied in memory to the canonical stores. Nothing was saved.

| Layer | Count |
| --- | --- |
| Sales Ready | 1, on W Rome |
| Account Development | 1, on W Rome: supported demand and a relevant named contact, not all Sales Ready gates |
| Research Continuation | 647 |
| Speculative Sales Ready | 0 |

The four certification hotels remain entirely research continuation under the independent-evidence rule. Their earlier confirmed labels came from generated thesis wording. Spice Island's seven published records were not reclassified or republished.

## Customer presentation

With the canonical data root, the hotel API returns 18 hotels across the United States, the United Kingdom, Switzerland, Germany, Spain, Italy, the Dominican Republic, and Grenada. Three of those hotels have no country recorded on their GDI config. Opportunity counts for the stored hotels match the files above. The 60-row relevance cap remains removed. Research-only rows are not written into the published registry. Bethesda's published count is 0. Spice Island's published count is 7.

Signed-in customer sessions, production Railway, and Airtable readback were not retested.

## Certification scorecard

| Layer | Result |
| --- | --- |
| Engine execution | PASS for the read-only inventory and the canonical selector. This is not a full research rerun. |
| Research quality | NOT VERIFIED. 624 of the stored opportunities have evidence that does not support the demand claim. |
| Commercial qualification | NOT VERIFIED. One shadow Sales Ready row is not a portfolio certification. |
| Customer delivery | NOT VERIFIED. Local selector and anonymous reads were checked. Production and signed-in delivery were not. |
| Publication safety | PASS. Spice Island's seven published records and the canonical opportunity hashes were not modified. |
| Full E2E | NOT VERIFIED. |

## Dry-run import plan

Not applied. No file was copied.

1. Do not overwrite any canonical `opportunities.json`. Bethesda, Renaissance, and Waterstone are larger than the historical copies.
2. Copying Aimbridge `full-e2e-fourteen-segment-state` and `opportunity-research-state` for Sheraton Guadalajara (`recCAGqkERSXgiku4`), voco Cancún (`recTk5I8VOzsPhxDz`), and Wyndham Alltra (`recXh4NgF8sDbi9cQ`) would restore the research dossiers beside the canonical hotel id. It would not create customer opportunities.
3. Copying their existing config files would make them eligible for an internal selector only after an explicit visibility decision. They must not be added to a customer share.
4. Mayflower, Nobu, Westin Tampa, Condesa, Chapultepec, and Falls Church have no opportunity file to import. Do not synthesize rows from need-period mirrors or report prose.

Approval is required before any of those copies.
