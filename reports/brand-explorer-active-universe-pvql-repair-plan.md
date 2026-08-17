# Active Universe — PVQL Repair Plan (dry-run)

Generated: 2026-07-23T10:30:05.963Z
Brands in `public_full_failing_pvql`: **16**
Field-level offender rows: **35**

Apply blocked in this normalization task. Allowed/forbidden write lists are below.

## Brands

- `ascend`
- `autograph-collection`
- `comfort-inn-suites`
- `country-inn-suites`
- `curio-collection`
- `design-hotels`
- `handwritten-collection`
- `hotel-indigo`
- `kimpton`
- `mgallery-collection`
- `radisson-individuals-by-choice`
- `small-luxury-hotels-of-the-world`
- `suburban-studios`
- `tribute-portfolio`
- `vignette-collection`
- `woodspring-suites`

## Field-level failures

| Brand | Tab | Section | Record ID | Field | Failure | Current (trim) | Proposed fix (trim) | Clean? |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `ascend` | Footprint & Growth | `footprint.openings` | `rec2Fr51vB58WwP8m` | Case Summary Interpretation | forbidden_owner_facing_language:fee_stack | Validate conversion PIP, Zona Rosa comp set, and fee stack locally—press is posi | Validate conversion PIP, Zona Rosa comp set, and participation costs and program | true |
| `comfort-inn-suites` | Footprint & Growth | `footprint.openings` | `recL0gIhmwYjCFTgU` | Body | forbidden_owner_facing_language:loi; forbidden_owner_facing_language:fdd | CALA, Puerto Rico, Urban / corridor

Levittown, Puerto Rico

Choice-affiliated · | CALA, Puerto Rico, Urban / corridor
Levittown, Puerto Rico
Choice-affiliated · l | true |
| `comfort-inn-suites` | Footprint & Growth | `footprint.openings` | `recL0gIhmwYjCFTgU` | Case Summary Interpretation | forbidden_owner_facing_language:item_19 | Validate levittown economics locally; census URL is a directory anchor, not Item | Validate levittown economics locally; census URL is a directory anchor, not publ | true |
| `comfort-inn-suites` | Footprint & Growth | `footprint.openings` | `recUIddvqwapbrYEB` | Body | forbidden_owner_facing_language:loi; forbidden_owner_facing_language:fdd | CALA, El Salvador, Urban / corridor

La Union, El Salvador

Choice-affiliated · | CALA, El Salvador, Urban / corridor
La Union, El Salvador
Choice-affiliated · li | true |
| `comfort-inn-suites` | Footprint & Growth | `footprint.openings` | `recUIddvqwapbrYEB` | Case Summary Interpretation | forbidden_owner_facing_language:item_19 | Validate la-union economics locally; census URL is a directory anchor, not Item | Validate la-union economics locally; census URL is a directory anchor, not publi | true |
| `country-inn-suites` | Footprint & Growth | `footprint.openings` | `recWrYO39O7mcoe8q` | Case Summary Interpretation | forbidden_owner_facing_language:item_19 | Validate Costa Rica authorization and Item 19 tables in disclosure. | Validate Costa Rica authorization and public performance materials tables in dis | true |
| `country-inn-suites` | Footprint & Growth | `footprint.openings` | `rec3sp9baczWMuiU4` | Case Summary Interpretation | forbidden_owner_facing_language:item_19 | Validate Costa Rica authorization and Item 19 tables in disclosure. | Validate Costa Rica authorization and public performance materials tables in dis | true |
| `country-inn-suites` | Footprint & Growth | `footprint.momentum` | `reclN4YCKttLKARXA` | Body | forbidden_owner_facing_language:revpar | Mar 2025

Two years after the Radisson Americas combination, Choice reported Cou | Mar 2025
Two years after the Radisson Americas combination, Choice reported Coun | true |
| `curio-collection` | Footprint & Growth | `footprint.openings` | `recg818yNxj1yjA3E` | Case Summary Interpretation | forbidden_owner_facing_language:adr | Historic-center assets need F&B and design capex matched to local ADR—do not ass | Historic-center assets need F&B and design capex matched to local average daily | true |
| `curio-collection` | Footprint & Growth | `footprint.openings` | `rec1WrllpfhNWetPi` | Case Summary Interpretation | forbidden_owner_facing_language:adr | AI economics differ materially from lifestyle boutique Curio—model package margi | AI economics differ materially from lifestyle boutique Curio—model package margi | true |
| `curio-collection` | Footprint & Growth | `footprint.openings` | `recVgcb1F49HzSoYs` | Body | forbidden_owner_facing_language:fee_stack | Resort, Honduras, CALA, Golf & beach, Meetings + leisure

Tela, Honduras (Bahía | Resort, Honduras, CALA, Golf & beach, Meetings + leisure
Tela, Honduras (Bahía d | true |
| `curio-collection` | Footprint & Growth | `footprint.openings` | `recrEapJy3r4jEUeq` | Case Summary Interpretation | forbidden_owner_facing_language:adr | San Telmo boutique economics depend on ADR and F&B margin in a competitive gatew | San Telmo boutique economics depend on average daily rate and F&B margin in a co | true |
| `kimpton` | Footprint & Growth | `footprint.openings` | `recttOTrQuiqEip4Z` | Case Summary Interpretation | forbidden_owner_facing_language:adr | Colonial-city assets need F&B and design capex matched to local ADR—do not assum | Colonial-city assets need F&B and design capex matched to local average daily ra | true |
| `kimpton` | Footprint & Growth | `footprint.openings` | `rec5FqPlYHgQ0LiFZ` | Case Summary Interpretation | forbidden_owner_facing_language:adr | Small-key boutiques live on ADR and F&B margin—match Polanco-style demand and op | Small-key boutiques live on average daily rate and F&B margin—match Polanco-styl | true |
| `radisson-individuals-by-choice` | Footprint & Growth | `footprint.openings` | `recto7QMu58eMf5jV` | Case Summary Owner Objective | forbidden_owner_facing_language:adr | Useful when underwriting coastal leisure ADR, F&B, and event demand with collect | Useful when underwriting coastal leisure average daily rate, F&B, and event dema | true |
| `autograph-collection` | corpus_scan | `(owner-facing rendered corpus)` | `—` | (locate via pvql-failure-scrub field scan on apply) | raw_url_scan | https://www.marriott.com/en-us/hotels/mspak-emery-autograph-collection/overview/ | Strip raw URL / forbidden language from the Presentation row embedding this snip | undefined |
| `autograph-collection` | corpus_scan | `(owner-facing rendered corpus)` | `—` | (locate via pvql-failure-scrub field scan on apply) | forbidden_owner_facing_language | https://www.marriott.com/en-us/hotels/mspak-emery-autograph-collection/overview/ | Strip raw URL / forbidden language from the Presentation row embedding this snip | undefined |
| `design-hotels` | corpus_scan | `(owner-facing rendered corpus)` | `—` | (locate via pvql-failure-scrub field scan on apply) | raw_url_scan | https://www.einpresswire.com/article/918483387/medell-n-joins-the-rise-of-luxury | Strip raw URL / forbidden language from the Presentation row embedding this snip | undefined |
| `design-hotels` | corpus_scan | `(owner-facing rendered corpus)` | `—` | (locate via pvql-failure-scrub field scan on apply) | forbidden_owner_facing_language | https://www.einpresswire.com/article/918483387/medell-n-joins-the-rise-of-luxury | Strip raw URL / forbidden language from the Presentation row embedding this snip | undefined |
| `handwritten-collection` | corpus_scan | `(owner-facing rendered corpus)` | `—` | (locate via pvql-failure-scrub field scan on apply) | raw_url_scan | https://all.accor.com/hotel/C344/index.en.shtml | Strip raw URL / forbidden language from the Presentation row embedding this snip | undefined |
| `handwritten-collection` | corpus_scan | `(owner-facing rendered corpus)` | `—` | (locate via pvql-failure-scrub field scan on apply) | forbidden_owner_facing_language | https://all.accor.com/hotel/C344/index.en.shtml | Strip raw URL / forbidden language from the Presentation row embedding this snip | undefined |
| `hotel-indigo` | corpus_scan | `(owner-facing rendered corpus)` | `—` | (locate via pvql-failure-scrub field scan on apply) | raw_url_scan | https://www.ihgplc.com/en/news-and-media/news-releases/2025/ihg-hotels-and-resor | Strip raw URL / forbidden language from the Presentation row embedding this snip | undefined |
| `hotel-indigo` | corpus_scan | `(owner-facing rendered corpus)` | `—` | (locate via pvql-failure-scrub field scan on apply) | forbidden_owner_facing_language | https://www.ihgplc.com/en/news-and-media/news-releases/2025/ihg-hotels-and-resor | Strip raw URL / forbidden language from the Presentation row embedding this snip | undefined |
| `mgallery-collection` | corpus_scan | `(owner-facing rendered corpus)` | `—` | (locate via pvql-failure-scrub field scan on apply) | raw_url_scan | https://www.hotel-online.com/news/accor-signs-with-terres-de-legendes-for-conver | Strip raw URL / forbidden language from the Presentation row embedding this snip | undefined |
| `mgallery-collection` | corpus_scan | `(owner-facing rendered corpus)` | `—` | (locate via pvql-failure-scrub field scan on apply) | forbidden_owner_facing_language | https://www.hotel-online.com/news/accor-signs-with-terres-de-legendes-for-conver | Strip raw URL / forbidden language from the Presentation row embedding this snip | undefined |
| `small-luxury-hotels-of-the-world` | corpus_scan | `(owner-facing rendered corpus)` | `—` | (locate via pvql-failure-scrub field scan on apply) | raw_url_scan | https://www.travelpulse.com/news/hotels-and-resorts/small-luxury-hotels-of-the-w | Strip raw URL / forbidden language from the Presentation row embedding this snip | undefined |
| `small-luxury-hotels-of-the-world` | corpus_scan | `(owner-facing rendered corpus)` | `—` | (locate via pvql-failure-scrub field scan on apply) | forbidden_owner_facing_language | https://www.travelpulse.com/news/hotels-and-resorts/small-luxury-hotels-of-the-w | Strip raw URL / forbidden language from the Presentation row embedding this snip | undefined |
| `suburban-studios` | corpus_scan | `(owner-facing rendered corpus)` | `—` | (locate via pvql-failure-scrub field scan on apply) | raw_url_scan | https://media.choicehotels.com/2026-01-26-Choice-Hotels-International-Announces- | Strip raw URL / forbidden language from the Presentation row embedding this snip | undefined |
| `suburban-studios` | corpus_scan | `(owner-facing rendered corpus)` | `—` | (locate via pvql-failure-scrub field scan on apply) | forbidden_owner_facing_language | https://media.choicehotels.com/2026-01-26-Choice-Hotels-International-Announces- | Strip raw URL / forbidden language from the Presentation row embedding this snip | undefined |
| `tribute-portfolio` | corpus_scan | `(owner-facing rendered corpus)` | `—` | (locate via pvql-failure-scrub field scan on apply) | raw_url_scan | https://www.journaldespalaces.com/en/pressrelease-78419-italy-tribute-portfolio- | Strip raw URL / forbidden language from the Presentation row embedding this snip | undefined |
| `tribute-portfolio` | corpus_scan | `(owner-facing rendered corpus)` | `—` | (locate via pvql-failure-scrub field scan on apply) | forbidden_owner_facing_language | https://www.journaldespalaces.com/en/pressrelease-78419-italy-tribute-portfolio- | Strip raw URL / forbidden language from the Presentation row embedding this snip | undefined |
| `vignette-collection` | corpus_scan | `(owner-facing rendered corpus)` | `—` | (locate via pvql-failure-scrub field scan on apply) | raw_url_scan | https://www.ihg.com/vignettecollection/hotels/us/en/nairobi/nbofn/hoteldetail | Strip raw URL / forbidden language from the Presentation row embedding this snip | undefined |
| `vignette-collection` | corpus_scan | `(owner-facing rendered corpus)` | `—` | (locate via pvql-failure-scrub field scan on apply) | forbidden_owner_facing_language | https://www.ihg.com/vignettecollection/hotels/us/en/nairobi/nbofn/hoteldetail | Strip raw URL / forbidden language from the Presentation row embedding this snip | undefined |
| `woodspring-suites` | corpus_scan | `(owner-facing rendered corpus)` | `—` | (locate via pvql-failure-scrub field scan on apply) | raw_url_scan | https://investor.choicehotels.com/news/news-details/2025/Choice-Hotels-Internati | Strip raw URL / forbidden language from the Presentation row embedding this snip | undefined |
| `woodspring-suites` | corpus_scan | `(owner-facing rendered corpus)` | `—` | (locate via pvql-failure-scrub field scan on apply) | forbidden_owner_facing_language | https://investor.choicehotels.com/news/news-details/2025/Choice-Hotels-Internati | Strip raw URL / forbidden language from the Presentation row embedding this snip | undefined |

## Allowed writes (later approved apply)

- Presentation Title
- Presentation Body
- Case Summary
- owner-facing chips/tags
- limited Brand Basics visible copy only if directly flagged

## Forbidden writes

- Company Validated
- Company Validation Date
- Source Library status
- Registry status
- Brand Status
- release fields
- unrelated content
- image fields unless directly flagged

Dry-run plan only. Apply via brand-explorer-pvql-failure-scrub with explicit flags in a later task.

