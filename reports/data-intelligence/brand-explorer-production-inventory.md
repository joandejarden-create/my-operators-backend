# Brand Explorer — Production Inventory (read-only)

> Generated: 2026-10-01T10:58:00.687Z
> Airtable base: `appvtnDurnMSjINP6`
> Mode: **READ-ONLY** · writesPerformed: `false`
> Protected baseline cross-check: 62-active-public-full-baseline-v1 (62 brands)
> Status: **READY FOR CHATGPT QA**

## Headline counts

| Metric | Count |
| --- | ---: |
| Brand Basics records scanned | 264 |
| Presentation rows scanned | 12208 |
| Factory Queue rows (Airtable) | 197 |
| Active / Live | 68 |
| In protected 62 baseline | 62 |
| Protected 62 missing from live Active/Live | 0 |
| Active/Live not in protected 62 | 6 |
| Presentation present | 84 |
| Presentation empty | 180 |
| Profile depth full | 70 |
| Profile depth partial | 13 |
| Profile depth sparse | 1 |
| Profile depth empty | 180 |

### By factory disposition

| Disposition | Count |
| --- | ---: |
| COMPLETE / PROTECTED | 61 |
| NEEDS QA | 20 |
| NEEDS REMEDIATION | 1 |
| READY TO BUILD | 178 |
| HOLD / EXCLUDED | 4 |
| NEEDS RESEARCH | 0 |

### By Brand Status

| Brand Status | Count |
| --- | ---: |
| Under Review | 131 |
| Active | 68 |
| Draft | 65 |

### By parent company (bucket)

| Parent | Total | Active/Live | Protected 62 | COMPLETE/PROTECTED | READY TO BUILD |
| --- | ---: | ---: | ---: | ---: | ---: |
| Accor | 45 | 9 | 9 | 8 | 33 |
| Choice | 31 | 11 | 11 | 11 | 7 |
| Hilton | 23 | 13 | 13 | 13 | 10 |
| Hyatt | 26 | 4 | 1 | 1 | 21 |
| IHG | 17 | 7 | 7 | 7 | 10 |
| Marriott | 34 | 18 | 15 | 15 | 15 |
| Other | 66 | 4 | 4 | 4 | 62 |
| Wyndham | 22 | 2 | 2 | 2 | 20 |

## Protected 62 cross-check

Expected protected Active/Live public-full count: **62**. Live Active/Live count: **68**.

All protected 62 baseline record IDs are present in the live Brand Basics pull with Active/Live status (or matched by slug).

### Active/Live brands NOT in protected 62 (do not treat as protected; may need QA / baseline revision)

- `hyatt-centric` — Hyatt Centric — status `Active` — presentation 97 — disposition **NEEDS QA**
- `hyatt-regency` — Hyatt Regency — status `Active` — presentation 97 — disposition **NEEDS QA**
- `thompson-hotels` — Thompson Hotels — status `Active` — presentation 97 — disposition **NEEDS QA**
- `delta-hotels-by-marriott` — Delta Hotels by Marriott — status `Active` — presentation 164 — disposition **NEEDS QA**
- `fairfield-by-marriott` — Fairfield by Marriott — status `Active` — presentation 97 — disposition **NEEDS QA**
- `four-points-by-sheraton` — Four Points by Sheraton — status `Active` — presentation 97 — disposition **NEEDS QA**

### Protected 62 drift (must NOT re-enter net-new factory build queue)

- `so-hotels-and-resorts` — SO/ — live `Active` — depth sparse (22 rows) — **NEEDS REMEDIATION** — protected_baseline_not_full_live

### Held / excluded (governance)

- `the-house-of-originals` — The House of Originals — Excluded from Wave 13
- `morgans-originals` — Morgans Originals — Not created / not modified in Wave 13
- `radisson-collection` — Radisson Collection — Excluded unless separately promoted to Active/Live
- `four-points-flex-by-sheraton` — Four Points Flex by Sheraton — Held / Under Review — not promoted with Wave 14 partial release

## Incomplete / malformed signals

Flagged brands: **1**

| Brand | Slug | Status | Depth | Pres. | Momentum | Gallery | VCS | Disposition | Notes |
| --- | --- | --- | --- | ---: | --- | ---: | --- | --- | --- |
| SO/ | `so-hotels-and-resorts` | Active | sparse | 22 | true | 6 | true | NEEDS REMEDIATION | protected_baseline_not_full_live |

## Proposed factory queue (ordered)

Protected COMPLETE brands are **excluded** from the build queue. HOLD/EXCLUDED are listed separately and must not re-enter build without governance.

| # | Brand | Parent | Status | Pres. | Disposition |
| ---: | --- | --- | --- | ---: | --- |
| 1 | Bulgari (`bulgari`) | Marriott | Under Review | 0 | READY TO BUILD |
| 2 | citizenM (`citizenm`) | Marriott | Under Review | 0 | READY TO BUILD |
| 3 | Edition (`edition`) | Marriott | Under Review | 0 | READY TO BUILD |
| 4 | Element by Westin (`element-by-westin`) | Marriott | Under Review | 0 | READY TO BUILD |
| 5 | Gaylord Hotels (`gaylord-hotels`) | Marriott | Under Review | 0 | READY TO BUILD |
| 6 | JW Marriott (`jw-marriott`) | Marriott | Under Review | 0 | READY TO BUILD |
| 7 | Le Meridien (`le-meridien`) | Marriott | Under Review | 0 | READY TO BUILD |
| 8 | Luxury Collection (`luxury-collection`) | Marriott | Under Review | 0 | READY TO BUILD |
| 9 | Marriott Conference Center (`marriott-conference-center`) | Marriott | Under Review | 0 | READY TO BUILD |
| 10 | Protea Hotels by Marriott (`protea-hotels-by-marriott`) | Marriott | Under Review | 0 | READY TO BUILD |
| 11 | Renaissance (`renaissance`) | Marriott | Under Review | 0 | READY TO BUILD |
| 12 | Ritz-Carlton (`ritz-carlton`) | Marriott | Under Review | 0 | READY TO BUILD |
| 13 | St. Regis (`st-regis`) | Marriott | Under Review | 0 | READY TO BUILD |
| 14 | The Ritz-Carlton Reserve (`the-ritz-carlton-reserve`) | Marriott | Under Review | 0 | READY TO BUILD |
| 15 | W Hotels (`w-hotels`) | Marriott | Under Review | 0 | READY TO BUILD |
| 16 | AutoCamp (`autocamp`) | Hilton | Under Review | 0 | READY TO BUILD |
| 17 | Conrad Hotels & Resorts (`conrad-hotels-and-resorts`) | Hilton | Under Review | 0 | READY TO BUILD |
| 18 | Embassy Suites by Hilton (`embassy-suites-by-hilton`) | Hilton | Under Review | 0 | READY TO BUILD |
| 19 | Graduate Hotels (`graduate-hotels`) | Hilton | Under Review | 0 | READY TO BUILD |
| 20 | Hilton Grand Vacations (`hilton-grand-vacations`) | Hilton | Under Review | 0 | READY TO BUILD |
| 21 | LivSmart Studios by Hilton (`livsmart-studios-by-hilton`) | Hilton | Under Review | 0 | READY TO BUILD |
| 22 | LXR Hotels & Resorts (`lxr-hotels-and-resorts`) | Hilton | Under Review | 0 | READY TO BUILD |
| 23 | NoMad (`nomad`) | Hilton | Under Review | 0 | READY TO BUILD |
| 24 | Signia by Hilton (`signia-by-hilton`) | Hilton | Under Review | 0 | READY TO BUILD |
| 25 | Waldorf Astoria (`waldorf-astoria`) | Hilton | Under Review | 0 | READY TO BUILD |
| 26 | Atwell Suites (`atwell-suites`) | IHG | Under Review | 0 | READY TO BUILD |
| 27 | Candlewood Suites (`candlewood-suites`) | IHG | Under Review | 0 | READY TO BUILD |
| 28 | Crowne Plaza (`crowne-plaza`) | IHG | Under Review | 0 | READY TO BUILD |
| 29 | Garner (`garner`) | IHG | Under Review | 0 | READY TO BUILD |
| 30 | Holiday Inn (`holiday-inn`) | IHG | Under Review | 0 | READY TO BUILD |
| 31 | HUALUXE Hotels and Resorts (`hualuxe-hotels-and-resorts`) | IHG | Under Review | 0 | READY TO BUILD |
| 32 | InterContinental (`intercontinental`) | IHG | Under Review | 0 | READY TO BUILD |
| 33 | Regent Hotels & Resorts (`regent-hotels-and-resorts`) | IHG | Under Review | 0 | READY TO BUILD |
| 34 | Six Senses Hotels Resorts Spas (`six-senses-hotels-resorts-spas`) | IHG | Under Review | 0 | READY TO BUILD |
| 35 | Staybridge Suites (`staybridge-suites`) | IHG | Under Review | 0 | READY TO BUILD |
| 36 | art'otel (`art-otel`) | Choice | Draft | 0 | READY TO BUILD |
| 37 | Country Inn & Suites by Radisson (`country-inn-and-suites-by-radisson`) | Choice | Draft | 0 | READY TO BUILD |
| 38 | Park Inn by Radisson (`park-inn-by-radisson`) | Choice | Draft | 0 | READY TO BUILD |
| 39 | Park Plaza (`park-plaza`) | Choice | Draft | 0 | READY TO BUILD |
| 40 | Prize by Radisson (`prize-by-radisson`) | Choice | Draft | 0 | READY TO BUILD |
| 41 | Radisson Blu (`radisson-blu`) | Choice | Draft | 0 | READY TO BUILD |
| 42 | Radisson RED (`radisson-red`) | Choice | Draft | 0 | READY TO BUILD |
| 43 | 25Hours Hotel (`25hours-hotel`) | Accor | Under Review | 0 | READY TO BUILD |
| 44 | Adagio Aparthotel (`adagio-aparthotel`) | Accor | Under Review | 0 | READY TO BUILD |
| 45 | Adagio Aparthotel Extra (`adagio-aparthotel-extra`) | Accor | Under Review | 0 | READY TO BUILD |
| 46 | Aparthotel Adagio Access (`aparthotel-adagio-access`) | Accor | Under Review | 0 | READY TO BUILD |
| 47 | Art Series (`art-series`) | Accor | Under Review | 0 | READY TO BUILD |
| 48 | BreakFree Resort (`breakfree-resort`) | Accor | Under Review | 0 | READY TO BUILD |
| 49 | Delano (`delano`) | Accor | Under Review | 0 | READY TO BUILD |
| 50 | Faena (`faena`) | Accor | Under Review | 0 | READY TO BUILD |
| 51 | Grand Mercure (`grand-mercure`) | Accor | Under Review | 0 | READY TO BUILD |
| 52 | Greet (`greet`) | Accor | Under Review | 0 | READY TO BUILD |
| 53 | hotelF1 (`hotelf1`) | Accor | Under Review | 0 | READY TO BUILD |
| 54 | Hyde (`hyde`) | Accor | Under Review | 0 | READY TO BUILD |
| 55 | ibis budget (`ibis-budget`) | Accor | Under Review | 0 | READY TO BUILD |
| 56 | ibis Styles (`ibis-styles`) | Accor | Under Review | 0 | READY TO BUILD |
| 57 | JO&JOE (`joandjoe`) | Accor | Under Review | 0 | READY TO BUILD |
| 58 | Mantis Collection (`mantis-collection`) | Accor | Under Review | 0 | READY TO BUILD |
| 59 | Mantra (`mantra`) | Accor | Under Review | 0 | READY TO BUILD |
| 60 | Mondrian (`mondrian`) | Accor | Under Review | 0 | READY TO BUILD |
| 61 | Mövenpick (`m-venpick`) | Accor | Under Review | 0 | READY TO BUILD |
| 62 | Novotel Suites (`novotel-suites`) | Accor | Under Review | 0 | READY TO BUILD |
| 63 | onefinestay (`onefinestay`) | Accor | Under Review | 0 | READY TO BUILD |
| 64 | Orient Express (`orient-express`) | Accor | Under Review | 0 | READY TO BUILD |
| 65 | Our Habitas (`our-habitas`) | Accor | Under Review | 0 | READY TO BUILD |
| 66 | Peppers (`peppers`) | Accor | Under Review | 0 | READY TO BUILD |
| 67 | Raffles (`raffles`) | Accor | Under Review | 0 | READY TO BUILD |
| 68 | Rixos (`rixos`) | Accor | Under Review | 0 | READY TO BUILD |
| 69 | SLS (`sls`) | Accor | Under Review | 0 | READY TO BUILD |
| 70 | Sofitel (`sofitel`) | Accor | Under Review | 0 | READY TO BUILD |
| 71 | Sofitel Legend (`sofitel-legend`) | Accor | Under Review | 0 | READY TO BUILD |
| 72 | Swissotel (`swissotel`) | Accor | Under Review | 0 | READY TO BUILD |
| 73 | The Hoxton (`the-hoxton`) | Accor | Under Review | 0 | READY TO BUILD |
| 74 | The Sebel (`the-sebel`) | Accor | Under Review | 0 | READY TO BUILD |
| 75 | TRIBE (`tribe`) | Accor | Under Review | 0 | READY TO BUILD |
| 76 | AmericInn by Wyndham (`americinn-by-wyndham`) | Wyndham | Under Review | 0 | READY TO BUILD |
| 77 | Baymont by Wyndham (`baymont-by-wyndham`) | Wyndham | Under Review | 0 | READY TO BUILD |
| 78 | Days Inn by Wyndham (`days-inn-by-wyndham`) | Wyndham | Under Review | 0 | READY TO BUILD |
| 79 | Dolce Hotels and Resorts by Wyndham (`dolce-hotels-and-resorts-by-wyndham`) | Wyndham | Under Review | 0 | READY TO BUILD |
| 80 | Esplendor by Wyndham (`esplendor-by-wyndham`) | Wyndham | Under Review | 0 | READY TO BUILD |
| 81 | Gala by Wyndham (`gala-by-wyndham`) | Wyndham | Under Review | 0 | READY TO BUILD |
| 82 | Hawthorn Suites by Wyndham (`hawthorn-suites-by-wyndham`) | Wyndham | Under Review | 0 | READY TO BUILD |
| 83 | Howard Johnson by Wyndham (`howard-johnson-by-wyndham`) | Wyndham | Under Review | 0 | READY TO BUILD |
| 84 | Knights Inn (`knights-inn`) | Wyndham | Under Review | 0 | READY TO BUILD |
| 85 | La Quinta by Wyndham (`la-quinta-by-wyndham`) | Wyndham | Under Review | 0 | READY TO BUILD |
| 86 | Microtel by Wyndham (`microtel-by-wyndham`) | Wyndham | Under Review | 0 | READY TO BUILD |
| 87 | Ramada by Wyndham (`ramada-by-wyndham`) | Wyndham | Under Review | 0 | READY TO BUILD |
| 88 | Registry Collection Hotels (`registry-collection-hotels`) | Wyndham | Under Review | 0 | READY TO BUILD |
| 89 | Super 8 by Wyndham (`super-8-by-wyndham`) | Wyndham | Under Review | 0 | READY TO BUILD |
| 90 | Travelodge by Wyndham (`travelodge-by-wyndham`) | Wyndham | Under Review | 0 | READY TO BUILD |
| 91 | TRYP by Wyndham (`tryp-by-wyndham`) | Wyndham | Under Review | 0 | READY TO BUILD |
| 92 | Wingate by Wyndham (`wingate-by-wyndham`) | Wyndham | Under Review | 0 | READY TO BUILD |
| 93 | Wyndham (`wyndham`) | Wyndham | Under Review | 0 | READY TO BUILD |
| 94 | Wyndham Garden (`wyndham-garden`) | Wyndham | Under Review | 0 | READY TO BUILD |
| 95 | Wyndham Grand (`wyndham-grand`) | Wyndham | Under Review | 0 | READY TO BUILD |
| 96 | Alila (`alila`) | Hyatt | Draft | 0 | READY TO BUILD |
| 97 | Andaz (`andaz`) | Hyatt | Draft | 0 | READY TO BUILD |
| 98 | Breathless Resorts & Spas (`breathless-resorts-and-spas`) | Hyatt | Draft | 0 | READY TO BUILD |
| 99 | Destination by Hyatt (`destination-by-hyatt`) | Hyatt | Under Review | 0 | READY TO BUILD |
| 100 | Dream Hotels (`dream-hotels`) | Hyatt | Under Review | 0 | READY TO BUILD |
| 101 | Dreams Resorts & Spas (`dreams-resorts-and-spas`) | Hyatt | Draft | 0 | READY TO BUILD |
| 102 | Grand Hyatt (`grand-hyatt`) | Hyatt | Draft | 0 | READY TO BUILD |
| 103 | Hyatt (`hyatt`) | Hyatt | Draft | 0 | READY TO BUILD |
| 104 | Hyatt House (`hyatt-house`) | Hyatt | Under Review | 0 | READY TO BUILD |
| 105 | Hyatt Place (`hyatt-place`) | Hyatt | Under Review | 0 | READY TO BUILD |
| 106 | Hyatt Residence Club (`hyatt-residence-club`) | Hyatt | Draft | 0 | READY TO BUILD |
| 107 | Hyatt Vivid (`hyatt-vivid`) | Hyatt | Draft | 0 | READY TO BUILD |
| 108 | Hyatt Zilara (`hyatt-zilara`) | Hyatt | Under Review | 0 | READY TO BUILD |
| 109 | Hyatt Ziva (`hyatt-ziva`) | Hyatt | Under Review | 0 | READY TO BUILD |
| 110 | Joie de Vivre Hotels (`joie-de-vivre-hotels`) | Hyatt | Under Review | 0 | READY TO BUILD |
| 111 | Miraval (`miraval`) | Hyatt | Draft | 0 | READY TO BUILD |
| 112 | Mr & Mrs Smith (`mr-and-mrs-smith`) | Hyatt | Under Review | 0 | READY TO BUILD |
| 113 | Park Hyatt (`park-hyatt`) | Hyatt | Draft | 0 | READY TO BUILD |
| 114 | Secrets Resorts & Spas (`secrets-resorts-and-spas`) | Hyatt | Under Review | 0 | READY TO BUILD |
| 115 | Sunscape Resorts & Spas (`sunscape-resorts-and-spas`) | Hyatt | Under Review | 0 | READY TO BUILD |
| 116 | Unbound Collection by Hyatt (`unbound-collection-by-hyatt`) | Hyatt | Under Review | 0 | READY TO BUILD |
| 117 | Aiden by Best Western (`aiden-by-best-western`) | Other | Under Review | 0 | READY TO BUILD |
| 118 | Aman (`aman`) | Other | Draft | 0 | READY TO BUILD |
| 119 | Americas Best Value Inn (`americas-best-value-inn`) | Other | Draft | 0 | READY TO BUILD |
| 120 | Anantara (`anantara`) | Other | Under Review | 0 | READY TO BUILD |
| … | 78 more in JSON/CSV | | | | |

## Major parent snapshots

### Marriott (34)

| Brand | Status | Active/Live | Protected62 | Pres. | Depth | Disposition |
| --- | --- | --- | --- | ---: | --- | --- |
| AC Hotels by Marriott | Active | true | true | 107 | full | COMPLETE / PROTECTED |
| Aloft Hotels | Active | true | true | 166 | full | COMPLETE / PROTECTED |
| Autograph Collection | Active | true | true | 128 | full | COMPLETE / PROTECTED |
| Bulgari | Under Review | false | false | 0 | empty | READY TO BUILD |
| citizenM | Under Review | false | false | 0 | empty | READY TO BUILD |
| City Express by Marriott | Active | true | true | 107 | full | COMPLETE / PROTECTED |
| Courtyard by Marriott | Active | true | true | 106 | full | COMPLETE / PROTECTED |
| Delta Hotels by Marriott | Active | true | false | 164 | full | NEEDS QA |
| Design Hotels | Active | true | true | 182 | full | COMPLETE / PROTECTED |
| Edition | Under Review | false | false | 0 | empty | READY TO BUILD |
| Element by Westin | Under Review | false | false | 0 | empty | READY TO BUILD |
| Fairfield by Marriott | Active | true | false | 97 | full | NEEDS QA |
| Four Points by Sheraton | Active | true | false | 97 | full | NEEDS QA |
| Four Points Flex by Sheraton | Under Review | false | false | 97 | full | HOLD / EXCLUDED |
| Gaylord Hotels | Under Review | false | false | 0 | empty | READY TO BUILD |
| JW Marriott | Under Review | false | false | 0 | empty | READY TO BUILD |
| Le Meridien | Under Review | false | false | 0 | empty | READY TO BUILD |
| Luxury Collection | Under Review | false | false | 0 | empty | READY TO BUILD |
| Marriott Conference Center | Under Review | false | false | 0 | empty | READY TO BUILD |
| Marriott Hotels | Active | true | true | 166 | full | COMPLETE / PROTECTED |
| Moxy Hotels | Active | true | true | 106 | full | COMPLETE / PROTECTED |
| Protea Hotels by Marriott | Under Review | false | false | 0 | empty | READY TO BUILD |
| Renaissance | Under Review | false | false | 0 | empty | READY TO BUILD |
| Residence Inn by Marriott | Active | true | true | 166 | full | COMPLETE / PROTECTED |
| Ritz-Carlton | Under Review | false | false | 0 | empty | READY TO BUILD |
| Sheraton | Active | true | true | 166 | full | COMPLETE / PROTECTED |
| SpringHill Suites by Marriott | Active | true | true | 101 | full | COMPLETE / PROTECTED |
| St. Regis | Under Review | false | false | 0 | empty | READY TO BUILD |
| StudioRes | Active | true | true | 101 | full | COMPLETE / PROTECTED |
| The Ritz-Carlton Reserve | Under Review | false | false | 0 | empty | READY TO BUILD |
| TownePlace Suites by Marriott | Active | true | true | 101 | full | COMPLETE / PROTECTED |
| Tribute Portfolio | Active | true | true | 170 | full | COMPLETE / PROTECTED |
| W Hotels | Under Review | false | false | 0 | empty | READY TO BUILD |
| Westin | Active | true | true | 102 | full | COMPLETE / PROTECTED |

### Hilton (23)

| Brand | Status | Active/Live | Protected62 | Pres. | Depth | Disposition |
| --- | --- | --- | --- | ---: | --- | --- |
| AutoCamp | Under Review | false | false | 0 | empty | READY TO BUILD |
| Canopy by Hilton | Active | true | true | 107 | full | COMPLETE / PROTECTED |
| Conrad Hotels & Resorts | Under Review | false | false | 0 | empty | READY TO BUILD |
| Curio Collection by Hilton | Active | true | true | 229 | full | COMPLETE / PROTECTED |
| DoubleTree by Hilton | Active | true | true | 100 | full | COMPLETE / PROTECTED |
| Embassy Suites by Hilton | Under Review | false | false | 0 | empty | READY TO BUILD |
| Graduate Hotels | Under Review | false | false | 0 | empty | READY TO BUILD |
| Hampton by Hilton | Active | true | true | 100 | full | COMPLETE / PROTECTED |
| Hilton Garden Inn | Active | true | true | 100 | full | COMPLETE / PROTECTED |
| Hilton Grand Vacations | Under Review | false | false | 0 | empty | READY TO BUILD |
| Hilton Hotels & Resorts | Active | true | true | 101 | full | COMPLETE / PROTECTED |
| Home2 Suites by Hilton | Active | true | true | 100 | full | COMPLETE / PROTECTED |
| Homewood Suites by Hilton | Active | true | true | 100 | full | COMPLETE / PROTECTED |
| LivSmart Studios by Hilton | Under Review | false | false | 0 | empty | READY TO BUILD |
| LXR Hotels & Resorts | Under Review | false | false | 0 | empty | READY TO BUILD |
| Motto by Hilton | Active | true | true | 107 | full | COMPLETE / PROTECTED |
| NoMad | Under Review | false | false | 0 | empty | READY TO BUILD |
| Signia by Hilton | Under Review | false | false | 0 | empty | READY TO BUILD |
| Spark by Hilton | Active | true | true | 100 | full | COMPLETE / PROTECTED |
| Tapestry Collection by Hilton | Active | true | true | 154 | full | COMPLETE / PROTECTED |
| Tempo by Hilton | Active | true | true | 107 | full | COMPLETE / PROTECTED |
| Tru by Hilton | Active | true | true | 100 | full | COMPLETE / PROTECTED |
| Waldorf Astoria | Under Review | false | false | 0 | empty | READY TO BUILD |

### IHG (17)

| Brand | Status | Active/Live | Protected62 | Pres. | Depth | Disposition |
| --- | --- | --- | --- | ---: | --- | --- |
| Atwell Suites | Under Review | false | false | 0 | empty | READY TO BUILD |
| avid hotels | Active | true | true | 106 | full | COMPLETE / PROTECTED |
| Candlewood Suites | Under Review | false | false | 0 | empty | READY TO BUILD |
| Crowne Plaza | Under Review | false | false | 0 | empty | READY TO BUILD |
| Even Hotels | Active | true | true | 107 | full | COMPLETE / PROTECTED |
| Garner | Under Review | false | false | 0 | empty | READY TO BUILD |
| Holiday Inn | Under Review | false | false | 0 | empty | READY TO BUILD |
| Holiday Inn Express | Active | true | true | 107 | full | COMPLETE / PROTECTED |
| Hotel Indigo | Active | true | true | 164 | full | COMPLETE / PROTECTED |
| HUALUXE Hotels and Resorts | Under Review | false | false | 0 | empty | READY TO BUILD |
| InterContinental | Under Review | false | false | 0 | empty | READY TO BUILD |
| Kimpton Hotels | Active | true | true | 224 | full | COMPLETE / PROTECTED |
| Regent Hotels & Resorts | Under Review | false | false | 0 | empty | READY TO BUILD |
| Six Senses Hotels Resorts Spas | Under Review | false | false | 0 | empty | READY TO BUILD |
| Staybridge Suites | Under Review | false | false | 0 | empty | READY TO BUILD |
| Vignette Collection | Active | true | true | 124 | full | COMPLETE / PROTECTED |
| Voco Hotels | Active | true | true | 107 | full | COMPLETE / PROTECTED |

### Choice (31)

| Brand | Status | Active/Live | Protected62 | Pres. | Depth | Disposition |
| --- | --- | --- | --- | ---: | --- | --- |
| art'otel | Draft | false | false | 0 | empty | READY TO BUILD |
| Ascend Hotel Collection | Active | true | true | 224 | full | COMPLETE / PROTECTED |
| Cambria Hotels | Under Review | false | false | 195 | partial | NEEDS QA |
| Clarion | Under Review | false | false | 199 | partial | NEEDS QA |
| Clarion Pointe | Under Review | false | false | 194 | partial | NEEDS QA |
| Comfort Inn & Suites | Active | true | true | 227 | full | COMPLETE / PROTECTED |
| Country Inn & Suites by Choice | Active | true | true | 224 | full | COMPLETE / PROTECTED |
| Country Inn & Suites by Radisson | Draft | false | false | 0 | empty | READY TO BUILD |
| Econo Lodge | Under Review | false | false | 194 | partial | NEEDS QA |
| Everhome Suites | Active | true | true | 215 | full | COMPLETE / PROTECTED |
| MainStay Suites | Under Review | false | false | 195 | partial | NEEDS QA |
| Park Inn by Choice | Under Review | false | false | 204 | partial | NEEDS QA |
| Park Inn by Radisson | Draft | false | false | 0 | empty | READY TO BUILD |
| Park Plaza | Draft | false | false | 0 | empty | READY TO BUILD |
| Park Plaza by Choice | Under Review | false | false | 200 | partial | NEEDS QA |
| Prize by Radisson | Draft | false | false | 0 | empty | READY TO BUILD |
| Quality Inn | Active | true | true | 208 | full | COMPLETE / PROTECTED |
| Radisson | Draft | false | false | 152 | partial | NEEDS QA |
| Radisson Blu | Draft | false | false | 0 | empty | READY TO BUILD |
| Radisson Blu by Choice | Active | true | true | 240 | full | COMPLETE / PROTECTED |
| Radisson by Choice | Active | true | true | 211 | full | COMPLETE / PROTECTED |
| Radisson Collection | Draft | false | false | 123 | partial | HOLD / EXCLUDED |
| Radisson Collection by Choice | Draft | false | false | 199 | partial | NEEDS QA |
| Radisson Individuals by Choice | Active | true | true | 227 | full | COMPLETE / PROTECTED |
| Radisson Inn & Suites | Draft | false | false | 194 | partial | NEEDS QA |
| Radisson RED | Draft | false | false | 0 | empty | READY TO BUILD |
| Radisson RED by Choice | Active | true | true | 212 | full | COMPLETE / PROTECTED |
| Rodeway Inn | Under Review | false | false | 194 | partial | NEEDS QA |
| Sleep Inn | Under Review | false | false | 200 | partial | NEEDS QA |
| Suburban Studios | Active | true | true | 219 | full | COMPLETE / PROTECTED |
| WoodSpring Suites | Active | true | true | 223 | full | COMPLETE / PROTECTED |

### Accor (45)

| Brand | Status | Active/Live | Protected62 | Pres. | Depth | Disposition |
| --- | --- | --- | --- | ---: | --- | --- |
| 21c Museum Hotel | Under Review | false | false | 177 | full | NEEDS QA |
| 25Hours Hotel | Under Review | false | false | 0 | empty | READY TO BUILD |
| Adagio Aparthotel | Under Review | false | false | 0 | empty | READY TO BUILD |
| Adagio Aparthotel Extra | Under Review | false | false | 0 | empty | READY TO BUILD |
| Aparthotel Adagio Access | Under Review | false | false | 0 | empty | READY TO BUILD |
| Art Series | Under Review | false | false | 0 | empty | READY TO BUILD |
| BreakFree Resort | Under Review | false | false | 0 | empty | READY TO BUILD |
| Delano | Under Review | false | false | 0 | empty | READY TO BUILD |
| Faena | Under Review | false | false | 0 | empty | READY TO BUILD |
| Fairmont | Active | true | true | 113 | full | COMPLETE / PROTECTED |
| Grand Mercure | Under Review | false | false | 0 | empty | READY TO BUILD |
| Greet | Under Review | false | false | 0 | empty | READY TO BUILD |
| Handwritten Collection | Active | true | true | 125 | full | COMPLETE / PROTECTED |
| hotelF1 | Under Review | false | false | 0 | empty | READY TO BUILD |
| Hyde | Under Review | false | false | 0 | empty | READY TO BUILD |
| ibis | Active | true | true | 112 | full | COMPLETE / PROTECTED |
| ibis budget | Under Review | false | false | 0 | empty | READY TO BUILD |
| ibis Styles | Under Review | false | false | 0 | empty | READY TO BUILD |
| JO&JOE | Under Review | false | false | 0 | empty | READY TO BUILD |
| Mama Shelter | Active | true | true | 112 | full | COMPLETE / PROTECTED |
| Mantis Collection | Under Review | false | false | 0 | empty | READY TO BUILD |
| Mantra | Under Review | false | false | 0 | empty | READY TO BUILD |
| Mercure | Active | true | true | 113 | full | COMPLETE / PROTECTED |
| MGallery Collection | Active | true | true | 98 | full | COMPLETE / PROTECTED |
| Mondrian | Under Review | false | false | 0 | empty | READY TO BUILD |
| Morgans Originals | Under Review | false | false | 0 | empty | HOLD / EXCLUDED |
| Mövenpick | Under Review | false | false | 0 | empty | READY TO BUILD |
| Novotel | Active | true | true | 112 | full | COMPLETE / PROTECTED |
| Novotel Suites | Under Review | false | false | 0 | empty | READY TO BUILD |
| onefinestay | Under Review | false | false | 0 | empty | READY TO BUILD |
| Orient Express | Under Review | false | false | 0 | empty | READY TO BUILD |
| Our Habitas | Under Review | false | false | 0 | empty | READY TO BUILD |
| Peppers | Under Review | false | false | 0 | empty | READY TO BUILD |
| Pullman | Active | true | true | 112 | full | COMPLETE / PROTECTED |
| Raffles | Under Review | false | false | 0 | empty | READY TO BUILD |
| Rixos | Under Review | false | false | 0 | empty | READY TO BUILD |
| SLS | Under Review | false | false | 0 | empty | READY TO BUILD |
| SO/ | Active | true | true | 22 | sparse | NEEDS REMEDIATION |
| Sofitel | Under Review | false | false | 0 | empty | READY TO BUILD |
| Sofitel Legend | Under Review | false | false | 0 | empty | READY TO BUILD |
| Swissotel | Under Review | false | false | 0 | empty | READY TO BUILD |
| The House of Originals | Under Review | false | false | 0 | empty | HOLD / EXCLUDED |
| The Hoxton | Under Review | false | false | 0 | empty | READY TO BUILD |
| The Sebel | Under Review | false | false | 0 | empty | READY TO BUILD |
| TRIBE | Under Review | false | false | 0 | empty | READY TO BUILD |

### Wyndham (22)

| Brand | Status | Active/Live | Protected62 | Pres. | Depth | Disposition |
| --- | --- | --- | --- | ---: | --- | --- |
| AmericInn by Wyndham | Under Review | false | false | 0 | empty | READY TO BUILD |
| Baymont by Wyndham | Under Review | false | false | 0 | empty | READY TO BUILD |
| Days Inn by Wyndham | Under Review | false | false | 0 | empty | READY TO BUILD |
| Dazzler by Wyndham | Active | true | true | 120 | full | COMPLETE / PROTECTED |
| Dolce Hotels and Resorts by Wyndham | Under Review | false | false | 0 | empty | READY TO BUILD |
| Esplendor by Wyndham | Under Review | false | false | 0 | empty | READY TO BUILD |
| Gala by Wyndham | Under Review | false | false | 0 | empty | READY TO BUILD |
| Hawthorn Suites by Wyndham | Under Review | false | false | 0 | empty | READY TO BUILD |
| Howard Johnson by Wyndham | Under Review | false | false | 0 | empty | READY TO BUILD |
| Knights Inn | Under Review | false | false | 0 | empty | READY TO BUILD |
| La Quinta by Wyndham | Under Review | false | false | 0 | empty | READY TO BUILD |
| Microtel by Wyndham | Under Review | false | false | 0 | empty | READY TO BUILD |
| Ramada by Wyndham | Under Review | false | false | 0 | empty | READY TO BUILD |
| Registry Collection Hotels | Under Review | false | false | 0 | empty | READY TO BUILD |
| Super 8 by Wyndham | Under Review | false | false | 0 | empty | READY TO BUILD |
| Trademark Collection by Wyndham | Active | true | true | 120 | full | COMPLETE / PROTECTED |
| Travelodge by Wyndham | Under Review | false | false | 0 | empty | READY TO BUILD |
| TRYP by Wyndham | Under Review | false | false | 0 | empty | READY TO BUILD |
| Wingate by Wyndham | Under Review | false | false | 0 | empty | READY TO BUILD |
| Wyndham | Under Review | false | false | 0 | empty | READY TO BUILD |
| Wyndham Garden | Under Review | false | false | 0 | empty | READY TO BUILD |
| Wyndham Grand | Under Review | false | false | 0 | empty | READY TO BUILD |

### Hyatt (26)

| Brand | Status | Active/Live | Protected62 | Pres. | Depth | Disposition |
| --- | --- | --- | --- | ---: | --- | --- |
| Alila | Draft | false | false | 0 | empty | READY TO BUILD |
| Andaz | Draft | false | false | 0 | empty | READY TO BUILD |
| Breathless Resorts & Spas | Draft | false | false | 0 | empty | READY TO BUILD |
| Bunkhouse Hotels | Active | true | true | 107 | full | COMPLETE / PROTECTED |
| Caption by Hyatt | Under Review | false | false | 168 | full | NEEDS QA |
| Destination by Hyatt | Under Review | false | false | 0 | empty | READY TO BUILD |
| Dream Hotels | Under Review | false | false | 0 | empty | READY TO BUILD |
| Dreams Resorts & Spas | Draft | false | false | 0 | empty | READY TO BUILD |
| Grand Hyatt | Draft | false | false | 0 | empty | READY TO BUILD |
| Hyatt | Draft | false | false | 0 | empty | READY TO BUILD |
| Hyatt Centric | Active | true | false | 97 | full | NEEDS QA |
| Hyatt House | Under Review | false | false | 0 | empty | READY TO BUILD |
| Hyatt Place | Under Review | false | false | 0 | empty | READY TO BUILD |
| Hyatt Regency | Active | true | false | 97 | full | NEEDS QA |
| Hyatt Residence Club | Draft | false | false | 0 | empty | READY TO BUILD |
| Hyatt Vivid | Draft | false | false | 0 | empty | READY TO BUILD |
| Hyatt Zilara | Under Review | false | false | 0 | empty | READY TO BUILD |
| Hyatt Ziva | Under Review | false | false | 0 | empty | READY TO BUILD |
| Joie de Vivre Hotels | Under Review | false | false | 0 | empty | READY TO BUILD |
| Miraval | Draft | false | false | 0 | empty | READY TO BUILD |
| Mr & Mrs Smith | Under Review | false | false | 0 | empty | READY TO BUILD |
| Park Hyatt | Draft | false | false | 0 | empty | READY TO BUILD |
| Secrets Resorts & Spas | Under Review | false | false | 0 | empty | READY TO BUILD |
| Sunscape Resorts & Spas | Under Review | false | false | 0 | empty | READY TO BUILD |
| Thompson Hotels | Active | true | false | 97 | full | NEEDS QA |
| Unbound Collection by Hyatt | Under Review | false | false | 0 | empty | READY TO BUILD |

### Best Western (0)

_No Brand Basics rows mapped to this parent bucket._

## Files

- JSON: `reports/data-intelligence/brand-explorer-production-inventory.json`
- CSV: `reports/data-intelligence/brand-explorer-production-inventory.csv`
- Markdown: `reports/data-intelligence/brand-explorer-production-inventory.md`
- Proposed queue CSV: `reports/data-intelligence/brand-explorer-factory-queue-proposed.csv`

---

**READY FOR CHATGPT QA**
