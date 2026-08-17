# Choice Legacy Brand Source Package v1

Generated: 2026-07-06T20:36:04.535Z
Mode: **dry_run**
Airtable modified: **no**
Company folder: `Choice Hotels International`

> Control excluded: **Radisson Blu by Choice** (`recWPEvxBQxVVzSq3`) — platform-ready.

## Executive summary

| Metric | Count |
|--------|------:|
| Brands planned | 8 |
| Brands with local PDFs on disk | 7 |
| P0 local files ready to register | 7 |
| P0 verified URLs needing capture | 23 |
| JS-shell development page warnings | 8 |
| Brands with regional/ownership caveats | 4 |

## Recommended first 3 brands to process

1. **Comfort Inn & Suites** (`recOzH5iAE1xEjyD0`)
1. **Everhome Suites** (`recqkkrsevi4r9ibj`)
1. **Quality Inn** (`recd8o4k1JddhkRWW`)

## Brand packages

### Ascend Hotel Collection

- Record: `reclkgOzvAcBheUSo`
- Explorer active: **yes**
- Profile completeness: **Strong Existing Profile**
- PI sources: **0/0** approved
- Local folder: `Choice Hotels International/{Ascend Hotel Collection | Ascend}`
- Registration ready (package): **no**
- JS-shell risk: **medium** — Choice development site may render Salesforce/LWC shell (see Radisson Blu recC9utJdNaKWR56k); prefer DAM PDF / one-pager when extract preview is thin
- Next action: **Capture verified Choice URLs (dry-run download first)**
- Warnings:
  - Choice development site may render Salesforce/LWC shell (see Radisson Blu recC9utJdNaKWR56k); prefer DAM PDF / one-pager when extract preview is thin

#### P0 sources

| Role | Type | URL / Local | Confidence | Status |
|------|------|-------------|------------|--------|
| Choice consumer brand page | Brand Page | https://www.choicehotels.com/ascend | verified | **capture_needed_url** |
| Choice development brand page | Development Page | https://www.choicehotelsdevelopment.com/our-brands/upscale/ascend | verified | **capture_needed_url** |
| development PDF / one-pager | Development Brochure | — | missing | **blocked_missing** |
| Choice press kit / media center | Press Release | https://media.choicehotels.com/ascend-hotel-collection-press-kit | verified | **capture_needed_url** |

_No local files found under scanned folders._

- Missing: Choice development brand page; development PDF / one-pager; Choice press kit / media center

```bash
npm run partner-reference:download -- --url "https://www.choicehotels.com/ascend" --company "Choice Hotels International" --brand "Ascend Hotel Collection" --type website-capture --title "Choice consumer brand page" --brand-id reclkgOzvAcBheUSo --dry-run
```

### Comfort Inn & Suites

- Record: `recOzH5iAE1xEjyD0`
- Explorer active: **yes**
- Profile completeness: **Strong Existing Profile**
- PI sources: **0/0** approved
- Local folder: `Choice Hotels International/{Comfort Inn & Suites | Comfort Inn | Comfort}`
- Registration ready (package): **partial/yes**
- JS-shell risk: **medium** — Choice development site may render Salesforce/LWC shell (see Radisson Blu recC9utJdNaKWR56k); prefer DAM PDF / one-pager when extract preview is thin
- Next action: **Register local PDF(s) then capture verified URLs**
- Warnings:
  - Choice development site may render Salesforce/LWC shell (see Radisson Blu recC9utJdNaKWR56k); prefer DAM PDF / one-pager when extract preview is thin

#### P0 sources

| Role | Type | URL / Local | Confidence | Status |
|------|------|-------------|------------|--------|
| Choice consumer brand page | Brand Page | https://www.choicehotels.com/comfort-hotels | verified | **capture_needed_url** |
| Choice development brand page | Development Page | https://www.choicehotelsdevelopment.com/our-brands/upper-midscale/comfort | verified | **capture_needed_url** |
| development PDF / one-pager (local) | Development Brochure | `Choice Hotels International/Comfort Inn/brochure--comfort-inn.pdf` | verified_local | **ready_to_register_local** |
| Choice press kit / media center | Press Release | https://media.choicehotels.com/comfort-press-kit | verified | **capture_needed_url** |

#### Local files found

- `Choice Hotels International/Comfort Inn/brochure--comfort-inn.pdf` (1784179 bytes; text 1858)
- `Choice Hotels International/Comfort Inn/CIS_OnePager_2024.pdf` (688186 bytes; text 2972)
- Missing: Choice development brand page; Choice press kit / media center

```bash
npm run choice-legacy-brand-source-package -- --dry-run --brand comfort-inn-suites
```

### Country Inn & Suites by Choice

- Record: `recaayt9u7YYg8h7Y`
- Explorer active: **yes**
- Profile completeness: **Strong Existing Profile**
- PI sources: **0/0** approved
- Local folder: `Choice Hotels International/{Country Inn & Suites | Country Inn & Suites by Choice | Country Inn}`
- Registration ready (package): **partial/yes**
- JS-shell risk: **medium** — Choice development site may render Salesforce/LWC shell (see Radisson Blu recC9utJdNaKWR56k); prefer DAM PDF / one-pager when extract preview is thin
- Next action: **Register local PDF(s) then capture verified URLs**
- Regional caveats: Americas brand owned by Choice; global RHG Country Inn materials are separate reference only
- Warnings:
  - Choice development site may render Salesforce/LWC shell (see Radisson Blu recC9utJdNaKWR56k); prefer DAM PDF / one-pager when extract preview is thin
  - Regional: Americas brand owned by Choice; global RHG Country Inn materials are separate reference only

#### P0 sources

| Role | Type | URL / Local | Confidence | Status |
|------|------|-------------|------------|--------|
| Choice consumer brand page | Brand Page | https://www.choicehotels.com/country-inn-suites | verified | **capture_needed_url** |
| Choice development brand page | Development Page | https://www.choicehotelsdevelopment.com/our-brands/upper-midscale/country-inn-and-suites | verified | **capture_needed_url** |
| development PDF / one-pager (local) | Development Brochure | `Choice Hotels International/Country Inn & Suites/Country Inn & Suites Prototype Brochure.pdf` | verified_local | **ready_to_register_local** |
| Choice press kit / media center | Press Release | — | uncertain | **candidate_url_only** |

#### Local files found

- `Choice Hotels International/Country Inn & Suites/Country Inn & Suites Prototype Overview (4-Page).pdf` (14418991 bytes; text 3004)
- `Choice Hotels International/Country Inn & Suites/Country Inn & Suites Prototype Brochure.pdf` (56571267 bytes; text 12178)
- `Choice Hotels International/Country Inn & Suites/CHD_Country_TargetMarkets_WEB.pdf` (519688 bytes; text 3047)
- `Choice Hotels International/Country Inn & Suites/CIS_OnePager_2024.pdf` (688186 bytes; text 2972)
- `Choice Hotels International/Country Inn & Suites/Country Inn & Suites by Radisson FDD 2026.pdf` (10438145 bytes; text 879894)
- Missing: Choice development brand page

```bash
npm run choice-legacy-brand-source-package -- --dry-run --brand country-inn-suites-choice
```

### Everhome Suites

- Record: `recqkkrsevi4r9ibj`
- Explorer active: **yes**
- Profile completeness: **Strong Existing Profile**
- PI sources: **0/0** approved
- Local folder: `Choice Hotels International/{Everhome Suites | Everhome}`
- Registration ready (package): **partial/yes**
- JS-shell risk: **medium** — Choice development site may render Salesforce/LWC shell (see Radisson Blu recC9utJdNaKWR56k); prefer DAM PDF / one-pager when extract preview is thin
- Next action: **Register local PDF(s) then capture verified URLs**
- Warnings:
  - Choice development site may render Salesforce/LWC shell (see Radisson Blu recC9utJdNaKWR56k); prefer DAM PDF / one-pager when extract preview is thin

#### P0 sources

| Role | Type | URL / Local | Confidence | Status |
|------|------|-------------|------------|--------|
| Choice consumer brand page | Brand Page | https://www.choicehotels.com/everhome-suites | verified | **capture_needed_url** |
| Choice development brand page | Development Page | https://www.choicehotelsdevelopment.com/our-brands/extended-stay/everhome-suites | verified | **capture_needed_url** |
| development PDF / one-pager (local) | Development Brochure | `Choice Hotels International/Everhome Suites/Everhome Suites_Franchise Development Presentation.pdf` | verified_local | **ready_to_register_local** |
| Choice press kit / media center | Press Release | https://media.choicehotels.com/everhome-suites | verified | **capture_needed_url** |

#### Local files found

- `Choice Hotels International/Everhome Suites/Everhome Suites Entitlement Guide.pdf` (9451494 bytes; text 8382)
- `Choice Hotels International/Everhome Suites/Everhome_Suites_Prototype_Guide_Digital.pdf` (10631206 bytes; text 6695)
- `Choice Hotels International/Everhome Suites/Everhome Suites_Franchise Development Presentation.pdf` (9983970 bytes; text 25577)
- Missing: Choice development brand page; Choice press kit / media center

```bash
npm run choice-legacy-brand-source-package -- --dry-run --brand everhome-suites
```

### Quality Inn

- Record: `recd8o4k1JddhkRWW`
- Explorer active: **yes**
- Profile completeness: **Strong Existing Profile**
- PI sources: **0/0** approved
- Local folder: `Choice Hotels International/{Quality Inn | Quality}`
- Registration ready (package): **partial/yes**
- JS-shell risk: **medium** — Choice development site may render Salesforce/LWC shell (see Radisson Blu recC9utJdNaKWR56k); prefer DAM PDF / one-pager when extract preview is thin
- Next action: **Register local PDF(s) then capture verified URLs**
- Warnings:
  - Choice development site may render Salesforce/LWC shell (see Radisson Blu recC9utJdNaKWR56k); prefer DAM PDF / one-pager when extract preview is thin

#### P0 sources

| Role | Type | URL / Local | Confidence | Status |
|------|------|-------------|------------|--------|
| Choice consumer brand page | Brand Page | https://www.choicehotels.com/quality-inn | verified | **capture_needed_url** |
| Choice development brand page | Development Page | https://www.choicehotelsdevelopment.com/our-brands/midscale/quality-inn | verified | **capture_needed_url** |
| development PDF / one-pager (local) | Development Brochure | `Choice Hotels International/Quality Inn/brochure--quality-inn.pdf` | verified_local | **ready_to_register_local** |
| Choice press kit / media center | Press Release | https://media.choicehotels.com/quality-press-kit | verified | **capture_needed_url** |

#### Local files found

- `Choice Hotels International/Quality Inn/brochure--quality-inn.pdf` (781538 bytes; text 1923)
- `Choice Hotels International/Quality Inn/Q_OnePager_2024.pdf` (768454 bytes; text 2663)
- Missing: Choice development brand page; Choice press kit / media center

```bash
npm run choice-legacy-brand-source-package -- --dry-run --brand quality-inn
```

### Radisson by Choice

- Record: `recywbx1YQSTCPqW1`
- Explorer active: **yes**
- Profile completeness: **Strong Existing Profile**
- PI sources: **0/0** approved
- Local folder: `Choice Hotels International/{Radisson by Choice | Radisson | Radisson (Choice)}`
- Registration ready (package): **partial/yes**
- JS-shell risk: **medium** — Choice development site may render Salesforce/LWC shell (see Radisson Blu recC9utJdNaKWR56k); prefer DAM PDF / one-pager when extract preview is thin
- Next action: **Register local PDF(s) then capture verified URLs**
- Regional caveats: Americas Radisson owned by Choice; do not register RHG global radissonhotels.com facts on this Brand Basics row; Press kit includes explicit Americas vs RHG Belgium ownership split
- Warnings:
  - Choice development site may render Salesforce/LWC shell (see Radisson Blu recC9utJdNaKWR56k); prefer DAM PDF / one-pager when extract preview is thin
  - Regional: Americas Radisson owned by Choice; do not register RHG global radissonhotels.com facts on this Brand Basics row

#### P0 sources

| Role | Type | URL / Local | Confidence | Status |
|------|------|-------------|------------|--------|
| Choice consumer brand page | Brand Page | https://www.choicehotels.com/radisson | verified | **capture_needed_url** |
| Choice development brand page | Development Page | https://www.choicehotelsdevelopment.com/our-brands/upscale/radisson | verified | **capture_needed_url** |
| development PDF / one-pager (local) | Development Brochure | `Choice Hotels International/Radisson/brochure--radisson.pdf` | verified_local | **ready_to_register_local** |
| Choice press kit / media center | Press Release | https://media.choicehotels.com/Radisson-press-kit | verified | **capture_needed_url** |

#### Local files found

- `Choice Hotels International/Radisson/RAD_OneSheet_Final.pdf` (846094 bytes; text 3225)
- `Choice Hotels International/Radisson/brochure--radisson.pdf` (5767970 bytes; text 181)
- `Choice Hotels International/Radisson/1. Brand Book - RD.pdf` (2609482 bytes; text 12456)
- `Choice Hotels International/Radisson/2. Architectural & Design Guide - RD (1).pdf` (16845456 bytes; text 20890)
- `Choice Hotels International/Radisson/Radisson One Pager 2025.pdf` (3488486 bytes; text 3110)
- `Choice Hotels International/Radisson/Radisson Pitch Deck Final.pdf` (6923052 bytes; text 11141)
- Missing: Choice development brand page; Choice press kit / media center

```bash
npm run choice-legacy-brand-source-package -- --dry-run --brand radisson-choice
```

### Radisson Individuals by Choice

- Record: `recRyvM8OmLlDj9G7`
- Explorer active: **yes**
- Profile completeness: **Strong Existing Profile**
- PI sources: **0/0** approved
- Local folder: `Choice Hotels International/{Radisson Individuals by Choice | Radisson Individuals | Radisson Individuals (Choice)}`
- Registration ready (package): **partial/yes**
- JS-shell risk: **medium** — Choice development site may render Salesforce/LWC shell (see Radisson Blu recC9utJdNaKWR56k); prefer DAM PDF / one-pager when extract preview is thin
- Next action: **Register local PDF(s) then capture verified URLs**
- Regional caveats: Americas Radisson Individuals owned by Choice; exclude RHG global portfolio facts
- Warnings:
  - Choice development site may render Salesforce/LWC shell (see Radisson Blu recC9utJdNaKWR56k); prefer DAM PDF / one-pager when extract preview is thin
  - Regional: Americas Radisson Individuals owned by Choice; exclude RHG global portfolio facts

#### P0 sources

| Role | Type | URL / Local | Confidence | Status |
|------|------|-------------|------------|--------|
| Choice consumer brand page | Brand Page | https://www.choicehotels.com/radisson-individuals | verified | **capture_needed_url** |
| Choice development brand page | Development Page | https://www.choicehotelsdevelopment.com/our-brands/upper-upscale/radisson-individuals | verified | **capture_needed_url** |
| development PDF / one-pager (local) | Development Brochure | `Choice Hotels International/Radisson Individuals/RADIN_PitchDeck_PPT_New_Final.pdf` | verified_local | **ready_to_register_local** |
| Choice press kit / media center | Press Release | https://media.choicehotels.com/Radisson-Individuals-press-kit | verified | **capture_needed_url** |

#### Local files found

- `Choice Hotels International/Radisson Individuals/RADIN_PitchDeck_PPT_New_Final.pdf` (9935215 bytes; text 10662)
- `Choice Hotels International/Radisson Individuals/RADIND_OneSheet_new.pdf` (1784376 bytes; text 2742)
- Missing: Choice development brand page; Choice press kit / media center

```bash
npm run choice-legacy-brand-source-package -- --dry-run --brand radisson-individuals-choice
```

### Radisson RED by Choice

- Record: `recmKqo7M7mLZgRqQ`
- Explorer active: **yes**
- Profile completeness: **Strong Existing Profile**
- PI sources: **0/0** approved
- Local folder: `Choice Hotels International/{Radisson RED by Choice | Radisson RED | Radisson Red}`
- Registration ready (package): **partial/yes**
- JS-shell risk: **medium** — Choice development site may render Salesforce/LWC shell (see Radisson Blu recC9utJdNaKWR56k); prefer DAM PDF / one-pager when extract preview is thin
- Next action: **Register local PDF(s) then capture verified URLs**
- Regional caveats: Americas Radisson RED owned by Choice; RHG Enjoy It brochure is separate global reference (see save-radisson-red-choice-development-pdfs.mjs)
- Warnings:
  - Choice development site may render Salesforce/LWC shell (see Radisson Blu recC9utJdNaKWR56k); prefer DAM PDF / one-pager when extract preview is thin
  - Regional: Americas Radisson RED owned by Choice; RHG Enjoy It brochure is separate global reference (see save-radisson-red-choice-development-pdfs.mjs)

#### P0 sources

| Role | Type | URL / Local | Confidence | Status |
|------|------|-------------|------------|--------|
| Choice consumer brand page | Brand Page | https://www.choicehotels.com/radisson-red | verified | **capture_needed_url** |
| Choice development brand page | Development Page | https://www.choicehotelsdevelopment.com/our-brands/upscale/radisson-red | verified | **capture_needed_url** |
| development PDF / one-pager (local) | Development Brochure | `Choice Hotels International/Radisson RED/Radisson RED - Enjoy It development brochure (RHG 2022).pdf` | verified_local | **ready_to_register_local** |
| Choice press kit / media center | Press Release | https://media.choicehotels.com/Radisson-Red-press-kit | verified | **capture_needed_url** |

#### Local files found

- `Choice Hotels International/Radisson RED/PIP Template - Radisson RED (2022).pdf` (617929 bytes; text 55815)
- `Choice Hotels International/Radisson RED/Radisson RED - Enjoy It development brochure (RHG 2022).pdf` (2233346 bytes; text 3779)
- `Choice Hotels International/Radisson RED/Upscale by Choice brand overview guide.pdf` (2142797 bytes; text 10002)
- `Choice Hotels International/Radisson Red/PIP Template - Radisson RED (2022).pdf` (617929 bytes; text 55815)
- `Choice Hotels International/Radisson Red/Radisson RED - Enjoy It development brochure (RHG 2022).pdf` (2233346 bytes; text 3779)
- `Choice Hotels International/Radisson Red/Upscale by Choice brand overview guide.pdf` (2142797 bytes; text 10002)
- Missing: Choice development brand page; Choice press kit / media center

```bash
npm run choice-legacy-brand-source-package -- --dry-run --brand radisson-red-choice
```

## Does not do

- Rebuild Brand Explorer presentation content
- Overwrite populated Brand Setup fields
- Extract facts or approve sources automatically
- Publish governance or set Company Validated
- Auto-download uncertain URLs
- Register RHG global sources on Choice Americas brand rows
