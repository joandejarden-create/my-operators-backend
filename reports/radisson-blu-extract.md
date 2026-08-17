# Radisson Blu by Choice — Narrow Extraction

Generated: 2026-07-06T19:12:39.595Z
Mode: **apply**
Brand: Radisson Blu by Choice — `recWPEvxBQxVVzSq3`

## Summary

- Sources in scope: `recC9utJdNaKWR56k`, `recH1ZepKU6zJp7M2`, `recWGLvwnDn0v5rmL`, `reczafLghta09o2sB`
- Target fact keys: 12
- Registry-supported keys: 12
- Registry-unsupported keys: 0
- HTML text clean (all sources): **yes**
- Existing facts (allowlisted sources): 0
- Clean facts that would be created on apply: **11**
- Skipped candidates: 13
- Duplicate warnings: 5

## Extraction Quality Assessment

- Overall: **good_for_steward_review**
- Readable sources: 3
- Substantive facts: 5
- Has brand name: yes
- Has parent company (Choice): yes
- Has Americas footprint: yes
- Has ownership caveat: yes
- Apply recommended: **yes — pending founder review**
- Governance publish still blocked: **yes** — Facts must be created, human-approved, and governance publish dry-run must pass. No governance apply from this script.

### Platform intelligence (read paths)

- Approved facts can feed Brand Explorer trust chip evidence and BAS brand-fit reads.
- Americas-scoped footprint facts support Scout/Radar corridor context without RHG global overclaim.
- Owner/development positioning facts support Deal Readiness owner-education prompts (read-only).
- Ownership disclaimer fact supports alignment snapshots when region scope is ambiguous.

## Region / Ownership Caveats

- PI package scoped to Radisson Blu by Choice (Americas) — recWPEvxBQxVVzSq3.
- Choice Hotels owns/franchises Radisson Blu in the Americas; Radisson Hotel Group operates the brand elsewhere.
- Do not import RHG global portfolio counts unless a Choice source states Americas scope.
- Separate Brand Basics row exists for RHG-global Radisson Blu — do not cross-link sources.

Apply completed — facts remain Pending; steward review required.

## User-Requested Key Mapping

| Requested key | Registry key | Supported | Note |
|---------------|--------------|-----------|------|
| `be.snapshot.brandName` | `be.identity.brandName` | yes | — |
| `be.snapshot.parentCompany` | `be.identity.parentCompany` | yes | — |
| `be.positioning.summary` | `be.positioning.summary` | yes | — |
| `be.positioning.segment` | — | no | No brand PI registry key; capture segment language in be.positioning.summary. |
| `be.positioning.chainScale` | — | no | Hotel Chain Scale is Brand Setup — not a PI fact key; use upper-upscale in be.positioning.summary. |
| `be.ownerConsiderations.developmentPositioning` | `be.overview.whyValue` | yes | — |
| `be.standards.conversionConsiderations` | `be.overview.developmentModel` | yes | — |
| `be.markets.regionsSupported` | `be.footprint.americasHotels` | yes | Brand registry uses be.footprint.americasHotels / geoIntro — not op.markets.regionsSupported. |
| `be.brandFamily.context` | — | no | No registry key; ownership disclaimer may inform be.footprint.geoIntro when source-backed. |
| `be.development.model` | `be.overview.developmentModel` | yes | — |

## Target Fact Keys

| Priority | Field key | Registry supported | Display label |
|----------|-----------|-------------------|---------------|
| P0 | `be.identity.brandName` | yes | Brand Name |
| P0 | `be.identity.parentCompany` | yes | Parent Company |
| P0 | `be.positioning.summary` | yes | Brand Positioning |
| P0 | `be.positioning.tagline` | yes | Brand Tagline |
| P0 | `be.positioning.guestPromise` | yes | Brand Customer Promise |
| P1 | `be.overview.developmentModel` | yes | Development Model |
| P1 | `be.overview.whyValue` | yes | Why Owners Choose Brand |
| P2 | `be.positioning.history` | yes | Brand History |
| P2 | `be.overview.typicalUseCase` | yes | Typical Use Case |
| P1 | `be.footprint.americasHotels` | yes | Americas Hotel Count |
| P2 | `be.footprint.geoIntro` | yes | Footprint Summary |
| P2 | `be.loyalty.programName` | yes | Loyalty Program |

## Duplicate Warnings

- `be.identity.brandName` from `recH1ZepKU6zJp7M2`: duplicate_field_key_keep_best
- `be.identity.brandName` from `reczafLghta09o2sB`: duplicate_field_key_keep_best
- `be.positioning.summary` from `recWGLvwnDn0v5rmL`: duplicate_field_key_keep_best
- `be.positioning.guestPromise` from `recWGLvwnDn0v5rmL`: duplicate_field_key_keep_best
- `be.footprint.americasHotels` from `reczafLghta09o2sB`: duplicate_field_key_keep_best

## Source: Radisson Blu Choice development brand page (`recC9utJdNaKWR56k`)

- Validation: pass
- HTML text clean: yes
- Document kind: html (25 chars)
- Raw candidates (target keys): 2
- Clean candidates: 0
- Skipped: 2

### Skipped candidates

- `be.identity.brandName`: gap_copy, gap_or_skipped, weak_evidence — "Not confirmed in available sources."
- `be.identity.parentCompany`: gap_copy, gap_or_skipped, weak_evidence — "Not confirmed in available sources."

## Source: Radisson Blu Choice consumer brand page (`recH1ZepKU6zJp7M2`)

- Validation: pass
- HTML text clean: yes
- Document kind: html (3554 chars)
- Raw candidates (target keys): 2
- Clean candidates: 2
- Skipped: 2

### Clean candidates

#### `be.identity.brandName` (P0)

- Value: Radisson Blu
- Evidence: Radisson Blu® by Choice Hotels.
- Extraction: Directly Stated · High
- Enriched by Radisson Blu narrow script

#### `be.loyalty.programName` (P2)

- Value: Choice Privileges
- Evidence: Choice Privileges
- Extraction: Directly Stated · High
- Enriched by Radisson Blu narrow script

### Skipped candidates

- `be.identity.brandName`: gap_copy, gap_or_skipped, weak_evidence — "Not confirmed in available sources."
- `be.identity.parentCompany`: gap_copy, gap_or_skipped, weak_evidence — "Not confirmed in available sources."

## Source: Radisson Blu Choice press kit Americas (`recWGLvwnDn0v5rmL`)

- Validation: pass
- HTML text clean: yes
- Document kind: html (3874 chars)
- Raw candidates (target keys): 2
- Clean candidates: 6
- Skipped: 2

### Clean candidates

#### `be.identity.brandName` (P0)

- Value: Radisson Blu
- Evidence: Radisson Blu
- Extraction: Directly Stated · High
- Enriched by Radisson Blu narrow script

#### `be.identity.parentCompany` (P0)

- Value: Choice Hotels International
- Evidence: owned in the Americas regions by Choice Hotels.
- Extraction: Directly Stated · High
- Enriched by Radisson Blu narrow script

#### `be.positioning.summary` (P0)

- Value: Upper-upscale hospitality brand combining style with substance, innovation with comfort, and a sense of belonging in an elevated environment.
- Evidence: upper-upscale and full-service experience, located in key urban and resort destinations in the Americas.
- Extraction: Directly Stated · High
- Enriched by Radisson Blu narrow script

#### `be.positioning.guestPromise` (P0)

- Value: combines style with substance, innovation with comfort and a sense of belonging in an elevated environment.
- Evidence: combines style with substance, innovation with comfort and a sense of belonging in an elevated environment.
- Extraction: Directly Stated · High
- Enriched by Radisson Blu narrow script

#### `be.footprint.americasHotels` (P1)

- Value: 10 hotels (2,433 rooms) in operation in the Americas
- Evidence: 10 hotels with a combined 2,433 rooms in operation a in the Americas as of September 30, 2024.
- Extraction: Directly Stated · High
- Enriched by Radisson Blu narrow script

#### `be.footprint.geoIntro` (P2)

- Value: Americas: Radisson Blu franchised by Choice Hotels International. Outside the Americas: Radisson Hotel Group (separate company).
- Evidence: Outside of the Americas, the brands are owned by Radisson Hotel Group, an unaffiliated company headquartered in Belgium.
- Extraction: Directly Stated · High
- Enriched by Radisson Blu narrow script

### Skipped candidates

- `be.identity.brandName`: gap_copy, gap_or_skipped, weak_evidence — "Not confirmed in available sources."
- `be.identity.parentCompany`: gap_copy, gap_or_skipped, weak_evidence — "Not confirmed in available sources."

## Source: Radisson Blu Choice development one-pager (`reczafLghta09o2sB`)

- Validation: pass
- HTML text clean: yes
- Document kind: pdf (2675 chars)
- Raw candidates (target keys): 2
- Clean candidates: 8
- Skipped: 2

### Clean candidates

#### `be.identity.brandName` (P0)

- Value: Radisson Blu
- Evidence: Radisson Blu 
brings distinctive design to top urban and resort markets.
- Extraction: Directly Stated · High
- Enriched by Radisson Blu narrow script

#### `be.positioning.summary` (P0)

- Value: Upper-upscale hospitality brand combining style with substance, innovation with comfort, and a sense of belonging in an elevated environment.
- Evidence: upper upscale brand that appeals to the inspired professional –  
allergic to boring our guest finds traditional spaces uninspired.
- Extraction: Directly Stated · High
- Enriched by Radisson Blu narrow script

#### `be.positioning.tagline` (P0)

- Value: Think in Black & White Blu
- Evidence: THINK IN BLACK & WHITE BLU
- Extraction: Directly Stated · High
- Enriched by Radisson Blu narrow script

#### `be.positioning.guestPromise` (P0)

- Value: combines style with substance, innovation with comfort and a sense of belonging in an elevated environment.
- Evidence: combines style with substance, innovation with comfort and a sense of belonging in an elevated environment.
- Extraction: Directly Stated · High
- Enriched by Radisson Blu narrow script

#### `be.overview.developmentModel` (P1)

- Value: Development types include new construction, conversions, adaptive reuse (per Choice brand materials).
- Evidence: new build, adaptive reuse or conversion, Radisson Blu 
brings distinctive design to top urban and resort markets.
- Extraction: Directly Stated · High
- Enriched by Radisson Blu narrow script

#### `be.overview.whyValue` (P1)

- Value: Whether new build, adaptive reuse or conversion, Radisson Blu brings distinctive design to top urban and resort markets.
- Evidence: Whether new build, adaptive reuse or conversion, Radisson Blu 
brings distinctive design to top urban and resort markets.
- Extraction: Directly Stated · High
- Enriched by Radisson Blu narrow script

#### `be.footprint.americasHotels` (P1)

- Value: 10 hotels in the Americas (3 domestic, 7 international)
- Evidence: AMERICAS BRAND PRESENCE 10 in the Americas (3 Domestic, 7 International) Pipeline: 1 U.S., 4 International (YE 2023) Radisson Blu Toron
- Extraction: Directly Stated · High
- Enriched by Radisson Blu narrow script

#### `be.overview.typicalUseCase` (P2)

- Value: The Inspired Professional — upper-upscale guest seeking distinctive, non-boring hospitality experiences.
- Evidence: TARGET GUEST The Inspired Professional
- Extraction: Directly Stated · High
- Enriched by Radisson Blu narrow script

### Skipped candidates

- `be.identity.brandName`: gap_copy, gap_or_skipped, weak_evidence — "Not confirmed in available sources."
- `be.identity.parentCompany`: gap_copy, gap_or_skipped, weak_evidence — "Not confirmed in available sources."

## Proposed Facts (global, after dedupe)

| Source | Field key | Value preview |
|--------|-----------|---------------|
| Radisson Blu Choice press kit Americas | `be.identity.brandName` | Radisson Blu |
| Radisson Blu Choice press kit Americas | `be.identity.parentCompany` | Choice Hotels International |
| Radisson Blu Choice development one-pager | `be.positioning.summary` | Upper-upscale hospitality brand combining style with substance, innovation with comfort, and a sense |
| Radisson Blu Choice development one-pager | `be.positioning.guestPromise` | combines style with substance, innovation with comfort and a sense of belonging in an elevated envir |
| Radisson Blu Choice press kit Americas | `be.footprint.americasHotels` | 10 hotels (2,433 rooms) in operation in the Americas |
| Radisson Blu Choice press kit Americas | `be.footprint.geoIntro` | Americas: Radisson Blu franchised by Choice Hotels International. Outside the Americas: Radisson Hot |
| Radisson Blu Choice development one-pager | `be.positioning.tagline` | Think in Black & White Blu |
| Radisson Blu Choice development one-pager | `be.overview.developmentModel` | Development types include new construction, conversions, adaptive reuse (per Choice brand materials) |
| Radisson Blu Choice development one-pager | `be.overview.whyValue` | Whether new build, adaptive reuse or conversion, Radisson Blu brings distinctive design to top urban |
| Radisson Blu Choice development one-pager | `be.overview.typicalUseCase` | The Inspired Professional — upper-upscale guest seeking distinctive, non-boring hospitality experien |
| Radisson Blu Choice consumer brand page | `be.loyalty.programName` | Choice Privileges |

## Would Write On Apply

- Pending fact rows: 11
- Sources patched (Status → Extracted): 3

**Does not write:**
- Human Review Status = Approved (facts remain Pending)
- Approved for Explorer Use changes
- Brand Setup profile governance fields
- Company Validated / Company Validation Date
- External Display Status / Show Trust Label
- Published Explorer Fields / platform field publishing
- Governance publish
- RHG-global portfolio facts without Americas Choice evidence
- Apply without --approve-radisson-blu-extract

## Apply Result

- Run ID: `pi-radisson-blu-a035a762`
- Facts created: 11

## Post-Apply Recommendation

```bash
npm run steward-partner-intelligence -- --entity-type brand --target-rec-id recWPEvxBQxVVzSq3 --dry-run --recompute
```

_Manually review and approve facts before governance publish._
