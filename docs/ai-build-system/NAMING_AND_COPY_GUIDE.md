# Dealality Naming and Copy Guide

Product language standards for UI, snapshots, explorers, and AI-assisted content.

## Preferred Terms

- Opportunity
- Alignment
- Snapshot
- Signals
- Considerations
- Options for Consideration
- Questions to Clarify
- Data Gaps
- Source-Informed
- AI-Assisted
- Platform-Derived
- Company Published
- Company Validated
- Owner-Controlled
- Activation Readiness
- Opportunity Intelligence

### AI Visibility / AI Recommendation Intelligence

- Brand workspace product surface: **Brand AI Visibility**
- Operator workspace product surface: **Operator AI Visibility** (future)
- Owner workspace product surface: **AI Recommendation Intelligence** (never “Owner AI Visibility”)
- Shared engine/library concepts may still say “AI Visibility” internally; product-facing Brand/Operator labels must be stakeholder-prefixed
- Always distinguish **Viewer** (who is asking) from **Subject** (what is analyzed)
- Brand/Operator: deep self-intelligence; competitors are **comparative context** only (not a full competitor diagnostic workspace)
- Owner: subject is the **deal / hotel asset**, not the owner company as a visibility entity
- Always distinguish **AI Recommendation Pattern** from **Dealality Analysis** (and keep **Owner Process** as a third layer)
- Prefer evidence descriptors (Repeated across engines/runs, Emerging pattern, Single-engine observation) over client-facing numeric confidence
- Do not invent a composite GEO score or a single portfolio “AI Visibility Score”
- Semantic distinction is binding in executive copy:
  - brand **appears / is mentioned / is represented**
  - source **cited / citation**
  - never use `cited` to describe brand presence
- Keep constructs distinct in copy:
  - `observation` ≠ `comparable response` ≠ `association span`
  - `association` ≠ `narrative`
  - `recurrence` ≠ `confidence`
  - `citation association` ≠ `causation`

## Use Carefully

- Recommendation
- Best fit
- Match
- Score
- Validated
- Approved
- Advisory
- Legal
- Expert opinion

## Avoid

- Guaranteed fit
- Best brand
- Best operator
- Dealality recommends
- Legally reviewed
- Brand approved, unless actually brand approved
- Validated, unless validation level supports it
- Marketplace/listing language
- Consultant-style advisory language

## Existing Naming Rules

- Operator Fit Assessment should be **Operator Alignment Snapshot**.
- Operator Match should be avoided unless specifically referring to legacy code or internal scoring.
- Brand Match should be framed as **Brand Alignment** or **Brand Alignment Snapshot** when user-facing.
- Brand/operator content should not be called "Brand Validated" or "Operator Validated" unless directly reviewed/confirmed by the company.
- Brand Explorer / Operator Explorer content can be labeled AI-Assisted, Source-Informed, Platform-Derived, Company Published, or Company Validated depending on validation status.
- Airtable **`Brand Model` / `Brand Model / Format`** → owner-facing **Affiliation Model** (Soft vs Hard standards control; Soft Brand and Collection Brand stay distinct options).
- Airtable **`Hotel Service Model`** → owner-facing **Service / Operating Model** (Full-Service, Select-Service, Extended Stay, Lifestyle / Boutique, All-Inclusive).
- Airtable **`Brand Architecture`** stays **Brand Architecture** (Masterbrand / Endorsed / Soft-Collection naming under the parent) — do not conflate with Affiliation Model.

## UI Copy Rules

- Proper Case for short UI labels, dropdown options, headings, and badges.
- Sentence case for helper text and explanatory copy.
- Keep language premium, calm, structured, and credible.
- Avoid hype.
- Avoid generic AI language.
- Use clarity over cleverness.

### Brand Explorer — “Where This Brand Creates the Most Value”

`overview.scenario.1–3` must be **three distinct owner-value investment topics** (gold bar: Kimpton, Curio, Design Hotels).

**Required**
- Proper Case titles naming a real owner situation (conversion, new-build, portfolio, CALA growth, lifestyle expansion, etc.)
- Distinct bodies with owner economics: underwrite / conversion / PIP / capital / weaker when / affiliation
- Three different topics — never three variants of “use a reference property”

**Forbidden in scenario titles or bodies**
- “Reference”, “References”, “International Reference Comparison”, “Corridor Example”, “Debut Reference”
- Source-pack / diligence meta: “source pack”, “match by property name”, “keep geography labels”, “keep the CALA label”, “no verified CALA opens”, “use International Reference properties”, “open reference for owners”
- Shared boilerplate closers that make all three cards identical (diligence pad or “Geography and product fit drive returns…”)

Geography labels (CALA vs International Reference) and property-name matching belong in **openings / footprint / proofs**, not in scenario cards.

**Gate:** `npm run brand-explorer-scenario-owner-value-bar-audit` · bar module `lib/partner-intelligence/brand-explorer-scenario-owner-value-bar.js` (v2+)

### Brand Explorer — Gallery (`materials.gallery.1–6`)

Ideal state:
1. Prefer **CALA / LATAM** property photography when the brand has it
2. Mix space types: Exterior / Arrival, Guest Room / Suite, Public Space / Lobby, F&B (plus design detail / setting)
3. Prefer **multiple properties** — not six shots from one hotel
4. Never reuse the same photo (duplicateGroupId)
5. If no CALA inventory exists, use International Reference hotels with the same variety rules

Do not invent Exterior/Lobby/F&B captions without filename / DAM / fixture role evidence.

**Module:** `lib/partner-intelligence/brand-explorer-gallery-selection.js` · test: `npm run test:brand-explorer-gallery-selection`

### Brand Explorer — Value Creation Scenarios

`valueOwners.scenario.1–4` must be **four distinct owner-value situations** with Proper Case titles and a **short paragraph** each (gold bar: Ascend Hotel Collection).

**Required**
- Exactly four cards
- Each body ~26–58 words (short paragraph — not a one-liner, not a wall of text; package drafts target ~26–45)
- Brand name (or short name) in each body explaining when the brand creates value

**Forbidden**
- Blank cards
- One long blob in card 1 with empty cards 2–4
- Relying only on legacy `valueOwners.scenarios` multi-paragraph text without per-card Title/Body rows

**Gate:** `npm run brand-explorer-value-creation-scenarios-audit` · packages in `brand-explorer-value-creation-scenarios-packages.js`

## Dealality Tone

Dealality should sound:

- Premium
- Structured
- Calm
- Confident
- Evidence-aware
- Founder-grade
- Hospitality-specific
- Not overly technical
- Not salesy
- Not consultant-heavy

## Related Documentation

- Validation levels: [../data-intelligence/INTELLIGENCE_GOVERNANCE.md](../data-intelligence/INTELLIGENCE_GOVERNANCE.md)
- Build decisions: [BUILD_DECISIONS.md](./BUILD_DECISIONS.md)
