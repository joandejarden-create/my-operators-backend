# Operator Fit — Founder Scoring Specification

**Status:** Definitive documentation of the **currently implemented** Operator Fit Engine v2  
**Date:** 2026-08-04  
**Authority:** Code under `lib/operator-fit/` (especially `config.js`, `eligibility.js`, `alignment-factors.js`, `evaluate-candidate.js`, `evidence-and-coverage.js`, `execution-risk.js`, `top5-selector.js`, `readiness.js`, `owner-presentation.js`)  
**Scoring:** Frozen — this document describes; it does not change  
**Owner pilot:** Not enabled · My Deals unwired · No Airtable or code changes in this assignment

Worked Deal C numbers verified by live read-only evaluation dump: `reports/operator-fit-deal-c-scoring-dump.json`.

---

# One-page executive explanation

Dealality Operator Fit answers one question:

> Given this hotel project, which operators in Dealality’s **currently verified** universe are the strongest **project fit** — and what must still be validated?

It does **not** predict RevPAR, GOP, fees, brand approval for this project, or operator interest.

### How a result is produced (plain English)

1. **Start with the hotel project** (country, chain scale, development type, asset type, complexity flags, structure preferences when known).
2. **Build a candidate list** from Active operators (and, for internal research only, Research Stage operators).
3. **Eligibility** decides who can appear at all. Hard conflicts (e.g. missing required Market Presence) remove the operator from owner-facing ranking.
4. **Three scored layers** combine into a primary score:
   - Operator–Project Alignment (70%)
   - Operating Structure Alignment (15%)
   - Brand–Operator Compatibility (15%, skipped when no preferred brands)
5. **Execution Risk** subtracts capped penalty points for confirmed risks and unconfirmed validations.
6. That produces **Raw Operator Alignment**.
7. **Evidence Strength** (Strong / Moderate / Limited) may **cap** the number shown as **Displayed Alignment**.
8. Displayed Alignment maps to an owner **Alignment Band** (Strong / Good / Potential / Limited).
9. Eligible candidates are **ranked** by eligibility preference, then displayed score, Evidence Strength, coverage, lower risk, then a stable ID tie-break.
10. For production owner ranking, only **Ranking Ready** operators are kept (critical fields present + ≥50% project coverage).
11. The product then shows **Why this operator** (≤3 deterministic reasons) and **Validate Next** (prioritized next confirmation).

### Missing data (the rule that prevents “sparse inflation”)

If a factor applies to the project but is unknown, it still counts in the denominator and contributes **zero**. Other factors are **not** reweighted upward. Coverage falls. Evidence Strength can fall. The operator cannot look better simply because less is known.

### What the owner sees first

**Alignment band**, not `/100`. Evidence Strength. Why. Concern / unknown. Validate Next. The number is available only in expanded detail (Option C).

---

# 1. Exact implemented decision flow

Corrected sequence (matches `evaluateCandidate` → `selectTop5OperatorAlignment` → readiness filter → `owner-presentation`):

```text
Hotel Project
→ Candidate Operator Universe
→ Eligibility
→ Operator–Project Alignment factors (weighted)
→ Operating Structure Alignment (layer)
→ Brand–Operator Compatibility (layer)
→ Compose primary raw (70 / 15 / 15)
→ Execution Risk (subtract, capped)
→ Raw Operator Alignment
→ Data Coverage (parallel measurement)
→ Evidence Strength (confidence label + ceiling)
→ Evidence Ceiling applied
→ Displayed Alignment
→ Alignment Band + Project Compatibility labels (presentation)
→ Ranking / Top-5 among eligible
→ Ranking Ready filter (production owner pool)
→ Why This Operator
→ Validate Next
```

| Stage | What it does | Changes numeric score? | Changes eligibility? | Changes rank? | Changes Evidence Strength? | Owner-visible? |
| ----- | ------------ | --------------------- | -------------------- | ------------- | -------------------------- | -------------- |
| Candidate universe | Who is evaluated | No | Sets who can be tested | Indirect | No | Indirect (list content) |
| Eligibility | Hard / conditional gates | No (status only) | **Yes** | **Yes** (ineligible excluded) | No | Band/compatibility; status in Advisor |
| Operator–Project factors | 8 weighted factors | **Yes** (70% of primary) | No | Indirect | Indirect via coverage | Why / expanded factors (Advisor) |
| Operating Structure | Structure match layer | **Yes** (15%) | Also gate in eligibility | Indirect | Indirect | Validate Next / Advisor |
| Brand Compatibility | Brand relationship layer | **Yes** (15%) or N/A | Soft conditions | Indirect | Indirect | Brand note; not Project Approval |
| Execution Risk | Penalties | **Yes** (subtract) | No | Tie-break #5 | No | Concerns / unknowns |
| Raw Alignment | After risk | Output | No | Indirect | No | Advisor / expanded |
| Data Coverage | Known weight / applicable | No direct | No | Tie-break #4 | **Yes** (inputs) | Advisor; hidden on Owner L1 |
| Evidence Strength | Strong/Moderate/Limited | Via ceiling | No | Tie-break #3 | **Is** this | **Yes** |
| Evidence Ceiling | Cap displayed | **Yes** (display only) | No | Indirect | No | Via displayed/band |
| Displayed Alignment | Final number | Output | No | Tie-break #2 | No | Expanded only (Option C) |
| Alignment Band | Owner label | No | Can override Strong→Good when With Conditions | No | No | **Yes** (headline) |
| Ranking | Order Top-5 | No | Uses eligibility | **Yes** | Uses strength | Order of cards |
| Ranking Ready | Production gate | No | Separate readiness | Removes non-RR from owner production list | No | Internal; Under Evaluation separate |
| Why / Validate Next | Explanation | No | No | No | No | **Yes** |

---

# 2. Candidate universe

### How an operator enters evaluation

Production path (`evaluateOperatorFitForDeal`): adapted Active operator prefills (Company Master / new-base rows + Operator Intelligence calibration overlay when available). Brand-managed candidates may be added unless the owner excludes brand management.

Internal-only: Research Stage operators when `allowResearchStage` is set.

### Status rules

| Status | Treatment |
| ------ | --------- |
| **Active** (exact `^active$`) | Production candidate |
| **Research Stage** | Internal lane only; not production Active; owner **Under Evaluation** |
| Other / unknown Active | Hard conflict or conditional (“Confirm Active status”) |

### Geography / Market Presence

Eligibility prefers **Operator Intelligence – Market Presence** records over raw Active Countries.

**Strong geographic support** (establishes current eligibility):

- Current Managed Property  
- Current Operating Portfolio  
- Regional Office or Team  

**Conditional only:** Active Development  

**Never alone for current eligibility:** Historical Presence · Strategic Interest · Claimed Capability · Unknown  

If the deal’s Market Presence Requirement is “Active country operations required,” Conditional geography becomes a **hard conflict** (not Ranking Ready geography).

### Other hard / soft filters (see Eligibility table)

Operating structures, chain scale, development-type overlap (usually conditional), deal-breakers, brand-managed openness.

### Labels

| Label | Meaning |
| ----- | ------- |
| Production candidate | Active; considered for owner production ranking when Ranking Ready |
| Ranking Ready | Critical fields + ≥50% project coverage; no eligibility conflict; not generic-claims-only |
| Under Evaluation / Research Stage | Separate from ranked production list |
| Eligible With Conditions | Owner-facing eligible, but validations remain → often **Potential Fit — Validation Needed** |
| Not Currently Eligible | Excluded from owner-facing Top-5 |

---

# 3. Eligibility specification

Source: `lib/operator-fit/eligibility.js` + `lib/operator-intelligence/market-presence.js`.

| Eligibility Rule | Input | Passing Condition | Conditional Condition | Failure Condition | Unknown Treatment | Code Reference |
| ---------------- | ----- | ----------------- | --------------------- | ----------------- | ----------------- | -------------- |
| Active status | `activeStatus` | Exact Active | Research Stage if internal allow flag | Non-Active (non-RS) | Condition + unknown if unconfirmed | `eligibility.js` ~59–78 |
| Brand-managed exclusion | Owner excludes brand-managed | N/A | — | Hard conflict if excluded | — | ~80–84 |
| Brand-managed availability | Confirmed brand management offer | Independently confirmed | Unconfirmed → conditions | — | Unknown + condition | ~85–95 |
| Geography / Market Presence | Presence records + country | Strong type in project country | Active Dev / Historical / Strategic / Claimed | No presence for country when records exist; or Active Countries mismatch | Unknown operator geo → condition | `market-presence.js` `evaluateGeographicEligibilityFromPresence`; eligibility ~98–136 |
| Active country requirement | SI “Active country operations required” | Strong presence match | — | Conditional geo → hard conflict | Unknown → condition | eligibility ~117–136 |
| Operating structure | Owner prefs vs operator structures | Overlap | Franchise/TBC paths | Documented conflict | Unknown op/project → condition/unknown | ~138–154 |
| Chain scale / segment | Project scale vs operator scales | Exact or partial | Partial → condition | Clear mismatch | Unknown scale → condition | ~156–168 |
| Development type | Project vs operator situations | Overlap reason | Limited overlap → condition | — | Unknown situations | ~170–189 |
| Deal breakers | Owner breakers vs operator less-ideal | No overlap | — | Overlap → hard conflict | — | ~191–198 |
| Status rollup | Conflicts / conditions / reasons | Preferred if ≥3 reasons + geo match + scale match/partial | Conditions or unknowns → With Conditions | Any hard conflict → Not Currently Eligible | — | ~200–207 |

Owner-facing eligible statuses: Preferred · Eligible · Eligible With Conditions (`isOwnerFacingEligible`).

---

# 4. Exact Operator Alignment factors

Source: `OPERATOR_PROJECT_FACTORS` in `config.js` + scorers in `alignment-factors.js`.

**Primary composition** (`PRIMARY_LAYER_WEIGHTS`): Operator–Project **70%** · Structure **15%** · Brand Compatibility **15%**.

### Operator–Project factor table (100 weight points among applicable factors)

| Factor | Weight | Inputs | How points are earned | Max contribution | Negative / unknown | Applicability | Code |
| ------ | -----: | ------ | --------------------- | ---------------: | ------------------ | ------------- | ---- |
| Geographic and market alignment | 22 | Project country/city; operator countries/markets | City hit 100; country hit 78; else 12 | 100 | Mismatch scored low (known); unknown if either side missing | Always when geo relevant | `scoreGeographyFactor` |
| Hotel segment and positioning | 14 | Project chain scale; operator scales | Exact 100; partial 62; none 18 | 100 | Mismatch known-low; unknown if missing | Always when scale on project | `scoreSegmentFactor` |
| Comparable asset and development experience | 20 | Asset type, development type, comparables, asset/situation lists | Relevant comps → up to 70+10×n; asset/dev overlap ratios; breadth without relevance capped ≤40 | 100 | Limited overlap negative notes; unknown if no inputs | Always when project/op experience exists | `scoreAssetDevelopmentFactor` |
| Project-complexity alignment | 12 | Mixed-use, residences, complex F&B, meetings | % of flagged needs with evidence | 100 | Missing need → 0 share; unknown if no op haystack | **N/A** if no elevated complexity flags | `scoreProjectComplexityFactor` |
| Brand experience (portfolio) | 10 | Preferred brands vs brands operated | Overlap ratio → 45+55×ratio; none 15 | 100 | No overlap known-low; unknown if op brands missing | **N/A** if no preferred brands | `scoreBrandExperienceFactor` |
| Ownership and governance | 10 | Owner reporting/control reqs vs op reporting level | Institutional match 88; mismatch 28; generic present 55 | 100 | Unknown if op reporting missing | **N/A** if no owner reqs | `scoreOwnershipGovernanceFactor` |
| Regional resource alignment | 6 | Regional resources list; country fallback | Documented resources → 70 | 100 | Country ops without team proof → **unknown** (not automatic points) | Always applicable when scored | `scoreRegionalResourcesFactor` |
| Project-specific commercial differentiator | 6 | Non–table-stakes differentiators vs project needs | Relevant diffs → 55+15×n; weak relevance 25; table-stakes-only → **0** | 100 | Unknown if none | Always when scored | `scoreCommercialDifferentiatorFactor` |

### Topics that are **not** separate numerical factors today

Urban vs resort, conversion, reflagging, new build, turnaround, mixed use, residences, F&B, meetings — handled **inside** asset/development and complexity factors (or eligibility conditions), not as standalone weight rows.

Operating structures and brand company relationship are **separate layers**, not rows in the 8-factor table.

---

# 5. Table-stakes treatment

Source: `TABLE_STAKES_CAPABILITY_TOKENS` in `config.js`; `isTableStakesToken` / `classifyServices` in `adapters/operator-from-prefill.js`; commercial factor in `alignment-factors.js`.

| Capability token (examples) | Presence adds positive points? | Absence can create concern? | Absence can fail eligibility? | Project-specific depth can differentiate? |
| --------------------------- | ------------------------------ | --------------------------- | ----------------------------- | ----------------------------------------- |
| Revenue management | **No** | Not by itself | No | Only if framed as project-specific differentiator (rare; token filtered) |
| Sales / Marketing | **No** | No | No | Same |
| Procurement | **No** | No | No | Same |
| Accounting / Financial reporting / Finance | **No** | No | No | Same |
| HR / training | **No** | No | No | Same |
| Digital distribution | **No** | No | No | Same |
| Owner relations / owner reporting (generic) | **No** | Governance factor uses reporting **level**, not “owner relations” keyword | No | Institutional reporting language can score in governance factor |
| Generic pre-opening / pre-opening support | **No** | Pre-opening **capacity unknown** can add execution-risk validation when project needs pre-opening | No | Specialist pre-opening evidence can reduce that risk item |
| Full hotel management | **No** | No | No | Structure layer handles management path |

If only table-stakes claims exist: commercial factor scores **0**; Ranking Ready also rejects “generic claims only” profiles.

---

# 6. Operating Structure Alignment

Source: `operating-structure.js` + eligibility structure overlap.

- **Contributes to numerical score:** Yes — 15% of primary raw (unless N/A).
- **Separate layer:** Yes.
- **Eligibility gate:** Yes — conflict can make Not Currently Eligible; unknown → With Conditions.
- **Independent rank sort key:** No (only via score / risk / Validate Next).

### Owner-facing preserved structures

Third-Party Management · Franchise + Operator · Franchise Only · Owner-Operated · Lease · Asset Management · To Be Confirmed  

(Plus internal Brand Managed candidate type.)

### Matching behavior (summary)

| Situation | Structure score | State |
| --------- | --------------: | ----- |
| Exact owner/operator key overlap | 100 | known |
| Franchise path with third-party operator support | 72 | known |
| Limited overlap | 22 | known |
| Owner or operator structures unknown | null | unknown (0 in composition denom) |
| Brand-managed compatible with owner prefs | 88 / 55 / 15 | known |

---

# 7. Brand–Operator Compatibility

Source: `brand-operator-compatibility.js` + `brand-relationship-depth.js`.

| Concept | Role |
| ------- | ---- |
| **Brand Experience** (portfolio factor) | Separate 10-weight factor when preferred brands exist |
| **Brand Company Relationship** | Compatibility category + numeric for 15% layer |
| **Approval Status** | Explicit Approved / Approved With Conditions / Historically Approved only (`isExplicitApprovalStatus`) |
| **Project-Specific Approval** | Almost always **Both Parties Must Confirm** until explicitly confirmed |

| Outcome | Numeric (composition) | Eligibility | Evidence | Informational |
| ------- | --------------------: | ----------- | -------- | ------------- |
| Supported (verified current approval status) | 88 (or 90 brand-managed confirmed) | Soft validation remains | Helps evidence if sourced | Rationale notes **not project approval** |
| Partially Supported | 70 / 55 / 48 | Validation | — | Yes |
| Unsupported | 20 | Validation + risk | — | Yes |
| Unknown | 0 in denom | Conditions | — | Yes |
| Not Applicable (no preferred brands) | Layer skipped | — | — | Yes |

**Verified Current Brand Relationship ≠ Approved for This Project.**  
Compatibility rationale explicitly states portfolio relationship is not project approval; Validate Next asks for project-specific confirmation.

---

# 8. Comparables

Source: `scoreAssetDevelopmentFactor` + evidence helpers.

- Selected when comparable records share development / asset / situation signals with the project (string overlap heuristics).
- **No** named High / Moderate / Limited Comparability Strength enum in code — strength is **score impact** and evidence class flags (`verified` / `referenced`).
- Number of relevant comps: score ≈ `min(100, 70 + n×10)`.
- One relevant comparable is enough to create a material score.
- Geographic / segment / brand similarity: via fields on the comparable object when present; not a separate weighted sub-model.
- Repeated experience: brand-relationship depth taxonomy exists for diagnostics; main alignment path uses portfolio overlap + comps.

---

# 9. Execution Risk

Source: `EXECUTION_RISK` in `config.js` + `execution-risk.js`.  
**Max total penalty:** 25 points subtracted from primary raw.

| Risk key | Trigger | Penalty / effect | Max impact | Unknown treatment | Owner explanation |
| -------- | ------- | ---------------- | ---------: | ----------------- | ----------------- |
| geographicMobilization | Country mismatch | 8 confirmed | 8 | Half points if countries unknown | Geographic mobilization risk |
| brandApprovalUncertainty | Unsupported or unknown brand compat | 6 | 6 | Full points as unknown_validation when unknown | Brand approval unconfirmed |
| missingStructureSupport | Structure score &lt; 40 | 10 confirmed | 10 | Half points if structure unknown | Structure unconfirmed / weak |
| limitedComparableExperience | No comps/assets/situations | 8 unknown | 8 | Potential half if broad portfolio without comps | Comparables missing |
| unconfirmedRegionalResources | No regional resources list | 5 unknown | 5 | Always validation kind when absent | Regional support not confirmed |
| unconfirmedPreOpeningCapacity | Project needs pre-opening; op blank | 5 unknown | 5 | Validation | Pre-opening capacity |
| materialDataGaps | Coverage &lt; 40% | 6 potential | 6 | Potential concern | Material data gaps |
| competitiveConflict | Deal-breaker overlap | 8 confirmed | 8 | — | Deal-breaker conflict |

Kinds are distinct: **confirmed_risk** · **potential_concern** · **unknown_validation** — unknown is not merged into confirmed risk.

---

# 10. Missing data (mathematical rule)

Source: `aggregateOperatorProjectAlignment` + `composePrimaryRaw`.

For each **applicable** factor:

- Known → `contribution = score × weight`
- Unknown → `contribution = 0`, **weight stays in denominator**
- Not applicable → removed from numerator **and** denominator

Other factors are **not** reweighted.

### Simple example (illustrative weights)

Suppose 10 weight units applicable.

| Operator | Known | Unknown | Scores on known | Raw factor layer |
| -------- | ----: | ------: | --------------- | ---------------- |
| A | 8 | 2 | average 80 on known | (80×8 + 0×2) / 10 = **64** |
| B | 5 | 5 | average 80 on known | (80×5 + 0×5) / 10 = **40** |

Operator B cannot beat A merely by having more blanks. Coverage is lower; Evidence Strength thresholds using known-weight % are harder to pass; ceiling may further cap display.

---

# 11. Data Coverage formula

Source: `calculateDataCoverage` in `evidence-and-coverage.js`.

```text
applicableWeight = sum(applicable operator–project factor weights)
                 + structure layer weight (15) unless structure N/A
                 + brand layer weight (15) unless brand N/A

knownWeight = weights of known factors/layers
unknownWeight = weights of unknown factors/layers

coveragePct = round(knownWeight / applicableWeight × 1000) / 10
```

| Effect | Directly modifies? |
| ------ | ------------------ |
| Raw Alignment | No (except via unknown = 0 contributions) |
| Displayed Alignment | Indirect (via Evidence Strength rules) |
| Evidence Strength | **Yes** (known % thresholds) |
| Ranking | Tie-break #4 |
| Ranking Ready | Uses project `dataCoveragePct` ≥ 50% |

Owner View: coverage is Advisor / internal. Owner sees Evidence Strength and unknowns instead.

---

# 12. Evidence Strength

Internal name in engine: **Evidence Confidence**. Owner label: **Evidence Strength**.

### Evidence classes (`EVIDENCE_CLASSES`)

| Evidence Class | Meaning | Rank | Can support Strong? |
| -------------- | ------- | ---: | ------------------- |
| verified_project_level | Verified project-level evidence | 5 | Yes |
| independently_referenced | Independently referenced | 4 | Yes (minimum for Strong) |
| detailed_operator_provided | Detailed operator-provided | 3 | Moderate max unless independent also present |
| portfolio_level_operator | Portfolio-level | 2 | Not Strong alone |
| general_operator_claim | General claim | 1 | Limited |
| unknown | Unknown | 0 | No |

There is **no** separate configured class named “Official documentation”; official docs map into verified / independent when flagged on sources.

### Strength rules (`EVIDENCE_CONFIDENCE_RULES`)

| Label | Rule (current code) |
| ----- | ------------------- |
| **Strong** | Best class rank ≥ independently_referenced **and** known factor weight % ≥ **40**; operator-reported alone **cannot** be Strong |
| **Moderate** | Best rank ≥ detailed_operator_provided **and** known weight % ≥ **45** |
| **Limited** | Otherwise (including general claims with known % &gt; 20 still Limited ceiling) |

---

# 13. Evidence ceilings

Source: `EVIDENCE_CONFIDENCE` in `config.js` + `applyEvidenceCeiling`.

| Evidence Strength | Displayed score ceiling |
| ----------------- | ----------------------: |
| Limited | **≤ 69** |
| Moderate | **≤ 84** |
| Strong | **Uncapped** (still clamped 0–100) |

**Raw Operator Alignment** = primary composition after execution-risk subtraction.  
**Displayed Operator Alignment** = min(raw, ceiling) when ceiling applies.

Example: Raw = 88, Evidence Strength = Moderate → Displayed = **84**.

Deal C #1 example: Raw = 38.6, Evidence Strength = Strong → Displayed = **38.6** (no ceiling).

---

# 14. Alignment bands (owner-facing)

Source: `OWNER_ALIGNMENT_BANDS` in `owner-presentation.js`.

| Band | Displayed score |
| ---- | --------------- |
| Strong Alignment | ≥ 70 |
| Good Alignment | ≥ 55 |
| Potential Alignment | ≥ 40 |
| Limited Alignment | ≥ 0 (&lt; 40) |

Overrides:

- Hard Not Currently Eligible → band forced to **Limited Alignment**
- Eligible With Conditions + would-be Strong → shown as **Good Alignment**, with Project Compatibility **Potential Fit — Validation Needed**
- Eligible With Conditions generally → Project Compatibility **Potential Fit — Validation Needed**

---

# 15. Ranking / tie-break logic

Source: `compareCandidates` in `top5-selector.js`.

Exact order:

1. Eligibility preference: Preferred &gt; Eligible &gt; With Conditions  
2. Higher **Displayed** Operator Alignment  
3. Higher Evidence Strength (Strong &gt; Moderate &gt; Limited)  
4. Higher Data Coverage %  
5. Lower Execution Risk penalty  
6. Stable `candidateId` string compare  

Not Eligible candidates are excluded (not padded to five).

**Research Stage:** excluded from production Ranking Ready pool; shown separately as Under Evaluation when internal allow flag is on.

**Fewer than five:** returned when fewer owner-facing-eligible (and, for production UI, Ranking Ready) candidates exist — **do not invent fillers**.

---

# 16. Readiness (Ranking Ready)

Source: `readiness.js`. Threshold: **`PRODUCTION_COVERAGE_THRESHOLD_PCT = 50`**.

Ranking Ready requires:

- Critical fields present: Active status, structured geography, operating structures, chain scales, project experience, evidence source  
- Project-applicable coverage ≥ 50%  
- No eligibility hard conflict  
- Not “generic table-stakes claims only”

An operator can have a decent alignment number and still be **Research Required** / Conditionally Rankable if critical fields are missing — that is why Ranking Ready is **internal** and owners see simpler bands + Under Evaluation separation.

---

# 17. Why This Operator

Source: `buildExplanations` → `buildWhyThisOperator`.

Deterministic priority:

1. Positive evidence from factors with score ≥ 70  
2. Eligibility reasons (fill to 3)  
3. Brand / structure rationales when strong  

Max **3** reasons. Filters out table-stakes / generic “Operator is Active.”  
**No AI invention.**

Deal C #1 (Grupo Hotelero Santa Fe) owner reasons:

1. Active country: Mexico  
2. Supports Upper Upscale  
3. Directly comparable assignment(s): GSF Mexico third-party managed hotels (portfolio)

---

# 18. Validate Next

Source: `buildValidateNext` + candidate `validationQuestions` + ranking-change classifications.

Sensitivity classes used in presentation: Eligibility-sensitive · Rank-sensitive · Confidence-sensitive · Final-selection-sensitive (plus informational).

Sort: sensitivity priority, then phase (Before Outreach → During Outreach → Before Final Decision). First item is primary Validate Next.

Deal C #1 primary: **Confirm supported management structures for this project.** (Before Outreach · Eligibility-sensitive)

Also typical: confirm operator interest/capacity (During Outreach); request project-specific fee proposal (Before Final Decision).

---

# 19. Worked Deal C calculation

**Project (redacted Deal C, current pilot evaluation path):** Mexico · Upper Upscale · Mixed-Use asset · New Build · Market Presence requirement = Active country operations · **no preferred brands captured on this evaluation path** · **owner structure preference unknown**.

## Operator #1 — Grupo Hotelero Santa Fe

### Eligibility

- Active: pass  
- Geography: pass (Mexico presence; Active country requirement satisfied via qualifying presence / country path used in eval)  
- Structure: **conditional / unknown owner preference** → Eligible With Conditions  
- Segment: Upper Upscale match  
- Brand: N/A on project  

Status: **Eligible With Conditions** → owner **Potential Fit — Validation Needed**

### Factor calculation

| Factor | Weight | Evidence | Factor score | Weighted points (score×weight) |
| ------ | -----: | -------- | -----------: | -----------------------------: |
| Geography | 22 | Country Mexico | 78 | 1716 |
| Segment | 14 | Exact Upper Upscale | 100 | 1400 |
| Asset / development | 20 | Mexico comps + New Build overlap; limited Mixed-Use | 80 | 1600 |
| Project complexity | 12 | Mixed-use flagged; no clear evidence | 0 | 0 |
| Brand experience | 10 | N/A | — | removed |
| Ownership governance | 10 | N/A | — | removed |
| Regional resources | 6 | Unknown | 0 | 0 (in denom) |
| Commercial differentiator | 6 | Unknown | 0 | 0 (in denom) |

```text
Applicable weight = 80
Known weight = 68
Unknown weight = 12
Operator–Project raw = 4716 / 80 = 59.0
```

### Layers

- Structure: unknown (owner preference missing) → 0 contribution, weight 15 in denom  
- Brand: Not Applicable → skipped  

```text
Primary raw before risk = (59.0 × 70) / (70 + 15) = 48.6
```

### Execution Risk

- missingStructureSupport (unknown): 5  
- unconfirmedRegionalResources: 5  
- Total capped: **10**

```text
Raw Operator Alignment = 48.6 − 10 = 38.6
```

### Data Coverage

```text
applicable = 80 + 15 = 95
known = 68
coveragePct = 68/95 × 100 = 71.6%
```

### Evidence Strength

Strong (independent/verified sources present; known factor weight % ≥ 40). Ceiling = none.

```text
Displayed Alignment = 38.6
Alignment band = Limited Alignment (< 40)
Project Compatibility = Potential Fit — Validation Needed
```

### Ranking

Engine rank **#1** among eligible / Ranking Ready pool.

### Why / Validate Next

As above (Mexico · Upper Upscale · GSF Mexico comps / Confirm management structures).

---

## Operator #2 — Highgate

### Eligibility

Same pattern: Eligible With Conditions · Potential Fit — Validation Needed.

### Factor calculation

Same numeric factor table as #1 (78 / 100 / 80 / 0 / N/A / N/A / unknown / unknown) → Operator–Project raw **59.0**.

Difference is **qualitative comparable text**: Ocean Club (DR) rather than GSF Mexico portfolio — **same factor score (80)**.

### Layers / risk / coverage / evidence

Identical arithmetic to #1:

```text
Primary before risk = 48.6
Risk = 10
Raw = Displayed = 38.6
Coverage = 71.6%
Evidence Strength = Strong
Band = Limited Alignment
```

### Ranking

Engine rank **#2**.

---

## Why Operator #1 ranks above Operator #2

Measured deltas (alignment, factors ≥2 pts, evidence, coverage ≥5 pts, risk): **none** (`explainRankingDifference` returns empty drivers).

| Driver | #1 | #2 | Score contribution difference | Ranking significance |
| ------ | -- | -- | ----------------------------: | -------------------- |
| Displayed Alignment | 38.6 | 38.6 | 0 | Tie |
| Evidence Strength | Strong | Strong | 0 | Tie |
| Data Coverage | 71.6% | 71.6% | 0 | Tie |
| Execution Risk | 10 | 10 | 0 | Tie |
| Stable candidate ID tie-break | `reckyv9…` sorts before `recLjxt…` in engine compare | — | n/a | **Decides #1 vs #2** |

Owner-facing narrative difference (not a score delta): Mexico third-party portfolio comparable vs DR luxury resort comparable.

> If I were explaining this ranking to a hotel owner, I would say: Santa Fe and Highgate currently score the same on Dealality’s verified fit math for this project. Both are Limited Alignment with Strong Evidence Strength and the same open validations (structure preference and regional resources). Santa Fe appears first because the engine’s tie-break keeps a stable order when scores match — you should treat them as a near-tie and use Validate Next plus outreach to separate them, not the 0.0 score gap.

---

# 20. Worked missing-data example (Deal C #1 unknowns)

**Known:** Geography, segment, asset/development, complexity (scored 0), structure layer unknown, brand N/A.  
**Unknown applicable:** Regional resources (6), commercial differentiator (6), plus structure layer (15) in coverage.

Raw 38.6 · Coverage 71.6% · Evidence Strong · Ceiling none · Displayed 38.6 · Band Limited.

### If regional resources were verified positively (illustrative)

Assume regional factor becomes known score **70** (per scorer when resources documented):

```text
Weighted points += 70×6 = 420
Operator–Project raw = (4716+420)/80 = 64.2
Primary = (64.2×70)/(85) ≈ 52.9
After same risk 10 → Raw ≈ 42.9
Still Strong → Displayed ≈ 42.9 → Potential Alignment band
```

Unknown commercial (6) would still dilute until verified. This is why blanks cannot inflate scores.

*(Playa on the same deal shows a **known-weak** asset score 35 — different from unknown: weak evidence lowers the numerator without removing weight.)*

---

# 21. Worked ineligible example

**Operator:** Driftwood Hospitality Management (among others: Cenote Azul, Atlantica, GHL on this Deal C run).

**Gate failed:** Market Presence requirement — qualifying Current Managed / Operating / Regional Office presence required (hard conflict).

**Why other strengths cannot override:** Eligibility runs as a hard filter before Top-5 membership (`isOwnerFacingEligible`). High segment/geo scores elsewhere never enter owner ranking while Not Currently Eligible.

**To reconsider:** Document qualifying Market Presence for Mexico (or change the deal’s presence requirement — product decision), then re-evaluate.

---

# 22. Airtable data map (concise)

```text
Operator / Company Master (+ Profile / Platform / Commercial / Governance)
        │  identity, Active status, countries, structures, scales, brands…
        ▼
Operator Intelligence – Claims ──► evidence / claims (evidence strength inputs)
Operator Intelligence – Market Presence ──► geography eligibility (strong vs claimed)
PI Source Library (or equivalent) ──► source verification flags
Case Studies ──► comparables (alignment + evidence)
Brand–Operator relationship data ──► compatibility / approval status (≠ project approval)
Operator Fit – Shortlist ──► workflow only (immutable snapshots; not scoring)
```

| Structure | Affects scoring? | Affects evidence? | Workflow only? |
| --------- | ---------------- | ----------------- | -------------- |
| Company Master / setup tables | Yes | Partial | No |
| Market Presence | Eligibility / geo | Indirect | No |
| Claims / Source Library | Indirect | **Yes** | No |
| Case Studies | **Yes** | **Yes** | No |
| Brand relationships | Compatibility layer | Partial | No |
| Operator Fit – Shortlist | No | No | **Yes** |

---

# 23. UI map

Same evaluation object feeds both views (`buildOwnerCandidatePresentation` / `buildAdvisorCandidatePresentation`).

### Owner View — first card

Operator · Alignment band · Evidence Strength · Why · Concern · Validate Next · Shortlist  

### Owner View — expanded

Numeric Displayed Alignment · more detail · comparables · brand context note · important unknowns  

### Advisor View — additional

Numeric score · Data Coverage · Eligibility · Readiness · Factor breakdown · Research status · Evidence diagnostics  

---

# 24. What the score DOES and DOES NOT mean

## Operator Alignment DOES mean

A structured, evidence-aware assessment of how well a verified operator profile fits **this project’s** known requirements and complexity — after eligibility, layered scoring, risk, and evidence ceilings — within Dealality’s current universe.

## Operator Alignment DOES NOT mean

- Predicted RevPAR or GOP  
- Guaranteed operator performance  
- Guaranteed brand approval for this project  
- Guaranteed operator interest or capacity  
- Final management recommendation  
- Lowest fees or contract recommendation  
- Completeness of the entire real-world operator market  

It is a **project-fit assessment based on currently verified information**, with explicit Validate Next for what outreach must still confirm.

---

# 25. Code reference index

| Topic | Primary file |
| ----- | ------------ |
| Weights, ceilings, table-stakes, risk caps | `lib/operator-fit/config.js` |
| Eligibility | `lib/operator-fit/eligibility.js` |
| Market Presence types | `lib/operator-intelligence/market-presence.js` |
| Factor scorers + unknown-in-denominator | `lib/operator-fit/alignment-factors.js` |
| Compose + evaluate | `lib/operator-fit/evaluate-candidate.js` |
| Coverage / evidence / ceiling | `lib/operator-fit/evidence-and-coverage.js` |
| Execution risk | `lib/operator-fit/execution-risk.js` |
| Structure layer | `lib/operator-fit/operating-structure.js` |
| Brand compatibility | `lib/operator-fit/brand-operator-compatibility.js` |
| Ranking | `lib/operator-fit/top5-selector.js` |
| Ranking Ready | `lib/operator-fit/readiness.js` |
| Why / Validate Next presentation | `lib/operator-fit/explanations.js`, `owner-presentation.js` |

Discrepancies vs older docs: `reports/operator-fit-scoring-documentation-discrepancies.md`.  
One-page cheat sheet: `docs/product/operator-fit-founder-cheat-sheet.md`.
