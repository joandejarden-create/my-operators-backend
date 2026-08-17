# Operator Setup — Field Semantic Contract v2

Phase D failed because writers filled sections without field contracts.
Blank is preferable to generic filler. Inference is **not** permitted unless explicitly marked.

## `cap_profile_operational` — Platform & Markets

- **Question:** How does this operator organize and execute day-to-day hotel operations?
- **Belongs:** Operating model, regional ops accountability, SOPs/labor/guest experience posture, local leadership model
- **Does NOT belong:** Brand lists, assignment counts as the main claim, commercial mix, diligence disclaimers
- **Form / length:** 1–3 concise operational sentences; company-specific / 40–280 chars typical; up to ~500
- **Evidence:** Official ops materials, known regional structure, verified operating model — not Assignment count alone
- **Inference permitted:** false
- **Blank rule:** Blank if no ops-organization evidence
- **Adjacent:** cap_profile_commercial (commercial engine), cap_profile_transition (openings/reflags)
- **Recommend:** NARROW — require ops evidence or blank
- **Only fixture examples?** No
- **Strong examples (Tier1/2):**
  - Arbor Lodging (CALA): SOPs, labor productivity, and guest experience with regional ops accountability.…
  - Hotel Equities (CALA): HE CALA is organized around local leadership and regional execution in Caribbean and Latin America markets. Public materials indicate a focus on hotel opening, …
  - Cenote Azul Operadores: Humidity, pool chemistry, and beach F&B holding temps follow protocols written for tropical reality—guest injury and brand scores rise together. Sargassum and f…

## `cap_profile_commercial` — Platform & Markets

- **Question:** How does the operator win revenue / commercialize assets?
- **Belongs:** Sales/RM/distribution posture, brand commercial dependency, owner-relevant commercial model
- **Does NOT belong:** Raw brand name dumps, portfolio % caveats, operating SOPs
- **Form / length:** 1–3 commercial sentences / 40–280
- **Evidence:** Documented commercial organization or brand-dependent commercial path
- **Inference permitted:** false
- **Blank rule:** Blank if only brand list known
- **Adjacent:** ov_card_commercial, specializations
- **Recommend:** NARROW
- **Only fixture examples?** No
- **Strong examples (Tier1/2):**
  - Arbor Lodging (CALA): Arbor Lodging (CALA) integrates regional commercial leadership with enterprise revenue management and brand relationships—built for Mexico-first growth and broa…
  - Hotel Equities (CALA): The commercial model appears to combine regional sales coverage with parent-platform support for revenue management, distribution, and brand relationships. Publ…
  - Cenote Azul Operadores: Package, transient, and corporate channels each carry playbooks by submarket—Cancún versus Mérida versus Playa—with BAR floors tied to comps and channel cost. P…

## `cap_profile_transition` — Platform & Markets

- **Question:** What is the operator’s opening / conversion / transition capability?
- **Belongs:** Pre-opening, reflag, conversion, takeover process — only if evidenced
- **Does NOT belong:** Restating Development Context enums as capability claims
- **Form / length:** 1–2 sentences or blank / variable
- **Evidence:** Documented transition programs or verified case examples
- **Inference permitted:** false
- **Blank rule:** Blank unless transition capability evidenced beyond Development Context tags
- **Adjacent:** undefined
- **Recommend:** NARROW or MOVE TO CLAIMS
- **Only fixture examples?** No
- **Strong examples (Tier1/2):**
  - Arbor Lodging (CALA): Transitions and openings use explicit milestones (cash, payroll, safety, brand systems, staffing, IT cutover) with regional accountability—Mexico City team posi…
  - Hotel Equities (CALA): CALA public materials show active pre-opening and pipeline work, including projects in multiple island and resort markets. This suggests the division is being u…
  - Cenote Azul Operadores: Brand and RMS cutovers sequence with owner sign-off on cash timing—payroll, AP, and group deposits protected before we touch pricing engines. Milestones live in…

## `ownerEngagementNarrative` — Commercial Fit & Terms

- **Question:** How does the operator engage owners (cadence, decision rights, relationship model)?
- **Belongs:** Owner communication model, reporting rhythm, asset-management interface — company-specific
- **Does NOT belong:** Generic 'underwrite from MA' disclaimers; Management Availability select restated as prose
- **Form / length:** Owner-relevance narrative; specific mechanisms when known / variable
- **Evidence:** Operator materials, owner case studies, verified reporting model
- **Inference permitted:** false
- **Blank rule:** Blank if unknown — NEVER fill with diligence boilerplate
- **Adjacent:** infra_asset_management_reporting, Engagement section
- **Recommend:** NARROW — TARGETED RESEARCH or blank
- **Only fixture examples?** No
- **Strong examples (Tier1/2):**
  - Hotel Equities (CALA): HE CALA is framed as an owner-aligned third-party management platform with regional leadership and local knowledge. For owners, the practical implication is a l…
  - Arbor Lodging (CALA): Arbor Lodging (CALA) is built for owners who want a long-term partner with local presence in Mexico and a credible path into broader CALA markets—backed by a ve…
  - Cenote Azul Operadores: We align rhythm to asset type: Riviera leisure owners get high-season flashes and channel mix commentary; Mérida corporate owners get negotiated-account pace an…

## `specializations` — Commercial Fit & Terms

- **Question:** What asset/situation specializations does the company credibly claim?
- **Belongs:** Documented specializations (AI resort, urban full-service, etc.)
- **Does NOT belong:** One-off hotel-type strings from a thin Assignment sample framed as company specialization
- **Form / length:** Short list or sentence of company-level specializations / variable
- **Evidence:** Company materials or strong portfolio pattern (≥ threshold)
- **Inference permitted:** true
- **Blank rule:** Blank if sample too thin
- **Adjacent:** undefined
- **Recommend:** STRUCTURE AS SELECT or NARROW
- **Only fixture examples?** No
- **Strong examples (Tier1/2):**
  - Hotel Equities (CALA): Resort and all-inclusive operating model; urban/lifestyle hotels; branded and independent assets; pre-opening and conversion support; owner-aligned commercial e…
  - Cenote Azul Operadores: Resort, conversion, institutional reporting.…

## `ov_card_commercial` — Commercial Fit & Terms

- **Question:** Explorer card: commercial value headline for owners
- **Belongs:** Short distinctive commercial positioning for UI
- **Does NOT belong:** Multi-brand evidence boilerplate
- **Form / length:** UI card body / variable
- **Evidence:** Same as commercial narrative or pack
- **Inference permitted:** false
- **Blank rule:** Blank preferred to generic
- **Adjacent:** undefined
- **Recommend:** DEPRECATE as Setup truth / keep as presentation only
- **Only fixture examples?** No
- **Strong examples (Tier1/2):**
  - Hotel Equities (CALA): The company says it combines global best practices with regional knowledge, and the broader Hotel Equities platform references training, brand relationships, an…
  - Arbor Lodging (CALA): Arbor Lodging (CALA) pairs in-market commercial leadership with enterprise revenue management, sales, and marketing discipline. We align pricing, channel mix, a…
  - Cenote Azul Operadores: Commercial rhythm connects leisure pace to city corporate demand with GOP truth—promos show margin trade; group pace ties to staffing. Sales and ops share forec…

## `ov_card_flexibility` — Commercial Fit & Terms

- **Question:** Explorer card: flexibility / deal-structure posture
- **Belongs:** Documented flexibility posture
- **Does NOT belong:** Development Context enum dump
- **Form / length:** UI card body / variable
- **Evidence:** undefined
- **Inference permitted:** false
- **Blank rule:** Blank if unknown
- **Adjacent:** undefined
- **Recommend:** DEPRECATE as Setup truth
- **Only fixture examples?** No
- **Strong examples (Tier1/2):**
  - Hotel Equities (CALA): The division appears designed to be flexible across resort, lifestyle, and all-inclusive settings in multiple countries. That suggests adaptability, though cont…
  - Arbor Lodging (CALA): CALA assets require pragmatic trade-offs across brand standards, labor, seasonality, and owner objectives. We present data-backed options and staged investments…
  - Cenote Azul Operadores: Capex and promo thresholds documented for consent; sargassum or border pivots handled with written tradeoffs—not ad hoc discounts that implode on exit.…

## `infra_systems_technology` — Governance, Delivery & Diligence

- **Question:** What technology / systems stack does the operator use or depend on?
- **Belongs:** Named or classed systems (PMS/RMS/CRS/BI) or explicit brand-dependent model with brands named
- **Does NOT belong:** Generic 'systems vary; confirm in diligence'
- **Form / length:** Structured bullets or short systems map (see HE exemplar) / variable
- **Evidence:** Operator/IT materials or verified brand-dependent statement
- **Inference permitted:** false
- **Blank rule:** Blank unless systems posture actually known
- **Adjacent:** undefined
- **Recommend:** NARROW — TARGETED RESEARCH
- **Only fixture examples?** No
- **Strong examples (Tier1/2):**
  - Arbor Lodging (CALA): Systems vary by brand and asset—state brand-dependent stacks. Summarize owner reporting cadence and secure channels without inventing vendor names.…
  - Hotel Equities (CALA): PMS: Brand-dependent by asset (Marriott / Hilton / Hyatt family systems)
RMS & commercial: HE enterprise revenue support + brand RMS where required
Distribution…
  - Cenote Azul Operadores: Cloud PMS, integrated POS, data lake for owner BI.…

## `infra_asset_management_reporting` — Governance, Delivery & Diligence

- **Question:** How does the operator report to owners / asset managers?
- **Belongs:** Cadence, portal, packages, SSC/finance model — specific
- **Does NOT belong:** Restating Management Availability + market-practice boilerplate
- **Form / length:** 1–3 specific reporting sentences / variable
- **Evidence:** Documented reporting model
- **Inference permitted:** false
- **Blank rule:** Blank if unknown
- **Adjacent:** undefined
- **Recommend:** NARROW — TARGETED RESEARCH
- **Only fixture examples?** No
- **Strong examples (Tier1/2):**
  - Arbor Lodging (CALA): Systems vary by brand and asset—state brand-dependent stacks. Summarize owner reporting cadence and secure channels without inventing vendor names.…
  - Hotel Equities (CALA): Weekly flash KPIs for open CALA assets (where live)
Monthly owner operating and financial packs
Quarterly business reviews with asset-level action plans
CapEx a…
  - Cenote Azul Operadores: Monthly AM pack: KPI tree, capex, risk register.…

## `risk_programs_narrative` — Governance, Delivery & Diligence

- **Question:** What risk / insurance / control programs exist?
- **Belongs:** Named programs or verified control environment
- **Does NOT belong:** Diligence disclaimers about scorecards
- **Form / length:** Short factual narrative or blank / variable
- **Evidence:** undefined
- **Inference permitted:** false
- **Blank rule:** Blank if unknown
- **Adjacent:** undefined
- **Recommend:** MOVE TO CLAIMS or DEPRECATE if unsupportable at scale
- **Only fixture examples?** No
- **Strong examples (Tier1/2):**
  - Arbor Lodging (CALA): ALM highlights accountability, owner alignment, and disciplined execution with long-term value orientation. Formal risk controls are not quantified in the deck;…
  - Hotel Equities (CALA): Our programs cover life safety, security, business continuity, insurance compliance, and crisis communications, scaled to asset type and location. Regional lead…
  - Accor (Managed): Accor management contracts place Accor as operator on behalf of the owner with brand standards and performance oversight — confirm management agreement terms, P…
  - Grupo Marta Hospitality: Franchise brand standards (IHG / Best Western where applicable) and local Costa Rica operating compliance. Confirm certifications and insurance per asset. Sourc…

## `companyDescription` — Profile & Positioning

- **Question:** Who is this company in owner-relevant terms?
- **Belongs:** Identity, footprint model, what they operate, geography — researched prose
- **Does NOT belong:** OE assignment-count meta descriptions
- **Form / length:** 2–5 sentences company description / variable
- **Evidence:** Official site / filings / packs
- **Inference permitted:** false
- **Blank rule:** Prefer pack/research; blank better than OE meta
- **Adjacent:** undefined
- **Recommend:** KEEP field; CLEAR Phase-D OE meta writes
- **Only fixture examples?** No
- **Strong examples (Tier1/2):**
  - Arbor Lodging (CALA): Arbor Lodging (CALA) is the Caribbean & Latin America practice of Arbor Lodging—a vertically integrated hotel investment and management company. From a fully op…
  - Hotel Equities (CALA): Hotel Equities (CALA) is the Caribbean & Latin America division of Hotel Equities, focused on third-party hotel and resort management in the region. It combines…
  - Remington Hospitality: Remington Hospitality (formerly Remington Hotels) is a U.S.-based third-party hotel management company founded in 1968. Official materials describe managing 120…
  - Aimbridge Hospitality (LATAM): Aimbridge LATAM is Aimbridge Hospitality’s Latin America third-party hotel management division. Official materials describe operating a diverse portfolio for th…

## `differentiators` — Profile & Positioning

- **Question:** What meaningfully differentiates this operator for owners?
- **Belongs:** True differentiators with evidence
- **Does NOT belong:** Brand list restated as differentiation
- **Form / length:** Short differentiator bullets/sentences / variable
- **Evidence:** undefined
- **Inference permitted:** false
- **Blank rule:** Blank if unknown
- **Adjacent:** undefined
- **Recommend:** NARROW
- **Only fixture examples?** No
- **Strong examples (Tier1/2):**
  - Arbor Lodging (CALA): Intimate senior-leadership access and hands-on execution
Owner-operator mindset that aligns investment and operating decisions
Proprietary methods (technology, …
  - Hotel Equities (CALA): Owner-value focus; in-market CALA leadership; operational and financial discipline; world-class sales, marketing, and revenue management; strong food and bevera…
  - Remington Hospitality: Nearly 60 years of U.S. hotel management experience
CALA platform with Miami regional HQ and in-market ops leadership (Costa Rica, DR, Mexico, Puerto Rico, Caym…
  - Aimbridge Hospitality (LATAM): Aimbridge enterprise depth plus in-market LATAM leadership
Dedicated All-Inclusive division
Public brand alliances with IHG, Wyndham, Marriott, and Hilton
Mexic…
