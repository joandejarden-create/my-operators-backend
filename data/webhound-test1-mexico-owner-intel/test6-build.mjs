import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const dir = path.dirname(fileURLToPath(import.meta.url));

const prompt = `DEALALITY WEBHOUND TEST 6 — BRAND EXPLORER INDEPENDENT VALIDATION BENCHMARK

Budget: exactly $10. Success = five independently researched brand intelligence packages + Mexico/CALA-first hotel censuses (one brand per row). Brand Explorer field quality takes priority over exhaustive global hotel enumeration.

CORE QUESTION
Can Webhound independently produce sufficiently complete and accurate Brand Explorer–grade intelligence that can later be reconciled against Dealality's Airtables?

This benchmark measures INDEPENDENT PUBLIC-EVIDENCE QUALITY.
It does NOT measure agreement with Dealality.
Do NOT use Airtable, Dealality Brand Explorer, Dealality Hotel Census, Dealality Operator Explorer, dealality.com private product pages, or any Dealality internal/repo dumps as research sources.
Treat this as a completely independent public-web research exercise.

THIS IS NOT: an Airtable comparison; a reconciliation run; an owner-discovery test; an outreach test; Operator Explorer validation.

BRANDS (exactly these five — do not substitute, add, or drop)
1) Hotel Indigo
2) Kimpton (Kimpton Hotels / Kimpton Hotels & Restaurants as publicly styled)
3) Tribute Portfolio
4) Avani
5) Radisson Individuals Americas (Choice Hotels) — explicitly resolve the Choice Hotels relationship and any regional operating/licensing arrangements for the Americas

ONE ROW PER BRAND. Prefer depth and evidence quality across all five over inventing sixth brands.

RESEARCH SCOPE PER BRAND

A) Brand identity
Parent company; collection structure; brand positioning; segment; typical key count; development model; conversion suitability; new-build suitability; geographic strategy; typical owner profile; typical operator profile; target customer; brand strengths; brand differentiators.

B) Brand growth
Current growth strategy; recent openings; recent conversions; pipeline; expansion priorities; development announcements.

C) Independent hotel census — GEOGRAPHIC PRIORITY (do NOT burn budget on exhaustive global enumeration)
Priority order for hotel-level research:
1) Mexico — attempt complete Mexico census first
2) CALA / Caribbean & Latin America — attempt complete CALA census second
3) Relevant Americas inventory — expand where practical
4) Global inventory ONLY if sufficient budget remains after Brand Explorer field quality is secured

Do NOT require exhaustive reconstruction of the full global hotel census for large brands before completing Brand Explorer intelligence fields.
Official global totals/directories may be cited as supporting evidence for scale — but do NOT manually enumerate hundreds of global hotels if that would materially reduce identity/growth/parent/recent-change field quality.
Never imply the census is global-complete unless it actually is.
Set hotel_census_complete=Yes only when the stated coverage scope (e.g. Mexico+CALA) is actually complete for that scope; otherwise No and explain in census_coverage_notes.

For EVERY hotel found in-scope, capture in hotel_census_entries using this pipe format (one hotel per array item):
Hotel name | Location (city/area) | Country | Operating status | Opening date if available | Conversion or new-build if identifiable | Operator if publicly known | Owner if publicly known | Management company if publicly known | Evidence source URL or citation
Rules:
- Public evidence only.
- Do NOT estimate, invent, or pad hotel totals.
- Do NOT invent owners/operators.
- If a field is unknown, write "unknown".
- Document coverage clearly (Mexico / CALA / Americas / Global partial).

C2) Mexico / CALA depth priorities (facts only — NOT a Diego brand-fit analysis)
For each brand, particularly strong research on:
- Mexico presence; CALA presence
- Existing resort/destination properties
- Small-key-count properties where identifiable
- New-build versus conversion examples
- Mixed-use examples
- Branded-residence experience where publicly documented
- Owner/developer relationships where publicly documented
- Operator/management structures where publicly documented
- Recent Mexico/CALA pipeline
- Current development appetite in Mexico/CALA
Do NOT produce a San José del Cabo / Diego brand-fit recommendation. Collect and validate underlying facts only.

D) Parent relationships (resolve the chain)
Brand → Parent company → Collection → Distribution platform → Loyalty program → Regional operating structure.
Identify special regional arrangements.
For Radisson Individuals Americas: explicitly identify the Choice Hotels relationship and how Americas operations/distribution/loyalty interact.

E) Recent changes (current information prioritized — do not recycle stale marketing copy)
Openings; closures; reflags; conversions; brand launches; geographic expansion; brand repositioning; owner announcements; operator announcements.
Prioritize Mexico/CALA/Americas currency when allocating budget.

F) Validation quality (for important facts)
Primary source; secondary source; confidence; last verified; evidence gaps; unknowns.

OUTPUT
One complete brand intelligence record per brand.
One independent hotel census embedded per brand (hotel_census_entries + coverage notes) — Mexico/CALA-first.
Evidence package fields filled.
Confidence assessment filled.
Independent summary in executive_summary — without any Dealality comparison language.

Candidate intelligence only. Preserve provenance. Do not invent facts.
`;

const attributes = [
  { name: "brand_package_id", type: "string", is_primary: true, required: true, description: "INDIGO-BE-VAL-001 | KIMPTON-BE-VAL-001 | TRIBUTE-BE-VAL-001 | AVANI-BE-VAL-001 | RADISSON-INDIV-AMERICAS-BE-VAL-001" },
  { name: "brand_name", type: "string", required: true, description: "Exact brand as researched: Hotel Indigo | Kimpton | Tribute Portfolio | Avani | Radisson Individuals Americas" },
  { name: "executive_summary", type: "string", required: true, description: "Independent brand intelligence summary — no Dealality comparison." },

  // Identity
  { name: "parent_company", type: "string", required: true },
  { name: "collection_structure", type: "string", required: true },
  { name: "brand_positioning", type: "string", required: true },
  { name: "segment", type: "string", required: true },
  { name: "typical_key_count", type: "string" },
  { name: "development_model", type: "string" },
  { name: "conversion_suitability", type: "string" },
  { name: "new_build_suitability", type: "string" },
  { name: "geographic_strategy", type: "string" },
  { name: "typical_owner_profile", type: "string" },
  { name: "typical_operator_profile", type: "string" },
  { name: "target_customer", type: "string" },
  { name: "brand_strengths", type: "string" },
  { name: "brand_differentiators", type: "string" },

  // Growth
  { name: "current_growth_strategy", type: "string", required: true },
  { name: "recent_openings", type: "string" },
  { name: "recent_conversions", type: "string" },
  { name: "pipeline", type: "string" },
  { name: "expansion_priorities", type: "string" },
  { name: "development_announcements", type: "string" },

  // Parent chain
  { name: "parent_relationship_map", type: "string", required: true, description: "Brand→Parent→Collection→Distribution→Loyalty→Regional structure." },
  { name: "distribution_platform", type: "string" },
  { name: "loyalty_program", type: "string" },
  { name: "regional_operating_structure", type: "string" },
  { name: "special_regional_arrangements", type: "string", description: "Required depth for Radisson Individuals Americas / Choice Hotels." },
  { name: "choice_hotels_relationship", type: "string", description: "Fill for Radisson Individuals Americas; N/A or none documented for others." },

  // Recent changes
  { name: "recent_changes", type: "string", required: true, description: "Openings, closures, reflags, conversions, launches, geo expansion, repositioning, owner/operator announcements — current only." },

  // Hotel census
  { name: "hotel_census_entries", type: "string", is_array: true, required: true, description: "One array item per verified hotel: Name | Location | Country | Status | Opening | Conversion/new-build | Operator | Owner | Management | Evidence source. unknown where missing. No invented hotels." },
  { name: "hotel_census_verified_count", type: "string", required: true, description: "Integer count of hotels listed in hotel_census_entries only — not an estimate of global total." },
  { name: "hotel_census_complete", type: "string", required: true, description: "Yes | No — Yes only if evidence supports that all current brand hotels were captured." },
  { name: "census_coverage_notes", type: "string", required: true, description: "State coverage scope (Mexico / CALA / Americas / Global partial). Directories used; regions covered/not covered; why incomplete if No; never invent totals; never imply global-complete unless true." },
  { name: "census_owner_coverage_quality", type: "string", description: "High | Medium | Low — how often owner was publicly identifiable." },
  { name: "census_operator_coverage_quality", type: "string", description: "High | Medium | Low — how often operator/management was publicly identifiable." },

  // Validation / confidence
  { name: "primary_sources", type: "string", is_array: true, required: true },
  { name: "secondary_sources", type: "string", is_array: true },
  { name: "evidence_confidence", type: "string", required: true, description: "High | Medium | Low for the brand package overall." },
  { name: "identity_confidence", type: "string", description: "High | Medium | Low" },
  { name: "growth_confidence", type: "string", description: "High | Medium | Low" },
  { name: "parent_structure_confidence", type: "string", description: "High | Medium | Low" },
  { name: "census_confidence", type: "string", description: "High | Medium | Low" },
  { name: "recent_changes_confidence", type: "string", description: "High | Medium | Low" },
  { name: "last_verified", type: "string", required: true },
  { name: "evidence_gaps", type: "string", required: true },
  { name: "unknowns", type: "string", required: true },
  { name: "independent_assessment_notes", type: "string", description: "Strengths/weaknesses of this independent package — still no Dealality comparison." },
];

const schema = {
  entity_name: "Dealality Independent Brand Explorer Validation Package",
  entity_description:
    "One independently researched brand intelligence + hotel census package per brand (Hotel Indigo, Kimpton, Tribute Portfolio, Avani, Radisson Individuals Americas). Public evidence only. Not an Airtable comparison. Not Dealality SoT.",
  entity_criteria: [
    "Exactly five brands: Hotel Indigo; Kimpton; Tribute Portfolio; Avani; Radisson Individuals Americas (Choice Hotels) — no substitutes",
    "Do not use Airtable, Dealality Brand Explorer, Hotel Census, or Operator Explorer as sources",
    "Hotel census geographic priority: Mexico complete first, then CALA, then Americas, then global only if budget remains — do not exhaust budget enumerating full global inventories for large brands",
    "Do not estimate hotel totals; list only evidenced hotels; never imply global-complete unless true; mark coverage scope in census_coverage_notes",
    "Strong Mexico/CALA depth: resort/destination, small-key, conversion vs new-build, mixed-use, branded residences, owner/operator, pipeline, development appetite — facts only, not Diego brand-fit",
    "For Radisson Individuals Americas, explicitly resolve Choice Hotels relationship and Americas regional structure",
    "Prioritize current openings/conversions/pipeline/reflags over recycled marketing copy",
    "One row per brand; fill identity, growth, parent chain, recent changes, census, evidence, confidence",
  ],
  attributes,
};

const title =
  "Dealality Brand Explorer — Test 6 Independent Public-Evidence Validation (Indigo + Kimpton + Tribute + Avani + Radisson Individuals Americas)";

fs.writeFileSync(path.join(dir, "test6-prompt.txt"), prompt);
fs.writeFileSync(path.join(dir, "test6-schema.json"), JSON.stringify(schema, null, 2));
fs.writeFileSync(
  path.join(dir, "test6-mcp-args.json"),
  JSON.stringify(
    {
      title,
      budget: 10,
      use_free_run_when_available: false,
      prompt,
      schema,
    },
    null,
    2
  )
);
fs.writeFileSync(
  path.join(dir, "test6-payload-preview.json"),
  JSON.stringify(
    {
      title,
      budget: 10,
      use_free_run_when_available: false,
      prompt_chars: prompt.length,
      attribute_count: attributes.length,
      test_type: "brand_explorer_independent_public_evidence_validation",
      brands: [
        "Hotel Indigo",
        "Kimpton",
        "Tribute Portfolio",
        "Avani",
        "Radisson Individuals Americas (Choice Hotels)",
      ],
      airtable_comparison_disabled: true,
      dealality_sources_forbidden: true,
      reconciliation_disabled: true,
      not_launched: true,
      field_names: attributes.map((a) => a.name),
    },
    null,
    2
  )
);

console.log(
  JSON.stringify(
    {
      prompt_chars: prompt.length,
      attribute_count: attributes.length,
      under_12k: prompt.length < 12000,
      budget: 10,
    },
    null,
    2
  )
);
