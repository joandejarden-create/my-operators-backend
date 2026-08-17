import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const dir = path.dirname(fileURLToPath(import.meta.url));

const prompt = `DEALALITY WEBHOUND TEST 4 — NON-GOVERNMENT EARLY OWNER SIGNAL DISCOVERY (MEXICO)

Budget: exactly $5. Success = credible OWNER-first opportunities discovered via non-government signals that likely precede environmental approvals and hotel announcements. Prefer 4–8 strong records (quality over geography coverage). Do NOT run another SEMARNAT/government-filing discovery test. Do NOT research Oleum, Venado, Pastizales, Punta Nayu, Punta Colorada, La Capilla, or Tamarindos Tierra Viva from Tests 1–3 as primary targets.

CORE QUESTION
Can Webhound consistently identify hotel owners, developers, and investment groups entering a hospitality decision cycle through NON-GOVERNMENT signals that occur earlier than environmental approvals or hotel announcements — discovering OWNER before PROJECT whenever possible?

Discover owners BEFORE: hotel brand selection; operator selection; public hotel announcement; environmental approval; construction announcement.

THIS IS NOT another SEMARNAT / Gaceta / municipal-permit discovery run.
Government filings (SEMARNAT MIA, Cabildo, permits, etc.) may be used ONLY to VALIDATE or date-check an opportunity AFTER it was found via a non-government signal. They must NOT be the primary discovery engine. If a candidate was first found via government filings, exclude it or reclassify as insufficient for this test's core question unless a clearly earlier non-government signal also exists and is documented as the discovery origin.

GEOGRAPHY (Mexico only; prefer stronger opportunities over more locations)
Prioritize: Baja California Sur; Quintana Roo; Riviera Nayarit; Puerto Vallarta; Mexico City; Guadalajara; Monterrey; Mérida. Other strong hospitality markets only when justified. Do not chase geographic coverage.

PHILOSOPHY
Find OWNER before PROJECT whenever possible. A future project > an already-known project. An owner preparing to invest > an owner who has already committed.

PRIMARY DISCOVERY SIGNALS (search aggressively — these ORIGINATE candidates)
1) Hospitality investment intentions: new hotel investment platform/fund; acquisition mandate; hospitality allocation; family office entering hospitality; institutional investor entering hotels; RE company hotel expansion; hospitality strategy presentations.
2) Land activity: strategic/coastal land acquisition; resort land banking; hospitality-zoned or mixed-use sites; tourism land assemblage; master-planned / destination development land.
3) Corporate activity: new hospitality subsidiary/SPV/JV; hospitality business unit; partnership/alliance; capital partner announcement; strategic cooperation.
4) Executive hiring: VP Development; Hospitality Development; Hotel Acquisitions; Hospitality Investments; Mixed-Use/Resort Development; Asset Management; Hotel Strategy; Branded Residences; Hotel Real Estate; Hospitality Expansion — a hire may signal pipeline before any project is public.
5) Investor communications: annual reports; investor presentations; earnings calls; capital raises; fundraising decks; LP updates; pipeline/expansion/hospitality strategy discussions.
6) Development ecosystem: architects; master planners; urban designers; hospitality consultants; engineers; destination planners; environmental consultants; resort designers — often reveal projects before hotel announcements.
7) Capital signals: development/construction financing prep; debt mandates; equity raises; hospitality lending; JV/preferred equity; opportunity/RE funds.
8) Hospitality partnerships: destination/tourism development agreements; infrastructure partnerships; mixed-use partnerships; resort alliances; residential developers entering hospitality.

EXPLICITLY DO NOT USE AS PRIMARY DISCOVERY ORIGIN
Hotel openings; brand/operator/franchise/HMA announcements; hotel rankings; existing hotel lists; generic owner databases; hospitality directories. These may VALIDATE but must not ORIGINATE a row. Do not burn budget on Brand Explorer / Operator Explorer validation or broad portfolio enrichment.

DISCOVERY CLASSES
Class 1 — Owner with a credible future hotel decision (e.g. land acquired; hospitality strategy announced; hospitality division created; mixed-use beginning; hospitality capital raised; hospitality JV; development team assembled).
Class 2 — Owner signal only (hospitality hiring; fundraising; acquisition mandate; expansion strategy; hospitality investment platform) without a clear near-term project decision yet.
Class 3 — Already committed hotel project — include ONLY if it reveals another earlier opportunity at the same owner; otherwise exclude.

PRIMARY ROW GRAIN
ONE ROW = ONE OWNER OPPORTUNITY (owner × distinct early signal / potential Dealality decision). Prefer owner-level opportunities over project-listing rows. If a specific site is known, include it, but the row centers on the OWNER SIGNAL.

MANDATORY FOR EVERY ROW
- discovery_origin_signal_type: which non-government signal family originated this candidate (Investment Intent | Land Activity | Corporate Activity | Executive Hiring | Investor Communications | Development Ecosystem | Capital Signal | Hospitality Partnership | Other Non-Government).
- discovered_before_government_filing: Yes | No | Unknown — evidence the owner/signal was public before material planning/environmental/permitting filings, OR no such filing found yet.
- discovered_before_public_hotel_announcement: Yes | No | Unknown.
- government_used_only_for_validation: Yes | No | N/A — if Yes, briefly note what was checked.
- Confirmed Facts vs Research Inferences.
- why_dealality_may_still_be_early AND evidence_may_already_be_too_late.
Do NOT infer openness from missing announcements. Use: "No public evidence of a completed brand/operator decision was identified in the reviewed sources as of [date]."

OWNERSHIP (resolve where possible; never assume)
Developer; property owner; parent; ultimate controlling owner; JV/investment partners; hospitality platform; family office; fund. "Not publicly disclosed" when needed. Never invent %.

CONTACT (secondary only)
Identify most relevant hospitality development / investment / mixed-use / strategy / acquisitions leader. Professional profile + company route acceptable. Do NOT spend excessive budget on direct email/phone discovery.

CLASSIFICATION ENUMS
Discovery Class: Class 1 — Future Hotel Decision Owner | Class 2 — Owner Signal Only | Class 3 — Post-Commitment (only if reveals earlier opportunity)
Dealality Status: Strong Pre-Decision Owner | Conditional Pre-Decision Owner | Owner Relationship Opportunity | Track / Monitor | Likely Too Late | Too Late | Insufficient Evidence
Earliness Confidence: High | Medium | Low | Unknown
Signal Strength: High | Medium | Low

BUDGET ORDER (strict)
1) Non-government early owner-signal discovery 2) Document discovery origin signal + date 3) Too-late / commitment check (without using brand announcements as discovery) 4) Light ownership resolution 5) Relevant decision-maker + professional route 6) Optional government validation only if it strengthens dating/earliness without becoming the discovery engine
Do NOT: SEMARNAT-first scanning; municipal permit crawling as discovery; broad Mexico hotel lists; Brand/Operator Explorer work; contact email hunting; full owner portfolios; Test 5 or any follow-on run.

Fill the provided schema. Unknown/Not publicly disclosed/No/empty as needed. Preserve provenance. Candidate intelligence only — do not invent facts.
`;

const attributes = [
  { name: "opportunity_id", type: "string", is_primary: true, required: true, description: "Stable ID e.g. MX-OWN-2026-001." },
  { name: "discovery_class", type: "string", required: true, description: "Class 1 — Future Hotel Decision Owner | Class 2 — Owner Signal Only | Class 3 — Post-Commitment (only if reveals earlier opportunity)" },
  { name: "opportunity_name", type: "string", required: true },
  { name: "owner_name", type: "string", required: true, description: "Owner / developer / investment group at center of the signal." },
  { name: "owner_type", type: "string", description: "Family office | PE/Fund | Developer | Hotel company | RE company | Institutional | JV platform | Other | Unknown" },
  { name: "geography_market", type: "string", description: "Primary Mexico market focus (e.g. BCS, QROO, Riviera Nayarit, CDMX)." },
  { name: "city_or_region", type: "string" },
  { name: "state", type: "string" },
  { name: "discovery_origin_signal_type", type: "string", required: true, description: "Investment Intent | Land Activity | Corporate Activity | Executive Hiring | Investor Communications | Development Ecosystem | Capital Signal | Hospitality Partnership | Other Non-Government" },
  { name: "early_signal", type: "string", required: true, description: "Concrete non-government signal that originated discovery." },
  { name: "signal_date", type: "string" },
  { name: "why_signal_matters", type: "string", required: true },
  { name: "potential_hotel_decision", type: "string", required: true, description: "What brand/operator/strategy/capital/development decision may still be open." },
  { name: "project_or_site_if_known", type: "string", description: "Optional; leave empty if owner-only signal." },
  { name: "discovered_before_government_filing", type: "string", required: true, description: "Yes | No | Unknown" },
  { name: "discovered_before_public_hotel_announcement", type: "string", required: true, description: "Yes | No | Unknown" },
  { name: "government_used_only_for_validation", type: "string", required: true, description: "Yes | No | N/A" },
  { name: "government_validation_notes", type: "string", description: "If government used: what checked and why it did not originate discovery." },
  { name: "dealality_status", type: "string", required: true },
  { name: "earliness_confidence", type: "string", required: true },
  { name: "signal_strength", type: "string", description: "High | Medium | Low" },
  { name: "why_dealality_may_still_be_early", type: "string", required: true },
  { name: "evidence_may_already_be_too_late", type: "string", required: true },
  { name: "confirmed_facts", type: "string" },
  { name: "research_inferences", type: "string" },
  { name: "developer", type: "string" },
  { name: "property_owner", type: "string" },
  { name: "parent_company", type: "string" },
  { name: "ultimate_controlling_owner", type: "string" },
  { name: "jv_or_investment_partners", type: "string" },
  { name: "hospitality_platform_or_fund", type: "string" },
  { name: "family_office_or_institution", type: "string" },
  { name: "ownership_structure_summary", type: "string" },
  { name: "ownership_confidence", type: "string", description: "High | Medium | Low | Unknown" },
  { name: "unresolved_ownership_questions", type: "string" },
  { name: "primary_contact_name", type: "string" },
  { name: "primary_contact_title", type: "string" },
  { name: "primary_contact_company", type: "string" },
  { name: "primary_contact_why_relevant", type: "string" },
  { name: "primary_contact_linkedin_or_profile", type: "string" },
  { name: "primary_contact_company_route", type: "string" },
  { name: "primary_contact_confidence", type: "string" },
  { name: "related_hospitality_pipeline_brief", type: "string", description: "Tier 2 — brief only; not full portfolio." },
  { name: "signal_sources", type: "string", is_array: true },
  { name: "ownership_sources", type: "string", is_array: true },
  { name: "contact_sources", type: "string", is_array: true },
  { name: "additional_sources", type: "string", is_array: true },
  { name: "latest_evidence_date", type: "string" },
  { name: "last_verified", type: "string" },
  { name: "evidence_confidence", type: "string" },
  { name: "evidence_gaps", type: "string" },
  { name: "comparative_signal_notes", type: "string", description: "Optional note for Tests 1–4 comparison: why this signal type is early/reliable/actionable vs government filings." },
];

const schema = {
  entity_name: "Dealality Mexico Early Owner Signal",
  entity_description:
    "One owner-centered early hospitality opportunity in Mexico discovered via NON-GOVERNMENT signals (investment, land, corporate, hiring, capital, partnerships, investor communications, development ecosystem). Government filings may validate only — not originate. Not a SEMARNAT discovery run; not Tests 1–3 project enrichment.",
  entity_criteria: [
    "Discovery MUST originate from a non-government signal family; government filings only for validation/dating",
    "Prefer OWNER before PROJECT; future decision > known project",
    "Mexico only; prioritize BCS, QROO, Riviera Nayarit, PV, CDMX, GDL, MTY, Mérida — quality over geographic coverage",
    "Do not originate from openings, brand/operator/franchise/HMA announcements, rankings, or generic hotel owner lists",
    "Do not invent emails, phones, ownership percentages, or brand/operator commitments",
    "Exclude or deprioritize repeating Tests 1–3 named projects as primary targets",
    "Prefer 4–8 strong owner opportunities over many weak rows",
  ],
  attributes,
};

const title =
  "Dealality Mexico Owner Intelligence — Test 4 Non-Government Early Owner Signal Discovery";

fs.writeFileSync(path.join(dir, "test4-prompt.txt"), prompt);
fs.writeFileSync(path.join(dir, "test4-schema.json"), JSON.stringify(schema, null, 2));
fs.writeFileSync(
  path.join(dir, "test4-mcp-args.json"),
  JSON.stringify(
    {
      title,
      budget: 5,
      use_free_run_when_available: false,
      prompt,
      schema,
    },
    null,
    2
  )
);
fs.writeFileSync(
  path.join(dir, "test4-payload-preview.json"),
  JSON.stringify(
    {
      title,
      budget: 5,
      use_free_run_when_available: false,
      prompt_chars: prompt.length,
      attribute_count: attributes.length,
      geography: "Mexico — prioritize BCS/QROO/Nayarit/PV/CDMX/GDL/MTY/Mérida; quality over coverage",
      government_filings_role: "validation_only_not_primary_discovery",
      semarnat_first_disabled: true,
      tests_1_3_project_enrichment_disabled: true,
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
    },
    null,
    2
  )
);
