import fs from "node:fs";

const prompt = fs.readFileSync(
  new URL("./test2-prompt.txt", import.meta.url),
  "utf8"
).replace(/^\uFEFF/, "");

const schema = {
  entity_name: "Dealality Mexico Owner Package",
  entity_description:
    "One primary record per priority project (Oleum, Venado, optionally Pastizales). Outreach-ready Owner Intelligence package — not broad discovery.",
  entity_criteria: [
    "Only Oleum Joint Ventures Riviera Maya Ecotourism and/or Residencial Punta Venado Proyecto Venado, unless both are strongly complete and budget remains for Pastizales",
    "Must attempt primary government/public filing access before relying on secondary press",
    "Must attempt ownership resolution and decision-maker contact paths",
    "Do not invent emails, phones, ownership percentages, or brand/operator decisions",
  ],
  attributes: [
    // Project
    { name: "project_id", type: "string", is_primary: true, required: true, description: "Stable ID: OLEUM-RIVM-ECOT-001 | RPV-QROO-VENADO-001 | HMP-QROO-PAST-001 or similar." },
    { name: "project_name", type: "string", required: true },
    { name: "location_city", type: "string" },
    { name: "location_state", type: "string" },
    { name: "location_detail", type: "string", description: "Corridor/site detail (e.g. Punta Venado, Punta Piedra)." },
    { name: "project_description", type: "string" },
    { name: "keys_or_units", type: "string" },
    { name: "development_stage", type: "string" },
    { name: "filing_date", type: "string" },
    { name: "filing_status", type: "string" },
    { name: "primary_government_source", type: "string", description: "URL or citation for primary filing if accessed." },
    { name: "primary_source_accessed", type: "string", description: "Yes | No" },
    { name: "primary_source_not_accessed_notes", type: "string", description: "If No: authority that should contain it; what was attempted; secondary evidence relied on." },
    { name: "secondary_sources", type: "string", is_array: true },

    // Entity / Ownership
    { name: "filing_applicant", type: "string" },
    { name: "legal_property_owner_or_spv", type: "string" },
    { name: "developer", type: "string" },
    { name: "parent_company", type: "string" },
    { name: "ultimate_controlling_owner", type: "string" },
    { name: "jv_partners", type: "string" },
    { name: "equity_partners", type: "string" },
    { name: "ownership_percentages", type: "string", description: "Only if explicitly disclosed; else Not publicly disclosed. Never infer." },
    { name: "ownership_structure_summary", type: "string" },
    { name: "entity_role_notes", type: "string", description: "Classify entities: Property Owner / Project SPV / Applicant-Promovente / Developer / etc. Promovente ≠ ownership." },
    { name: "ownership_confidence", type: "string", description: "High | Medium | Low | Unknown" },
    { name: "unresolved_ownership_questions", type: "string" },

    // Pre-Decision Assessment
    { name: "dealality_stage", type: "string" },
    { name: "potential_decision", type: "string" },
    { name: "evidence_of_early_status", type: "string" },
    { name: "too_late_evidence", type: "string" },
    { name: "dealality_status", type: "string", required: true, description: "Strong Pre-Decision Candidate | Conditional Pre-Decision Candidate | Owner Relationship Opportunity | Likely Too Late | Too Late | Insufficient Evidence" },
    { name: "why_dealality_may_still_be_early", type: "string" },
    { name: "why_dealality_may_already_be_too_late", type: "string" },
    { name: "earliness_confidence", type: "string", description: "High | Medium | Low | Unknown" },

    // Corridor
    { name: "corridor_relationship_to_other_priority_project", type: "string", description: "Oleum↔Venado relationship if any: sites/ownership/developers/etc. Do not assume from adjacency. If none, say so." },
    { name: "corridor_relationship_more_valuable_than_single_project", type: "string", description: "Yes/No/Unknown + explanation." },

    // Primary Contact
    { name: "primary_contact_name", type: "string" },
    { name: "primary_contact_title", type: "string" },
    { name: "primary_contact_company", type: "string" },
    { name: "primary_contact_role", type: "string" },
    { name: "primary_contact_why_relevant", type: "string" },
    { name: "primary_contact_relationship_to_project", type: "string" },
    { name: "primary_contact_linkedin_or_profile", type: "string", standard_format: "url" },
    { name: "primary_contact_verified_business_email", type: "string", description: "Verified public only; never guess." },
    { name: "primary_contact_verified_business_phone", type: "string", description: "Verified public only; never guess." },
    { name: "primary_contact_company_contact_route", type: "string" },
    { name: "primary_contact_outreach_priority", type: "string", description: "High | Medium | Low" },
    { name: "primary_contact_confidence", type: "string", description: "High | Medium | Low | Unknown" },
    { name: "primary_contact_source", type: "string" },

    // Secondary contacts
    { name: "secondary_contacts", type: "string", is_array: true, description: "Max 3: Name | Title | Company | Role | Profile | Verified Email | Verified Phone | Why Relevant" },

    // Owner relationship potential
    { name: "other_hospitality_assets_or_projects", type: "string" },
    { name: "other_pre_decision_opportunities", type: "string" },
    { name: "mexico_or_cala_pipeline", type: "string" },
    { name: "why_owner_may_matter_beyond_this_project", type: "string" },

    // Evidence
    { name: "primary_filing_source", type: "string" },
    { name: "ownership_sources", type: "string", is_array: true },
    { name: "contact_sources", type: "string", is_array: true },
    { name: "other_sources", type: "string", is_array: true },
    { name: "last_verified", type: "string" },
    { name: "evidence_confidence", type: "string", description: "High | Medium | Low | Unknown" },
    { name: "important_evidence_gaps", type: "string" },
  ],
};

const payload = {
  prompt,
  schema,
  budget: 5,
  title:
    "Dealality Mexico Owner Intelligence — Test 2 Outreach-Ready Packages (Oleum + Venado)",
  use_free_run_when_available: false,
};

fs.writeFileSync(
  new URL("./test2-schema.json", import.meta.url),
  JSON.stringify(schema, null, 2)
);
fs.writeFileSync(
  new URL("./test2-payload-preview.json", import.meta.url),
  JSON.stringify({
    ...payload,
    prompt_len: prompt.length,
    attr_count: schema.attributes.length,
    note: "use_free_run_when_available=false — free pass already consumed in Test 1; requires paid credits or new funding before launch.",
  }, null, 2)
);

console.log(
  JSON.stringify({
    prompt_len: prompt.length,
    attrs: schema.attributes.length,
    payload_bytes: Buffer.byteLength(JSON.stringify(payload)),
  })
);
