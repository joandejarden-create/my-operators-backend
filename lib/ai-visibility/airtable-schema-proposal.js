/**
 * Field map / schema proposal for AI Visibility Airtable tables.
 * Phase 2D — apply only via ensure script with AI_VISIBILITY_SCHEMA_APPLY=true.
 */

export const AI_VISIBILITY_PROMPTS_TABLE = "AI Visibility - Prompts";
export const AI_VISIBILITY_OPPORTUNITIES_TABLE = "AI Visibility - Opportunities";

export const AI_VISIBILITY_BRAND_BASICS_TABLE =
  process.env.AIRTABLE_BRAND_SETUP_BASICS_TABLE || "Brand Setup - Brand Basics";
export const AI_VISIBILITY_OPERATOR_MASTER_TABLE =
  process.env.AIRTABLE_OPERATOR_SETUP_MASTER_TABLE || "Operator Setup - Master";

const INTENT_TERRITORIES = [
  "Brand Selection",
  "Operator Selection",
  "Conversion",
  "New Build",
  "HMA vs Franchise",
  "Owner Economics",
  "Owner Flexibility",
  "Branded Residences",
  "Mixed Use",
  "Market / Geography",
  "Chain Scale / Positioning",
  "Development Strategy",
  "Other",
  // Phase 3A.9 showcase territories
  "Collection / Soft Brand",
  "Lifestyle Positioning",
  "Upper-Upscale Positioning",
  "Soft-Brand Affiliation Flexibility",
];

const GEOGRAPHY_SCOPE_CHOICES = ["Global", "Region", "Subregion", "Country", "Market"];
const COMMERCIAL_REGION_CHOICES = [
  "CALA",
  "Europe",
  "North America",
  "APAC",
  "Middle East & Africa",
];

/** @type {Array<{ name: string, type: string, description?: string, options?: object, primary?: boolean }>} */
export const AI_VISIBILITY_PROMPT_FIELD_SPECS = [
  {
    name: "Prompt Name",
    type: "singleLineText",
    description: "Primary field — short governed label for the owner-intent prompt.",
    primary: true,
  },
  {
    name: "Prompt ID",
    type: "singleLineText",
    description: "Stable external id (e.g. p_mx_uu_conversion_brand_v1).",
  },
  {
    name: "Prompt Family",
    type: "singleLineText",
    description: "Shared family key across geographies (e.g. upper_upscale_conversion_brand_selection).",
  },
  {
    name: "Prompt Text",
    type: "multilineText",
    description: "Full prompt text sent to providers.",
  },
  {
    name: "Version",
    type: "singleLineText",
    description: "Prompt version string (e.g. 1). Material wording changes require a new Version row.",
  },
  {
    name: "Intent Territory",
    type: "singleSelect",
    description: "Taxonomy territory.",
    options: { choices: INTENT_TERRITORIES.map((name) => ({ name })) },
  },
  {
    name: "Stakeholder Relevance",
    type: "multipleSelects",
    options: {
      choices: ["Brand", "Operator", "Owner", "Admin"].map((name) => ({ name })),
    },
  },
  {
    name: "Entity Scope",
    type: "singleSelect",
    options: {
      choices: ["Brand", "Operator", "Both"].map((name) => ({ name })),
    },
  },
  {
    name: "Geography Scope",
    type: "singleSelect",
    description: "Explicit analytical scope — never infer Global from regional prompts.",
    options: { choices: GEOGRAPHY_SCOPE_CHOICES.map((name) => ({ name })) },
  },
  {
    name: "Commercial Region",
    type: "singleSelect",
    description: "AI Visibility commercial region (distinct from Radar subregion).",
    options: { choices: COMMERCIAL_REGION_CHOICES.map((name) => ({ name })) },
  },
  {
    name: "Subregion",
    type: "singleLineText",
    description: "e.g. Caribbean (radar/Dealality subregion when applicable).",
  },
  {
    name: "Country",
    type: "singleLineText",
    description: "Canonical Dealality country display name when geographyScope=Country.",
  },
  {
    name: "Country Code",
    type: "singleLineText",
    description: "Optional ISO-3166 alpha-2 when known.",
  },
  {
    name: "Market",
    type: "singleLineText",
    description: "Dealality commercial Market when market-scoped prompts exist.",
  },
  {
    name: "Geography Model Version",
    type: "singleLineText",
    description: "e.g. ai_visibility_geography_v1",
  },
  {
    name: "Peer Set ID",
    type: "singleLineText",
    description: "Optional reference to governed peer-set config id.",
  },
  // Phase 3A.6 proposes Language + Semantic Pair ID on AI Visibility - Prompts.
  // Phase 3A.9 applies these additive fields via ensure script (dry-run first).
  {
    name: "Language",
    type: "singleSelect",
    description:
      "Monitoring language for this prompt (Airtable: English|Spanish → runtime en|es). First-class; never encode geography as locale.",
    options: {
      choices: ["English", "Spanish"].map((name) => ({ name })),
    },
  },
  {
    name: "Semantic Pair ID",
    type: "singleLineText",
    description:
      "Stable id linking bilingual equivalent prompts (same owner decision; natural EN/ES wording). Required for bilingual pairs.",
  },
  { name: "Chain Scale", type: "singleLineText" },
  { name: "Asset Type", type: "singleLineText" },
  { name: "Hotel Type", type: "singleLineText" },
  {
    name: "Development Type",
    type: "singleSelect",
    options: {
      choices: ["New Build", "Conversion", "Either", "Unspecified"].map((name) => ({ name })),
    },
  },
  {
    name: "Branded Residences Relevance",
    type: "checkbox",
    options: { icon: "check", color: "greenBright" },
  },
  {
    name: "Decision Stage",
    type: "singleSelect",
    options: {
      choices: ["Early Evaluation", "Shortlist", "Contracting", "Unspecified"].map((name) => ({
        name,
      })),
    },
  },
  {
    name: "Active",
    type: "checkbox",
    options: { icon: "check", color: "greenBright" },
  },
  {
    name: "Monitoring Eligible",
    type: "checkbox",
    options: { icon: "check", color: "greenBright" },
  },
  {
    name: "Cadence",
    type: "singleSelect",
    options: {
      choices: ["Weekly", "Monthly", "Ad-hoc", "Paused"].map((name) => ({ name })),
    },
  },
  {
    name: "Governance Status",
    type: "singleSelect",
    options: {
      choices: ["Draft", "Approved", "Paused", "Retired"].map((name) => ({ name })),
    },
  },
  {
    name: "Review Status",
    type: "singleSelect",
    options: {
      choices: ["Not Reviewed", "Needs Review", "Reviewed", "Deferred"].map((name) => ({ name })),
    },
  },
  {
    name: "Review Notes",
    type: "multilineText",
    description: "Human review notes including neutrality / bias checks.",
  },
  {
    name: "Source / Rationale",
    type: "multilineText",
    description: "Why this prompt is in the governed library.",
  },
  {
    name: "Prompt Origin",
    type: "singleSelect",
    description:
      "Where the question came from: OBSERVED demand, DERIVED from observed, SCENARIO (expert), or LEGACY_UNCLASSIFIED. Optional until provenance review. Do not apply live without AI_VISIBILITY_SCHEMA_APPLY.",
    options: {
      choices: ["OBSERVED", "DERIVED", "SCENARIO", "LEGACY_UNCLASSIFIED"].map((name) => ({
        name,
      })),
    },
  },
  {
    name: "Origin Source Type",
    type: "singleSelect",
    description: "Observed-demand source class. Not an AI-response citation source.",
    options: {
      choices: [
        "SEARCH_DEMAND_DATASET",
        "PAA",
        "RELATED_QUESTION",
        "SEARCH_QUERY_DATASET",
        "PUBLIC_QUESTION_SOURCE",
        "SEARCH_CONSOLE",
        "DEALALITY_USER_BEHAVIOR",
        "LICENSED_SEO_DATASET",
        "OTHER_OBSERVED_SOURCE",
        "EXPERT_SCENARIO",
      ].map((name) => ({ name })),
    },
  },
  { name: "Origin Source Name", type: "singleLineText" },
  {
    name: "Origin Source Reference",
    type: "singleLineText",
    description: "Short reference id or URL. Large evidence lives in the file store, not Airtable.",
  },
  { name: "Observed Query", type: "singleLineText" },
  { name: "Observed Theme", type: "singleLineText" },
  {
    name: "Demand Tier",
    type: "singleSelect",
    description: "Qualitative only. HIGH/MEDIUM/LOW require Demand Methodology. Never invent volume.",
    options: {
      choices: ["HIGH", "MEDIUM", "LOW", "UNKNOWN"].map((name) => ({ name })),
    },
  },
  { name: "Demand Signal Type", type: "singleLineText" },
  { name: "Demand Geography", type: "singleLineText" },
  {
    name: "Date Observed",
    type: "date",
    options: { dateFormat: { name: "iso" } },
  },
  { name: "Demand Evidence Count", type: "number", options: { precision: 0 } },
  {
    name: "Demand Methodology",
    type: "multilineText",
    description: "Required before assigning HIGH/MEDIUM/LOW demand tier.",
  },
  { name: "Derived From Observed Prompt ID", type: "singleLineText" },
  { name: "Derived From Demand Signal ID", type: "singleLineText" },
  {
    name: "Owner Intent Subtheme",
    type: "singleLineText",
    description: "Optional finer owner-intent label. Intent Territory remains the family.",
  },
  {
    name: "Provenance Status",
    type: "singleSelect",
    options: {
      choices: ["VALIDATED", "CANDIDATE", "NEEDS_EVIDENCE", "LEGACY"].map((name) => ({ name })),
    },
  },
  { name: "Provenance Notes", type: "multilineText" },
  {
    name: "Created By Method",
    type: "singleSelect",
    options: {
      choices: [
        "OBSERVED_DEMAND",
        "DERIVED_FROM_OBSERVED",
        "EXPERT_SCENARIO",
        "LEGACY_UNCLASSIFIED",
      ].map((name) => ({ name })),
    },
  },
  {
    name: "Last Provenance Review At",
    type: "dateTime",
    options: {
      dateFormat: { name: "iso" },
      timeFormat: { name: "24hour" },
      timeZone: "utc",
    },
  },
  {
    name: "Sampling Priority",
    type: "singleSelect",
    description: "Future repeated-testing hook. Scheduler remains off.",
    options: {
      choices: ["CRITICAL", "HIGH", "STANDARD", "EXPLORATORY"].map((name) => ({ name })),
    },
  },
  {
    name: "Last Monitored At",
    type: "dateTime",
    options: {
      dateFormat: { name: "iso" },
      timeFormat: { name: "24hour" },
      timeZone: "utc",
    },
  },
  {
    name: "Linked Brands",
    type: "multipleRecordLinks",
    description: "Optional links to Brand Setup - Brand Basics (wired at apply time).",
    linkTableEnv: "brandBasics",
  },
  {
    name: "Linked Operators",
    type: "multipleRecordLinks",
    description: "Optional links to Operator Setup - Master (wired at apply time).",
    linkTableEnv: "operatorMaster",
  },
];

/** @type {Array<{ name: string, type: string, description?: string, options?: object, primary?: boolean }>} */
export const AI_VISIBILITY_OPPORTUNITY_FIELD_SPECS = [
  {
    name: "Opportunity Name",
    type: "singleLineText",
    description: "Primary field — short label (e.g. Upper-upscale conversions — Mexico).",
    primary: true,
  },
  { name: "Opportunity ID", type: "singleLineText" },
  {
    name: "Entity Type",
    type: "singleSelect",
    options: {
      choices: ["Brand", "Operator"].map((name) => ({ name })),
    },
  },
  {
    name: "Linked Brand",
    type: "multipleRecordLinks",
    description: "Link to Brand Setup - Brand Basics when Entity Type = Brand.",
    linkTableEnv: "brandBasics",
  },
  {
    name: "Linked Operator",
    type: "multipleRecordLinks",
    description: "Link to Operator Setup - Master when Entity Type = Operator.",
    linkTableEnv: "operatorMaster",
  },
  {
    name: "Intent Territory",
    type: "singleSelect",
    options: { choices: INTENT_TERRITORIES.map((name) => ({ name })) },
  },
  {
    name: "Geography Scope",
    type: "singleSelect",
    options: { choices: GEOGRAPHY_SCOPE_CHOICES.map((name) => ({ name })) },
  },
  {
    name: "Commercial Region",
    type: "singleSelect",
    options: { choices: COMMERCIAL_REGION_CHOICES.map((name) => ({ name })) },
  },
  { name: "Subregion", type: "singleLineText" },
  { name: "Country", type: "singleLineText" },
  { name: "Market", type: "singleLineText" },
  {
    name: "Peer Set ID",
    type: "singleLineText",
    description: "Governed peer-set config id used for Competitive Position context.",
  },
  {
    name: "Observation Window",
    type: "singleLineText",
    description: "e.g. 2026-Q3 or period key.",
  },
  {
    name: "Observation",
    type: "multilineText",
    description: "Deterministic observation summary (evidence-backed; not advisory prose).",
  },
  { name: "Competitor Leader", type: "singleLineText" },
  { name: "Current Presence", type: "singleLineText" },
  { name: "Competitor Presence", type: "singleLineText" },
  {
    name: "Evidence Descriptor",
    type: "singleSelect",
    options: {
      choices: [
        "Repeated across engines",
        "Repeated across runs",
        "Emerging pattern",
        "Single-engine observation",
      ].map((name) => ({ name })),
    },
  },
  {
    name: "Evidence Store Refs",
    type: "multilineText",
    description: "Non-Airtable evidence IDs / run IDs (JSON or newline list).",
  },
  {
    name: "Diagnostic Reason",
    type: "singleSelect",
    options: {
      choices: [
        "Persistent Absence",
        "Competitor Dominance",
        "Source Gap",
        "Representation Gap",
        "Visibility Loss",
        "Visibility Gain",
        "Other",
      ].map((name) => ({ name })),
    },
  },
  {
    name: "Recommended Action",
    type: "multilineText",
    description: "Human or later AI-assisted next action; not auto-executed.",
  },
  {
    name: "Interpretation Status",
    type: "singleSelect",
    options: {
      choices: ["Evidence Only", "Needs Review", "Human Confirmed", "AI-Assisted Draft"].map(
        (name) => ({ name })
      ),
    },
  },
  {
    name: "Status",
    type: "singleSelect",
    options: {
      choices: [
        "New",
        "Reviewing",
        "Action Planned",
        "Monitoring",
        "Improved",
        "Closed",
      ].map((name) => ({ name })),
    },
  },
  {
    name: "Human Review Status",
    type: "singleSelect",
    options: {
      choices: ["Not Required", "Needs Review", "Reviewed", "Deferred"].map((name) => ({
        name,
      })),
    },
  },
  {
    name: "Rule Version",
    type: "singleLineText",
    description: "e.g. ai_visibility_opportunity_rules_v1",
  },
  {
    name: "Metric Version",
    type: "singleLineText",
    description: "e.g. ai_visibility_metrics_v1",
  },
  { name: "Evidence Count", type: "number", options: { precision: 0 } },
  { name: "Engines Observed", type: "singleLineText" },
  {
    name: "First Detected",
    type: "dateTime",
    options: {
      dateFormat: { name: "iso" },
      timeFormat: { name: "24hour" },
      timeZone: "utc",
    },
  },
  {
    name: "Last Detected",
    type: "dateTime",
    options: {
      dateFormat: { name: "iso" },
      timeFormat: { name: "24hour" },
      timeZone: "utc",
    },
  },
  {
    name: "Resolved At",
    type: "dateTime",
    options: {
      dateFormat: { name: "iso" },
      timeFormat: { name: "24hour" },
      timeZone: "utc",
    },
  },
];

export function getPromptCoreFieldSpecs() {
  return AI_VISIBILITY_PROMPT_FIELD_SPECS.filter((f) => f.type !== "multipleRecordLinks");
}

export function getPromptLinkFieldSpecs() {
  return AI_VISIBILITY_PROMPT_FIELD_SPECS.filter((f) => f.type === "multipleRecordLinks");
}

export function getOpportunityCoreFieldSpecs() {
  return AI_VISIBILITY_OPPORTUNITY_FIELD_SPECS.filter((f) => f.type !== "multipleRecordLinks");
}

export function getOpportunityLinkFieldSpecs() {
  return AI_VISIBILITY_OPPORTUNITY_FIELD_SPECS.filter((f) => f.type === "multipleRecordLinks");
}

export function classifyFieldEnsureAction(existing, spec) {
  if (!existing) return { action: "create" };
  if (existing.type !== spec.type) {
    return {
      action: "conflict",
      reason: `type_mismatch existing=${existing.type} desired=${spec.type}`,
    };
  }

  // Additive singleSelect / multipleSelects choices (never delete existing choices)
  if (
    (spec.type === "singleSelect" || spec.type === "multipleSelects") &&
    Array.isArray(spec.options?.choices) &&
    spec.options.choices.length
  ) {
    const existingNames = new Set(
      (existing.options?.choices || []).map((c) => String(c.name || c).trim()).filter(Boolean)
    );
    const desiredNames = spec.options.choices
      .map((c) => String(c.name || c).trim())
      .filter(Boolean);
    const missing = desiredNames.filter((n) => !existingNames.has(n));
    if (missing.length) {
      const allChoices = [...existingNames, ...missing];
      return { action: "add_choices", missingChoices: missing, allChoices };
    }
  }

  return { action: "skip", reason: "exists_same_type" };
}
