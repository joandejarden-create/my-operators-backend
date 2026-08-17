/**
 * P1 profile-level governance fields for Brand/Operator Setup root tables.
 * Source URL / File Path and Source Date intentionally excluded — Partner Intelligence SSOT.
 *
 * @see docs/data-intelligence/brand-operator-validation-fields-plan.md
 */

export const P1_GOVERNANCE_SELECT_OPTIONS = {
  validationStatus: [
    "Company Validated",
    "Company Published",
    "Source-Informed",
    "Owner-Provided",
    "AI-Assisted",
    "Needs Review",
    "Stale / Refresh Needed",
    "Do Not Use",
  ],
  usagePermission: [
    "Internal Only",
    "Platform Display Allowed",
    "Scoring Allowed",
    "External Snapshot Allowed",
    "Company Validated",
    "Do Not Use",
  ],
  sourceType: [
    "Company Submission",
    "Company Website",
    "Company PDF / Brochure",
    "Investor Materials",
    "Press Release",
    "Hospitality Media",
    "Third-Party Database",
    "Owner-Provided",
    "Partner Intelligence",
    "AI-Assisted",
    "Unknown",
  ],
  sourceRegion: [
    "CALA-Specific",
    "Regional",
    "Global Reference",
    "Market-Specific",
    "Unknown",
  ],
  confidenceLevel: ["High", "Medium", "Low", "Unknown"],
  externalDisplayStatus: [
    "Show Trust Label",
    "Hide Trust Label",
    "Internal Only",
    "Needs Review",
    "Do Not Display",
  ],
};

/**
 * Canonical field name → live column aliases that satisfy the spec (skip create).
 * Document in setup report when alias used.
 */
export const P1_GOVERNANCE_FIELD_ALIASES = {
  "Confidence Level": {
    aliases: ["Data Confidence Level"],
    skipCreate: true,
    reason:
      "Data Confidence Level exists on Operator Master (OAS/admin semantics). Treat as partial equivalent; map in read paths until unified or options aligned.",
  },
  "Last Reviewed Date": {
    aliases: ["Profile Last Reviewed", "Last Reviewed"],
    skipCreate: true,
    reason:
      "Profile Last Reviewed or Last Reviewed satisfies profile review date — do not duplicate Last Reviewed Date.",
  },
  "Internal Notes": {
    aliases: ["Notes"],
    skipCreate: false,
    reason:
      "Generic Notes is not equivalent to Internal Notes governance — create Internal Notes unless Notes is explicitly repurposed (not recommended).",
  },
};

/** Fields intentionally excluded from P1 Setup roots (Partner Intelligence SSOT). */
export const P1_EXCLUDED_FROM_SETUP_ROOTS = ["Source URL / File Path", "Source Date"];

function selectChoices(key) {
  return { choices: P1_GOVERNANCE_SELECT_OPTIONS[key].map((name) => ({ name })) };
}

function dateField(name, description) {
  return {
    name,
    type: "date",
    description,
    options: { dateFormat: { name: "iso" } },
  };
}

/**
 * Airtable Meta API field definitions for P1 profile governance.
 * @returns {Array<{ name: string, type: string, description?: string, options?: object }>}
 */
export function buildP1ProfileGovernanceFieldDefs() {
  return [
    {
      name: "Validation Status",
      type: "singleSelect",
      description: "P1 profile trust level for Explorer / snapshots.",
      options: selectChoices("validationStatus"),
    },
    {
      name: "Usage Permission",
      type: "singleSelect",
      description: "What this profile row may power (display, scoring, snapshots).",
      options: selectChoices("usagePermission"),
    },
    {
      name: "Source Type",
      type: "singleSelect",
      description: "Primary profile source class (not per-document — see Partner Intelligence).",
      options: selectChoices("sourceType"),
    },
    {
      name: "Source Region",
      type: "singleSelect",
      description: "Geographic scope of profile evidence.",
      options: selectChoices("sourceRegion"),
    },
    dateField("Last Reviewed Date", "Human review timestamp for profile-level governance."),
    dateField("Refresh Due Date", "Planned refresh / staleness review date."),
    {
      name: "Confidence Level",
      type: "singleSelect",
      description: "Profile-level confidence (distinct from OAS Data Confidence Level where both exist).",
      options: selectChoices("confidenceLevel"),
    },
    {
      name: "Evidence Notes",
      type: "multilineText",
      description: "Internal summary of evidence backing profile trust (not full source registry).",
    },
    {
      name: "Missing Data Flags",
      type: "multilineText",
      description: "Known gaps in profile data.",
    },
    {
      name: "Company Validated",
      type: "checkbox",
      description: "Company directly confirmed profile claims. Never auto-set by AI.",
      options: { icon: "check", color: "greenBright" },
    },
    dateField("Company Validation Date", "When company validation was recorded."),
    {
      name: "Reviewed By",
      type: "singleLineText",
      description: "Reviewer name or email (text — no collaborator field in repo pattern).",
    },
    {
      name: "External Display Status",
      type: "singleSelect",
      description: "Whether trust labels appear in owner-facing Explorer UI.",
      options: selectChoices("externalDisplayStatus"),
    },
    {
      name: "Internal Notes",
      type: "multilineText",
      description: "Reviewer-only commentary. Not owner-facing.",
    },
  ];
}

/**
 * @param {string} fieldName
 * @param {Set<string>} existingNames
 * @returns {{ status: 'exact'|'alias'|'missing', liveName: string|null, aliasConfig?: object }}
 */
export function resolveExistingGovernanceField(fieldName, existingNames) {
  if (existingNames.has(fieldName)) {
    return { status: "exact", liveName: fieldName };
  }
  const aliasConfig = P1_GOVERNANCE_FIELD_ALIASES[fieldName];
  if (aliasConfig) {
    for (const alias of aliasConfig.aliases) {
      if (existingNames.has(alias)) {
        return {
          status: "alias",
          liveName: alias,
          aliasConfig,
        };
      }
    }
    if (aliasConfig.skipCreate && aliasConfig.aliases.length) {
      // skipCreate aliases checked above; if none found, fall through to missing
    }
  }
  const norm = (s) => String(s).trim().toLowerCase();
  const caseHit = [...existingNames].find((n) => norm(n) === norm(fieldName));
  if (caseHit) return { status: "exact", liveName: caseHit };
  return { status: "missing", liveName: null };
}

export const P1_BRAND_TABLES = [
  { tableKey: "brandBasics", tableName: "Brand Setup - Brand Basics" },
  { tableKey: "brandExplorerPresentation", tableName: "Brand Setup - Brand Explorer Presentation" },
];

export const P1_OPERATOR_TABLES = [
  {
    tableKey: "operatorMaster",
    tableName: process.env.AIRTABLE_OPERATOR_SETUP_MASTER_TABLE || "Operator Setup - Master",
  },
  { tableKey: "operatorExplorerMaterials", tableName: "Operator Setup - Explorer Materials" },
];
