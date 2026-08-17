/**
 * Operator Leadership & Team — Explorer presentation fields (Governance 1:1 table).
 * Values are JSON arrays stored in long-text columns (see docs/operator-leadership-explorer-airtable-fields.md).
 */

export const OPERATOR_LEADERSHIP_EXPLORER_FIELD_MAP = {
  organizationStructure: "lead_org_structure_json",
  teamDepthByFunction: "lead_team_depth_json",
  languageCapability: "lead_language_capability_json",
  governanceCadence: "lead_governance_cadence_json",
  teamExperienceMarkets: "lead_team_markets_json",
  ownerRelationshipModel: "lead_owner_relationship_json",
  avgHospitalityExperience: "lead_avg_hospitality_experience",
};

export const OPERATOR_LEADERSHIP_EXPLORER_AIRTABLE_FIELDS = [
  {
    formKey: "lead_org_structure_json",
    airtableName: "Leadership Org Structure (JSON)",
    table: "Operator Setup - Governance, Delivery & Diligence",
    type: "multilineText",
  },
  {
    formKey: "lead_team_depth_json",
    airtableName: "Leadership Team Depth (JSON)",
    table: "Operator Setup - Governance, Delivery & Diligence",
    type: "multilineText",
  },
  {
    formKey: "lead_language_capability_json",
    airtableName: "Leadership Languages (JSON)",
    table: "Operator Setup - Governance, Delivery & Diligence",
    type: "multilineText",
  },
  {
    formKey: "lead_governance_cadence_json",
    airtableName: "Leadership Governance Cadence (JSON)",
    table: "Operator Setup - Governance, Delivery & Diligence",
    type: "multilineText",
  },
  {
    formKey: "lead_team_markets_json",
    airtableName: "Leadership Team Markets (JSON)",
    table: "Operator Setup - Governance, Delivery & Diligence",
    type: "multilineText",
  },
  {
    formKey: "lead_owner_relationship_json",
    airtableName: "Leadership Owner Relationship (JSON)",
    table: "Operator Setup - Governance, Delivery & Diligence",
    type: "multilineText",
  },
  {
    formKey: "lead_avg_hospitality_experience",
    airtableName: "Leadership Avg Hospitality Experience",
    table: "Operator Setup - Governance, Delivery & Diligence",
    type: "singleLineText",
  },
];

function nz(v) {
  return v != null && String(v).trim() !== "" ? String(v).trim() : "";
}

/** @param {unknown} raw */
export function parseLeadTeamMarketsJson(raw) {
  if (raw == null || raw === "") return [];
  let parsed = raw;
  if (typeof raw === "string") {
    try {
      parsed = JSON.parse(raw);
    } catch {
      return [];
    }
  }
  if (!Array.isArray(parsed)) return [];
  const seen = new Set();
  const out = [];
  for (const row of parsed) {
    const market = nz(row && (row.market || row.title));
    if (!market) continue;
    const key = market.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(market);
  }
  return out;
}

/**
 * Derive multiselect-style chip labels for Three-Layer Market Experience (team layer).
 * @param {Record<string, unknown>} prefill
 */
export function applyTeamExperienceMarketsFromLeadJson(prefill) {
  if (!prefill || typeof prefill !== "object") return prefill;
  const existing = prefill.teamExperienceMarkets;
  if (Array.isArray(existing) && existing.length) return prefill;
  const regional = nz(prefill.lead_narrative_regional);
  if (regional) {
    const lines = regional
      .split(/\n+/)
      .map((line) => nz(line))
      .filter(Boolean);
    if (lines.length) {
      prefill.teamExperienceMarkets = lines;
      return prefill;
    }
  }
  const markets = parseLeadTeamMarketsJson(prefill.lead_team_markets_json);
  if (markets.length) prefill.teamExperienceMarkets = markets;
  return prefill;
}
