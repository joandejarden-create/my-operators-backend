/**
 * Packet 2.8A — shared Webhound / provider research preamble.
 */

export const COMMON_PREAMBLE_VERSION = "hi-common-preamble-v1";

export const SOURCE_HIERARCHY_DEFAULT = Object.freeze([
  "Government / legal / registry / securities filings",
  "Current hotel first-party",
  "Owner / sponsor first-party",
  "Operator first-party",
  "Brand first-party",
  "Lender / capital-provider first-party",
  "Credible transaction / trade / local press",
  "Professional profiles",
  "Guest / reputation evidence (signal only — not structural proof)",
  "Secondary / discovery sources",
]);

export function buildCommonResearchPreamble() {
  return [
    "## Shared Dealality research contract",
    "- Research the EXACT hotel/entity specified. Resolve aliases/former names before conclusions.",
    "- Distinguish CURRENT, HISTORICAL, ANNOUNCED, and SUPERSEDED facts.",
    "- Distinguish owner, PropCo, parent, sponsor, operator, brand, developer, lender, and asset manager.",
    "- Do not infer ownership from operation.",
    "- Do not infer operation from brand.",
    "- Do not infer beneficial ownership from executive title.",
    "- Do not treat adjacent/similarly named hotels as the subject property.",
    "- Do not treat an announced conversion as completed without current evidence.",
    "- Prefer strongest available current sources (see source hierarchy).",
    "- Preserve conflicting evidence; do not silently pick a winner.",
    "- Explicitly identify unresolved questions.",
    "- Provide URLs for every substantive source.",
    "- Capture person-level professional-profile URLs when discovered; else mark NOT_FOUND.",
    "- Return structured findings suitable for Dealality normalization.",
    "- Do not invent Airtable/Census/internal product mechanics in findings.",
    "",
    "## Source hierarchy (prefer earlier when applicable)",
    ...SOURCE_HIERARCHY_DEFAULT.map((s, i) => `${i + 1}. ${s}`),
  ].join("\n");
}
