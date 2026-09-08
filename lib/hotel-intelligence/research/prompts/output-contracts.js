/**
 * Packet 2.8A — provider output contract blocks (Webhound-facing).
 */

export const OUTPUT_CONTRACT_VERSION = "hi-research-output-contract-v1";

export const STRUCTURED_OUTPUT_BLOCKS = Object.freeze([
  "SUBJECT_IDENTITY",
  "SOURCES",
  "FINDINGS",
  "CLAIMS",
  "ENTITIES",
  "RELATIONSHIPS",
  "PEOPLE",
  "EVENTS",
  "PROPERTY_FACTS",
  "CONFLICTS",
  "OPEN_QUESTIONS",
  "RESEARCH_NOTES",
]);

export function buildStructuredOutputContractText(template = {}) {
  const lines = [
    "## Structured output contract",
    "Where possible, organize the report so Dealality can normalize into:",
    ...STRUCTURED_OUTPUT_BLOCKS.map((b) => `- ${b}`),
    "",
    "Every finding should include: finding_key, statement, status, confidence, source_urls, temporal_status.",
    "Every relationship: subject, predicate, object, current/historical, evidence.",
    "Every person: name, title, organization, role, professional_profile_url (or NOT_FOUND), source.",
    "Do not treat final prose as the only deliverable — structured blocks are required.",
  ];
  if (template.template_id === "CHANGE_OPPORTUNITY") {
    lines.push(
      "",
      "CHANGE_OPPORTUNITY required sections: Executive Answer (Why Now / What Could Derail / What Looks Stable / What Needs Verification), Opportunity Thesis, Asset & Product Risk, Product Investment / Capex, Operating Quality & Management Risk, Operator / Management Stability, Deal Risk Flags, What to Verify Next, Open Questions, Sources."
    );
    lines.push(
      "Guest evidence is a SIGNAL only — not automatic structural proof (GUEST_PATTERN_NOT_STRUCTURAL_PROOF)."
    );
  } else if (template.report_template === "FULL_INVESTIGATION") {
    lines.push(
      "",
      "FULL HI should cover: property identity, ownership chain, PropCo, economic owner/sponsor, parent/control, operator, current/historical/announced brand, development/renovation, organization/portfolio, people/decision authority, transactions/capital, commercial pursuit, open questions, sources.",
      "Unresolved items must be labeled UNRESOLVED — do not invent."
    );
  }
  return lines.join("\n");
}

export function buildWebhoundOutputInstructionsV28(template = {}) {
  const base = [
    "Structure the final report so Dealality can normalize it.",
    "Prefer explicit section headings matching the investigation template.",
    "Cite sources inline. Distinguish verified facts from unresolved questions.",
    "Do not claim Dealality product canonical status.",
    "Include SUBJECT_IDENTITY, SOURCES, FINDINGS (with finding_key), PEOPLE (with professional_profile_url or NOT_FOUND), RELATIONSHIPS, OPEN_QUESTIONS.",
  ];
  if (template.template_id === "CHANGE_OPPORTUNITY") {
    base.push(
      "Include sections: Executive Answer (Why Now / What Could Derail / What Looks Stable / What Needs Verification), Opportunity Thesis, Asset & Product Risk, Product Investment / Capex, Operating Quality & Management Risk, Operator / Management Stability, Deal Risk Flags, What to Verify Next, Open Questions, Sources."
    );
  } else if (template.report_template === "FULL_INVESTIGATION") {
    base.push(
      "Include Executive Answer, Key Findings (finding_key each), Ownership, Operator, Brand chronology, Organization/Portfolio, People table, Transactions/Capital, Commercial pursuit, Open Questions, Sources."
    );
  } else {
    base.push("Include Executive Answer, Key Findings, Investigation detail, Open Questions, Sources.");
  }
  return base.join(" ");
}
