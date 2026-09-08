/**
 * Packet 2.8A — provider-neutral ResearchInvestigationSpec.
 * Canonical research definition (not Webhound prose).
 */

import crypto from "node:crypto";
import { getTemplate } from "./templates.js";
import { buildHotelIdentitySeed } from "./hotel-seed.js";
import {
  COMMON_HI_NEGATIVE_SCREENS,
  normalizeNegativeScreens,
} from "./prompts/negative-screens-registry.js";
import { SOURCE_HIERARCHY_DEFAULT } from "./prompts/common-preamble.js";
import { OUTPUT_CONTRACT_VERSION } from "./prompts/output-contracts.js";

export const INVESTIGATION_SPEC_VERSION = "research-investigation-spec-v1";

/**
 * @param {object} input
 * @returns {object} ResearchInvestigationSpec
 */
export function buildResearchInvestigationSpec(input = {}) {
  const templateId = String(input.template_id || input.template?.template_id || "")
    .trim()
    .toUpperCase();
  const template = input.template || getTemplate(templateId);
  if (!template) {
    const err = new Error(`unknown_research_template:${templateId || "missing"}`);
    err.code = "unknown_research_template";
    throw err;
  }

  const hotel_seed = buildHotelIdentitySeed(input.hotel_seed || input.subject || {});
  const known_facts = Array.isArray(input.known_facts)
    ? input.known_facts
    : hotel_seed.known_facts_hints || [];
  const unresolved_questions = Array.isArray(input.unresolved_questions)
    ? input.unresolved_questions
    : Array.isArray(input.known_gaps)
      ? input.known_gaps
      : [];

  const templateScreens = normalizeNegativeScreens(template.negative_screens || []);
  const negative_screens = normalizeNegativeScreens([
    ...COMMON_HI_NEGATIVE_SCREENS,
    ...templateScreens,
    ...(input.negative_screens || []),
  ]);

  const investigation_id =
    input.investigation_id ||
    `inv_${crypto.randomBytes(8).toString("hex")}`;

  return {
    investigation_id,
    spec_version: INVESTIGATION_SPEC_VERSION,
    template_id: template.template_id,
    template_version: template.version,
    subject: {
      hotel_id: hotel_seed.hotel_id,
      hotel_name: hotel_seed.hotel_name,
      aliases: [
        ...(hotel_seed.former_names || []),
        ...(hotel_seed.announced_names || []),
      ],
      address: hotel_seed.address,
      city: input.subject?.city || null,
      country: hotel_seed.country,
      coordinates: input.subject?.coordinates || null,
      known_brand: hotel_seed.current_brand,
      known_operator: hotel_seed.operator,
      known_owner: hotel_seed.parent_company,
      identity_collisions: hotel_seed.identity_collisions || [],
      kgpv_identity_guard: hotel_seed.kgpv_identity_guard || null,
    },
    hotel_seed,
    known_facts,
    unresolved_questions,
    research_objectives: [
      template.research_objective,
      ...(input.research_objectives || []),
    ].filter(Boolean),
    research_lanes: template.research_lanes || [],
    source_priorities: template.preferred_source_classes?.length
      ? template.preferred_source_classes
      : SOURCE_HIERARCHY_DEFAULT.slice(),
    negative_screens,
    temporal_rules: [
      "CURRENT",
      "HISTORICAL",
      "ANNOUNCED",
      "SUPERSEDED",
      ...(template.temporal_rules || []),
    ],
    requested_entities: input.requested_entities || ["entities"],
    requested_relationships: input.requested_relationships || ["relationships"],
    requested_people: input.requested_people || ["people"],
    requested_events: input.requested_events || ["events"],
    requested_property_facts: input.requested_property_facts || ["property_fundamentals"],
    extraction_targets: template.extraction_targets || [],
    validation_rules: template.validation_rules || [],
    stop_conditions: template.stop_conditions || [],
    escalation_conditions: template.escalation_conditions || [],
    output_contract: {
      version: OUTPUT_CONTRACT_VERSION,
      report_template: template.report_template,
      report_sections: template.report_sections || null,
    },
    budget: {
      provider_budget_usd: Number(input.budget_usd ?? template.default_provider_budget_usd ?? 5),
      max_provider_budget_usd: Number(template.max_provider_budget_usd ?? 5),
      cost_class: input.cost_class || template.pricing_tier_key || null,
    },
    claim_handoff: template.claim_handoff || { auto_promote: false },
    learning_tags: input.learning_tags || [],
    created_at: new Date().toISOString(),
  };
}
