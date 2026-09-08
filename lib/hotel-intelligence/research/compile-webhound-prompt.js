/**
 * Packet 2.8A — compileWebhoundPrompt(spec)
 * Sole allowed path from ResearchInvestigationSpec → provider prompt.
 * UI/agents must not invent ad-hoc Webhound prose.
 */

import crypto from "node:crypto";
import { getTemplate } from "./templates.js";
import { buildResearchInvestigationSpec } from "./investigation-spec.js";
import {
  buildCommonResearchPreamble,
  COMMON_PREAMBLE_VERSION,
} from "./prompts/common-preamble.js";
import {
  buildStructuredOutputContractText,
  buildWebhoundOutputInstructionsV28,
  OUTPUT_CONTRACT_VERSION,
} from "./prompts/output-contracts.js";
import { HI_NEGATIVE_SCREENS_REGISTRY_VERSION } from "./prompts/negative-screens-registry.js";

export const PROMPT_COMPILER_VERSION = "webhound-prompt-compiler-v1";

function hashPrompt(text) {
  return crypto.createHash("sha256").update(String(text || ""), "utf8").digest("hex");
}

/**
 * Compile a provider-ready Webhound prompt from a ResearchInvestigationSpec
 * (or from loose input that is first normalized into a spec).
 */
export function compileWebhoundPrompt(input = {}) {
  const spec =
    input.spec_version && input.template_id && input.subject
      ? input
      : buildResearchInvestigationSpec(input);

  const template = getTemplate(spec.template_id);
  if (!template) {
    const err = new Error(`unknown_research_template:${spec.template_id}`);
    err.code = "unknown_research_template";
    throw err;
  }

  const seed = spec.hotel_seed || spec.subject || {};
  const budget = Number(spec.budget?.provider_budget_usd ?? 5);
  const lines = [];

  lines.push(`# Dealality Deep Research — ${template.display_name || template.template_id}`);
  lines.push("");
  lines.push(buildCommonResearchPreamble());
  lines.push("");
  lines.push("## Hotel identity (authoritative)");
  lines.push(`- Canonical hotel ID: ${seed.hotel_id || spec.subject?.hotel_id || ""}`);
  lines.push(`- Current name: ${seed.hotel_name || spec.subject?.hotel_name || ""}`);
  if (seed.address || spec.subject?.address) {
    lines.push(`- Address / location: ${seed.address || spec.subject.address}`);
  }
  if (seed.market) lines.push(`- Market: ${seed.market}`);
  if (seed.country || spec.subject?.country) {
    lines.push(`- Country: ${seed.country || spec.subject.country}`);
  }
  if (seed.current_brand || spec.subject?.known_brand) {
    lines.push(`- Current brand: ${seed.current_brand || spec.subject.known_brand}`);
  }
  if (seed.parent_company || spec.subject?.known_owner) {
    lines.push(`- Parent / owner (known, verify): ${seed.parent_company || spec.subject.known_owner}`);
  }
  if (seed.operator || spec.subject?.known_operator) {
    lines.push(`- Operator (known, verify): ${seed.operator || spec.subject.known_operator}`);
  }
  if (seed.owner_operator) lines.push("- Operating model hint: owner-operated (self-operated) — verify");
  if (seed.former_names?.length) lines.push(`- Former names: ${seed.former_names.join("; ")}`);
  if (seed.announced_names?.length) {
    lines.push(`- Announced / contemplated names: ${seed.announced_names.join("; ")}`);
  }

  const guard = seed.kgpv_identity_guard || spec.subject?.kgpv_identity_guard;
  if (guard) {
    lines.push("");
    lines.push("## HARD PROPERTY IDENTITY GUARD");
    lines.push(`${guard.must_equal} ≠ ${guard.must_not_equal}`);
    lines.push(guard.note);
    lines.push("Do not mix evidence between these two hotels.");
  }
  for (const c of seed.identity_collisions || spec.subject?.identity_collisions || []) {
    lines.push(`- Collision: do not use "${c.wrong_name}" — ${c.note || ""}`);
  }

  lines.push("");
  lines.push("## Research template");
  lines.push(`- Template ID: ${template.template_id}`);
  lines.push(`- Template version: ${template.version}`);
  lines.push(`- Prompt compiler version: ${PROMPT_COMPILER_VERSION}`);
  lines.push(`- Customer question: ${template.customer_question}`);
  lines.push(`- Research objective: ${template.research_objective}`);
  if (template.research_lanes?.length) {
    lines.push(`- Lanes: ${template.research_lanes.join(", ")}`);
  }
  if (template.scope_groups?.length) {
    lines.push("- Scope groups:");
    for (const g of template.scope_groups) {
      lines.push(`  - ${g.label}: ${(g.items || []).join(", ")}`);
    }
  }
  if (template.report_sections?.length) {
    lines.push(`- Required report sections: ${template.report_sections.join(", ")}`);
  }
  lines.push(`- Negative screens: ${(spec.negative_screens || []).join(", ")}`);
  if (template.validation_rules?.length) {
    lines.push(`- Validation rules: ${template.validation_rules.join(", ")}`);
  }
  lines.push(`- Maximum provider budget: $${budget.toFixed(2)} USD (HARD CAP — do not exceed).`);
  lines.push("");
  lines.push("## Known facts (verify material conflicts; do not treat as unquestionable)");
  const facts = spec.known_facts || [];
  if (facts.length) {
    for (const f of facts) lines.push(`- ${typeof f === "string" ? f : JSON.stringify(f)}`);
  } else {
    lines.push("- (none provided)");
  }
  lines.push("");
  lines.push("## Unresolved questions (concentrate effort here)");
  const gaps = spec.unresolved_questions || [];
  if (gaps.length) {
    for (const g of gaps) {
      lines.push(`- ${typeof g === "string" ? g : g.title || g.question || JSON.stringify(g)}`);
    }
  } else {
    lines.push("- Investigate according to the template lanes and produce open questions where unresolved.");
  }

  if (template.template_id === "CHANGE_OPPORTUNITY") {
    lines.push("");
    lines.push("## Change & Opportunity special requirements");
    lines.push("Cover WHY NOW, physical asset/product risk, product investment/deferred capex,");
    lines.push("operating quality, management/operator stability, deal risk flags, and what to verify next.");
    lines.push("Keep physical condition, operating quality, and operator-change risk conceptually separate.");
    lines.push("Single guest reviews are NOT findings — require consistent themes (REPEATED/PERSISTENT/MULTI-SOURCE).");
    lines.push("No invented dollar capex estimates. No fake 0–100 risk scores.");
  }

  lines.push("");
  lines.push(buildStructuredOutputContractText(template));

  const prompt = lines.join("\n");
  const output_instructions = buildWebhoundOutputInstructionsV28(template);
  const prompt_hash = hashPrompt(`${prompt}\n---\n${output_instructions}`);

  return {
    prompt,
    output_instructions,
    prompt_hash,
    template_id: template.template_id,
    template_version: template.version,
    prompt_compiler_version: PROMPT_COMPILER_VERSION,
    common_preamble_version: COMMON_PREAMBLE_VERSION,
    negative_screens_registry_version: HI_NEGATIVE_SCREENS_REGISTRY_VERSION,
    output_contract_version: OUTPUT_CONTRACT_VERSION,
    investigation_id: spec.investigation_id,
    hotel_id: seed.hotel_id || spec.subject?.hotel_id || null,
    budget_usd: budget,
    spec,
  };
}

/**
 * QA gates for a compiled prompt (Packet 2.8A Part 44).
 */
export function validateCompiledWebhoundPrompt(compiled) {
  const errors = [];
  const p = String(compiled?.prompt || "");
  const checks = {
    SUBJECT_IDENTITY_PRESENT: /Canonical hotel ID:/.test(p) && /Current name:/.test(p),
    TEMPLATE_VERSION_PRESENT: /Template version:/.test(p),
    OBJECTIVE_PRESENT: /Research objective:/.test(p),
    KNOWN_FACTS_PRESENT: /## Known facts/.test(p),
    UNRESOLVED_QUESTIONS_PRESENT: /## Unresolved questions/.test(p),
    SOURCE_HIERARCHY_PRESENT: /## Source hierarchy/.test(p),
    NEGATIVE_SCREENS_PRESENT: /Negative screens:/.test(p),
    TEMPORAL_RULES_PRESENT: /CURRENT, HISTORICAL, ANNOUNCED/.test(p) || /Distinguish CURRENT/.test(p),
    OUTPUT_CONTRACT_PRESENT: /Structured output contract/.test(p),
    BUDGET_PRESENT: /Maximum provider budget:/.test(p),
    PROMPT_HASH_PRESENT: Boolean(compiled?.prompt_hash),
    COMPILER_VERSION_PRESENT: Boolean(compiled?.prompt_compiler_version),
  };
  for (const [k, ok] of Object.entries(checks)) {
    if (!ok) errors.push(k);
  }
  return { ok: errors.length === 0, errors, checks };
}
