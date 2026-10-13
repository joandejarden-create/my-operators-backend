/**
 * compileParallelTask(spec) — provider-neutral ResearchInvestigationSpec → Parallel task payload.
 * Subject identity is a separate structured field from research objective.
 * No hidden gold answers.
 */

import crypto from "node:crypto";
import { getTemplate } from "../../templates.js";
import { buildResearchInvestigationSpec } from "../../investigation-spec.js";
import { buildParallelTaskSpec } from "./output-schema.js";
import {
  SUBJECT_IDENTITY_CONTRACT_VERSION,
  buildCanonicalSubjectInput,
  buildSubjectIdentityLockText,
  buildPostIdentityResearchObjective,
} from "./subject-identity.js";

export const PARALLEL_COMPILER_VERSION = "parallel-task-compiler-v2-identity";

function hashInput(obj) {
  return crypto.createHash("sha256").update(JSON.stringify(obj), "utf8").digest("hex");
}

function sanitizeKnownFacts(spec, blindMode) {
  const facts = Array.isArray(spec.known_facts) ? spec.known_facts : [];
  if (!blindMode) return facts;
  return facts.filter((f) => {
    const t = String(typeof f === "string" ? f : JSON.stringify(f)).toLowerCase();
    if (/economic owner|propco|sponsor|deed|ubo|ownership chain|parent company confirmed/i.test(t)) {
      return false;
    }
    return true;
  });
}

export function compileParallelTask(input = {}) {
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

  const blindMode = input.blind_mode === true || input.benchmark_blind === true;
  const seed = {
    ...(spec.hotel_seed || {}),
    ...(spec.subject || {}),
    hotel_name:
      input.hotel_seed?.hotel_name ||
      spec.hotel_seed?.hotel_name ||
      spec.subject?.hotel_name ||
      null,
    city: input.hotel_seed?.city || spec.subject?.city || input.hotel_seed?.market || null,
    country: input.hotel_seed?.country || spec.hotel_seed?.country || spec.subject?.country || null,
    latitude: input.hotel_seed?.latitude ?? spec.hotel_seed?.latitude ?? null,
    longitude: input.hotel_seed?.longitude ?? spec.hotel_seed?.longitude ?? null,
    coordinates: input.hotel_seed?.coordinates || spec.subject?.coordinates || null,
    airtable_record_id:
      input.hotel_seed?.airtable_record_id ||
      input.hotel_seed?.census_record_id ||
      null,
    aliases: [
      ...(spec.subject?.aliases || []),
      ...(input.hotel_seed?.former_names || []),
      ...(spec.hotel_seed?.former_names || []),
    ].filter(Boolean),
  };

  const subject = buildCanonicalSubjectInput(seed);
  const identityLock = buildSubjectIdentityLockText(subject);
  const knownFacts = sanitizeKnownFacts(spec, blindMode);
  const openQuestions = (spec.unresolved_questions || []).map((q) =>
    typeof q === "string" ? q : q.title || q.question || JSON.stringify(q)
  );
  const researchObjective = buildPostIdentityResearchObjective(template, {
    negative_screens: spec.negative_screens || [],
    open_questions: openQuestions.slice(0, 12),
  });

  // Native L4-DOCUMENT → Parallel L4 interpretation. Never gold labels.
  const candidateDocuments = (input.candidate_documents || spec.candidate_documents || [])
    .slice(0, 12)
    .map((d) => ({
      url: d.url || d.source_url || null,
      title: d.title || null,
      source_family: d.source_family || null,
      authority_estimate: d.authority_estimate || null,
      evidence_excerpt: (d.evidence_span || d.excerpt || d.evidence_excerpt || null)
        ? String(d.evidence_span || d.excerpt || d.evidence_excerpt).slice(0, 1200)
        : null,
      extracted_entities: d.entity_relevance || d.legal_entities || d.extracted_entities || null,
    }))
    .filter((d) => d.url);

  const augmentationNote =
    candidateDocuments.length > 0
      ? `\n\nNATIVE DOCUMENT CANDIDATES (Dealality discovery — interpret/triangulate; do not ignore; snippets alone are not decisive):\n${candidateDocuments
          .map(
            (d, i) =>
              `${i + 1}. [${d.source_family || "UNKNOWN"}] ${d.title || d.url}\n   URL: ${d.url}${
                d.evidence_excerpt ? `\n   Excerpt: ${d.evidence_excerpt}` : ""
              }${
                d.extracted_entities
                  ? `\n   Extracted entities: ${JSON.stringify(d.extracted_entities).slice(0, 400)}`
                  : ""
              }`
          )
          .join("\n")}`
      : "";

  const researchObjectiveWithDocs = `${researchObjective}${augmentationNote}`;

  const structuredInput = {
    investigation_id: spec.investigation_id,
    template_id: template.template_id,
    template_version: template.version,
    subject,
    research_objectives: [researchObjectiveWithDocs],
    source_priorities: spec.source_priorities || [],
    negative_screens: spec.negative_screens || [],
    temporal_rules: spec.temporal_rules || [],
    known_facts: knownFacts,
    unresolved_questions: openQuestions,
    candidate_documents: candidateDocuments,
    output_contract_version: spec.output_contract?.version || null,
    blind_mode: blindMode,
    subject_identity_contract_version: SUBJECT_IDENTITY_CONTRACT_VERSION,
    routing_lane: candidateDocuments.length ? "L4-PARALLEL" : input.routing_lane || null,
  };

  const inputHash = hashInput(structuredInput);
  const specHash = hashInput({
    investigation_id: spec.investigation_id,
    template_id: spec.template_id,
    template_version: spec.template_version,
    blind_mode: blindMode,
    subject_identity_contract_version: SUBJECT_IDENTITY_CONTRACT_VERSION,
  });

  const processor =
    input.processor ||
    input.parallel_processor ||
    process.env.HOTEL_INTELLIGENCE_PARALLEL_PROCESSOR ||
    "core";

  const taskSpec = buildParallelTaskSpec({
    template: {
      ...template,
      include_contacts: input.include_contacts === true || template.include_contacts === true,
    },
    prompt: identityLock,
    research_prompt: identityLock,
  });

  // Subject is authoritative and separate from research_objective.
  const parallelInput = {
    subject,
    identity_lock: identityLock,
    research_objective: researchObjectiveWithDocs,
    negative_screens: spec.negative_screens || [],
    known_facts: knownFacts,
    unresolved_questions: openQuestions.slice(0, 12),
    candidate_documents: candidateDocuments,
  };

  taskSpec.input_schema = {
    type: "json",
    json_schema: {
      type: "object",
      properties: {
        subject: {
          type: "object",
          description: "AUTHORITATIVE subject. Do not research any other hotel.",
          properties: {
            name: { type: "string" },
            city: { type: "string" },
            country: { type: "string" },
            address: { type: "string" },
            coordinates: {
              type: "object",
              properties: { lat: { type: "number" }, lng: { type: "number" } },
            },
            canonical_id: { type: "string" },
            census_record_id: { type: "string" },
            aliases: { type: "array", items: { type: "string" } },
            market: { type: "string" },
            rooms: { type: "number" },
          },
          required: ["name", "city", "country"],
          additionalProperties: false,
        },
        identity_lock: {
          type: "string",
          description: "Hard identity rules. Apply before any ownership research.",
        },
        research_objective: {
          type: "string",
          description:
            "Ownership research ONLY after subject identity is proven TRUE. May include native document candidates for interpretation.",
        },
        negative_screens: { type: "array", items: { type: "string" } },
        known_facts: { type: "array", items: { type: "string" } },
        unresolved_questions: { type: "array", items: { type: "string" } },
        candidate_documents: {
          type: "array",
          description:
            "Native Dealality document-discovery candidates. Interpret/triangulate. Do not treat as gold answers. Follow URLs; snippets alone are not decisive.",
          items: {
            type: "object",
            properties: {
              url: { type: "string" },
              title: { type: "string" },
              source_family: { type: "string" },
              authority_estimate: { type: "string" },
              evidence_excerpt: { type: "string" },
              extracted_entities: {},
            },
          },
        },
      },
      required: ["subject", "identity_lock", "research_objective"],
      additionalProperties: false,
    },
  };

  return {
    compiler_version: PARALLEL_COMPILER_VERSION,
    investigation_id: spec.investigation_id,
    template_id: template.template_id,
    template_version: template.version,
    prompt_hash: hashInput(identityLock + "\n" + researchObjective),
    spec_hash: specHash,
    input_hash: inputHash,
    blind_mode: blindMode,
    processor,
    input: structuredInput,
    parallel_input: parallelInput,
    research_prompt: `${identityLock}\n\n${researchObjective}`,
    task_spec: taskSpec,
    output_instructions:
      "Return SUBJECT_IDENTITY first with identity_match TRUE|FALSE|UNRESOLVED and identity_evidence. Only after identity_match=TRUE populate ownership/people/contacts blocks.",
    spec,
    metadata: {
      provider: "PARALLEL",
      dealality_template: template.template_id,
      investigation_id: spec.investigation_id,
      hotel_id: subject.canonical_id || subject.census_record_id || null,
      hotel_name: subject.name || null,
      city: subject.city || null,
      country: subject.country || null,
      blind_mode: blindMode,
      compiler_version: PARALLEL_COMPILER_VERSION,
      subject_identity_contract_version: SUBJECT_IDENTITY_CONTRACT_VERSION,
      attempt_reason: input.attempt_reason || null,
      attempt_number: input.attempt_number || null,
    },
  };
}

export function validateCompiledParallelTask(compiled) {
  const errors = [];
  if (!compiled?.compiler_version) errors.push("COMPILER_VERSION_MISSING");
  if (!compiled?.parallel_input?.subject?.name) errors.push("SUBJECT_NAME_MISSING");
  if (!compiled?.parallel_input?.subject?.country) errors.push("SUBJECT_COUNTRY_MISSING");
  if (!compiled?.parallel_input?.identity_lock) errors.push("IDENTITY_LOCK_MISSING");
  if (!compiled?.parallel_input?.research_objective) errors.push("RESEARCH_OBJECTIVE_MISSING");
  if (!compiled?.task_spec?.output_schema?.json_schema) errors.push("OUTPUT_SCHEMA_MISSING");
  if (!compiled?.task_spec?.input_schema) errors.push("INPUT_SCHEMA_MISSING");
  if (!compiled?.spec_hash) errors.push("SPEC_HASH_MISSING");
  // Ensure research_objective does not bury a second conflicting hotel identity as primary
  const ro = String(compiled?.parallel_input?.research_objective || "");
  if (/rowan tree/i.test(ro)) errors.push("CONTAMINATED_RESEARCH_OBJECTIVE");
  return { ok: errors.length === 0, errors };
}
