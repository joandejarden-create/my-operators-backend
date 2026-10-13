/**
 * Lean Parallel output schema for ownership comparison.
 * FULL_HOTEL_INTELLIGENCE schema exceeds Parallel's 100-property limit (111).
 * Same research objective / processor; schema only slimmed for API acceptance.
 */

export const COMPARISON_OUTPUT_SCHEMA_VERSION = "parallel-vs-native-lean-output-v1";
export const PARALLEL_MAX_SCHEMA_PROPERTIES = 100;
export const FULL_HOTEL_SCHEMA_PROPERTY_COUNT_OBSERVED = 111;

function countProperties(node, seen = 0) {
  if (!node || typeof node !== "object") return seen;
  if (node.properties && typeof node.properties === "object") {
    for (const key of Object.keys(node.properties)) {
      seen += 1;
      seen = countProperties(node.properties[key], seen);
    }
  }
  if (node.items) seen = countProperties(node.items, seen);
  return seen;
}

export function buildComparisonLeanOutputJsonSchema() {
  const schema = {
    type: "object",
    description:
      "Lean ownership comparison output. Prove SUBJECT_IDENTITY first. Do not promote brand/operator/portfolio into ownership. Keep publication vs transaction dates separate.",
    properties: {
      SUBJECT_IDENTITY: {
        type: "object",
        properties: {
          resolved_name: { type: "string" },
          resolved_city: { type: "string" },
          resolved_country: { type: "string" },
          identity_match: { type: "string", enum: ["TRUE", "FALSE", "UNRESOLVED"] },
          identity_confidence: { type: "string", enum: ["HIGH", "MODERATE", "LOW"] },
          identity_evidence_url: { type: "string" },
          identity_note: { type: "string" },
        },
        required: ["identity_match", "resolved_name"],
        additionalProperties: false,
      },
      OWNERSHIP_CLAIMS: {
        type: "array",
        description:
          "PROPERTY_OWNER | ECONOMIC_SPONSOR | OPERATOR | BRAND | REGISTERED_BUSINESS | HISTORICAL_OWNER | UNRESOLVED",
        items: {
          type: "object",
          properties: {
            named_entity_or_person: { type: "string" },
            relationship: {
              type: "string",
              enum: [
                "PROPERTY_OWNER",
                "ECONOMIC_SPONSOR",
                "OPERATOR",
                "BRAND",
                "REGISTERED_BUSINESS",
                "HISTORICAL_OWNER",
                "UNRESOLVED",
              ],
            },
            supporting_passage: { type: "string" },
            source_url: { type: "string" },
            publication_date: { type: "string" },
            transaction_or_event_date: { type: "string" },
            currentness: { type: "string" },
            contrary_evidence_note: { type: "string" },
          },
          required: ["named_entity_or_person", "relationship", "supporting_passage", "source_url"],
          additionalProperties: false,
        },
      },
      ORGANIZATION: {
        type: "object",
        properties: {
          candidate_official_domain: { type: "string" },
          domain_org_evidence_passage: { type: "string" },
          domain_source_url: { type: "string" },
          domain_status: { type: "string", enum: ["CONFIRMED", "CANDIDATE", "ABSENT", "UNVERIFIED"] },
        },
        additionalProperties: false,
      },
      PEOPLE: {
        type: "array",
        items: {
          type: "object",
          properties: {
            name: { type: "string" },
            role: { type: "string" },
            organization: { type: "string" },
            affiliation_passage: { type: "string" },
            source_url: { type: "string" },
            source_date: { type: "string" },
            current_versus_former: { type: "string", enum: ["CURRENT", "FORMER", "UNKNOWN"] },
            relevance: { type: "string" },
          },
          required: ["name"],
          additionalProperties: false,
        },
      },
      PUBLIC_CONTACTS: {
        type: "array",
        items: {
          type: "object",
          properties: {
            route_kind: { type: "string", enum: ["HOTEL", "CORPORATE", "MEDIA", "NAMED_PERSON"] },
            value: { type: "string" },
            source_url: { type: "string" },
            attribution: { type: "string" },
            purpose: { type: "string" },
          },
          additionalProperties: false,
        },
      },
      OPEN_QUESTIONS: {
        type: "array",
        items: {
          type: "object",
          properties: {
            question: { type: "string" },
            missing_link: { type: "string" },
          },
          additionalProperties: false,
        },
      },
      RESEARCH_NOTES: { type: "string" },
    },
    required: ["SUBJECT_IDENTITY", "OWNERSHIP_CLAIMS", "OPEN_QUESTIONS"],
    additionalProperties: false,
  };

  const property_count = countProperties(schema);
  if (property_count > PARALLEL_MAX_SCHEMA_PROPERTIES) {
    throw new Error(`comparison_schema_too_large:${property_count}`);
  }
  return { schema, property_count, version: COMPARISON_OUTPUT_SCHEMA_VERSION };
}

export function applyLeanComparisonOutputSchema(compiled) {
  const { schema, property_count, version } = buildComparisonLeanOutputJsonSchema();
  compiled.task_spec = {
    ...(compiled.task_spec || {}),
    output_schema: {
      type: "json",
      json_schema: schema,
    },
    description: compiled.research_prompt || compiled.task_spec?.description || null,
  };
  compiled.comparison_output_schema = {
    version,
    property_count,
    reason:
      "FULL_HOTEL include_contacts schema rejected by Parallel API (111>100 properties). Lean schema preserves same research objective and processor.",
    full_hotel_observed_properties: FULL_HOTEL_SCHEMA_PROPERTY_COUNT_OBSERVED,
  };
  return compiled;
}
