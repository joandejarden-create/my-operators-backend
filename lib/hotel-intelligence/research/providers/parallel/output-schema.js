/**
 * Dealality-aligned JSON output schema for Parallel Task API.
 * Maps to existing HI evidence blocks — not a Parallel-only schema.
 */

import { STRUCTURED_OUTPUT_BLOCKS } from "../../prompts/output-contracts.js";

export const PARALLEL_OUTPUT_SCHEMA_VERSION = "hi-parallel-output-schema-v1";

const contactItemSchema = {
  type: "object",
  properties: {
    subject_type: { type: "string", description: "PERSON | ORGANIZATION | HOTEL" },
    subject_name: { type: "string" },
    person_name: { type: "string" },
    organization: { type: "string" },
    role: { type: "string" },
    role_currentness: {
      type: "string",
      enum: ["CURRENT", "FORMER", "ANNOUNCED", "UNKNOWN"],
    },
    relationship_to_hotel: { type: "string" },
    email: { type: "string" },
    email_status: {
      type: "string",
      enum: [
        "PUBLISHED",
        "INFERRED",
        "NOT_FOUND",
        "CANDIDATE_ONLY",
        "UNVERIFIED",
      ],
    },
    phone: { type: "string" },
    phone_type: { type: "string" },
    profile_url: { type: "string" },
    source_url: { type: "string" },
    source_type: { type: "string" },
    confidence: { type: "string" },
    last_seen: { type: "string" },
    verification_notes: { type: "string" },
  },
  additionalProperties: false,
};

export function buildParallelOutputJsonSchema(template = {}) {
  const blocksNote = STRUCTURED_OUTPUT_BLOCKS.join(", ");
  const isDev = template.template_id === "DEVELOPMENT_PROJECT_INTELLIGENCE";
  const isContact =
    template.template_id === "CONTACT_INTELLIGENCE" || template.include_contacts === true;

  const properties = {
    SUBJECT_IDENTITY: {
      type: "object",
      description:
        "Mandatory first. Prove exact subject match before ownership/contact findings. If identity cannot be established, set identity_match=UNRESOLVED and leave ownership empty.",
      properties: {
        requested_name: { type: "string" },
        resolved_name: { type: "string" },
        requested_city: { type: "string" },
        resolved_city: { type: "string" },
        requested_country: { type: "string" },
        resolved_country: { type: "string" },
        requested_address: { type: "string" },
        resolved_address: { type: "string" },
        requested_coordinates: {
          type: "object",
          properties: { lat: { type: "number" }, lng: { type: "number" } },
          additionalProperties: false,
        },
        resolved_coordinates: {
          type: "object",
          properties: { lat: { type: "number" }, lng: { type: "number" } },
          additionalProperties: false,
        },
        identity_match: {
          type: "string",
          enum: ["TRUE", "FALSE", "UNRESOLVED"],
          description: "TRUE only when exact same property is proven.",
        },
        identity_confidence: {
          type: "string",
          enum: ["HIGH", "MODERATE", "LOW"],
        },
        identity_evidence: {
          type: "array",
          items: {
            type: "object",
            properties: {
              url: { type: "string" },
              title: { type: "string" },
              source_type: { type: "string" },
              note: { type: "string" },
            },
            additionalProperties: false,
          },
        },
        identity_conflicts: {
          type: "array",
          items: { type: "string" },
        },
        // Legacy compatibility fields
        subject_type: { type: "string" },
        canonical_name: { type: "string" },
        hotel_id: { type: "string" },
        address: { type: "string" },
        city: { type: "string" },
        country: { type: "string" },
        current_brand: { type: "string" },
        identity_collisions: { type: "array", items: { type: "string" } },
        disambiguation_notes: { type: "string" },
      },
      required: [
        "requested_name",
        "resolved_name",
        "requested_country",
        "resolved_country",
        "identity_match",
        "identity_confidence",
        "identity_evidence",
      ],
      additionalProperties: true,
    },
    SOURCES: {
      type: "array",
      description: "All sources consulted with URL, title, publisher, source_type, date.",
      items: {
        type: "object",
        properties: {
          url: { type: "string" },
          title: { type: "string" },
          publisher: { type: "string" },
          source_type: { type: "string" },
          date: { type: "string" },
          authority_tier: { type: "string" },
        },
        additionalProperties: false,
      },
    },
    FINDINGS: {
      type: "array",
      items: {
        type: "object",
        properties: {
          finding_key: { type: "string" },
          statement: { type: "string" },
          status: { type: "string" },
          confidence: { type: "string" },
          temporal_status: { type: "string" },
          source_urls: { type: "array", items: { type: "string" } },
        },
        required: ["finding_key", "statement"],
        additionalProperties: false,
      },
    },
    CLAIMS: {
      type: "array",
      items: {
        type: "object",
        properties: {
          claim_id: { type: "string" },
          text: { type: "string" },
          status: { type: "string" },
          confidence: { type: "string" },
          source_urls: { type: "array", items: { type: "string" } },
        },
        required: ["text"],
        additionalProperties: false,
      },
    },
    ENTITIES: {
      type: "array",
      items: {
        type: "object",
        properties: {
          entity_id: { type: "string" },
          name: { type: "string" },
          entity_type: { type: "string" },
          jurisdiction: { type: "string" },
          status: { type: "string" },
          source_urls: { type: "array", items: { type: "string" } },
        },
        required: ["name", "entity_type"],
        additionalProperties: false,
      },
    },
    RELATIONSHIPS: {
      type: "array",
      items: {
        type: "object",
        properties: {
          subject: { type: "string" },
          predicate: { type: "string" },
          object: { type: "string" },
          temporal_status: { type: "string" },
          evidence: { type: "string" },
          source_urls: { type: "array", items: { type: "string" } },
        },
        required: ["subject", "predicate", "object"],
        additionalProperties: false,
      },
    },
    PEOPLE: {
      type: "array",
      items: {
        type: "object",
        properties: {
          name: { type: "string" },
          title: { type: "string" },
          organization: { type: "string" },
          role: { type: "string" },
          professional_profile_url: { type: "string" },
          temporal_status: { type: "string" },
          source_urls: { type: "array", items: { type: "string" } },
        },
        required: ["name"],
        additionalProperties: false,
      },
    },
    EVENTS: {
      type: "array",
      items: {
        type: "object",
        properties: {
          event_type: { type: "string" },
          date: { type: "string" },
          description: { type: "string" },
          source_urls: { type: "array", items: { type: "string" } },
        },
        additionalProperties: false,
      },
    },
    PROPERTY_FACTS: {
      type: "array",
      items: {
        type: "object",
        properties: {
          fact_key: { type: "string" },
          value: { type: "string" },
          status: { type: "string" },
          source_urls: { type: "array", items: { type: "string" } },
        },
        additionalProperties: false,
      },
    },
    CONFLICTS: {
      type: "array",
      items: {
        type: "object",
        properties: {
          topic: { type: "string" },
          conflict_description: { type: "string" },
          positions: { type: "array", items: { type: "string" } },
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
          why_it_matters: { type: "string" },
          what_would_resolve: { type: "string" },
        },
        required: ["question"],
        additionalProperties: false,
      },
    },
    RESEARCH_NOTES: {
      type: "string",
      description: "Internal research notes — method, rejected candidates, follow-up paths.",
    },
  };

  if (isContact || template.include_contacts) {
    properties.CONTACTS = {
      type: "array",
      description:
        "Candidate contact evidence only. Never mark inferred email as VERIFIED mailbox.",
      items: contactItemSchema,
    };
  }

  if (isDev) {
    properties.PROJECT_FACTS = {
      type: "array",
      items: {
        type: "object",
        properties: {
          fact_key: { type: "string" },
          value: { type: "string" },
          status: { type: "string" },
        },
        additionalProperties: false,
      },
    };
  }

  return {
    type: "object",
    description: `Dealality Hotel Intelligence structured research output. Required blocks: ${blocksNote}. Do not invent verified status without primary-source support.`,
    properties,
    required: [
      "SUBJECT_IDENTITY",
      "SOURCES",
      "FINDINGS",
      "OPEN_QUESTIONS",
    ],
    additionalProperties: false,
  };
}

export function buildParallelTaskSpec(compiled = {}) {
  const template = compiled.template || { template_id: compiled.template_id };
  return {
    output_schema: {
      type: "json",
      json_schema: buildParallelOutputJsonSchema(template),
    },
    description: compiled.research_prompt || compiled.prompt || null,
  };
}
