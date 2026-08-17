/**
 * Normalize provider-agnostic adapter output into NormalizedResponse.
 */

import { PARSER_VERSION } from "./config.js";
import { randomUUID } from "crypto";

function newId(prefix) {
  return `${prefix}_${randomUUID().replace(/-/g, "").slice(0, 16)}`;
}

/**
 * @param {{
 *   runId: string,
 *   promptId: string,
 *   providerResult: object,
 *   responseId?: string,
 *   createdAt?: string,
 * }} args
 */
export function normalizeProviderResponse(args) {
  const { runId, promptId, providerResult, responseId, createdAt } = args;
  if (!providerResult || typeof providerResult !== "object") {
    return {
      ok: false,
      error: { type: "malformed_response", message: "Provider result missing" },
      response: null,
    };
  }

  const text = providerResult.text == null ? "" : String(providerResult.text);
  const citations = Array.isArray(providerResult.citations)
    ? providerResult.citations.map((c, i) => ({
        url: c?.url ?? null,
        domain: c?.domain ?? null,
        title: c?.title ?? null,
        citationPosition: c?.citationPosition ?? i + 1,
        providerSupplied: c?.providerSupplied !== false,
        firstParty: c?.firstParty ?? null,
        entityAssociation: c?.entityAssociation ?? null,
        sourceType: c?.sourceType || "other",
      }))
    : [];

  return {
    ok: true,
    error: null,
    response: {
      responseId: responseId || newId("resp"),
      runId,
      promptId,
      provider: providerResult.provider || "unknown",
      model: providerResult.model || null,
      text,
      citations,
      usage: providerResult.usage || null,
      providerMeta: providerResult.providerMeta || {},
      raw: providerResult.raw ?? null,
      createdAt: createdAt || new Date().toISOString(),
      parserVersion: providerResult.parserVersion || PARSER_VERSION,
      citationCapability: providerResult.citationCapability || "unavailable",
      latencyMs: providerResult.latencyMs ?? null,
    },
  };
}
