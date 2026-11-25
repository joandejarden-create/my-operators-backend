/**
 * Preferred Lodging Partnership — dedicated Exa Agent contracts V1.
 * Parallel to Events / Scaling contracts. Does NOT reuse Scaling prompt or schema.
 * Live Agent calls still require explicit paid authorization via provider adapter.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  applyExaSegmentRequestTitle,
  extractExaQueryHeading,
  normalizeCanonicalExaSegmentId,
  resolveExaSegmentRequestDisplayName,
} from "./gdi-exa-segment-request-titles-v1.js";

export const EXA_PREFERRED_LODGING_DISCOVERY_CONTRACT_V1 =
  "gdi-exa-preferred-lodging-partnership-discovery-v1";
export const EXA_PREFERRED_LODGING_DEEP_RESEARCH_CONTRACT_V1 =
  "gdi-exa-preferred-lodging-partnership-deep-research-v1";

const HERE = path.dirname(fileURLToPath(import.meta.url));

function readUtf8(rel) {
  return fs.readFileSync(path.join(HERE, rel), "utf8");
}

function readJson(rel) {
  return JSON.parse(readUtf8(rel));
}

let _cache = null;

export function loadExaPreferredLodgingAgentContractsV1() {
  if (_cache) return _cache;
  _cache = Object.freeze({
    discovery: Object.freeze({
      contractId: EXA_PREFERRED_LODGING_DISCOVERY_CONTRACT_V1,
      promptMarkdown: readUtf8(
        "contracts/preferred-lodging-discovery-agent-prompt-v1.md",
      ),
      outputSchema: readJson(
        "schemas/preferred-lodging-discovery-structured-output-v1.json",
      ),
      schemaPath: "schemas/preferred-lodging-discovery-structured-output-v1.json",
      version: "v1",
    }),
    deepResearch: Object.freeze({
      contractId: EXA_PREFERRED_LODGING_DEEP_RESEARCH_CONTRACT_V1,
      promptMarkdown: readUtf8(
        "contracts/preferred-lodging-deep-research-agent-prompt-v1.md",
      ),
      outputSchema: readJson(
        "schemas/preferred-lodging-deep-research-structured-output-v1.json",
      ),
      schemaPath:
        "schemas/preferred-lodging-deep-research-structured-output-v1.json",
      version: "v1",
    }),
  });
  return _cache;
}

export function buildExaPreferredLodgingAgentQuery({
  mode,
  contextBlock = "",
  segmentId = "preferred_lodging_partnerships",
  opportunityName = null,
} = {}) {
  const contracts = loadExaPreferredLodgingAgentContractsV1();
  const pack =
    mode === "discovery"
      ? contracts.discovery
      : mode === "deep_research"
        ? contracts.deepResearch
        : null;
  if (!pack) throw new Error(`unknown_exa_preferred_lodging_agent_mode:${mode}`);
  const raw = [
    pack.promptMarkdown.trim(),
    "",
    "## RUN CONTEXT (Dealality-supplied)",
    String(contextBlock || "").trim() || "(no additional context)",
    "",
    `Contract ID: ${pack.contractId}`,
  ].join("\n");
  return applyExaSegmentRequestTitle({
    query: raw,
    segmentId: segmentId || "preferred_lodging_partnerships",
    stage: mode,
    opportunityName,
  });
}

export function getExaPreferredLodgingAgentOutputSchema(mode) {
  const contracts = loadExaPreferredLodgingAgentContractsV1();
  if (mode === "discovery") return contracts.discovery.outputSchema;
  if (mode === "deep_research") return contracts.deepResearch.outputSchema;
  throw new Error(`unknown_exa_preferred_lodging_agent_mode:${mode}`);
}

/**
 * Build Exa Agent create body for Preferred Lodging (zero-paid safe to inspect).
 * Does not call the network. Does not use Scaling prompt/schema.
 */
export function buildExaPreferredLodgingAgentCreatePayload({
  mode = "discovery",
  contextBlock = "",
  effort = "medium",
  maxCostDollars = null,
  exclusion = null,
  segmentId = "preferred_lodging_partnerships",
  opportunityName = null,
} = {}) {
  const canonicalSegmentId =
    normalizeCanonicalExaSegmentId(segmentId) ||
    "preferred_lodging_partnerships";
  const query = buildExaPreferredLodgingAgentQuery({
    mode,
    contextBlock,
    segmentId: canonicalSegmentId,
    opportunityName,
  });
  const outputSchema = getExaPreferredLodgingAgentOutputSchema(mode);
  const body = { query, effort, outputSchema };
  if (exclusion?.length) {
    body.input = {
      exclusion: exclusion.map((item) => {
        if (item && typeof item === "object") return item;
        const s = String(item || "").trim();
        return s ? { id: s, name: s } : null;
      }).filter(Boolean),
    };
  }
  const e = String(effort).toLowerCase();
  if ((e === "auto" || e === "max" || e === "ultra") && maxCostDollars != null) {
    body.budget = { maxCostDollars: Number(maxCostDollars) };
  }
  const displayTitle = resolveExaSegmentRequestDisplayName(canonicalSegmentId);
  return {
    body,
    meta: {
      contractId:
        mode === "discovery"
          ? EXA_PREFERRED_LODGING_DISCOVERY_CONTRACT_V1
          : EXA_PREFERRED_LODGING_DEEP_RESEARCH_CONTRACT_V1,
      mode,
      effort,
      segmentId: canonicalSegmentId,
      segmentDisplayTitle: displayTitle,
      requestHeading: extractExaQueryHeading(query),
      agentFamily: "PREFERRED_LODGING",
      liveCall: false,
      scalingPromptUsed: false,
    },
  };
}

/** Redact long query for reports while proving dedicated PLP payload construction. */
export function redactExaPreferredLodgingPayloadForReport(payloadPack) {
  const body = payloadPack?.body || {};
  const q = String(body.query || "");
  return {
    meta: payloadPack?.meta || null,
    body: {
      effort: body.effort,
      budget: body.budget || null,
      hasOutputSchema: Boolean(body.outputSchema),
      outputSchemaId: body.outputSchema?.$id || null,
      queryChars: q.length,
      queryHead: q.slice(0, 400),
      queryTail: q.slice(-200),
      contractIdInQuery:
        /gdi-exa-preferred-lodging-partnership-(discovery|deep-research)-v1/.test(
          q,
        ),
      scalingContractAbsent: !/gdi-scaling-exa-agent-/i.test(q),
      venueReferralMissionPresent:
        /Preferred Lodging|referral|overflow|preferred-hotel|venue/i.test(q),
      stakeholderRolesPresent:
        /Venue owner|Wedding director|Event sales director/i.test(q),
      hotelContextPresent:
        /HOTEL RESEARCH CONTEXT|Hotel ID:|hotelId|guestrooms/i.test(q),
    },
  };
}
