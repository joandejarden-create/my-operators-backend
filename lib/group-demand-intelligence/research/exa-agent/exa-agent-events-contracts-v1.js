/**
 * Association / Conference / Events — Exa Agent contracts V1 (runtime-loaded).
 * Parallel to Scaling contracts; does not replace Boolean Search strategies.
 * Live Agent calls still require explicit paid authorization via provider adapter.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

export const EXA_EVENTS_DISCOVERY_CONTRACT_V1 = "gdi-exa-events-discovery-v1";
export const EXA_EVENTS_DEEP_RESEARCH_CONTRACT_V1 =
  "gdi-exa-events-deep-research-v1";
export const EXA_EVENTS_DISCOVERY_CONTRACT_ACTIVE =
  EXA_EVENTS_DISCOVERY_CONTRACT_V1;

const HERE = path.dirname(fileURLToPath(import.meta.url));

function readUtf8(rel) {
  return fs.readFileSync(path.join(HERE, rel), "utf8");
}

function readJson(rel) {
  return JSON.parse(readUtf8(rel));
}

let _cache = null;

export function loadExaEventsAgentContractsV1() {
  if (_cache) return _cache;
  _cache = Object.freeze({
    discovery: Object.freeze({
      contractId: EXA_EVENTS_DISCOVERY_CONTRACT_V1,
      promptMarkdown: readUtf8("contracts/event-discovery-agent-prompt-v1.md"),
      outputSchema: readJson("schemas/event-discovery-structured-output-v1.json"),
      schemaPath: "schemas/event-discovery-structured-output-v1.json",
      version: "v1",
    }),
    deepResearch: Object.freeze({
      contractId: EXA_EVENTS_DEEP_RESEARCH_CONTRACT_V1,
      promptMarkdown: readUtf8(
        "contracts/event-deep-research-agent-prompt-v1.md",
      ),
      outputSchema: readJson(
        "schemas/event-deep-research-structured-output-v1.json",
      ),
      schemaPath: "schemas/event-deep-research-structured-output-v1.json",
      version: "v1",
    }),
  });
  return _cache;
}

export function buildExaEventsAgentQuery({ mode, contextBlock = "" } = {}) {
  const contracts = loadExaEventsAgentContractsV1();
  const pack =
    mode === "discovery"
      ? contracts.discovery
      : mode === "deep_research"
        ? contracts.deepResearch
        : null;
  if (!pack) throw new Error(`unknown_exa_events_agent_mode:${mode}`);
  return [
    pack.promptMarkdown.trim(),
    "",
    "## RUN CONTEXT (Dealality-supplied)",
    String(contextBlock || "").trim() || "(no additional context)",
    "",
    `Contract ID: ${pack.contractId}`,
  ].join("\n");
}

export function getExaEventsAgentOutputSchema(mode) {
  const contracts = loadExaEventsAgentContractsV1();
  if (mode === "discovery") return contracts.discovery.outputSchema;
  if (mode === "deep_research") return contracts.deepResearch.outputSchema;
  throw new Error(`unknown_exa_events_agent_mode:${mode}`);
}

/**
 * Build Exa Agent create body for Events segment (zero-paid safe to inspect).
 * Does not call the network.
 */
export function buildExaEventsAgentCreatePayload({
  mode = "discovery",
  contextBlock = "",
  effort = "medium",
  maxCostDollars = null,
  exclusion = null,
} = {}) {
  const query = buildExaEventsAgentQuery({ mode, contextBlock });
  const outputSchema = getExaEventsAgentOutputSchema(mode);
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
  return {
    body,
    meta: {
      contractId:
        mode === "discovery"
          ? EXA_EVENTS_DISCOVERY_CONTRACT_V1
          : EXA_EVENTS_DEEP_RESEARCH_CONTRACT_V1,
      mode,
      effort,
      segment: "associations_government",
      customerUmbrella: "Association / Conference / Events",
      liveCall: false,
    },
  };
}

/** Redact long query for reports while proving payload construction. */
export function redactExaEventsPayloadForReport(payloadPack) {
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
      contractIdInQuery: /gdi-exa-events-(discovery|deep-research)-v1/.test(q),
      hotelContextPresent:
        /HOTEL RESEARCH CONTEXT|Hotel ID:|hotelId|guestrooms|meeting/i.test(q),
      accessibleDemandRulePresent: /Accessible demand/i.test(q),
      tracksPresent: /TRACK_A_UPCOMING_VENUE|TRACK_B_RECURRING/i.test(q),
      scalingSiblingWarningPresent: /gdi-scaling-exa-agent-discovery-v2/i.test(q),
    },
  };
}
