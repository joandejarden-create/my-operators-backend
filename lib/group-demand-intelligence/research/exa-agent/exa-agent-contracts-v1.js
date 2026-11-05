/**
 * Scaling Exa Agent research contracts — runtime-loaded prompts + schemas.
 * Discovery defaults to V2 (hospitality-demand-first). Deep research remains V1.
 * Critical instructions live here (not only in Cursor skills/chat).
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

export const EXA_AGENT_DISCOVERY_CONTRACT_V1 =
  "gdi-scaling-exa-agent-discovery-v1";
export const EXA_AGENT_DISCOVERY_CONTRACT_V2 =
  "gdi-scaling-exa-agent-discovery-v2";
export const EXA_AGENT_DEEP_RESEARCH_CONTRACT_V1 =
  "gdi-scaling-exa-agent-deep-research-v1";

/** Active discovery contract for live payload construction. */
export const EXA_AGENT_DISCOVERY_CONTRACT_ACTIVE =
  EXA_AGENT_DISCOVERY_CONTRACT_V2;

const HERE = path.dirname(fileURLToPath(import.meta.url));

function readUtf8(rel) {
  return fs.readFileSync(path.join(HERE, rel), "utf8");
}

function readJson(rel) {
  return JSON.parse(readUtf8(rel));
}

let _cache = null;

export function loadExaAgentContractsV1({ discoveryVersion = "v2" } = {}) {
  if (_cache && _cache._discoveryVersion === discoveryVersion) return _cache;
  const useV2 = discoveryVersion !== "v1";
  _cache = Object.freeze({
    _discoveryVersion: discoveryVersion,
    discovery: Object.freeze(
      useV2
        ? {
            contractId: EXA_AGENT_DISCOVERY_CONTRACT_V2,
            promptMarkdown: readUtf8("contracts/discovery-agent-prompt-v2.md"),
            outputSchema: readJson("schemas/discovery-structured-output-v2.json"),
            schemaPath: "schemas/discovery-structured-output-v2.json",
            version: "v2",
          }
        : {
            contractId: EXA_AGENT_DISCOVERY_CONTRACT_V1,
            promptMarkdown: readUtf8("contracts/discovery-agent-prompt-v1.md"),
            outputSchema: readJson("schemas/discovery-structured-output-v1.json"),
            schemaPath: "schemas/discovery-structured-output-v1.json",
            version: "v1",
          },
    ),
    discoveryV1: Object.freeze({
      contractId: EXA_AGENT_DISCOVERY_CONTRACT_V1,
      promptMarkdown: readUtf8("contracts/discovery-agent-prompt-v1.md"),
      outputSchema: readJson("schemas/discovery-structured-output-v1.json"),
      schemaPath: "schemas/discovery-structured-output-v1.json",
      version: "v1",
    }),
    discoveryV2: Object.freeze({
      contractId: EXA_AGENT_DISCOVERY_CONTRACT_V2,
      promptMarkdown: readUtf8("contracts/discovery-agent-prompt-v2.md"),
      outputSchema: readJson("schemas/discovery-structured-output-v2.json"),
      schemaPath: "schemas/discovery-structured-output-v2.json",
      version: "v2",
    }),
    deepResearch: Object.freeze({
      contractId: EXA_AGENT_DEEP_RESEARCH_CONTRACT_V1,
      promptMarkdown: readUtf8("contracts/deep-research-agent-prompt-v1.md"),
      outputSchema: readJson("schemas/deep-research-structured-output-v1.json"),
      schemaPath: "schemas/deep-research-structured-output-v1.json",
    }),
  });
  return _cache;
}

/**
 * Build the Agent `query` string: contract instructions + concrete run context.
 */
export function buildExaAgentQuery({
  mode,
  contextBlock = "",
  discoveryVersion = "v2",
} = {}) {
  const contracts = loadExaAgentContractsV1({ discoveryVersion });
  const pack =
    mode === "discovery"
      ? contracts.discovery
      : mode === "deep_research"
        ? contracts.deepResearch
        : null;
  if (!pack) {
    throw new Error(`unknown_exa_agent_mode:${mode}`);
  }
  return [
    pack.promptMarkdown.trim(),
    "",
    "## RUN CONTEXT (Dealality-supplied)",
    String(contextBlock || "").trim() || "(no additional context)",
    "",
    `Contract ID: ${pack.contractId}`,
  ].join("\n");
}

export function getExaAgentOutputSchema(mode, { discoveryVersion = "v2" } = {}) {
  const contracts = loadExaAgentContractsV1({ discoveryVersion });
  if (mode === "discovery") return contracts.discovery.outputSchema;
  if (mode === "deep_research") return contracts.deepResearch.outputSchema;
  throw new Error(`unknown_exa_agent_mode:${mode}`);
}
