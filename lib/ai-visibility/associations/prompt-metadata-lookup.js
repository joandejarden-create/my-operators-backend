/**
 * Prompt metadata lookup for evidence records (P0B).
 * Stored evidence often omits promptFamily — resolve from governed seeds.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const REPO_ROOT = path.join(__dirname, "..", "..", "..");
const PROMPT_SEED_PATHS = [
  path.join(REPO_ROOT, "fixtures", "ai-visibility", "phase3a9-showcase-prompt-seed.json"),
  path.join(REPO_ROOT, "fixtures", "ai-visibility", "phase2d-prompt-seed.json"),
];

let cachedMap = null;

/**
 * @returns {Map<string, object>}
 */
export function buildPromptMetadataById() {
  if (cachedMap) return cachedMap;
  const map = new Map();
  for (const seedPath of PROMPT_SEED_PATHS) {
    if (!fs.existsSync(seedPath)) continue;
    const raw = JSON.parse(fs.readFileSync(seedPath, "utf8"));
    for (const p of raw.prompts || []) {
      if (p.promptId && !map.has(p.promptId)) {
        map.set(p.promptId, p);
      }
    }
  }
  cachedMap = map;
  return map;
}

/**
 * Enrich evidence row with prompt metadata for scenario resolution.
 * @param {object} evidence
 */
export function enrichEvidenceWithPromptMetadata(evidence) {
  if (!evidence?.promptId) return evidence;
  const meta = buildPromptMetadataById().get(evidence.promptId);
  if (!meta) return evidence;
  return {
    ...evidence,
    promptFamily: evidence.promptFamily || meta.promptFamily || null,
    intentTerritory: evidence.intentTerritory || meta.intentTerritory || null,
    commercialRegion:
      evidence.commercialRegion || meta.commercialRegion || evidence.regionName || null,
    geographyScope: evidence.geographyScope || meta.geographyScope || null,
    countryName: evidence.countryName || meta.country || null,
    language: evidence.language || meta.language || evidence.payload?.language || null,
  };
}

export function resetPromptMetadataCache() {
  cachedMap = null;
}
