/**
 * Stored provider response corpus — read-only, no provider calls.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..", "..", "..");

export const DEFAULT_RESPONSE_DIRS = Object.freeze([
  path.join(ROOT, "data", "ai-visibility", "legacy-language-backfill-checkpoints", "responses"),
  path.join(ROOT, "data", "ai-visibility", "validation", "presence-validation-candidates", "responses"),
  path.join(ROOT, "data", "ai-visibility", "validation", "presence-holdout-v3-candidates", "responses"),
]);

export const DATASET_NAMESPACE = "DEMO_VALIDATION";

function extractResponseText(obj) {
  if (!obj) return "";
  if (typeof obj === "string") return obj;
  return (
    obj.text ||
    obj.rawText ||
    obj.responseText ||
    obj.content ||
    obj.output ||
    obj.answer ||
    (obj.message && obj.message.content) ||
    (obj.response && extractResponseText(obj.response)) ||
    ""
  );
}

function inferTimestamp(raw, fileName) {
  if (raw.capturedAt) return String(raw.capturedAt);
  if (raw.timestamp) return String(raw.timestamp);
  if (raw.raw?.created_at) {
    const sec = Number(raw.raw.created_at);
    if (Number.isFinite(sec)) return new Date(sec * 1000).toISOString();
  }
  const batchId = raw.batchId || raw.runId || "";
  const m = String(batchId).match(/(\d{8})/);
  if (m) {
    const d = m[1];
    return `${d.slice(0, 4)}-${d.slice(4, 6)}-${d.slice(6, 8)}T00:00:00.000Z`;
  }
  const fm = String(fileName).match(/(\d{8})/);
  if (fm) {
    const d = fm[1];
    return `${d.slice(0, 4)}-${d.slice(4, 6)}-${d.slice(6, 8)}T00:00:00.000Z`;
  }
  return null;
}

function normalizeResponseRecord(raw, fileName, sourceDir) {
  const text = extractResponseText(raw.response || raw);
  if (!text) return null;
  return {
    fileName,
    sourceDir,
    responseId: raw.responseId || raw.slotId || fileName.replace(/\.json$/, ""),
    waveId: raw.batchId || raw.waveId || null,
    runId: raw.runId || null,
    provider: raw.provider || raw.providerMeta?.provider || "unknown",
    model: raw.model || raw.providerMeta?.model || raw.providerMeta?.api || null,
    promptId: raw.promptId || null,
    promptVersion: raw.promptVersion || "1",
    language: raw.language || null,
    geography: raw.geography || raw.commercialRegion || null,
    intentTerritory: raw.intentTerritory || null,
    timestamp: inferTimestamp(raw, fileName),
    text: String(text),
    datasetNamespace: DATASET_NAMESPACE,
  };
}

export function collectStoredResponses(dirs = DEFAULT_RESPONSE_DIRS) {
  const records = [];
  for (const dir of dirs) {
    if (!fs.existsSync(dir)) continue;
    for (const file of fs.readdirSync(dir)) {
      if (!file.endsWith(".json")) continue;
      try {
        const raw = JSON.parse(fs.readFileSync(path.join(dir, file), "utf8"));
        const rec = normalizeResponseRecord(raw, file, dir);
        if (rec) records.push(rec);
      } catch {
        /* skip malformed */
      }
    }
  }
  return records;
}

export function commonCohortKey(record) {
  return [
    record.promptId || "unknown_prompt",
    record.provider || "unknown_provider",
    record.language || "unknown_lang",
    record.geography || "unknown_geo",
    record.promptVersion || "1",
  ].join("|");
}

export function matchBrandInText(text, aliases = []) {
  if (!text || !aliases.length) return { matched: false, ambiguous: false };
  const lower = text.toLowerCase();
  let matched = false;
  for (const alias of aliases) {
    const a = alias.toLowerCase().trim();
    if (!a) continue;
    const escaped = a.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    const re = new RegExp(`\\b${escaped}\\b`, "i");
    if (re.test(text) || lower.includes(a)) {
      matched = true;
      break;
    }
  }
  return { matched, ambiguous: false };
}
