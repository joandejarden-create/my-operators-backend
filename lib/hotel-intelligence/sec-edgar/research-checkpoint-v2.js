/**
 * Persistent checkpoints for resumable historical SEC research.
 */

import fs from "node:fs";
import path from "node:path";

export const RESEARCH_CHECKPOINT_V2 = "sec-edgar-research-checkpoint-v2";

export function defaultCheckpointPath(cwd = process.cwd()) {
  return path.join(cwd, "data", "sec-edgar", "checkpoints", "marriott-hilton-historical-v2.json");
}

export function loadCheckpoint(filePath = defaultCheckpointPath()) {
  if (!fs.existsSync(filePath)) {
    return {
      version: RESEARCH_CHECKPOINT_V2,
      updatedAt: null,
      companies: {},
      processedDocuments: {},
      stats: {},
    };
  }
  try {
    return JSON.parse(fs.readFileSync(filePath, "utf8"));
  } catch {
    return {
      version: RESEARCH_CHECKPOINT_V2,
      updatedAt: null,
      companies: {},
      processedDocuments: {},
      stats: {},
      error: "CHECKPOINT_PARSE_FAILED",
    };
  }
}

export function saveCheckpoint(checkpoint, filePath = defaultCheckpointPath()) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  const payload = {
    ...checkpoint,
    version: RESEARCH_CHECKPOINT_V2,
    updatedAt: new Date().toISOString(),
  };
  fs.writeFileSync(filePath, JSON.stringify(payload, null, 2), "utf8");
  return payload;
}

export function docKey(cik, accession, primaryDocument) {
  return `${cik}|${accession}|${primaryDocument}`;
}

export function isDocumentProcessed(checkpoint, cik, accession, primaryDocument) {
  return Boolean(checkpoint.processedDocuments?.[docKey(cik, accession, primaryDocument)]);
}

export function markDocumentProcessed(checkpoint, cik, accession, primaryDocument, meta = {}) {
  const key = docKey(cik, accession, primaryDocument);
  checkpoint.processedDocuments = checkpoint.processedDocuments || {};
  checkpoint.processedDocuments[key] = {
    at: new Date().toISOString(),
    ...meta,
  };
  return checkpoint;
}

export function markCompanyCoverage(checkpoint, slug, coverage = {}) {
  checkpoint.companies = checkpoint.companies || {};
  checkpoint.companies[slug] = {
    ...(checkpoint.companies[slug] || {}),
    ...coverage,
    updatedAt: new Date().toISOString(),
  };
  return checkpoint;
}
