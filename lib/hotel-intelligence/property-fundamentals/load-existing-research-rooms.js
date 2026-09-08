/**
 * Load existing Hotel Intelligence research corpora for property-fundamental fallback.
 * Read-only — never starts paid Webhound.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { extractRoomsObservationsForHotels } from "./rooms-extractor.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(__dirname, "../../..");

export const CAMBRIDGE_RESEARCH_RAW_DIR = path.join(
  REPO_ROOT,
  "data/hotel-intelligence/research/hotels/recIwaP1etgx2g9nA/raw"
);

const DEFAULT_CORPUS_FILES = [
  "webhound-9d6b0a8d-output.md",
  "webhound-9d6b0a8d-evidence-slim.json",
];

function extractMarkdownFromSlimEvidence(slim) {
  if (!slim || typeof slim !== "object") return "";
  const docs = slim.documents || slim.working_documents || slim.docs || [];
  if (Array.isArray(docs)) {
    return docs
      .map((d) => d.content_markdown || d.markdown || d.content || "")
      .filter(Boolean)
      .join("\n\n");
  }
  return "";
}

/**
 * Load text corpora already on disk for a hotel research folder.
 */
export function loadExistingResearchCorpusTexts(rawDir = CAMBRIDGE_RESEARCH_RAW_DIR, files = DEFAULT_CORPUS_FILES) {
  const texts = [];
  for (const file of files) {
    const full = path.join(rawDir, file);
    if (!fs.existsSync(full)) continue;
    const raw = fs.readFileSync(full, "utf8");
    if (file.endsWith(".json")) {
      try {
        const parsed = JSON.parse(raw);
        const md = extractMarkdownFromSlimEvidence(parsed);
        if (md) texts.push(md);
        // Also stringify claim evidence snippets lightly
        const claims = parsed.claims || parsed.claim_traces || [];
        if (Array.isArray(claims) && claims.length) {
          texts.push(
            claims
              .map((c) => [c.claim, c.evidence, c.explanation].filter(Boolean).join(" "))
              .join("\n")
          );
        }
      } catch {
        texts.push(raw);
      }
    } else {
      texts.push(raw);
    }
  }
  return texts;
}

/**
 * Build research rooms observations from existing corpora (no network).
 */
export function buildResearchRoomsObservationsFromExistingCorpus(hotels, opts = {}) {
  const texts = opts.texts || loadExistingResearchCorpusTexts(opts.rawDir, opts.files);
  return extractRoomsObservationsForHotels(texts, hotels, {
    source_type: opts.source_type || "official_press_or_research_corpus",
    source_provider: opts.source_provider || "webhound_existing_artifact",
    source_title: opts.source_title || "Existing Hotel Intelligence research corpus",
    defaultConfidence: opts.defaultConfidence || "HIGH",
    observed_at: opts.observed_at || null,
  });
}

/**
 * Merge extracted observations into portfolio asset rooms (in-memory only).
 * Does not invent hard-coded per-hotel constants.
 */
export function mergeResearchRoomsIntoAssets(assets = [], observations = []) {
  const byName = new Map(
    observations.map((o) => [String(o.hotel_name || "").toLowerCase(), o])
  );
  return (assets || []).map((asset) => {
    const key = String(asset.name || "").toLowerCase();
    const obs = byName.get(key);
    if (!obs) return { ...asset };
    const next = { ...asset };
    if (obs.field === "accommodation_units") {
      next.accommodation_units = obs.value;
      next.accommodation_units_provenance = obs;
      // Do not coerce tents/units into rooms
      return next;
    }
    if ((next.rooms == null || Number(next.rooms) <= 0) && obs.value) {
      next.rooms = obs.value;
      next.rooms_confidence = obs.confidence || "HIGH";
      next.rooms_source_type = obs.source_type || "validated_research";
      next.rooms_source_url = obs.source_url || null;
      next.rooms_source_title = obs.source_title || null;
      next.rooms_provenance = obs;
    }
    return next;
  });
}
