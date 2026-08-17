/**
 * Master candidate universe builder — Cvent challenges + VIC (+ hooks for official dirs).
 * Cvent never becomes production evidence.
 */

import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { cventCandidateFromUrl } from "../census-autopilot-v1/challenge-adapters.js";
import { CANDIDATE_ORIGINS } from "./constants.js";

function hashId(parts) {
  return createHash("sha1").update(parts.filter(Boolean).join("|")).digest("hex").slice(0, 16);
}

/**
 * Load all hotel URLs from cached Cvent country harvests.
 * @param {string} root repo root
 */
export function loadCventHarvestInventories(root) {
  const dir = path.join(root, "reports/cvent-venue-cache/country-results");
  const files = fs.readdirSync(dir).filter((f) => f.startsWith("harvest-") && f.endsWith(".json"));
  const inventories = [];
  for (const f of files) {
    const j = JSON.parse(fs.readFileSync(path.join(dir, f), "utf8"));
    inventories.push({
      file: f,
      country: j.country,
      slug: j.slug,
      hotel_url_count: j.hotel_url_count ?? (j.urls || []).length,
      urls: j.urls || [],
      complete: j.complete,
      harvested_at: j.harvested_at,
    });
  }
  return inventories;
}

/**
 * @param {string} root
 */
export function loadVicRecords(root) {
  const fp = path.join(
    root,
    "data/research-engine-v2/verified-independent-census-mexico-combined-4family/01_combined_4family_index.json"
  );
  const j = JSON.parse(fs.readFileSync(fp, "utf8"));
  return j.records || [];
}

/**
 * Optional V1.3 completeness overlay for Mexico VIC hotels.
 * @param {string} root
 */
export function loadV13Completeness(root) {
  const fp = path.join(
    root,
    "data/research-engine-v2/census-autopilot-v1-3-gap-closure/17-final-hotel-completeness.json"
  );
  if (!fs.existsSync(fp)) return new Map();
  const j = JSON.parse(fs.readFileSync(fp, "utf8"));
  return new Map((j.hotels || []).map((h) => [h.independent_record_id, h]));
}

/**
 * Build master candidate rows (challenge layer + independent layer).
 * @param {{ inventories: object[], vicRecords: object[], now?: string }} input
 */
export function buildMasterCandidateUniverse(input) {
  const now = input.now || new Date().toISOString();
  const candidates = [];

  for (const inv of input.inventories || []) {
    for (const url of inv.urls || []) {
      const parsed = cventCandidateFromUrl(url);
      const name = parsed.candidate_name_from_slug || null;
      const candidate_id = `cand_${hashId(["cvent", parsed.cvent_candidate_id || url])}`;
      candidates.push({
        candidate_id,
        candidate_origin: CANDIDATE_ORIGINS.CVENT_CHALLENGE,
        origin_source: "cvent_latam_caribbean_harvest",
        origin_source_record_id: parsed.cvent_candidate_id || null,
        origin_name: name,
        origin_country: inv.country,
        origin_city: null,
        origin_url: url,
        first_seen: inv.harvested_at || now,
        last_seen: now,
        candidate_status: "ACTIVE_CHALLENGE",
        cvent_used_as_production_evidence: false,
        // NO cvent rooms/address/coords/amenities copied
      });
    }
  }

  for (const r of input.vicRecords || []) {
    const candidate_id = `cand_${hashId(["vic", r.independent_record_id || r.name])}`;
    candidates.push({
      candidate_id,
      candidate_origin: CANDIDATE_ORIGINS.VERIFIED_INDEPENDENT,
      origin_source: "verified_independent_census_mexico_4family",
      origin_source_record_id: r.independent_record_id,
      origin_name: r.name || null,
      origin_country: r.country || "Mexico",
      origin_city: r.city || null,
      origin_url: r.website || null,
      first_seen: now,
      last_seen: now,
      candidate_status: "INDEPENDENT_VERIFIED_SEED",
      cvent_used_as_production_evidence: false,
      brand: r.brand || null,
      family: r.family || null,
      property_ids: r.property_ids || [],
      website: r.website || null,
    });
  }

  return {
    generated_at: now,
    total_candidates: candidates.length,
    cvent_origin_count: candidates.filter((c) => c.candidate_origin === CANDIDATE_ORIGINS.CVENT_CHALLENGE)
      .length,
    independent_origin_count: candidates.filter(
      (c) => c.candidate_origin === CANDIDATE_ORIGINS.VERIFIED_INDEPENDENT
    ).length,
    candidates,
  };
}
