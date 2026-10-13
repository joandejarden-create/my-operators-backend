/**
 * Hotel-only shared inputs for Webhound ↔ native teacher diagnostic.
 * No owners, domains, people, curated URLs, or scoring keys in runner inputs.
 */
import { createHash } from "node:crypto";
import { readFileSync, existsSync } from "node:fs";
import path from "node:path";
import {
  COHORT_LABEL,
  EXPOSURE,
  FORBIDDEN_INPUT_KEYS,
  HOTEL_IDS,
  SOURCE_FREEZE_REL,
  WEBHOUND_NATIVE_TEACHER_DIAGNOSTIC_VERSION,
} from "./config.js";

export function stripForbiddenKeys(obj) {
  if (!obj || typeof obj !== "object") return obj;
  const out = { ...obj };
  for (const k of FORBIDDEN_INPUT_KEYS) {
    if (k in out) delete out[k];
  }
  // Nested gold / teacher payloads
  delete out.gold;
  delete out.scoring;
  delete out.expected;
  return out;
}

export function hotelOnlyInput(row) {
  const r = stripForbiddenKeys(row || {});
  return {
    hotel_id: String(r.hotel_id || r.id || "").trim(),
    hotel_name: String(r.hotel_name || r.name || "").trim(),
    city: r.city != null ? String(r.city).trim() : null,
    country: r.country != null ? String(r.country).trim() : null,
    language: r.language != null ? String(r.language).trim() : null,
  };
}

export function loadSourceFreeze(repoRoot) {
  const p = path.join(repoRoot, SOURCE_FREEZE_REL);
  if (!existsSync(p)) {
    throw new Error(`Missing source freeze: ${SOURCE_FREEZE_REL}`);
  }
  const raw = JSON.parse(readFileSync(p, "utf8"));
  const hotels = Array.isArray(raw.hotels) ? raw.hotels : [];
  return { path: SOURCE_FREEZE_REL, raw, hotels };
}

/**
 * Build frozen cohort of exactly five hotels — no protected held-out hotels.
 */
export function buildFrozenCohort(repoRoot) {
  const { path: freezePath, hotels: sourceHotels } = loadSourceFreeze(repoRoot);
  const byId = new Map(sourceHotels.map((h) => [String(h.hotel_id || h.id), h]));
  const hotels = [];
  for (const id of HOTEL_IDS) {
    const src = byId.get(id);
    if (!src) {
      throw new Error(`Hotel ${id} missing from ${freezePath}`);
    }
    hotels.push(hotelOnlyInput(src));
  }
  if (hotels.length !== 5) {
    throw new Error(`Expected 5 hotels, got ${hotels.length}`);
  }
  const payload = {
    version: WEBHOUND_NATIVE_TEACHER_DIAGNOSTIC_VERSION,
    cohort_label: COHORT_LABEL,
    exposure: EXPOSURE,
    protected_held_out_hotels: [],
    note:
      "Five hotels only. No held-out set. Hotel identity fields only — no known owners, domains, people, or curated URLs.",
    source_freeze: freezePath,
    hotel_ids: hotels.map((h) => h.hotel_id),
    hotels,
    frozen_at: new Date().toISOString(),
  };
  payload.sha256 = createHash("sha256").update(JSON.stringify(hotels)).digest("hex");
  return payload;
}

/**
 * Webhound research query: hotel identity only (approved adapter startReport query).
 */
export function buildWebhoundHotelOnlyQuery(hotel) {
  const h = hotelOnlyInput(hotel);
  return [
    "Research hotel property ownership / economic sponsor for this hotel only.",
    "Hotel identity (only inputs provided — no known owners, domains, people, or curated URLs):",
    `- hotel_id: ${h.hotel_id}`,
    `- hotel_name: ${h.hotel_name}`,
    `- city: ${h.city || "(unknown)"}`,
    `- country: ${h.country || "(unknown)"}`,
    `- language: ${h.language || "(unknown)"}`,
    "",
    "Goals:",
    "1) Find hotel-specific ownership or sponsor evidence with passages and dates.",
    "2) Prefer current ownership; flag historical/sale language and follow up.",
    "3) Separate operator vs brand vs owner when possible.",
    "4) If an owner organization appears, note any public organization domain.",
    "5) Do not invent parties. If uncertain, say so.",
    "6) Contact enrichment is out of scope — do not prioritize paid people/email discovery.",
    "",
    "Return structured findings when possible: owner/sponsor candidates, relationship type,",
    "currentness/date, organization/domain, person candidates (if incidental), confidence, uncertainty.",
  ].join("\n");
}

/**
 * Native path hotel object — same hotel-only shape.
 */
export function buildNativeHotelInput(hotel) {
  return hotelOnlyInput(hotel);
}
