/**
 * Packet 2.8B — pilot hotel seed store (manifest + local fixtures).
 * No new cohort files. No hotel-specific presentation logic.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../../..");

const MANIFEST_PATH = path.join(ROOT, "hotel-intelligence-batches/batch_cala_25_v1/manifest.json");
const TA_PHASE2 = path.join(
  ROOT,
  "reports/hotel-intelligence/tripadvisor-apify-benchmark-v1/phase2-known-results.json"
);
const TA_PHASE3 = path.join(
  ROOT,
  "reports/hotel-intelligence/tripadvisor-apify-benchmark-v1/phase3-missing-results.json"
);

let _cache = null;

function loadJson(p) {
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function buildSeedIndex() {
  const byId = new Map();
  const manifest = loadJson(MANIFEST_PATH);
  for (const h of manifest?.hotels || []) {
    byId.set(h.hotel_id, {
      hotel_id: h.hotel_id,
      name: h.name,
      country: h.country,
      difficulty: h.difficulty,
      source: "batch_manifest",
    });
  }
  for (const file of [TA_PHASE2, TA_PHASE3]) {
    const rows = loadJson(file);
    const list = Array.isArray(rows) ? rows : rows?.results || rows?.hotels || [];
    for (const row of list) {
      const id = row.dealality_record_id || row.hotel_id || row.record_id;
      if (!id) continue;
      const prev = byId.get(id) || { hotel_id: id };
      byId.set(id, {
        ...prev,
        hotel_id: id,
        name: prev.name || row.name || row.hotel_name,
        country: prev.country || row.country,
        city: row.city || row.market || prev.city || null,
        rooms: row.rooms ?? row.keys ?? prev.rooms ?? null,
        brand: row.brand || row.affiliation || prev.brand || null,
        chain_scale: row.chain_scale || row.chainScale || prev.chain_scale || null,
        lat: row.lat ?? row.latitude ?? prev.lat ?? null,
        lng: row.lng ?? row.longitude ?? prev.lng ?? null,
        source: prev.source ? `${prev.source}+tripadvisor_benchmark` : "tripadvisor_benchmark",
      });
    }
  }
  // Golden four display names for regression assemble
  const goldens = [
    { hotel_id: "recUNycnMwOVFX0hc", name: "Krystal Grand Puerto Vallarta", country: "Mexico", city: "Puerto Vallarta" },
    { hotel_id: "recIwaP1etgx2g9nA", name: "Cambridge Beaches Resort & Spa", country: "Bermuda", city: "Sandys" },
    { hotel_id: "recsYJb2R1jarPpK3", name: "Sheraton Guadalajara Expo", country: "Mexico", city: "Zapopan" },
    { hotel_id: "recTYaiA4S6fR6ixx", name: "voco Cancún Zona Hotelera", country: "Mexico", city: "Cancún" },
  ];
  for (const g of goldens) {
    const prev = byId.get(g.hotel_id) || {};
    byId.set(g.hotel_id, { ...g, ...prev, hotel_id: g.hotel_id, name: prev.name || g.name });
  }
  return byId;
}

export function getHotelSeed(hotelId) {
  if (!_cache) _cache = buildSeedIndex();
  const id = String(hotelId || "").trim();
  return _cache.get(id) || { hotel_id: id, name: null, source: "unknown" };
}

export function listPilotHotelIds() {
  const manifest = loadJson(MANIFEST_PATH);
  return (manifest?.hotels || []).map((h) => h.hotel_id);
}

export function getCanaryHotelIds() {
  // Stratified canary from approved 25 — max archetype variety (Packet 2.8B Part 7)
  return [
    "rec0qmO7Xj7uyjWLZ", // Comfort Inn Querétaro — easy franchise MX
    "recsn3BUKJ9PNfeZW", // JW Monterrey Valle — Aimbridge third-party
    "recGZZCek9vDQGG1L", // voco GDL Expo — Alliance package / reflag
    "receb9kXfXpygywj3", // Occidental Caribe — DR reflag
    "recRGqxkCLrdrgc9y", // Kimpton Seafire — island luxury hard
  ];
}

export function getGenericHotel5ProofId() {
  // No cohort, no adapter — Comfort Inn Querétaro
  return "rec0qmO7Xj7uyjWLZ";
}
