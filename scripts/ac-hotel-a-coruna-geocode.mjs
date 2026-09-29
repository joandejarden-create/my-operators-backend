/**
 * Mapbox geocode for AC Hotel A Coruña validated address (optional permanent).
 *   node scripts/ac-hotel-a-coruna-geocode.mjs
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT = path.join(
  __dirname,
  "../reports/hotel-census/ac-hotel-a-coruna-onboarding-v1/PHASE2_GEOCODE.json"
);

const token = process.env.MAPBOX_ACCESS_TOKEN;
const permanent = String(process.env.MAPBOX_PERMANENT_GEOCODING || "") === "1";
const address = "Enrique Mariñas 36, 15009 A Coruña, Spain";

if (!token) {
  const r = { ok: false, reason: "no_mapbox_token", address };
  fs.mkdirSync(path.dirname(OUT), { recursive: true });
  fs.writeFileSync(OUT, JSON.stringify(r, null, 2) + "\n");
  console.log(JSON.stringify(r));
  process.exit(0);
}

const q = encodeURIComponent(address);
const url = `https://api.mapbox.com/geocoding/v5/mapbox.places/${q}.json?access_token=${token}&limit=1&types=address,place&language=es`;
const res = await fetch(url);
const json = await res.json();
const f = json.features?.[0] || null;
const report = {
  ok: res.ok && Boolean(f),
  permanentFlag: permanent,
  address,
  placeName: f?.place_name || null,
  longitude: f?.center?.[0] ?? null,
  latitude: f?.center?.[1] ?? null,
  relevance: f?.relevance ?? null,
  mapboxId: f?.id || null,
  note: permanent
    ? "MAPBOX_PERMANENT_GEOCODING=1 — coordinates eligible for HPC storage"
    : "Permanent geocoding flag off — coordinates for research only until flag enabled",
};
fs.mkdirSync(path.dirname(OUT), { recursive: true });
fs.writeFileSync(OUT, JSON.stringify(report, null, 2) + "\n");
console.log(JSON.stringify(report, null, 2));
