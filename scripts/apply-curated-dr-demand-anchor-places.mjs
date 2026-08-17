#!/usr/bin/env node
/**
 * Apply curated Google/official place data to DR demand anchors.
 *   node scripts/apply-curated-dr-demand-anchor-places.mjs
 *   node scripts/apply-curated-dr-demand-anchor-places.mjs --apply
 */
import "../load-env.js";
import { DEMAND_ANCHORS_FIELDS as F } from "../lib/demand-anchors/airtable-demand-anchors-fields.js";
import { getDemandAnchorsAirtableConfig, resolveDemandAnchorsTableName } from "../lib/demand-anchors/demand-anchors-base.js";
import { fetchDemandAnchorRecords } from "../lib/demand-anchors/airtable-demand-anchors-io.js";
import { DR_CURATED_MAP_PLACES } from "../lib/radar-buildout/dominican-republic-curated-map-places.js";
import { haversineKm } from "../lib/demand-anchors/coordinate-verification.js";
import { todayIsoDate } from "../lib/demand-anchors/coordinate-verification.js";

const APPLY = process.argv.includes("--apply");
const MIN_DRIFT_KM = 0.15;

const { allPoints } = await fetchDemandAnchorRecords({
  country: "Dominican Republic",
  includeHidden: true,
});

const updates = [];
for (const p of allPoints || []) {
  const curated = DR_CURATED_MAP_PLACES[p.name];
  if (!curated) continue;
  const drift = haversineKm(p.latitude, p.longitude, curated.latitude, curated.longitude);
  const nameChange = curated.name && curated.name !== p.name;
  if (drift < MIN_DRIFT_KM && !nameChange && !curated.address) continue;
  updates.push({ point: p, curated, driftKm: drift, nameChange });
}

console.log(APPLY ? "=== APPLY ===" : "=== DRY RUN ===");
console.log("Curated entries:", Object.keys(DR_CURATED_MAP_PLACES).length);
console.log("Updates needed:", updates.length);

for (const u of updates.sort((a, b) => b.driftKm - a.driftKm)) {
  console.log(
    `${u.driftKm.toFixed(2)} km | ${u.point.name}` +
      (u.nameChange ? ` → rename ${u.curated.name}` : "") +
      ` | (${u.point.latitude}, ${u.point.longitude}) → (${u.curated.latitude}, ${u.curated.longitude})`
  );
}

if (!APPLY || !updates.length) {
  if (!APPLY) console.log("\nRe-run with --apply to write Airtable.");
  process.exit(0);
}

const cfg = getDemandAnchorsAirtableConfig();
const table = await resolveDemandAnchorsTableName(cfg.baseId, cfg.apiKey);
let ok = 0;
let fail = 0;

for (const u of updates) {
  const patch = {
    [F.lat]: u.curated.latitude,
    [F.lng]: u.curated.longitude,
    [F.lastVerified]: todayIsoDate(),
    [F.includeOnRadarMap]: true,
  };
  if (u.curated.name) patch[F.name] = u.curated.name;
  if (u.curated.city) patch[F.city] = u.curated.city;
  if (u.curated.address) patch[F.address] = u.curated.address;
  try {
    await cfg.base(table).update(u.point.id, patch, { typecast: true });
    ok += 1;
  } catch (err) {
    fail += 1;
    console.error("FAIL", u.point.name, err?.message || err);
  }
  await new Promise((r) => setTimeout(r, 220));
}

console.log("\nApplied:", ok, "failed:", fail);
