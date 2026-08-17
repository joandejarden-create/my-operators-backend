#!/usr/bin/env node
/**
 * Dominican Republic radar fixture writer — source-backed demand anchors.
 *   node scripts/build-dominican-republic-countrywide-fixtures.mjs
 */
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { DOMINICAN_REPUBLIC_DEMAND_ANCHOR_POINTS } from "../lib/radar-buildout/dominican-republic-demand-anchors-points.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");

const DR_SUBMARKETS = [
  "Punta Cana / Bávaro / Cap Cana",
  "Santo Domingo Metro",
  "Puerto Plata / Sosúa / Cabarete",
  "La Romana / Bayahibe",
  "Samaná / Las Terrenas",
  "Santiago / Cibao",
  "Miches / Costa Esmeralda",
  "Barahona / Pedernales",
  "Boca Chica / Juan Dolio",
  "Jarabacoa / Constanza",
  "Other",
];

const POINT_TYPE_TARGETS = {
  "Beach / Waterfront": { min: 12, max: 18 },
  "Tourist Attraction": { min: 10, max: 15 },
  "Entertainment District": { min: 5, max: 8 },
  "Convention Center": { min: 4, max: 6 },
  "Medical Campus": { min: 4, max: 6 },
  "University / College": { min: 4, max: 6 },
  "Business District": { min: 4, max: 6 },
  "Mixed-Use Development": { min: 4, max: 6 },
  "Sports Venue": { min: 3, max: 5 },
  "Industrial / Logistics Zone": { min: 3, max: 5 },
  "Government / Civic Center": { min: 3, max: 5 },
  "Future Growth Node": { min: 3, max: 6 },
};

const byType = {};
const bySubmarket = {};
for (const p of DOMINICAN_REPUBLIC_DEMAND_ANCHOR_POINTS) {
  byType[p.pointType] = (byType[p.pointType] || 0) + 1;
  bySubmarket[p.submarket] = (bySubmarket[p.submarket] || 0) + 1;
}

const demandAnchorsFixture = {
  market: "Dominican Republic",
  country: "Dominican Republic",
  region: "Caribbean",
  buildStrategy: "Corridor-Based Resort Country",
  status: "source_backed",
  generatedAt: new Date().toISOString().slice(0, 10),
  submarkets: DR_SUBMARKETS,
  pointTypeTargets: POINT_TYPE_TARGETS,
  summary: {
    totalPoints: DOMINICAN_REPUBLIC_DEMAND_ANCHOR_POINTS.length,
    byPointType: byType,
    bySubmarket,
  },
  points: DOMINICAN_REPUBLIC_DEMAND_ANCHOR_POINTS,
};

const travelInfraFixture = {
  market: "Dominican Republic",
  country: "Dominican Republic",
  region: "Caribbean",
  buildStrategy: "Corridor-Based Resort Country",
  status: "template_only",
  instructions: [
    "Add source-backed Travel Infrastructure records to points[] before import.",
    "Target first pass: 15–25 additional records beyond global airport/cruise backfill.",
  ],
  submarkets: DR_SUBMARKETS,
  points: [],
};

const paths = [
  "fixtures/demand-anchors-dominican-republic-countrywide-real.json",
  "public/fixtures/demand-anchors-dominican-republic-countrywide-real.json",
  "fixtures/travel-infrastructure-dominican-republic-additional-real.json",
  "public/fixtures/travel-infrastructure-dominican-republic-additional-real.json",
];

writeFileSync(join(root, paths[0]), JSON.stringify(demandAnchorsFixture, null, 2) + "\n");
writeFileSync(join(root, paths[1]), JSON.stringify(demandAnchorsFixture, null, 2) + "\n");
writeFileSync(join(root, paths[2]), JSON.stringify(travelInfraFixture, null, 2) + "\n");
writeFileSync(join(root, paths[3]), JSON.stringify(travelInfraFixture, null, 2) + "\n");

console.log("DR demand anchors fixture written:", DOMINICAN_REPUBLIC_DEMAND_ANCHOR_POINTS.length, "records");
console.log("By submarket:", bySubmarket);
console.log("By point type:", byType);
