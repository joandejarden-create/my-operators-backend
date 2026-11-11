/**
 * Dry-run manifest for copying Aimbridge research-state directories.
 * Refuses --apply. Does not write hotel stores or configs.
 */
import fs from "node:fs";
import path from "node:path";
import { RESEARCH_ONLY_HOTELS_V1 } from "../lib/group-demand-intelligence/research/gdi-research-only-hotel-inventory-v1.js";

const sourceRoot = "C:/Dev/dealality/my-operators-backend-aimbridge-three-hotel-e2e";
const hotels = [];
for (const hotel of RESEARCH_ONLY_HOTELS_V1) {
  const dir = path.join(sourceRoot, "data", "group-demand-intelligence", "hotels", hotel.hotelId);
  const config = path.join(sourceRoot, "config", "group-demand-intelligence", "hotels", `${hotel.hotelId}.json`);
  const segmentDir = path.join(dir, "full-e2e-fourteen-segment-state");
  const researchDir = path.join(dir, "opportunity-research-state");
  hotels.push({
    hotelId: hotel.hotelId,
    displayName: hotel.displayName,
    visibility: "RESEARCH_ONLY",
    configPresent: fs.existsSync(config),
    segmentFiles: fs.existsSync(segmentDir) ? fs.readdirSync(segmentDir).filter((file) => file.endsWith(".json")).length : 0,
    researchStateFiles: fs.existsSync(researchDir) ? fs.readdirSync(researchDir).filter((file) => file.endsWith(".json")).length : 0,
    wouldCreateOpportunitiesJson: false,
    wouldPublish: false,
  });
}

if (process.argv.includes("--apply")) {
  console.log(JSON.stringify({ ok: false, error: "import_not_approved" }));
  process.exit(2);
}

console.log(JSON.stringify({
  ok: true,
  applied: false,
  action: "DRY_RUN_ONLY",
  note: "Copy would place research-state directories and config files beside the canonical hotel id. It would not create opportunities.json or customer publication.",
  hotels,
}, null, 2));
