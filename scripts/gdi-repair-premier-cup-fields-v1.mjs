/**
 * Repair Bethesda Premier Cup geography + peak-rooms/housing field split.
 * Dry-run by default; --apply writes via canonical Airtable persistence.
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  loadOpportunitiesCanonical,
  saveOpportunitiesCanonical,
} from "../lib/group-demand-intelligence/opportunity-persistence.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const apply = process.argv.includes("--apply");
const hotelId = "recLuxvwwxID7U2B8";
const oppId = "gdi_opp_bethesda_premier_cup_2026";
const outDir = path.join(
  ROOT,
  "reports/gdi-operational-baseline/drawer-field-repairs/premier-cup-apply"
);
fs.mkdirSync(outDir, { recursive: true });

process.env.GDI_OPPORTUNITIES_PERSISTENCE = "airtable";
delete process.env.GDI_DATA_ROOT;

const doc = await loadOpportunitiesCanonical(hotelId);
const before = (doc.opportunities || []).find((o) => o.id === oppId);
if (!before) {
  console.error("Premier Cup opportunity not found");
  process.exit(1);
}

const multiSite =
  "Multi-site DMV fields (not Bethesda-centered): Alexandria VA (U9–U12); Poolesville; Olney; Liberty Park Upper Marlboro (U15 boys); Maryland SoccerPlex Boyds (U16–U19)";

const patched = {
  ...before,
  peakRooms: null,
  estimatedPeakRooms: null,
  peakRoomsStatus: "UNKNOWN",
  roomDemandStatus: "VERIFIED_HOUSING_PROGRAM",
  roomDemandStatusLabel: "Verified Housing Program",
  roomDemandStatusNote:
    "Official stay-to-play housing program via HBC Event Services. Housing-program status is not a peak-room count and alone does not establish confirmed overflow or pursue-now buying.",
  housingStatus: before.housingStatus || "HOUSING_OPEN",
  destinationStatus: multiSite,
  eventLocationSummary: multiSite,
  summaryWhat:
    "Bethesda Soccer Club hosts Bethesda Premier Cup (2026-11-13 – 2026-11-22) across multi-site DMV fields. HBC Event Services runs the official stay-to-play housing program.",
  hotelOpportunityThesis:
    "Local Bethesda identity and inventory can matter for teams whose fields are within a commercially reasonable drive of Bethesda Marriott — only if the hotel is on the official HBC list for those age groups. Multi-site geography means Bethesda is not automatically the right lodging node for every division.",
  summaryWhyHotel:
    "Local Bethesda identity and inventory can matter for teams whose fields are within a commercially reasonable drive of Bethesda Marriott — only if the hotel is on the official HBC list for those age groups. Multi-site geography means Bethesda is not automatically the right lodging node for every division.",
  competitiveContextNote:
    "Housing-program evidence alone does not prove peak rooms, confirmed overflow at this hotel, or an open buying mandate. Field locations vary by age group.",
  updatedAt: new Date().toISOString(),
  drawerFieldRepair: "gdi-drawer-field-repairs-v1-premier-cup",
};

const nextOpps = (doc.opportunities || []).map((o) =>
  o.id === oppId ? patched : o
);

const diff = {
  id: oppId,
  before: {
    peakRooms: before.peakRooms,
    estimatedPeakRooms: before.estimatedPeakRooms,
    roomDemandStatusLabel: before.roomDemandStatusLabel,
    eventLocationSummary: before.eventLocationSummary,
    destinationStatus: (before.destinationStatus || "").slice(0, 120),
  },
  after: {
    peakRooms: patched.peakRooms,
    estimatedPeakRooms: patched.estimatedPeakRooms,
    roomDemandStatusLabel: patched.roomDemandStatusLabel,
    eventLocationSummary: patched.eventLocationSummary.slice(0, 120),
    destinationStatus: patched.destinationStatus.slice(0, 120),
  },
};

fs.writeFileSync(
  path.join(outDir, `diff-${apply ? "apply" : "dryrun"}.json`),
  JSON.stringify(diff, null, 2) + "\n"
);
fs.writeFileSync(
  path.join(outDir, `backup-before.json`),
  JSON.stringify(before, null, 2) + "\n"
);

if (apply) {
  await saveOpportunitiesCanonical(hotelId, {
    ...doc,
    opportunities: nextOpps,
    updatedAt: new Date().toISOString(),
  });
}

console.log(
  JSON.stringify({ ok: true, apply, hotelId, oppId, diff }, null, 2)
);
