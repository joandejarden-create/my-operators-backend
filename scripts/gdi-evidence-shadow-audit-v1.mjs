/**
 * Read-only shadow audit. Prints ids and classes only. Does not write stores.
 */
import fs from "node:fs";
import path from "node:path";
import { classifyCommercialDemandThesis } from "../lib/group-demand-intelligence/commercial-demand-thesis-standard-v1.js";
import { shadowQualification } from "./gdi-complete-hotel-inventory-audit-v1.mjs";

const hotelsDir = path.resolve("data/group-demand-intelligence/hotels");
const totals = { rows: 0, withFactText: 0, withoutFactText: 0 };
const thesis = {};
const layers = {};
const survivors = [];
const ranked = [];

for (const hotelId of fs.readdirSync(hotelsDir)) {
  if (hotelId.startsWith("gdi_hotel_")) continue;
  const file = path.join(hotelsDir, hotelId, "opportunities.json");
  if (!fs.existsSync(file)) continue;
  const doc = JSON.parse(fs.readFileSync(file, "utf8"));
  for (const opportunity of doc.opportunities || []) {
    totals.rows += 1;
    const factText = [
      ...(opportunity.evidence || []).map((item) => item?.value),
      ...(opportunity.sources || []).map((source) => source?.claim || source?.establishes),
    ].filter((value) => typeof value === "string" && value.trim());
    if (factText.length) totals.withFactText += 1;
    else totals.withoutFactText += 1;
    const classified = classifyCommercialDemandThesis(opportunity);
    const shadow = shadowQualification(opportunity);
    thesis[classified.thesisStandard] = (thesis[classified.thesisStandard] || 0) + 1;
    layers[shadow.layer] = (layers[shadow.layer] || 0) + 1;
    if (classified.thesisStandard !== "SPECULATIVE_DEMAND" || shadow.layer !== "RESEARCH_CONTINUATION") {
      survivors.push({
        hotelId,
        id: opportunity.id,
        thesis: classified.thesisStandard,
        layer: shadow.layer,
        contactClass: shadow.contactClass,
        eventStartDate: opportunity.eventStartDate || null,
      });
    }
    const relevant = shadow.contactClass === "DIRECT_BUYER" || shadow.contactClass === "RELEVANT_COMMERCIAL_ENTRY_POINT";
    const future = String(opportunity.eventStartDate || "") >= "2026-11-11";
    if (classified.thesisStandard === "SPECULATIVE_DEMAND" && relevant && future) {
      ranked.push({
        hotelId,
        id: opportunity.id,
        contactClass: shadow.contactClass,
        eventStartDate: opportunity.eventStartDate,
        score: (shadow.contactClass === "DIRECT_BUYER" ? 3 : 2) + (factText.length ? 1 : 0),
      });
    }
  }
}

ranked.sort((a, b) => b.score - a.score || String(a.eventStartDate).localeCompare(String(b.eventStartDate)));
console.log(JSON.stringify({ totals, thesis, layers, survivors, ranked: ranked.slice(0, 12) }, null, 2));
