#!/usr/bin/env node
/**
 * CB4 — Build investigated-hotels ledger (~250 NEW distinct identities).
 * Free research / no paid APIs. CUSTOMER_BLOCKED.
 * Dedupes against CB3 cumulative ledger.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/sec-edgar-hif-v6-track-a-cb4-global-commercial-expansion");
const CB3 = path.join(ROOT, "reports/sec-edgar-hif-v6-track-a-cb3-commercial-discovery/INVESTIGATED_HOTELS.json");

const prior = JSON.parse(fs.readFileSync(CB3, "utf8"));
const hotels = [];
const seen = new Set();

const keyOf = (row) => `${row.hotelName}|${row.country}|${row.city || ""}`.toLowerCase();

const add = (row, { isNew = true } = {}) => {
  const key = keyOf(row);
  if (seen.has(key)) return false;
  seen.add(key);
  hotels.push({ ...row, isNewCb4: isNew });
  return true;
};

for (const h of prior.hotels || []) {
  add(
    {
      hotelName: h.hotelName,
      country: h.country,
      city: h.city || null,
      sourceFamily: h.sourceFamily || "CB3_CARRY",
      disposition: h.disposition || "INVESTIGATED",
      note: "CB3_CUMULATIVE_CARRY",
    },
    { isNew: false },
  );
}

const priorCount = hotels.length;

/** AHIP AIF portfolio (Dec 2025 / Mar 2026 AIF) — MCT replacement scope through Apr 30 2027 */
const ahipSoldYtd = new Set([
  "TownePlace Suites Pittsburgh Airport Robinson Township",
  "Embassy Suites Cleveland Rockside",
  "Embassy Suites Columbus Dublin",
  "SpringHill Suites Arundel Mills BWI Airport",
  "Hilton Garden Inn Baltimore White Marsh",
  "Homewood Suites Dover Rockaway",
  "Homewood Suites Allentown West Fogelsville",
  "Embassy Suites Tempe",
  "Holiday Inn Express and Suites Fort Myers East",
  "Residence Inn Chattanooga",
  "TownePlace Suites Chattanooga",
  "Fairfield Inn & Suites Jacksonville West",
]);

const ahip = [
  ["Homewood Suites Allentown West Fogelsville", "US", "Allentown"],
  ["Hampton Inn Asheboro", "US", "Asheboro"],
  ["Hilton Garden Inn Baltimore White Marsh", "US", "Baltimore"],
  ["Courtyard Bismarck North", "US", "Bismarck"],
  ["Residence Inn Chattanooga", "US", "Chattanooga"],
  ["TownePlace Suites Chattanooga", "US", "Chattanooga"],
  ["Embassy Suites Covington", "US", "Covington"],
  ["Homewood Suites Dover Rockaway", "US", "Dover"],
  ["Embassy Suites Columbus Dublin", "US", "Dublin"],
  ["Hampton Inn Emporia", "US", "Emporia"],
  ["Holiday Inn Express and Suites Fort Myers East", "US", "Fort Myers"],
  ["Hampton Inn Hanover", "US", "Hanover"],
  ["SpringHill Suites Arundel Mills BWI Airport", "US", "Hanover"],
  ["Hampton Inn Harrisonburg", "US", "Harrisonburg"],
  ["Embassy Suites Cleveland Rockside", "US", "Independence"],
  ["Fairfield Inn & Suites Jacksonville West", "US", "Jacksonville"],
  ["Staybridge Suites Midland", "US", "Midland"],
  ["Home2 Suites Midland", "US", "Midland"],
  ["Hilton Garden Inn Milford", "US", "Milford"],
  ["Residence Inn Mount Laurel at Bishop's Gate", "US", "Mount Laurel"],
  ["Residence Inn Neptune", "US", "Neptune"],
  ["Fairfield Inn & Suites Orlando Ocoee", "US", "Ocoee"],
  ["Fairfield Inn Pittsburgh", "US", "Pittsburgh"],
  ["TownePlace Suites Pittsburgh Airport Robinson Township", "US", "Pittsburgh"],
  ["Home2 Suites San Angelo", "US", "San Angelo"],
  ["Residence Inn San Angelo", "US", "San Angelo"],
  ["Fairfield Inn & Suites South Hill", "US", "South Hill"],
  ["Courtyard Tampa", "US", "Tampa"],
  ["Embassy Suites Tempe", "US", "Tempe"],
  ["Fairfield Inn & Suites Titusville", "US", "Titusville"],
  ["Courtyard Wall Township", "US", "Wall Township"],
];
for (const [hotelName, country, city] of ahip) {
  const sold = ahipSoldYtd.has(hotelName);
  add({
    hotelName,
    country,
    city,
    sourceFamily: "OWNER_EARNINGS_AHIP_AIMBRIDGE_MCT",
    disposition: sold ? "COMPLETED_OR_HISTORICAL" : "PRIVATE_OPPORTUNITY_PROBABLE_ACCESS",
    note: sold
      ? "Disposed 2026 — investigated for MCT scope completeness"
      : "Remaining AHIP portfolio under Aimbridge through 2027-04-30; replacement manager search open",
  });
}

/** Apple Hospitality 10-Q Mar 31 2026 portfolio scan — completed MCT transitions (contractual history) */
const applePath = path.join(OUT, "APPLE_10Q_HOTELS_EXTRACT.json");
if (fs.existsSync(applePath)) {
  const apple = JSON.parse(fs.readFileSync(applePath, "utf8"));
  for (const h of apple) {
    add({
      hotelName: `${h.brand} ${h.city} ${h.state} (${h.rooms} keys / ${h.manager})`,
      country: "US",
      city: h.city,
      sourceFamily: "OWNER_EARNINGS_APPLE_PORTFOLIO_SCAN",
      disposition: "COMPLETED_OR_HISTORICAL",
      note: `APLE 10-Q Mar 31 2026 portfolio scan; Jan 2026 Marriott-managed→franchise MCT transition completed (not open)`,
    });
  }
}

/** RLJ Midway sale + committed conversions */
const rlj = [
  ["Chicago Marriott Midway", "US", "Chicago", "COMPLETED_OR_HISTORICAL", "RLJ_MIDWAY_SALE"],
  ["Courtyard Chicago Midway Airport", "US", "Chicago", "COMPLETED_OR_HISTORICAL", "RLJ_MIDWAY_SALE"],
  ["Fairfield Inn & Suites Chicago Midway Airport", "US", "Chicago", "COMPLETED_OR_HISTORICAL", "RLJ_MIDWAY_SALE"],
  ["Hampton Inn Chicago Midway Airport", "US", "Chicago", "COMPLETED_OR_HISTORICAL", "RLJ_MIDWAY_SALE"],
  ["Hilton Garden Inn Chicago Midway Airport", "US", "Chicago", "COMPLETED_OR_HISTORICAL", "RLJ_MIDWAY_SALE"],
  ["Holiday Inn Express & Suites Chicago Midway Airport", "US", "Chicago", "COMPLETED_OR_HISTORICAL", "RLJ_MIDWAY_SALE"],
  ["Sleep Inn Midway Airport (Spark by Hilton path)", "US", "Chicago", "COMMITTED_TRANSACTION", "RLJ_MIDWAY_SALE"],
  ["Wyndham Boston Beacon Hill (Tapestry Collection)", "US", "Boston", "COMMITTED_TRANSACTION", "RLJ_CONVERSION"],
  ["Fairfield Inn & Suites Key West (Compass by Margaritaville)", "US", "Key West", "COMMITTED_TRANSACTION", "RLJ_CONVERSION"],
];
for (const [hotelName, country, city, disposition, sourceFamily] of rlj) {
  add({ hotelName, country, city, disposition, sourceFamily });
}

/** Grand Teton CC-GRTE001-27 lodging units — award pending / submissions closed */
const grte = [
  "Jackson Lake Lodge",
  "Jenny Lake Lodge",
  "Colter Bay Village Cabins",
  "Colter Bay Tent Village",
  "Signal Mountain Lodge",
  "Headwaters Lodge & Cabins at Flagg Ranch",
  "Triangle X Ranch",
  "Grand Teton Lodge Company campgrounds cluster",
  "Jenny Lake campground",
  "Signal Mountain campground",
  "Colter Bay RV park / marina services",
  "Jackson Lake marina / boat concessions",
];
for (const hotelName of grte) {
  add({
    hotelName,
    country: "US",
    city: "Grand Teton NP",
    sourceFamily: "PUBLIC_RFP_CONCESSION_NPS",
    disposition: "AWARD_PENDING",
    note: "CC-GRTE001-27 proposals due Jan 2026 — award pending; not open for new submissions",
  });
}

/** Maldives open city-hotel development plots */
const maldives = [
  ["Thimarafushi City Hotel development plot", "MV", "Thimarafushi", "OPEN_COMPETITIVE_OPPORTUNITY"],
  ["Fonadhoo Tourist Hotel plot A (32,129 sq ft)", "MV", "Fonadhoo", "OPEN_COMPETITIVE_OPPORTUNITY"],
  ["Fonadhoo Tourist Hotel plot B (33,801 sq ft)", "MV", "Fonadhoo", "OPEN_COMPETITIVE_OPPORTUNITY"],
  ["Fonadhoo Tourist Hotel plot C (26,736 sq ft)", "MV", "Fonadhoo", "OPEN_COMPETITIVE_OPPORTUNITY"],
];
for (const [hotelName, country, city, disposition] of maldives) {
  add({
    hotelName,
    country,
    city,
    disposition,
    sourceFamily: "PUBLIC_DEV_LEASE_MALDIVES",
    note: "MOTCA bids due 2027-01-07; developer/operator lease 50y",
  });
}

/** Additional open / classified named research */
const named = [
  ["AHIP Remaining Portfolio (Aimbridge MCT replacement)", "US", "Multi", "PRIVATE_OPPORTUNITY_PROBABLE_ACCESS", "OWNER_EARNINGS_AHIP_AIMBRIDGE_MCT"],
  ["Cedar Lake Outdoor Center (Waterloo Recreation Area)", "US", "Chelsea", "OPEN_COMPETITIVE_OPPORTUNITY", "PUBLIC_RFP_CONCESSION_MI"],
  ["Calaveras Big Trees State Park lodging (future RFP)", "US", "Arnold", "COMMERCIALLY_RELEVANT_CONTRACTUAL_CHANGE", "PUBLIC_CONCESSION_CA_PIPELINE"],
  ["Clear Lake State Park lodging (future RFP)", "US", "Kelseyville", "COMMERCIALLY_RELEVANT_CONTRACTUAL_CHANGE", "PUBLIC_CONCESSION_CA_PIPELINE"],
  ["Radisson Hotel La Crosse", "US", "La Crosse", "COMMERCIALLY_RELEVANT_CONTRACTUAL_CHANGE", "ACQUISITION_RELATED_SELECTION"],
  ["ÓLEO Cancún Playa (Distinctive Concept Hotels)", "MX", "Cancún", "COMMITTED_TRANSACTION", "OPERATOR_AWARD_PRESS"],
  ["Apple Lake Union conversion (brand-family change)", "US", "Seattle", "COMMITTED_TRANSACTION", "OWNER_EARNINGS_APPLE"],
  ["DiamondRock remaining franchise decade pipeline (unnamed)", "US", "Multi", "COMMERCIALLY_RELEVANT_CONTRACTUAL_CHANGE", "OWNER_EARNINGS_DRH"],
  ["RLJ Lodging conversion pipeline target (2/year unnamed)", "US", "Multi", "COMMERCIALLY_RELEVANT_CONTRACTUAL_CHANGE", "OWNER_EARNINGS_RLJ"],
  ["India — Lemon Tree conversion demand (unnamed supply)", "IN", "Multi", "COMMERCIALLY_RELEVANT_CONTRACTUAL_CHANGE", "BRAND_DEMAND_IN"],
  ["India — IHCL conversion / management demand (unnamed)", "IN", "Multi", "COMMERCIALLY_RELEVANT_CONTRACTUAL_CHANGE", "BRAND_DEMAND_IN"],
  ["India — Sarovar / Radisson expansion demand (unnamed)", "IN", "Multi", "COMMERCIALLY_RELEVANT_CONTRACTUAL_CHANGE", "BRAND_DEMAND_IN"],
  ["Brazil — Atlantica / Accor conversion demand (unnamed supply)", "BR", "Multi", "COMMERCIALLY_RELEVANT_CONTRACTUAL_CHANGE", "BRAND_DEMAND_BR"],
];
for (const [hotelName, country, city, disposition, sourceFamily] of named) {
  add({ hotelName, country, city, disposition, sourceFamily });
}

const newCount = hotels.filter((h) => h.isNewCb4).length;
const byDisposition = {};
const byCountry = {};
const bySourceFamily = {};
for (const h of hotels) {
  byDisposition[h.disposition] = (byDisposition[h.disposition] || 0) + 1;
  byCountry[h.country] = (byCountry[h.country] || 0) + 1;
  bySourceFamily[h.sourceFamily] = (bySourceFamily[h.sourceFamily] || 0) + 1;
}

const newBySource = {};
for (const h of hotels.filter((x) => x.isNewCb4)) {
  newBySource[h.sourceFamily] = (newBySource[h.sourceFamily] || 0) + 1;
}

const ledger = {
  asOf: "2026-11-21",
  publicationState: "CUSTOMER_BLOCKED",
  wave: "HIF_V6_TRACK_A_CB4_GLOBAL_COMMERCIAL_EXPANSION",
  researchVolumeTargetNew: 250,
  priorCumulativeInvestigated: priorCount,
  newDistinctHotelsInvestigated: newCount,
  cumulativeDeduplicatedHotelsInvestigated: hotels.length,
  stopReason:
    newCount >= 250
      ? "RESEARCH_VOLUME_TARGET_MET"
      : "EXECUTION_LIMIT_BEFORE_VOLUME_TARGET",
  byDisposition,
  byCountry,
  bySourceFamily,
  newBySourceFamily: newBySource,
  hotels,
};

fs.mkdirSync(OUT, { recursive: true });
fs.writeFileSync(path.join(OUT, "INVESTIGATED_HOTELS.json"), JSON.stringify(ledger, null, 2));
console.log(
  JSON.stringify(
    {
      priorCumulativeInvestigated: priorCount,
      newDistinctHotelsInvestigated: newCount,
      cumulativeDeduplicatedHotelsInvestigated: hotels.length,
      stopReason: ledger.stopReason,
      newBySourceFamily: newBySource,
    },
    null,
    2,
  ),
);
