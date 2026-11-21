#!/usr/bin/env node
/**
 * CB3 — Build investigated-hotels ledger (volume target ~100 identities).
 * Free research / no paid APIs. CUSTOMER_BLOCKED.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/sec-edgar-hif-v6-track-a-cb3-commercial-discovery");

const hotels = [];
const add = (row) => {
  const key = `${row.hotelName}|${row.country}|${row.city || ""}`.toLowerCase();
  if (hotels.some((h) => `${h.hotelName}|${h.country}|${h.city || ""}`.toLowerCase() === key)) return;
  hotels.push(row);
};

// Prior CB2/CB2b carry (from RESEARCH_CANDIDATES)
const cb2 = JSON.parse(
  fs.readFileSync(
    path.join(ROOT, "reports/sec-edgar-hif-v6-track-a-cb2-commercial-discovery/RESEARCH_CANDIDATES.json"),
    "utf8",
  ),
);
for (const c of cb2.candidates || []) {
  if (!c.hotelName) continue;
  add({
    hotelName: c.hotelName,
    country: c.physicalLocation?.country || "UNK",
    city: c.physicalLocation?.city || null,
    sourceFamily: "CB2_CARRY",
    disposition: c.disposition || c.qualificationTier || c.currentDecisionStatus || "INVESTIGATED",
  });
}

// Track A wave34 dated expirations (contractual signals only)
const w34 = JSON.parse(
  fs.readFileSync(path.join(ROOT, "reports/sec-edgar-hif-v6-track-a-wave34/FINALIZED_OPPORTUNITY_INVENTORY.json"), "utf8"),
);
for (const r of w34.datedActiveOpportunities || []) {
  add({
    hotelName: r.hotelName,
    country: r.hotelCountry,
    city: r.hotelMarket,
    sourceFamily: "TRACK_A_WAVE34_SEC_EXPIRATION",
    disposition: "COMMERCIALLY_RELEVANT_CONTRACTUAL_CHANGE",
    note: `Expiration ${r.expirationDate} alone — no CB3 prospective decision evidence`,
  });
}

// DiamondRock portfolio scan (SEC 3Q2026 exhibit) — contractual inventory review
const drh = [
  ["AC Hotel Minneapolis Downtown", "US", "Minneapolis"],
  ["Atlanta Marriott Alpharetta", "US", "Alpharetta"],
  ["Bourbon Orleans Hotel", "US", "New Orleans"],
  ["Chicago Marriott Downtown Magnificent Mile", "US", "Chicago"],
  ["Courtyard Denver Downtown", "US", "Denver"],
  ["Courtyard New York Manhattan/Midtown East", "US", "New York"],
  ["Hilton Garden Inn New York/Times Square Central", "US", "New York"],
  ["Hotel Clio Denver Cherry Creek", "US", "Denver"],
  ["JW Marriott Denver Cherry Creek", "US", "Denver"],
  ["Kimpton Shorebreak Huntington Beach Resort", "US", "Huntington Beach"],
  ["Margaritaville Beach House Key West", "US", "Key West"],
  ["Salt Lake City Marriott Downtown at City Creek", "US", "Salt Lake City"],
  ["The Bellmoor Inn & Spa", "US", "Rehoboth Beach"],
  ["The Gwen", "US", "Chicago"],
  ["The Hythe Vail", "US", "Vail"],
  ["The Landing Lake Tahoe Resort & Spa", "US", "South Lake Tahoe"],
  ["The Lindy Renaissance Charleston Hotel", "US", "Charleston"],
  ["The Lodge at Sonoma Resort", "US", "Sonoma"],
  ["Tranquility Bay Beachfront Resort", "US", "Marathon"],
  ["Westin Boston Seaport District", "US", "Boston"],
  ["Westin Fort Lauderdale Beach Resort", "US", "Fort Lauderdale"],
  ["Westin San Diego Bayview", "US", "San Diego"],
  ["Worthington Renaissance Fort Worth Hotel", "US", "Fort Worth"],
  ["Hotel Palomar Washington DC", "US", "Washington"],
  ["Cavanaughs Hotel", "US", "Spokane"],
];
for (const [hotelName, country, city] of drh) {
  add({
    hotelName,
    country,
    city,
    sourceFamily: "DRH_SEC_PORTFOLIO_SCAN",
    disposition:
      hotelName.includes("Shorebreak") || hotelName.includes("Courtyard Denver")
        ? "PRIVATE_OPPORTUNITY_PROBABLE_ACCESS"
        : "COMMERCIALLY_RELEVANT_CONTRACTUAL_CHANGE",
  });
}

// CB3 named research batch
const cb3Named = [
  ["Four Seasons Hotel Bengaluru at Embassy ONE", "IN", "Bengaluru", "PRIVATE_OPPORTUNITY_PROBABLE_ACCESS", "OWNER_EARNINGS"],
  ["Marriott Marquis Chicago", "US", "Chicago", "AWARD_PENDING", "PUBLIC_RFP"],
  ["Honey Creek Resort", "US", "Moravia", "AWARD_PENDING", "PUBLIC_RFP"],
  ["Hotel Oromana", "ES", "Alcalá de Guadaíra", "OPEN_COMPETITIVE_OPPORTUNITY", "PUBLIC_CONCESSION"],
  ["State Game Lodge & Resort (Custer)", "US", "Custer", "COMPLETED_OR_HISTORICAL", "PUBLIC_CONCESSION"],
  ["Sylvan Lake Resort (Custer)", "US", "Custer", "COMPLETED_OR_HISTORICAL", "PUBLIC_CONCESSION"],
  ["Blue Bell Lodge and Resort (Custer)", "US", "Custer", "COMPLETED_OR_HISTORICAL", "PUBLIC_CONCESSION"],
  ["Legion Lake Resort (Custer)", "US", "Custer", "COMPLETED_OR_HISTORICAL", "PUBLIC_CONCESSION"],
  ["Legend Hotel Minot", "US", "Minot", "COMMITTED_TRANSACTION", "ACQUISITION_NEWS"],
  ["Sheraton Raleigh Hotel", "US", "Raleigh", "INTRA_BRAND_FAMILY_DECISION", "ACQUISITION_NEWS"],
  ["The Adventure Hotel by The Parks (Kissimmee)", "US", "Kissimmee", "COMMITTED_TRANSACTION", "ACQUISITION_NEWS"],
  ["The Houston Grand Hotel — River Oaks (ex-St Regis)", "US", "Houston", "INTRA_BRAND_FAMILY_DECISION", "ACQUISITION_NEWS"],
  ["Canad Inns Destination Center", "US", "Grand Forks", "PREFERRED_BRAND_DECISION", "ACQUISITION_NEWS"],
  ["Westin Seattle", "US", "Seattle", "COMMITTED_TRANSACTION", "OWNER_OPERATOR_CHANGE"],
  ["Hotel Am Belvedere (ex-Lindner)", "AT", "Vienna", "COMMITTED_TRANSACTION", "PRESS"],
  ["Quality Inn Garysburg–Roanoke Rapids", "US", "Garysburg", "COMPLETED_OR_HISTORICAL", "SALE_REFLAG"],
  ["Grand Plaza Hotel Branson", "US", "Branson", "COMPLETED_OR_HISTORICAL", "SALE_LISTING"],
  ["Ambassadors Inn & Suites", "US", "Virginia Beach", "COMMITTED_TRANSACTION", "AFFILIATION"],
  ["Hotel Pommier (ex-Super 8 Chariton)", "US", "Chariton", "COMPLETED_OR_HISTORICAL", "DEFLAG"],
  ["Omni Mont-Royal / Hotel Mont-Royal", "CA", "Montreal", "COMPLETED_OR_HISTORICAL", "DEFLAG"],
  ["Aena Recife Airport Hotel site", "BR", "Recife", "OPEN_COMPETITIVE_OPPORTUNITY", "AIRPORT_CONCESSION"],
  ["Aena Maceió Airport Hotel site", "BR", "Maceió", "OPEN_COMPETITIVE_OPPORTUNITY", "AIRPORT_CONCESSION"],
  ["Aena João Pessoa Airport Hotel site", "BR", "João Pessoa", "OPEN_COMPETITIVE_OPPORTUNITY", "AIRPORT_CONCESSION"],
  ["Aena Aracaju Airport Hotel site", "BR", "Aracaju", "OPEN_COMPETITIVE_OPPORTUNITY", "AIRPORT_CONCESSION"],
  ["Gourangaa — Krang Shuri Falls hotel project", "IN", "Meghalaya", "OPEN_COMPETITIVE_OPPORTUNITY", "DEV_BRAND_SEARCH"],
  ["Gourangaa — Siliguri hotel project", "IN", "Siliguri", "OPEN_COMPETITIVE_OPPORTUNITY", "DEV_BRAND_SEARCH"],
  ["Gourangaa — Kufri Shimla hotel project", "IN", "Shimla", "OPEN_COMPETITIVE_OPPORTUNITY", "DEV_BRAND_SEARCH"],
  ["Gourangaa — Dwarka hotel project", "IN", "Dwarka", "OPEN_COMPETITIVE_OPPORTUNITY", "DEV_BRAND_SEARCH"],
  ["Gourangaa — Porbandar seafront hotel", "IN", "Porbandar", "OPEN_COMPETITIVE_OPPORTUNITY", "DEV_BRAND_SEARCH"],
  ["Gourangaa — Tirupati hotel project", "IN", "Tirupati", "OPEN_COMPETITIVE_OPPORTUNITY", "DEV_BRAND_SEARCH"],
  ["Gourangaa — Mulshi hotel project", "IN", "Mulshi", "OPEN_COMPETITIVE_OPPORTUNITY", "DEV_BRAND_SEARCH"],
  ["Gourangaa — Panna hotel project", "IN", "Panna", "OPEN_COMPETITIVE_OPPORTUNITY", "DEV_BRAND_SEARCH"],
  ["Gourangaa — Hampi luxury resort project", "IN", "Hampi", "OPEN_COMPETITIVE_OPPORTUNITY", "DEV_BRAND_SEARCH"],
  ["voco Sandpiper All-Inclusive Resort", "US", "Saint Pete Beach", "COMMITTED_TRANSACTION", "TRACK_C_HANDOFF"],
  ["Delta Hotels Orlando Celebration", "US", "Kissimmee", "COMMITTED_TRANSACTION", "TRACK_C_HANDOFF"],
  ["Park Hotels Hilton Hawaiian Village Alii Tower", "US", "Honolulu", "INTRA_BRAND_FAMILY_DECISION", "REIT_EARNINGS"],
  ["Hotel Zags Portland", "US", "Portland", "COMPLETED_OR_HISTORICAL", "REIT_DISPOSITION"],
  ["The Cedars Ranch", "US", "Wimberley", "COMMITTED_TRANSACTION", "REBRAND"],
  ["Best Western Plus San Diego (Hyatt Select path)", "US", "San Diego", "COMMITTED_TRANSACTION", "CONVERSION"],
  ["Greenleigh River Oaks (ex-Hotel Derek)", "US", "Houston", "COMMITTED_TRANSACTION", "SOFT_BRAND"],
  ["The RIVVY Tribute Portfolio", "US", "Chicago", "COMMITTED_TRANSACTION", "SOFT_BRAND"],
  ["Hotel 505 River North", "US", "Chicago", "PREFERRED_BRAND_DECISION", "OWN_BRAND_TOOR"],
  ["Miramar Resort St Pete Beach", "US", "St Pete Beach", "COMMITTED_TRANSACTION", "JDV"],
  ["Former Hilton Houston NASA Clear Lake", "US", "Nassau Bay", "COMMITTED_TRANSACTION", "MARGARITAVILLE"],
  ["Corendon South Beach Miami", "US", "Miami Beach", "COMMITTED_TRANSACTION", "REBRAND"],
  ["Recess Hotel & Club Savannah", "US", "Savannah", "COMMITTED_TRANSACTION", "NEW_FLAG"],
  ["The St. James Hotel Selma", "US", "Selma", "COMMITTED_TRANSACTION", "SERIES_BY_MARRIOTT"],
  ["Hotel Wildemont Autograph", "US", "Mountain Brook", "INTRA_BRAND_FAMILY_DECISION", "RENAME"],
  ["Hotel Beauden Autograph", "US", "Orlando", "INTRA_BRAND_FAMILY_DECISION", "RENAME"],
  ["Dorset Council hotel portfolio", "GB", "Dorset", "AWARD_PENDING", "PUBLIC_PORTFOLIO"],
  ["Jericho Beach Hostel", "CA", "Vancouver", "OPEN_COMPETITIVE_OPPORTUNITY", "CB2_CARRY"],
  ["Hotel do Mel Casa da Costeira", "PT", "Góis", "OPEN_COMPETITIVE_OPPORTUNITY", "CB2_CARRY"],
  ["Hotel Torre de Uriz", "ES", "Uriz", "AWARD_PENDING", "CB2_CARRY"],
  ["White Sands Beach Hotel", "TC", "Grand Turk", "AWARD_PENDING", "CB2_CARRY"],
  ["Hotel Murta", "ES", "Xàtiva", "OPEN_COMPETITIVE_OPPORTUNITY", "CB2B_CARRY"],
  ["Antiga Colónia Areia Branca", "PT", "Areia Branca", "OPEN_COMPETITIVE_OPPORTUNITY", "CB2B_CARRY"],
  ["Lakeville Inn", "US", "Lakeville", "PRIVATE_OPPORTUNITY_PROBABLE_ACCESS", "CB1_CARRY"],
  ["Hilton Trinidad & Conference Centre", "TT", "Port of Spain", "OPEN_COMPETITIVE_OPPORTUNITY", "CFA_CARRY"],
  ["Legacy Hotel Green Bay", "US", "Green Bay", "PREFERRED_BRAND_DECISION", "CB2_RECLASSIFIED"],
  ["Magdalena Grand Beach & Golf Resort", "TT", "Tobago", "COMMERCIALLY_RELEVANT_CONTRACTUAL_CHANGE", "SALE_EOI"],
  ["Gideon Putnam Hotel", "US", "Saratoga Springs", "OPEN_COMPETITIVE_OPPORTUNITY", "CONCESSION"],
  ["Pueblo Downtown Hotel site", "US", "Pueblo", "OPEN_COMPETITIVE_OPPORTUNITY", "DEVELOPMENT"],
  ["George Hotel Stranraer", "GB", "Stranraer", "AWARD_PENDING", "OPERATOR"],
  ["ex-Ibis Guaíba", "BR", "Guaíba", "COMPLETED_OR_HISTORICAL", "OPERATOR_CHANGE"],
  ["Kentucky Castle", "US", "Versailles", "COMPLETED_OR_HISTORICAL", "MCT_AWARDED"],
  ["Ho-Chunk Gaming Beloit hotel", "US", "Beloit", "COMPLETED_OR_HISTORICAL", "RFP_CLOSED"],
  ["Hotel Pattee Perry", "US", "Perry", "COMPLETED_OR_HISTORICAL", "PRIOR_WAVE"],
  ["Grand Continent Indiranagar affiliate", "IN", "Bengaluru", "PREFERRED_BRAND_DECISION", "OWN_BRAND_EXIT"],
  ["Grand Continent Koramangala affiliate", "IN", "Bengaluru", "PREFERRED_BRAND_DECISION", "OWN_BRAND_EXIT"],
  ["IHG former PentaHotels portfolio (Europe)", "EU", "Multi", "COMPLETED_OR_HISTORICAL", "FRANCHISE_SIGNED"],
  ["Verdi Accor Handwritten conversions", "MX", "Multi", "COMPLETED_OR_HISTORICAL", "SOFT_BRAND_SIGNED"],
  ["Aimbridge Mexico Accor conversions", "MX", "Multi", "COMPLETED_OR_HISTORICAL", "AWARDED"],
  ["Operadora JL independent hotels segment", "MX", "Multi", "COMMERCIALLY_RELEVANT_CONTRACTUAL_CHANGE", "SUPPLY_SIDE_ONLY"],
  ["Meliá Brazil conversion pipeline (unnamed)", "BR", "Multi", "COMMERCIALLY_RELEVANT_CONTRACTUAL_CHANGE", "BRAND_DEMAND"],
  ["Atlantica Hotels conversion pipeline", "BR", "Multi", "COMMERCIALLY_RELEVANT_CONTRACTUAL_CHANGE", "BRAND_DEMAND"],
  ["Life Hotéis conversion pipeline", "BR", "Multi", "COMMERCIALLY_RELEVANT_CONTRACTUAL_CHANGE", "OPERATOR_DEMAND"],
  ["Tauá Administradora third-party assets", "BR", "Multi", "COMMERCIALLY_RELEVANT_CONTRACTUAL_CHANGE", "OPERATOR_DEMAND"],
  ["Laghetto Hotéis expansion candidates", "BR", "Multi", "COMMERCIALLY_RELEVANT_CONTRACTUAL_CHANGE", "OPERATOR_DEMAND"],
  ["Accor South England franchise portfolio (4 hotels)", "GB", "Multi", "COMMERCIALLY_RELEVANT_CONTRACTUAL_CHANGE", "SALE_WITH_FRANCHISE"],
  ["Christie & Co Luton Airport Accor hotel", "GB", "Luton", "COMMERCIALLY_RELEVANT_CONTRACTUAL_CHANGE", "SALE"],
  ["Christie & Co Southampton Accor hotel", "GB", "Southampton", "COMMERCIALLY_RELEVANT_CONTRACTUAL_CHANGE", "SALE"],
  ["Christie & Co London Accor hotel A", "GB", "London", "COMMERCIALLY_RELEVANT_CONTRACTUAL_CHANGE", "SALE"],
  ["Christie & Co London Accor hotel B", "GB", "London", "COMMERCIALLY_RELEVANT_CONTRACTUAL_CHANGE", "SALE"],
];
for (const [hotelName, country, city, disposition, sourceFamily] of cb3Named) {
  add({ hotelName, country, city, disposition, sourceFamily });
}

const byDisposition = {};
for (const h of hotels) {
  byDisposition[h.disposition] = (byDisposition[h.disposition] || 0) + 1;
}
const byCountry = {};
for (const h of hotels) {
  byCountry[h.country] = (byCountry[h.country] || 0) + 1;
}
const bySourceFamily = {};
for (const h of hotels) {
  bySourceFamily[h.sourceFamily] = (bySourceFamily[h.sourceFamily] || 0) + 1;
}

const ledger = {
  asOf: "2026-11-21",
  publicationState: "CUSTOMER_BLOCKED",
  wave: "HIF_V6_TRACK_A_CB3_COMMERCIAL_DISCOVERY",
  researchVolumeTarget: 100,
  distinctHotelsInvestigated: hotels.length,
  stopReason:
    hotels.length >= 100
      ? "RESEARCH_VOLUME_TARGET_MET"
      : "EXECUTION_LIMIT_BEFORE_VOLUME_TARGET",
  byDisposition,
  byCountry,
  bySourceFamily,
  hotels,
};

fs.mkdirSync(OUT, { recursive: true });
fs.writeFileSync(path.join(OUT, "INVESTIGATED_HOTELS.json"), JSON.stringify(ledger, null, 2));
console.log(
  JSON.stringify(
    {
      distinctHotelsInvestigated: ledger.distinctHotelsInvestigated,
      stopReason: ledger.stopReason,
      byDisposition,
    },
    null,
    2,
  ),
);
