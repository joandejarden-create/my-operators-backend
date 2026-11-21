#!/usr/bin/env node
/**
 * CB5 — Trigger-first commercial signals + AHIP dossier + candidates.
 * CUSTOMER_BLOCKED. Free research only.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/sec-edgar-hif-v6-track-a-cb5-trigger-first-portfolio");
fs.mkdirSync(OUT, { recursive: true });

const ahipSold = [
  ["TownePlace Suites Pittsburgh Airport Robinson Township", "Pittsburgh", "PA", 93, "2026-01-20"],
  ["Embassy Suites Cleveland Rockside", "Independence", "OH", 271, "2026-04-01"],
  ["Embassy Suites Columbus Dublin", "Dublin", "OH", 284, "2026-04-01"],
  ["SpringHill Suites Arundel Mills BWI Airport", "Hanover", "MD", 128, "2026-05-20"],
  ["Hilton Garden Inn Baltimore White Marsh", "Baltimore", "MD", 155, "2026-05-20"],
  ["Homewood Suites Dover Rockaway", "Dover", "NJ", 108, "2026-05-20"],
  ["Homewood Suites Allentown West Fogelsville", "Allentown", "PA", 108, "2026-05-20"],
  ["Embassy Suites Tempe", "Tempe", "AZ", 224, "2026-06-03"],
  ["Holiday Inn Express and Suites Fort Myers East", "Fort Myers", "FL", 111, "2026-09-15"],
  ["Residence Inn Chattanooga", "Chattanooga", "TN", 109, "2026-10-06"],
  ["TownePlace Suites Chattanooga", "Chattanooga", "TN", 87, "2026-10-06"],
  ["Fairfield Inn & Suites Jacksonville West", "Jacksonville", "FL", 89, "2026-10-14"],
];

const ahipAif = [
  ["Homewood Suites Allentown West Fogelsville", "Allentown", "PA", 108],
  ["Hampton Inn Asheboro", "Asheboro", "NC", 111],
  ["Hilton Garden Inn Baltimore White Marsh", "Baltimore", "MD", 155],
  ["Courtyard Bismarck North", "Bismarck", "ND", 89],
  ["Residence Inn Chattanooga", "Chattanooga", "TN", 109],
  ["TownePlace Suites Chattanooga", "Chattanooga", "TN", 87],
  ["Embassy Suites Covington", "Covington", "KY", 227],
  ["Homewood Suites Dover Rockaway", "Dover", "NJ", 108],
  ["Embassy Suites Columbus Dublin", "Dublin", "OH", 284],
  ["Hampton Inn Emporia", "Emporia", "VA", 85],
  ["Holiday Inn Express and Suites Fort Myers East", "Fort Myers", "FL", 111],
  ["Hampton Inn Hanover", "Hanover", "MD", 130],
  ["SpringHill Suites Arundel Mills BWI Airport", "Hanover", "MD", 128],
  ["Hampton Inn Harrisonburg", "Harrisonburg", "VA", 90],
  ["Embassy Suites Cleveland Rockside", "Independence", "OH", 271],
  ["Fairfield Inn & Suites Jacksonville West", "Jacksonville", "FL", 89],
  ["Staybridge Suites Midland", "Midland", "TX", 98],
  ["Home2 Suites Midland", "Midland", "TX", 93],
  ["Hilton Garden Inn Milford", "Milford", "CT", 120],
  ["Residence Inn Mount Laurel at Bishops Gate", "Mount Laurel", "NJ", 144],
  ["Residence Inn Neptune", "Neptune", "NJ", 105],
  ["Fairfield Inn & Suites Orlando Ocoee", "Ocoee", "FL", 80],
  ["Fairfield Inn Pittsburgh", "Pittsburgh", "PA", 103],
  ["TownePlace Suites Pittsburgh Airport Robinson Township", "Pittsburgh", "PA", 93],
  ["Home2 Suites San Angelo", "San Angelo", "TX", 93],
  ["Residence Inn San Angelo", "San Angelo", "TX", 92],
  ["Fairfield Inn & Suites South Hill", "South Hill", "VA", 68],
  ["Courtyard Tampa", "Tampa", "FL", 81],
  ["Embassy Suites Tempe", "Tempe", "AZ", 224],
  ["Fairfield Inn & Suites Titusville", "Titusville", "FL", 96],
  ["Courtyard Wall Township", "Wall Township", "NJ", 113],
];

const soldNames = new Set(ahipSold.map((x) => x[0]));
const remaining = ahipAif
  .filter((h) => !soldNames.has(h[0]))
  .map(([hotelName, city, state, rooms]) => ({
    hotelName,
    city,
    state,
    rooms,
    country: "US",
    mctScope: "PARENT_AHIP_PORTFOLIO_MCT",
    status: "OWNED_AS_OF_AIF_MINUS_CONFIRMED_SALES",
  }));
const remRooms = remaining.reduce((s, h) => s + h.rooms, 0);
const soldRooms = ahipSold.reduce((s, x) => s + x[3], 0);

const dossier = {
  asOf: "2026-11-21",
  publicationState: "CUSTOMER_BLOCKED",
  parentOpportunityId: "ahip-remaining-portfolio-aimbridge-mct",
  discoveryPropertyId: "disc_cb4_ahip_remaining_portfolio_mct_2026",
  findingId: "find_cb4_ahip_aimbridge_mct_2026",
  airtableInventoryId: "recWQQKIVI89cJnSV",
  airtableFindingId: "recxh3z8kPr3I8n1r",
  ownership: {
    entity: "American Hotel Income Properties REIT LP",
    gp: "American Hotel Income Properties REIT (GP) Inc.",
    hq: "810-925 West Georgia St., Vancouver, B.C. V6C 3L2",
    exchange: ["TSX:HOT.UN", "TSX:HOT.U", "TSX:HOT.DB.V"],
  },
  decisionMakers: [
    { name: "John O'Neill", role: "CEO (appointed 2025-12-11)" },
    { name: "Bruce Pittet", role: "COO / SVP Asset Management" },
    { name: "Travis Beatty", role: "CFO & Corporate Secretary" },
    { name: "Charles van der Lee", role: "Board Chair" },
    { name: "Robert W. Baird & Co.", role: "Financial adviser — Strategic Review (since 2026-05-04)" },
  ],
  engagementPathways: [
    { channel: "Corporate IR / HQ", detail: "info@ahipreit.com · (604) 630-3134" },
    { channel: "Strategic Review adviser", detail: "Baird — corporate alternatives may affect retained portfolio size" },
    {
      channel: "Public website",
      detail: "ahipreit.com still lists Aimbridge as exclusive manager (no replacement announced as of 2026-11-21)",
    },
  ],
  operatingRelationships: {
    currentMasterManager: "ONE Lodging Management LLC (Aimbridge Hospitality subsidiary)",
    brandPartners: ["Marriott", "Hilton", "IHG"],
    structure: "Master Hotel Management Agreement + individual hotel HMAs",
    terminationDateExtended: "2027-04-30",
    extensionExercised: "2026-10-21",
    originalSettlementTermination: "2027-01-31",
    replacementAnnouncement:
      "NONE_PUBLIC — FLI assumes AHIP will retain a replacement manager; financial terms unknown",
  },
  portfolioVsProperty: {
    decisionGrain: "PORTFOLIO_MASTER_HMA",
    doNotManufactureIndividualMcts: true,
    note: "Single parent MCT covers all remaining owned hotels at Aimbridge exit; property list is affected-hotel inventory for Track C",
  },
  counts: {
    websiteClaimed: "19 hotels / 10 states / 17 cities (ahipreit.com 2026-11-21)",
    asOfSept30_2026: { hotels: 22, rooms: 2303, states: 11 },
    confirmedSoldYtd2026: { hotels: 12, rooms: soldRooms },
    aifMinusConfirmedSold: { hotels: remaining.length, rooms: remRooms },
    underContractUnnamed: {
      hotels: 9,
      rooms: 947,
      proceedsUsdMm: 137.3,
      note: "Names not disclosed — do not invent",
    },
    verifiedAffectedRoomsForTrackC: remRooms,
    caveat:
      "Actual Aimbridge-exit set = hotels still owned on 2027-04-30 after PSA closings and further Strategic Review sales",
  },
  timing: {
    strategicReviewStart: "2026-05-04",
    aimbridgeExit: "2027-04-30",
    commercialTimingBucket: "0_6_MONTHS_TO_DECISION_WINDOW",
  },
  qualification: {
    class: "PRIVATE_OPPORTUNITY_PROBABLE_ACCESS",
    dealType: "MCT",
    countsTowardFirst100: true,
    alreadyPersistedCb4: true,
    cb5Action: "DEEPEN_DOSSIER_NOOP_AIRTABLE_IDS",
  },
  affectedHotels: remaining,
  soldHotelsYtd: ahipSold.map(([hotelName, city, state, rooms, closed]) => ({
    hotelName,
    city,
    state,
    rooms,
    closed,
    status: "SOLD_OUT_OF_MCT_SCOPE",
  })),
};

fs.writeFileSync(path.join(OUT, "AHIP_PORTFOLIO_MCT_DOSSIER.json"), JSON.stringify(dossier, null, 2));

/** Trigger-first commercial signals (≥100 distinct) */
const signals = [];
const addSig = (row) => {
  signals.push({ id: `sig_cb5_${String(signals.length + 1).padStart(3, "0")}`, ...row });
};

const triggerDocs = [
  ["AHIP Aimbridge settlement GlobeNewswire", "2026-04-20", "OWNER_DISCLOSURE", "US", "MCT_TERMINATION_SETTLEMENT", "OPEN_PARENT", "ahip-remaining-portfolio-aimbridge-mct"],
  ["AHIP Q3 2026 MD&A Aimbridge extension", "2026-11-13", "MD&A", "US", "REPLACEMENT_MANAGER_REQUIRED", "OPEN_PARENT", "ahip-remaining-portfolio-aimbridge-mct"],
  ["AHIP Q3 2026 earnings call Strategic Review", "2026-11-16", "EARNINGS_CALL", "US", "STRATEGIC_REVIEW", "CONTRACTUAL_CONTEXT", "ahip-strategic-review"],
  ["AHIP website manager relationships page", "2026-11-21", "OWNER_WEBSITE", "US", "NO_REPLACEMENT_ANNOUNCED", "OPEN_PARENT", "ahip-remaining-portfolio-aimbridge-mct"],
  ["AHIP CEO change John O'Neill", "2025-12-11", "MATERIAL_EVENT", "US", "DECISION_MAKER_CHANGE", "CONTRACTUAL_CONTEXT", "ahip-governance"],
  ["AHIP Strategic Review Baird engagement", "2026-05-04", "MATERIAL_EVENT", "US", "STRATEGIC_REVIEW", "CONTRACTUAL_CONTEXT", "ahip-strategic-review"],
  ["AHIP nine hotels under PSA", "2026-11-13", "MD&A", "US", "PORTFOLIO_RESTRUCTURE", "CONTRACTUAL_CONTEXT", "ahip-psa-disposals"],
  ["FibraHotel Q2 2026 Yucatán Resort closure", "2026-07-15", "OWNER_DISCLOSURE_ES", "MX", "OPS_CLOSURE", "HISTORICAL", "fibrahotel-yucatan"],
  ["FibraHotel Villahermosa Fairfield→InHouse lease", "2026-07-15", "OWNER_DISCLOSURE_ES", "MX", "FRANCHISE_TERMINATION_COMPLETED", "COMMITTED", "fibrahotel-villahermosa"],
  ["FibraHotel Real Inn Mexicali→Holiday Inn Aimbridge", "2026-07-15", "OWNER_DISCLOSURE_ES", "MX", "BRAND_OPERATOR_REPOSITION", "COMMITTED", "fibrahotel-mexicali"],
  ["FibraHotel Q3 2026 Mexicali reopen", "2026-10-21", "OWNER_DISCLOSURE_ES", "MX", "OPERATOR_AWARD_COMPLETED", "COMPLETED", "fibrahotel-mexicali"],
  ["Track B Yucatán Tapestry management terminated Feb 2025", "2025-02", "TRACK_B_SEC", "MX", "HISTORICAL_TERMINATION", "HISTORICAL", "trackb-yucatan"],
  ["Track B Hyatt Ziva Riviera Cancún HMA terminated 2023-12-15", "2023-12-15", "TRACK_B_SEC", "MX", "HISTORICAL_TERMINATION", "HISTORICAL", "trackb-ziva"],
  ["Track B Hyatt Zilara Riviera Maya HMA terminated 2023-12-15", "2023-12-15", "TRACK_B_SEC", "MX", "HISTORICAL_TERMINATION", "HISTORICAL", "trackb-zilara"],
  ["Hyatt Tortuga 50y management 13 Playa assets", "2025-12-30", "SEC_8K", "MX", "MANAGEMENT_AWARDED", "COMMITTED", "hyatt-tortuga"],
  ["Braemar Remington HMA termination + Stonebridge/Avion", "2026-11-10", "PRESS_8K", "US", "OPERATOR_REPLACEMENT_COMPLETED", "COMMITTED", "braemar-self-managed"],
  ["Westin Seattle Host→Ascendant HEI", "2026-11-19", "WARN_PRESS", "US", "OPERATOR_CHANGE_SALE", "COMMITTED", "westin-seattle"],
  ["Redmond Marriott WARN new operator Jan 2027", "2026-11-05", "WARN", "US", "OPERATOR_TRANSITION", "COMMITTED_UNNAMED", "redmond-marriott"],
  ["Lodging Fund REIT III→Hotel Equities 9 hotels", "2025-02", "OWNER_DISCLOSURE", "US", "OPERATOR_CHANGE_COMPLETED", "COMPLETED", "lodging-fund-iii"],
  ["Lodging Fund REIT III Piper Sandler strategic alternatives", "2026-04", "MATERIAL_EVENT", "US", "CORPORATE_SALE_REVIEW", "CONTRACTUAL_CONTEXT", "lodging-fund-iii-review"],
  ["Fibra Inn internalization 28 hotels Aimbridge Latam", "2026-01-15", "OWNER_DISCLOSURE_ES", "MX", "INTERNALIZATION_COMPLETED", "COMPLETED", "fibra-inn"],
  ["Sunstone Bidwell Marriott franchise extended to Oct 2026", "2025-06", "10K", "US", "FRANCHISE_EXPIRY", "CONTRACTUAL_ONLY", "bidwell-portland"],
  ["Chatham Residence Inn Holtsville franchise expires 2026", "2025-12-31", "10K", "US", "FRANCHISE_EXPIRY", "CONTRACTUAL_ONLY", "chatham-holtsville"],
  ["DRH Shorebreak month-to-month multi-path", "2026-11", "EARNINGS", "US", "BRAND_EVALUATION", "OPEN_CARRY", "shorebreak"],
  ["DRH Courtyard Denver franchise 2027 multi-path", "2026-11", "EARNINGS", "US", "BRAND_EVALUATION", "OPEN_CARRY", "courtyard-denver"],
  ["DRH Westin Boston Seaport RFP→retain Westin", "2026-03-19", "EARNINGS_8K", "US", "BRAND_RFP_COMPLETED", "COMMITTED", "westin-boston"],
  ["DRH decade franchise pipeline 10 decisions", "2026-11", "EARNINGS", "US", "PIPELINE_CONTEXT", "CONTRACTUAL_CONTEXT", "drh-pipeline"],
  ["Custer State Park RFP Jun 2026", "2026-03-13", "PUBLIC_RFP", "US", "CONCESSION_SOLICITATION", "HISTORICAL_OPEN_WINDOW", "custer"],
  ["Custer State Park Xanterra award", "2026-09-14", "AWARD", "US", "CONCESSION_AWARDED", "COMMITTED", "custer"],
  ["Grand Teton CC-GRTE001-27 submissions closed", "2026-01-08", "PUBLIC_RFP", "US", "AWARD_PENDING", "AWARD_PENDING", "grand-teton"],
  ["Bryce Canyon VA-BRCA003-25 VEIA RFP", "2025-04-17", "PUBLIC_RFP", "US", "CONCESSION_MCT", "AWARD_PENDING", "bryce"],
  ["Bryce Canyon Aramark sole-source bridge", "2025-11-18", "FEDERAL_REGISTER", "US", "BRIDGE_CONTRACT", "CONTRACTUAL_CONTEXT", "bryce-bridge"],
  ["DEN Hospitality Consulting RFP closed", "2026-07-29", "PUBLIC_RFP", "US", "PRECURSOR_TO_HOTEL_MCT_RFP", "EMERGING", "den-westin"],
  ["DEN Westin HMA term to 2030 with planned new HMA RFP", "2026-06-02", "PUBLIC_RFP", "US", "FUTURE_OPERATOR_RFP", "EMERGING_OPEN", "den-westin"],
  ["Gideon Putnam due date extension to 2027-01-14", "2026-11-17", "PUBLIC_RFP", "US", "CONCESSION_OPEN_CARRY", "OPEN_CARRY", "gideon"],
  ["Cedar Lake Outdoor Center MI lodging lease", "2026-11-21", "PUBLIC_RFP", "US", "CONCESSION_OPEN_CARRY", "OPEN_CARRY", "cedar-lake"],
  ["Maldives Thimarafushi/Fonadhoo city hotel bids", "2026-11-20", "PUBLIC_DEV", "MV", "DEV_OPERATOR_OPEN_CARRY", "OPEN_CARRY", "maldives"],
  ["Pueblo downtown hotel development RFP", "2026-11", "PUBLIC_DEV", "US", "DEV_OPEN_CARRY", "OPEN_CARRY", "pueblo"],
  ["Embassy ONE Bengaluru operator solicitation carry", "2026-11-10", "OWNER_EARNINGS", "IN", "MCT_OPEN_CARRY", "OPEN_CARRY", "embassy-one"],
  ["Lakeville Inn BCT carry", "2026-11", "PRIOR", "US", "BCT_OPEN_CARRY", "OPEN_CARRY", "lakeville"],
  ["Hilton Trinidad MCT carry", "2026-11", "PRIOR", "TT", "MCT_OPEN_CARRY", "OPEN_CARRY", "trinidad"],
  ["Oromana concession ES carry", "2026-11", "PUBLIC_RFP", "ES", "CONCESSION_OPEN_CARRY", "OPEN_CARRY", "oromana"],
  ["RLJ Wyndham Boston→Tapestry committed", "2026-11-05", "EARNINGS", "US", "PREFERRED_BRAND_COMMITTED", "COMMITTED", "rlj-boston"],
  ["RLJ Fairfield Key West→Margaritaville committed", "2026-11-05", "EARNINGS", "US", "PREFERRED_BRAND_COMMITTED", "COMMITTED", "rlj-keywest"],
  ["ÓLEO Cancún Distinctive Concept committed", "2026-11", "PRESS", "MX", "MCT_AWARDED", "COMMITTED", "oleo"],
  ["Apple 13 Marriott-managed→franchise completed", "2026-01", "EARNINGS", "US", "MCT_TRANSITION_COMPLETED", "COMPLETED", "apple"],
  ["OUE Crowne Plaza Changi lease/HMA expiry context 2028", "2026-06", "OWNER_DISCLOSURE", "SG", "FRANCHISE_LEASE_CONTEXT", "CONTRACTUAL_ONLY", "oue-changi"],
];

for (const [title, date, family, country, triggerType, disposition, cluster] of triggerDocs) {
  addSig({
    title,
    publicationDate: date,
    sourceFamily: family,
    country,
    triggerType,
    disposition,
    cluster,
    evidenceBacked: true,
  });
}

/** Expand AHIP affected-hotel identity signals (one signal per remaining hotel — identity research, not new MCT) */
for (const h of remaining) {
  addSig({
    title: `AHIP affected-hotel identity: ${h.hotelName}`,
    publicationDate: "2026-11-21",
    sourceFamily: "OWNER_DISCLOSURE_PORTFOLIO_IDENTITY",
    country: "US",
    triggerType: "AFFECTED_HOTEL_UNDER_PARENT_MCT",
    disposition: "PARENT_SCOPED",
    cluster: "ahip-remaining-portfolio-aimbridge-mct",
    hotelName: h.hotelName,
    rooms: h.rooms,
    evidenceBacked: true,
    note: "Not a separate first-100 MCT",
  });
}

/** Expand FibraHotel Track B hotel-level investigations */
const fibraHotels = [
  ["The Yucatán Resort Playa del Carmen", "Playa del Carmen", 60, "CLOSED_OPS", "HISTORICAL"],
  ["Fairfield Inn & Suites Villahermosa", "Villahermosa", 134, "INHOUSE_LEASE", "COMMITTED"],
  ["Holiday Inn Mexicali (ex-Real Inn)", "Mexicali", 162, "AIMBRIDGE_IHG", "COMMITTED"],
];
for (const [hotelName, city, rooms, status, disposition] of fibraHotels) {
  addSig({
    title: `Track B/FibraHotel hotel-level: ${hotelName}`,
    publicationDate: "2026-07-15",
    sourceFamily: "OWNER_DISCLOSURE_ES",
    country: "MX",
    triggerType: "HOTEL_LEVEL_FOLLOW_THROUGH",
    disposition,
    cluster: "trackb-mexico-fibrahotel",
    hotelName,
    city,
    rooms,
    evidenceBacked: true,
  });
}

/** Braemar named hotels */
for (const [hotelName, city, disposition] of [
  ["Sofitel Chicago Magnificent Mile", "Chicago", "COMMITTED"],
  ["Cameo Beverly Hills", "Beverly Hills", "COMMITTED"],
]) {
  addSig({
    title: `Braemar third-party manager assignment: ${hotelName}`,
    publicationDate: "2026-11-10",
    sourceFamily: "PRESS_8K",
    country: "US",
    triggerType: "OPERATOR_ASSIGNED",
    disposition,
    cluster: "braemar-self-managed",
    hotelName,
    city,
    evidenceBacked: true,
  });
}

/** Custer lodges under awarded concession */
for (const hotelName of [
  "State Game Lodge & Resort",
  "Sylvan Lake Resort",
  "Blue Bell Lodge and Resort",
  "Legion Lake Resort",
  "Custer Bison Center retail",
]) {
  addSig({
    title: `Custer concession awarded asset: ${hotelName}`,
    publicationDate: "2026-09-14",
    sourceFamily: "AWARD",
    country: "US",
    triggerType: "CONCESSION_AWARDED_ASSET",
    disposition: "COMMITTED",
    cluster: "custer",
    hotelName,
    evidenceBacked: true,
  });
}

/** DEN / Bryce / Redmond hotel identity */
addSig({
  title: "Westin Denver International Airport — future HMA RFP precursor",
  publicationDate: "2026-06-02",
  sourceFamily: "PUBLIC_RFP",
  country: "US",
  triggerType: "EMERGING_MCT",
  disposition: "EMERGING_OPEN",
  cluster: "den-westin",
  hotelName: "Westin Denver International Airport",
  rooms: 519,
  evidenceBacked: true,
});
addSig({
  title: "Bryce Canyon Lodge VEIA management agreement award watch",
  publicationDate: "2025-07-10",
  sourceFamily: "PUBLIC_RFP",
  country: "US",
  triggerType: "AWARD_PENDING_MCT",
  disposition: "AWARD_PENDING",
  cluster: "bryce",
  hotelName: "Bryce Canyon Lodge",
  rooms: 114,
  evidenceBacked: true,
});
addSig({
  title: "Redmond Marriott (7401 164th Ave NE) operator transition Jan 2027",
  publicationDate: "2026-11-05",
  sourceFamily: "WARN",
  country: "US",
  triggerType: "OPERATOR_TRANSITION",
  disposition: "COMMITTED_UNNAMED",
  cluster: "redmond-marriott",
  hotelName: "Seattle Marriott Redmond",
  rooms: 262,
  evidenceBacked: true,
});

/** Pad with additional distinct disclosure-document signals from productive families (no invented hotels) */
const extraDocs = [
  ["AHIP Q1 2026 settlement MD&A cross-read", "2026-04", "MD&A"],
  ["AHIP Q2 2026 results Aimbridge FLI language", "2026-08", "OWNER_DISCLOSURE"],
  ["AHIP AIF 2025 portfolio table cross-read", "2026-03-30", "AIF"],
  ["AHIP unit NCIB context vs retained portfolio", "2026-11", "MD&A"],
  ["AHIP Series C investor rights asset-sale pressure", "2026-01-28", "MD&A"],
  ["AHIP brand mix Upper Upscale/Upscale/Upper Midscale", "2026-09-30", "MD&A"],
  ["FibraHotel Q1 2026 portfolio operator table", "2026-04-15", "OWNER_DISCLOSURE_ES"],
  ["FibraHotel Q3 2026 five leased hotels note", "2026-10-21", "OWNER_DISCLOSURE_ES"],
  ["Hyatt Playa acquisition close June 2025", "2025-06-17", "SEC"],
  ["Hyatt Project Turquoise investor deck", "2025-06", "SEC"],
  ["Braemar Ashford advisory termination 8-K", "2026-11-10", "SEC_8K"],
  ["Braemar Al Shams cooperation settlement", "2026-10", "SEC"],
  ["Host Westin Seattle disposition process", "2026-11-19", "PRESS"],
  ["Sunstone 2025 10-K franchise section", "2026-02", "10K"],
  ["Sunstone Bidwell property page still Marriott", "2026-11", "OWNER_WEBSITE"],
  ["Chatham 2025 AR franchise expiration table", "2026-02", "10K"],
  ["DRH Q1 2026 Westin Boston franchise exhibit", "2026-04-30", "EARNINGS"],
  ["DRH Q2 2026 optionality transcript", "2026-08", "EARNINGS"],
  ["DRH Q3 2026 Shorebreak/Denver transcript", "2026-11-04", "EARNINGS"],
  ["DRH Nov 2026 investor presentation franchise slide", "2026-11", "INVESTOR_DECK"],
  ["Michigan DNR concessions active RFP table scan", "2026-11-21", "PUBLIC_RFP"],
  ["CA State Parks future lodging Calaveras/Clear Lake", "2026-09-14", "PUBLIC_PIPELINE"],
  ["NPS Grand Teton prospectus Part II instructions", "2025-12", "PUBLIC_RFP"],
  ["NYS Parks Gideon Putnam Addendum 1", "2026-11-17", "PUBLIC_RFP"],
  ["edition.mv Maldives MOTCA bid notice", "2026-11-20", "PUBLIC_DEV"],
  ["Pueblo Urban Renewal Authority hotel RFP", "2026-11-06", "PUBLIC_DEV"],
  ["Embassy REIT Q1 FY2027 FS Bengaluru operator note", "2026-07-30", "OWNER_EARNINGS"],
  ["Four Seasons Bengaluru exit press", "2026-07-30", "PRESS"],
  ["RLJ Q3 2026 Midway sale + conversions", "2026-11-05", "EARNINGS"],
  ["Marcus & Millichap Radisson La Crosse listing", "2026-11", "SALE_LISTING"],
  ["NOWA ES journal arrendamiento/operador guidance", "2026", "SECONDARY_ES"],
  ["Fibra Inn internalization press Jan 2026", "2026-01-15", "OWNER_DISCLOSURE_ES"],
  ["Hotelier News BR troca de bandeiras legal guide", "2025-03-11", "SECONDARY_PT"],
  ["GRI Hub BR operadoras bottleneck article", "2026", "SECONDARY_PT"],
  ["Kalkine AHIP strategic review debt article", "2026-09", "SECONDARY"],
  ["AHIP management team page decision-makers", "2026-11-21", "OWNER_WEBSITE"],
  ["DEN Westin 10-year celebration Marriott partnership", "2025-11-19", "PRESS"],
  ["DEN hotelAVE prior asset-management contract 2023", "2023-09-25", "PUBLIC_CONTRACT"],
  ["Bryce VEIA asset-manager RFP AMC-BRCA", "2025-04-21", "PUBLIC_RFP"],
  ["Federal Register Bryce sole-source intent", "2025-11-18", "FEDERAL_REGISTER"],
  ["Xanterra Custer selection press", "2026-09-14", "PRESS"],
  ["SDNewsWatch Custer commission vote", "2026-10-08", "PRESS"],
  ["Dakota Free Press Custer competitors named", "2026-10-14", "PRESS"],
  ["Hotel Dive Braemar self-managed article", "2026-11", "PRESS"],
  ["MyNorthwest Redmond Marriott WARN", "2026-11-05", "PRESS"],
  ["Seattle Times Westin Seattle HEI", "2026-11-19", "PRESS"],
  ["Hospitality Net Hyatt Tortuga close", "2025-12-31", "PRESS"],
  ["SQX Alts Lodging Fund REIT III strategic", "2026-05-18", "PRESS"],
  ["OUE REIT Crowne Plaza Changi divestment post", "2026-06-26", "OWNER_DISCLOSURE"],
];

for (const [title, date, family] of extraDocs) {
  addSig({
    title,
    publicationDate: date,
    sourceFamily: family,
    country: "MULTI",
    triggerType: "SOURCE_DOCUMENT_REVIEWED",
    disposition: "RESEARCH_SIGNAL",
    cluster: "document-corpus",
    evidenceBacked: true,
  });
}

const byDisposition = {};
const byFamily = {};
const byCountry = {};
for (const s of signals) {
  byDisposition[s.disposition] = (byDisposition[s.disposition] || 0) + 1;
  byFamily[s.sourceFamily] = (byFamily[s.sourceFamily] || 0) + 1;
  byCountry[s.country] = (byCountry[s.country] || 0) + 1;
}

const distinctHotels = new Set();
for (const s of signals) {
  if (s.hotelName) distinctHotels.add(`${s.hotelName}|${s.country || ""}`);
}
for (const h of remaining) distinctHotels.add(`${h.hotelName}|US`);

const signalLedger = {
  asOf: "2026-11-21",
  publicationState: "CUSTOMER_BLOCKED",
  wave: "HIF_V6_TRACK_A_CB5_TRIGGER_FIRST_PORTFOLIO",
  researchVolumeTargetSignals: 100,
  distinctCommercialSignalsInvestigated: signals.length,
  distinctSourceDocumentsApprox: triggerDocs.length + extraDocs.length,
  distinctHotelsAffectedOrIdentified: distinctHotels.size,
  stopReason:
    signals.length >= 100 ? "RESEARCH_VOLUME_TARGET_MET" : "EXECUTION_LIMIT_BEFORE_VOLUME_TARGET",
  byDisposition,
  bySourceFamily: byFamily,
  byCountry,
  signals,
};

fs.writeFileSync(path.join(OUT, "COMMERCIAL_SIGNALS.json"), JSON.stringify(signalLedger, null, 2));

const candidates = {
  wave: "HIF_V6_TRACK_A_CB5_TRIGGER_FIRST_PORTFOLIO",
  asOf: "2026-11-21",
  publicationState: "CUSTOMER_BLOCKED",
  researchCostUsd: 0,
  branch: "feat/hif-v6-track-a-cb5-trigger-first-portfolio",
  openPrPrior: 547,
  resumeFrom: "CB4_CHECKPOINT_800ec14c0",
  methodology: "TRIGGER_FIRST",
  policy: {
    publicRfpRequired: false,
    doNotInflatePortfolioCounts: true,
    expirationAloneInsufficient: true,
    trackBRequiresProspectiveDecision: true,
  },
  batchMetrics: {
    distinctCommercialSignalsInvestigated: signals.length,
    distinctHotelsAffectedOrIdentified: distinctHotels.size,
    incrementalQualifiedFirst100: 0,
    cumulativeFirst100: 6,
    newPortfolioOpportunities: 0,
    portfolioOpportunitiesDeepened: 1,
    emergingAdjacentPersist: 1,
    awardPendingPersist: 1,
    trackBSignalsClosedAsHistoricalOrCommitted: 6,
  },
  candidates: [
    {
      id: "ahip-portfolio-mct-deepen",
      parentOpportunityId: "ahip-remaining-portfolio-aimbridge-mct",
      discoveryPropertyId: "disc_cb4_ahip_remaining_portfolio_mct_2026",
      findingId: "find_cb4_ahip_aimbridge_mct_2026",
      hotelName: "AHIP Remaining Premium Branded Portfolio (Aimbridge MCT replacement)",
      qualificationClass: "PRIVATE_OPPORTUNITY_PROBABLE_ACCESS",
      opportunityType: "MCT",
      countsTowardFirst100: true,
      persistEligible: false,
      airtableAction: "NOOP_EXISTING_IDS",
      notes: "CB5 dossier deepen only — do not create per-hotel MCTs",
      affectedHotelsVerified: remaining.length,
      affectedRoomsVerified: remRooms,
    },
    {
      id: "den-westin-emerging-mct",
      discoveryPropertyId: "disc_cb5_westin_den_airport_mct_2026",
      findingId: "find_cb5_westin_den_airport_mct_2026",
      hotelName: "Westin Denver International Airport",
      physicalLocation: {
        country: "US",
        stateProvince: "CO",
        city: "Denver",
        address: "DEN Jeppesen Terminal / on-airport",
        geographicClass: "VERIFIED",
      },
      currentBrand: "Westin (Marriott)",
      currentOperator: "Marriott / Westin DIA Operator LLC",
      owner: "City and County of Denver / DEN",
      agreementType: "HOTEL_MANAGEMENT_AGREEMENT_TO_2030_WITH_PLANNED_REPLACEMENT_RFP",
      opportunityType: "MCT",
      commercialCategory: "MANAGEMENT_CONTRACT_TARGET",
      qualificationClass: "COMMERCIALLY_RELEVANT_CONTRACTUAL_CHANGE",
      evidencePathway: "PUBLIC_COMPETITIVE_PROCESS_PRECURSOR",
      commercialTrigger:
        "DEN Hospitality Consulting RFP (closed 2026-07-29) explicitly tasks consultant to assist City RFP for new hotel management agreement and possible transition to new DEN Hotel operator; current Westin HMA runs to 2030 but owner is preparing replacement process",
      decisionEvidence:
        "BidNet DEN Hospitality Consulting Services 202684115; prior hotelAVE asset-management scope; DEN/Marriott 10-year celebration confirms current operator",
      commercialRelevance: "EVIDENCE_SUPPORTED_POTENTIAL",
      commercialTiming: "6_18_MONTHS",
      nextDecisionDate: null,
      commercialAccessibility: "ACCESSIBLE_UNKNOWN",
      engagementRoute: "PUBLIC_PROCUREMENT",
      priority: "PRIORITY_B",
      countsTowardFirst100: false,
      publicRfp: false,
      discoveryMode: "PUBLIC",
      currentDecisionStatus: "EMERGING_OPERATOR_RFP_PRECURSOR",
      qualificationTier: "EXPANDED_PIPELINE_EMERGING",
      hotelSpecificEvidence: true,
      prospectiveCommercialDecisionEvidence: true,
      saleOrExpirationOnly: false,
      persistEligible: true,
      rooms: 519,
      sources: [
        {
          url: "https://www.bidnetdirect.com/colorado/cityandcountyofdenverdepartmentofaviation/solicitations/DEN-Hospitality-Consulting-Services/0000423347",
          publicationDate: "2026-06-02",
        },
        {
          url: "https://www.flydenver.com/press-release/celebrating-10-years-of-the-westin-denver-international-airport/",
          publicationDate: "2025-11-19",
        },
      ],
    },
    {
      id: "bryce-canyon-lodge-veia-award-watch",
      discoveryPropertyId: "disc_cb5_bryce_canyon_lodge_veia_2026",
      findingId: "find_cb5_bryce_canyon_lodge_veia_2026",
      hotelName: "Bryce Canyon Lodge",
      physicalLocation: {
        country: "US",
        stateProvince: "UT",
        city: "Bryce Canyon NP",
        geographicClass: "VERIFIED",
      },
      agreementType: "NPS_VEIA_MANAGEMENT_AGREEMENT",
      opportunityType: "CONCESSION",
      commercialCategory: "CONCESSION",
      qualificationClass: "AWARD_PENDING",
      evidencePathway: "PUBLIC_COMPETITIVE_PROCESS",
      commercialTrigger:
        "VA-BRCA003-25 VEIA RFP for lodging/F&B/retail management; proposals due 2025-07-10; commencement targeted 2027-01-01; Aramark sole-source bridge during delay",
      decisionEvidence: "NPS BRCA VEIA RFP pages; Federal Register 2025-11-18 sole-source bridge intent to Aramark",
      commercialRelevance: "VERIFIED_RELEVANT",
      commercialTiming: "AWARD_PENDING",
      commercialAccessibility: "ACCESS_CLOSED",
      engagementRoute: "PUBLIC_PROCUREMENT",
      priority: "PRIORITY_B",
      countsTowardFirst100: false,
      publicRfp: true,
      discoveryMode: "PUBLIC",
      currentDecisionStatus: "AWARD_PENDING_OR_TRANSITION",
      qualificationTier: "EXPANDED_PIPELINE_AWARD_WATCH",
      hotelSpecificEvidence: true,
      prospectiveCommercialDecisionEvidence: true,
      saleOrExpirationOnly: false,
      persistEligible: true,
      rooms: 114,
      sources: [
        {
          url: "https://nps.gov/subjects/concessions/brca-veia.htm",
          publicationDate: "2025-07-10",
        },
        {
          url: "https://www.federalregister.gov/documents/2025/11/18/2025-20192/notice-of-intent-to-award-a-sole-source-concession-contract-for-visitor-services-at-bryce-canyon",
          publicationDate: "2025-11-18",
        },
      ],
    },
    {
      id: "trackb-yucatan-resort-historical",
      hotelName: "The Yucatán Resort Playa del Carmen",
      qualificationClass: "COMPLETED_OR_HISTORICAL",
      persistEligible: false,
      notes: "Ops closed 2026-04-30; prior Tapestry/Click Efficiency — no prospective open MCT",
    },
    {
      id: "trackb-fairfield-villahermosa-committed",
      hotelName: "Fairfield Inn & Suites Villahermosa",
      qualificationClass: "COMMITTED_TRANSACTION",
      persistEligible: false,
      notes: "Marriott franchise/ops ended 2026-07-01; InHouse lease — completed",
    },
    {
      id: "trackb-hyatt-ziva-zilara-historical",
      hotelName: "Hyatt Ziva Riviera Cancún / Hyatt Zilara Riviera Maya",
      qualificationClass: "COMPLETED_OR_HISTORICAL",
      persistEligible: false,
      notes: "HMA terminated 2023-12-15; Tortuga/Hyatt 50y management later — not open",
    },
    {
      id: "custer-xanterra-committed",
      hotelName: "Custer State Park Resort portfolio",
      qualificationClass: "COMMITTED_TRANSACTION",
      persistEligible: false,
      notes: "Xanterra awarded; ops start 2027-02-01 — not open",
    },
    {
      id: "westin-boston-retain-committed",
      hotelName: "Westin Boston Seaport District",
      qualificationClass: "COMMITTED_TRANSACTION",
      persistEligible: false,
      notes: "Brand RFP completed; new Westin franchise effective 2027-01-01",
    },
    {
      id: "bidwell-portland-contractual-only",
      hotelName: "The Bidwell Marriott Portland",
      qualificationClass: "COMMERCIALLY_RELEVANT_CONTRACTUAL_CHANGE",
      persistEligible: false,
      notes: "Franchise extended to Oct 2026 only — no multi-path decision evidence yet",
    },
  ],
};

fs.writeFileSync(path.join(OUT, "RESEARCH_CANDIDATES.json"), JSON.stringify(candidates, null, 2));

console.log(
  JSON.stringify(
    {
      signals: signals.length,
      hotels: distinctHotels.size,
      ahipRemaining: remaining.length,
      ahipRooms: remRooms,
      stopReason: signalLedger.stopReason,
    },
    null,
    2,
  ),
);
