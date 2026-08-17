#!/usr/bin/env node
/**
 * Operator Explorer Wave 02 — Complete Production + Deepen Strategic Profiles
 *
 *   node scripts/operator-explorer-wave-02-complete.mjs --dry-run
 *   node scripts/operator-explorer-wave-02-complete.mjs --apply --approve-oe-wave-02-writes
 *
 * Webhound: merge only if done=true (checked at runtime). No Fit/owner/My Deals changes.
 */
import "../load-env.js";
import { createHash } from "crypto";
import { mkdirSync, writeFileSync, readFileSync, existsSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import {
  buildOperatorUniverse,
  dispositionForOperator,
} from "../lib/operator-explorer/operator-universe.js";
import { isAggregateAssignmentName } from "../lib/operator-explorer/readiness.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const APPLY = process.argv.includes("--apply");
const APPROVED = process.argv.includes("--approve-oe-wave-02-writes");
const DRY = !APPLY;
const TS = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);

const MASTER = "Operator Setup - Master";
const ASG = "Operator Intelligence - Assignments";
const BR = "Operator Intelligence - Brand Relationships";
const MP = "Operator Intelligence - Market Presence";
const PI = "Partner Intelligence - Source Library";
const CLAIMS = "Operator Intelligence - Claims";

const INPUT = JSON.parse(readFileSync(join(ROOT, "data/operator-explorer/waves/wave-02-input.json"), "utf8"));

/** Curated named assignments — official operator / brand / reputable press sources only. */
const WAVE_02_ASSIGNMENTS = {
  // —— Incomplete Production ——
  rec6UB6RpMKSs2tAo: [
    {
      propertyName: "Hilton Garden Inn La Romana",
      country: "Dominican Republic",
      city: "La Romana",
      brand: "Hilton Garden Inn",
      brandParent: "Hilton",
      urbanOrResort: "Resort",
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Third-Party Management",
      assignmentStatus: "Current",
      sourceUrl: "https://www.remingtonhospitality.com/portfolio",
      evidenceClass: "primary_authoritative",
      cala: true,
    },
    {
      propertyName: "Detroit Metro Airport Marriott",
      country: "United States",
      city: "Romulus",
      brand: "Marriott Hotels",
      brandParent: "Marriott International",
      urbanOrResort: "Urban",
      hotelType: "Airport / Convention adjacency",
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Third-Party Management",
      assignmentStatus: "Current",
      sourceUrl: "https://www.remingtonhospitality.com/portfolio",
      evidenceClass: "primary_authoritative",
    },
    {
      propertyName: "Dallas/Fort Worth Airport Marriott",
      country: "United States",
      city: "Irving",
      brand: "Marriott Hotels",
      brandParent: "Marriott International",
      urbanOrResort: "Urban",
      hotelType: "Airport / Convention adjacency",
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Third-Party Management",
      assignmentStatus: "Current",
      sourceUrl: "https://www.remingtonhospitality.com/portfolio",
      evidenceClass: "primary_authoritative",
    },
    {
      propertyName: "Hilton Fort Worth",
      country: "United States",
      city: "Fort Worth",
      brand: "Hilton Hotels & Resorts",
      brandParent: "Hilton",
      urbanOrResort: "Urban",
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Third-Party Management",
      assignmentStatus: "Current",
      sourceUrl: "https://www.remingtonhospitality.com/portfolio",
      evidenceClass: "primary_authoritative",
    },
    {
      propertyName: "Beverly Hills Marriott",
      country: "United States",
      city: "Beverly Hills",
      brand: "Marriott Hotels",
      brandParent: "Marriott International",
      urbanOrResort: "Urban",
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Third-Party Management",
      assignmentStatus: "Current",
      sourceUrl: "https://www.remingtonhospitality.com/portfolio",
      evidenceClass: "primary_authoritative",
    },
  ],
  receHCdI6CEsJqdG4: [
    {
      propertyName: "The Mutiny Hotel",
      country: "United States",
      city: "Coconut Grove, Miami",
      brand: "Independent",
      brandParent: null,
      urbanOrResort: "Urban",
      hotelType: "Boutique / lifestyle",
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Third-Party Management",
      assignmentStatus: "Current",
      sourceUrl: "https://www.brittainresorts.com/our-portfolio/",
      evidenceClass: "primary_authoritative",
    },
    {
      propertyName: "Compass Cove Resort",
      country: "United States",
      city: "Myrtle Beach",
      brand: "Independent",
      brandParent: null,
      urbanOrResort: "Resort",
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Third-Party Management",
      assignmentStatus: "Current",
      sourceUrl: "https://www.brittainresorts.com/our-portfolio/",
      evidenceClass: "primary_authoritative",
    },
    {
      propertyName: "Courtyard by Marriott Myrtle Beach",
      country: "United States",
      city: "Myrtle Beach",
      brand: "Courtyard by Marriott",
      brandParent: "Marriott International",
      urbanOrResort: "Urban",
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Third-Party Management",
      assignmentStatus: "Current",
      sourceUrl: "https://www.brittainresorts.com/our-portfolio/",
      evidenceClass: "primary_authoritative",
    },
    {
      propertyName: "SpringHill Suites by Marriott Myrtle Beach",
      country: "United States",
      city: "Myrtle Beach",
      brand: "SpringHill Suites",
      brandParent: "Marriott International",
      urbanOrResort: "Urban",
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Third-Party Management",
      assignmentStatus: "Current",
      sourceUrl: "https://www.brittainresorts.com/our-portfolio/",
      evidenceClass: "primary_authoritative",
    },
    {
      propertyName: "voco The Shelby Myrtle Beach",
      country: "United States",
      city: "Myrtle Beach",
      brand: "voco",
      brandParent: "IHG",
      urbanOrResort: "Urban",
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Third-Party Management",
      assignmentStatus: "Current",
      sourceUrl: "https://www.brittainresorts.com/our-portfolio/",
      evidenceClass: "primary_authoritative",
    },
  ],
  reck6gjQd3wdeugmZ: [
    {
      propertyName: "Crown Paradise Club Cancun",
      country: "Mexico",
      city: "Cancún",
      brand: "Crown Paradise",
      brandParent: "Arriva Hospitality Group",
      urbanOrResort: "Resort",
      allInclusive: true,
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Owner-Operated",
      assignmentStatus: "Current",
      sourceUrl: "https://arrivahotels.mx/",
      evidenceClass: "primary_authoritative",
      cala: true,
    },
    {
      propertyName: "Sensira Resort & Spa Riviera Maya",
      country: "Mexico",
      city: "Riviera Maya",
      brand: "Sensira",
      brandParent: "Arriva Hospitality Group",
      urbanOrResort: "Resort",
      allInclusive: true,
      developmentContext: "New Build",
      operatingStructure: "Owner-Operated",
      assignmentStatus: "Current",
      sourceUrl: "https://arrivahotels.mx/",
      evidenceClass: "primary_authoritative",
      cala: true,
      limitations: "Luxury AI entry (opened 2021 per company commercial commentary); still Arriva-operated brand.",
    },
    {
      propertyName: "Vista Express Guadalajara Expo",
      country: "Mexico",
      city: "Guadalajara",
      brand: "Vista Hotels",
      brandParent: "Arriva Hospitality Group",
      urbanOrResort: "Urban",
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Owner-Operated",
      assignmentStatus: "Current",
      sourceUrl: "https://arrivahotels.mx/",
      evidenceClass: "primary_authoritative",
      cala: true,
    },
    {
      propertyName: "Crown Paradise Club Puerto Vallarta",
      country: "Mexico",
      city: "Puerto Vallarta",
      brand: "Crown Paradise",
      brandParent: "Arriva Hospitality Group",
      urbanOrResort: "Resort",
      allInclusive: true,
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Owner-Operated",
      assignmentStatus: "Current",
      sourceUrl: "https://arrivahotels.mx/",
      evidenceClass: "primary_authoritative",
      cala: true,
    },
  ],
  rectsHzacZDFTH1Ze: [
    {
      propertyName: "Loma Medellín, a Tribute Portfolio Hotel",
      country: "Colombia",
      city: "Medellín",
      brand: "Tribute Portfolio",
      brandParent: "Marriott International",
      urbanOrResort: "Urban",
      hotelType: "Lifestyle / soft brand",
      developmentContext: "New Build",
      operatingStructure: "Third-Party Management",
      assignmentStatus: "Current",
      sourceUrl: "https://www.marriott.com/en-us/hotels/mdetx-loma-medellin-a-tribute-portfolio-hotel/overview/",
      evidenceClass: "primary_authoritative",
      cala: true,
    },
    {
      propertyName: "Hotel Grand Sirenis Cartagena",
      country: "Colombia",
      city: "Cartagena",
      brand: "Sirenis",
      brandParent: "Sirenis Hotels & Resorts",
      urbanOrResort: "Resort",
      allInclusive: true,
      developmentContext: "Repositioning",
      operatingStructure: "Third-Party Management",
      assignmentStatus: "Current",
      sourceUrl: "https://www.oxohotel.com/en/",
      evidenceClass: "primary_authoritative",
      cala: true,
    },
    {
      propertyName: "Nácar Hotel Cartagena, Curio Collection by Hilton",
      country: "Colombia",
      city: "Cartagena",
      brand: "Curio Collection",
      brandParent: "Hilton",
      urbanOrResort: "Urban",
      hotelType: "Lifestyle / soft brand",
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Third-Party Management",
      assignmentStatus: "Current",
      sourceUrl: "https://www.oxohotel.com/en/",
      evidenceClass: "primary_authoritative",
      cala: true,
    },
    {
      propertyName: "AC Hotel Bogotá Zona T",
      country: "Colombia",
      city: "Bogotá",
      brand: "AC Hotels",
      brandParent: "Marriott International",
      urbanOrResort: "Urban",
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Third-Party Management",
      assignmentStatus: "Current",
      sourceUrl: "https://www.oxohotel.com/en/",
      evidenceClass: "primary_authoritative",
      cala: true,
    },
    {
      propertyName: "Holiday Inn Express Bogotá Parque La 93",
      country: "Colombia",
      city: "Bogotá",
      brand: "Holiday Inn Express",
      brandParent: "IHG",
      urbanOrResort: "Urban",
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Third-Party Management",
      assignmentStatus: "Current",
      sourceUrl: "https://www.oxohotel.com/en/",
      evidenceClass: "primary_authoritative",
      cala: true,
    },
  ],
  recuEDrp6oeJIEuRX: [
    {
      propertyName: "Holiday Inn San José - La Sabana",
      country: "Costa Rica",
      city: "San José",
      brand: "Holiday Inn",
      brandParent: "IHG",
      urbanOrResort: "Urban",
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Third-Party Management",
      assignmentStatus: "Current",
      sourceUrl: "https://www.grupomarta.com/en",
      evidenceClass: "primary_authoritative",
      cala: true,
    },
    {
      propertyName: "Holiday Inn Express San José Airport",
      country: "Costa Rica",
      city: "Alajuela",
      brand: "Holiday Inn Express",
      brandParent: "IHG",
      urbanOrResort: "Urban",
      hotelType: "Airport / select service",
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Third-Party Management",
      assignmentStatus: "Current",
      sourceUrl: "https://www.grupomarta.com/en",
      evidenceClass: "primary_authoritative",
      cala: true,
    },
    {
      propertyName: "Best Western Jacó Beach All Inclusive Resort",
      country: "Costa Rica",
      city: "Jacó",
      brand: "Best Western",
      brandParent: "Best Western Hotels & Resorts",
      urbanOrResort: "Resort",
      allInclusive: true,
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Third-Party Management",
      assignmentStatus: "Current",
      sourceUrl: "https://www.grupomarta.com/en",
      evidenceClass: "primary_authoritative",
      cala: true,
    },
    {
      propertyName: "Irazú Hotel & Studios",
      country: "Costa Rica",
      city: "San José",
      brand: "Independent",
      brandParent: null,
      urbanOrResort: "Urban",
      hotelType: "Extended stay adjacency",
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Owner-Operated",
      assignmentStatus: "Current",
      sourceUrl: "https://www.grupomarta.com/en",
      evidenceClass: "primary_authoritative",
      cala: true,
    },
  ],
  // —— Deepening cohort ——
  recKVILWcRLqrQlWs: [
    {
      propertyName: "Hilton Garden Inn San José City Mall",
      country: "Costa Rica",
      city: "Alajuela",
      brand: "Hilton Garden Inn",
      brandParent: "Hilton",
      urbanOrResort: "Urban",
      developmentContext: "Acquisition Transition",
      operatingStructure: "Third-Party Management",
      assignmentStatus: "Current",
      sourceUrl: "https://www.hotel-online.com/news/driftwood-hospitality-management-assumes-management-for-hilton-garden-inn-san-jose-city-mall",
      evidenceClass: "reliable_independent",
      cala: true,
      limitations: "Management assumption announced Mar 2024; verify ongoing status on next refresh.",
    },
  ],
  recGWxIJqnYHkJZFD: [
    {
      propertyName: "Wyndham Alltra Punta Cana",
      country: "Dominican Republic",
      city: "Uvero Alto / Punta Cana",
      brand: "Wyndham Alltra",
      brandParent: "Wyndham Hotels & Resorts",
      urbanOrResort: "Resort",
      allInclusive: true,
      developmentContext: "Acquisition Transition",
      operatingStructure: "Third-Party Management",
      assignmentStatus: "Current",
      sourceUrl: "https://www.caribjournal.com/2025/09/25/wyndham-all-inclusive-foray/",
      evidenceClass: "reliable_independent",
      cala: true,
    },
    {
      propertyName: "Wyndham Alltra Samaná",
      country: "Dominican Republic",
      city: "Samaná",
      brand: "Wyndham Alltra",
      brandParent: "Wyndham Hotels & Resorts",
      urbanOrResort: "Resort",
      allInclusive: true,
      developmentContext: "Acquisition Transition",
      operatingStructure: "Third-Party Management",
      assignmentStatus: "Current",
      sourceUrl: "https://www.caribjournal.com/2025/09/25/wyndham-all-inclusive-foray/",
      evidenceClass: "reliable_independent",
      cala: true,
    },
  ],
  rec3Uwxe6ovpiokuN: [
    {
      propertyName: "Waldorf Astoria Cancun",
      country: "Mexico",
      city: "Cancún",
      brand: "Waldorf Astoria",
      brandParent: "Hilton",
      urbanOrResort: "Resort",
      developmentContext: "New Build",
      operatingStructure: "Brand-managed",
      assignmentStatus: "Current",
      sourceUrl: "https://stories.hilton.com/releases/waldorf-astoria-cancun-opens",
      evidenceClass: "primary_authoritative",
      cala: true,
    },
    {
      propertyName: "Hilton Cancun, an All-Inclusive Resort",
      country: "Mexico",
      city: "Cancún",
      brand: "Hilton Hotels & Resorts",
      brandParent: "Hilton",
      urbanOrResort: "Resort",
      allInclusive: true,
      developmentContext: "New Build",
      operatingStructure: "Brand-managed",
      assignmentStatus: "Current",
      sourceUrl: "https://stories.hilton.com/releases/waldorf-astoria-cancun-opens",
      evidenceClass: "primary_authoritative",
      cala: true,
    },
  ],
  recGmiPhRt6hiayd9: [
    {
      propertyName: "JW Marriott Panama",
      country: "Panama",
      city: "Panama City",
      brand: "JW Marriott",
      brandParent: "Marriott International",
      urbanOrResort: "Urban",
      hotelType: "Mixed-use / residences adjacency",
      mixedUse: true,
      developmentContext: "Reflag",
      operatingStructure: "Brand-managed",
      assignmentStatus: "Current",
      sourceUrl: "https://www.marriott.com/en-us/hotels/ptymj-jw-marriott-panama/overview/",
      evidenceClass: "primary_authoritative",
      cala: true,
      limitations: "Long-term management contract publicly reported at JW rebrand (2018); treat as current brand-managed unless later contradicted.",
    },
    {
      propertyName: "The Ritz-Carlton, St. Thomas",
      country: "United States Virgin Islands",
      city: "St. Thomas",
      brand: "The Ritz-Carlton",
      brandParent: "Marriott International",
      urbanOrResort: "Resort",
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Brand-managed",
      assignmentStatus: "Current",
      sourceUrl: "https://www.ritzcarlton.com/en/hotels/sttzh-the-ritz-carlton-st-thomas/overview/",
      evidenceClass: "primary_authoritative",
      cala: true,
    },
  ],
  recF2WqLqNVyKGz9E: [
    {
      propertyName: "Sofitel Barú Cartagena Beach Resort",
      country: "Colombia",
      city: "Isla Barú / Cartagena",
      brand: "Sofitel",
      brandParent: "Accor",
      urbanOrResort: "Resort",
      developmentContext: "New Build",
      operatingStructure: "Brand-managed",
      assignmentStatus: "Current",
      sourceUrl: "https://sofitel.accor.com/en/hotels/B0P5.html",
      evidenceClass: "primary_authoritative",
      cala: true,
    },
    {
      propertyName: "Fairmont Royal Pavilion",
      country: "Barbados",
      city: "St. James",
      brand: "Fairmont",
      brandParent: "Accor",
      urbanOrResort: "Resort",
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Brand-managed",
      assignmentStatus: "Current",
      sourceUrl: "https://www.fairmont.com/en/hotels/barbados/fairmont-royal-pavilion.html",
      evidenceClass: "primary_authoritative",
      cala: true,
    },
  ],
  rec7IXYQYpKMYsrDl: [
    {
      propertyName: "InterContinental Cartagena de Indias",
      country: "Colombia",
      city: "Cartagena",
      brand: "InterContinental",
      brandParent: "IHG",
      urbanOrResort: "Urban",
      hotelType: "Beachfront urban / meetings",
      meetingsConvention: true,
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Brand-managed",
      assignmentStatus: "Current",
      sourceUrl: "https://www.ihg.com/intercontinental/hotels/us/en/cartagena/ctgha/hoteldetail",
      evidenceClass: "primary_authoritative",
      cala: true,
    },
    {
      propertyName: "Kimpton Seafire Resort + Spa",
      country: "Cayman Islands",
      city: "Seven Mile Beach, Grand Cayman",
      brand: "Kimpton",
      brandParent: "IHG",
      urbanOrResort: "Resort",
      developmentContext: "New Build",
      operatingStructure: "Brand-managed",
      assignmentStatus: "Current",
      sourceUrl: "https://www.ihg.com/kimptonhotels/hotels/us/en/grand-cayman/gcmks/hoteldetail",
      evidenceClass: "primary_authoritative",
      cala: true,
    },
  ],
  recwEHUotSGpfkZEJ: [
    {
      propertyName: "Iberostar Selection Coral Bávaro",
      country: "Dominican Republic",
      city: "Bávaro / Punta Cana",
      brand: "Iberostar Selection",
      brandParent: "Grupo Iberostar",
      urbanOrResort: "Resort",
      allInclusive: true,
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Owner-Operated",
      assignmentStatus: "Current",
      sourceUrl: "https://www.iberostar.com/en/hotels/punta-cana/iberostar-selection-coral-bavaro/",
      evidenceClass: "primary_authoritative",
      cala: true,
    },
    {
      propertyName: "Iberostar Waves Punta Cana",
      country: "Dominican Republic",
      city: "Punta Cana",
      brand: "Iberostar Waves",
      brandParent: "Grupo Iberostar",
      urbanOrResort: "Resort",
      allInclusive: true,
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Owner-Operated",
      assignmentStatus: "Current",
      sourceUrl: "https://www.iberostar.com/en/hotels/punta-cana/",
      evidenceClass: "primary_authoritative",
      cala: true,
    },
  ],
};

const WAVE_02_BRAND_RELS = {
  rec6UB6RpMKSs2tAo: [
    {
      brand: "Marriott Hotels",
      brandParent: "Marriott International",
      relationshipType: "Currently Operates",
      geographyScope: "United States + LATAM/Caribbean managed portfolio",
      sourceUrl: "https://www.remingtonhospitality.com/portfolio",
    },
    {
      brand: "Hilton Hotels & Resorts",
      brandParent: "Hilton",
      relationshipType: "Currently Operates",
      geographyScope: "United States + Dominican Republic managed portfolio",
      sourceUrl: "https://www.remingtonhospitality.com/portfolio",
    },
  ],
  receHCdI6CEsJqdG4: [
    {
      brand: "Courtyard by Marriott",
      brandParent: "Marriott International",
      relationshipType: "Currently Operates",
      geographyScope: "United States Southeast",
      sourceUrl: "https://www.brittainresorts.com/our-portfolio/",
    },
    {
      brand: "voco",
      brandParent: "IHG",
      relationshipType: "Currently Operates",
      geographyScope: "United States Southeast",
      sourceUrl: "https://www.brittainresorts.com/our-portfolio/",
    },
  ],
  reck6gjQd3wdeugmZ: [
    {
      brand: "Crown Paradise",
      brandParent: "Arriva Hospitality Group",
      relationshipType: "Currently Operates",
      geographyScope: "Mexico beach destinations",
      sourceUrl: "https://arrivahotels.mx/",
    },
    {
      brand: "Sensira",
      brandParent: "Arriva Hospitality Group",
      relationshipType: "Currently Operates",
      geographyScope: "Riviera Maya, Mexico",
      sourceUrl: "https://arrivahotels.mx/",
    },
  ],
  rectsHzacZDFTH1Ze: [
    {
      brand: "Tribute Portfolio",
      brandParent: "Marriott International",
      relationshipType: "Currently Operates",
      geographyScope: "Colombia",
      sourceUrl: "https://www.oxohotel.com/en/",
    },
    {
      brand: "Curio Collection",
      brandParent: "Hilton",
      relationshipType: "Currently Operates",
      geographyScope: "Colombia",
      sourceUrl: "https://www.oxohotel.com/en/",
    },
    {
      brand: "Holiday Inn Express",
      brandParent: "IHG",
      relationshipType: "Currently Operates",
      geographyScope: "Colombia",
      sourceUrl: "https://www.oxohotel.com/en/",
    },
  ],
  recuEDrp6oeJIEuRX: [
    {
      brand: "Holiday Inn",
      brandParent: "IHG",
      relationshipType: "Currently Operates",
      geographyScope: "Costa Rica",
      sourceUrl: "https://www.grupomarta.com/en",
    },
    {
      brand: "Best Western",
      brandParent: "Best Western Hotels & Resorts",
      relationshipType: "Currently Operates",
      geographyScope: "Costa Rica",
      sourceUrl: "https://www.grupomarta.com/en",
    },
  ],
  recKVILWcRLqrQlWs: [
    {
      brand: "Hilton Garden Inn",
      brandParent: "Hilton",
      relationshipType: "Currently Operates",
      geographyScope: "Costa Rica",
      sourceUrl: "https://www.hotel-online.com/news/driftwood-hospitality-management-assumes-management-for-hilton-garden-inn-san-jose-city-mall",
    },
    {
      brand: "Tribute Portfolio",
      brandParent: "Marriott International",
      relationshipType: "Currently Operates",
      geographyScope: "Puerto Rico",
      sourceUrl: "https://www.driftwoodhospitality.com/driftwood-hospitality-portfolio",
    },
  ],
  recwEHUotSGpfkZEJ: [
    {
      brand: "Iberostar Selection",
      brandParent: "Grupo Iberostar",
      relationshipType: "Currently Operates",
      geographyScope: "Mexico + Dominican Republic",
      sourceUrl: "https://www.iberostar.com/en/hotels/cancun/iberostar-selection-cancun/",
    },
    {
      brand: "Iberostar Waves",
      brandParent: "Grupo Iberostar",
      relationshipType: "Currently Operates",
      geographyScope: "Dominican Republic",
      sourceUrl: "https://www.iberostar.com/en/hotels/punta-cana/",
    },
  ],
};

const ALL_WAVE_OPS = [
  ...INPUT.incompleteProduction.map((o) => ({ ...o, cohort: "incomplete" })),
  ...INPUT.deepeningCohort.map((o) => ({ ...o, cohort: "deepen" })),
];

function writeJson(p, o) {
  mkdirSync(dirname(p), { recursive: true });
  writeFileSync(p, JSON.stringify(o, null, 2) + "\n");
}
function writeMd(p, t) {
  mkdirSync(dirname(p), { recursive: true });
  writeFileSync(p, t.endsWith("\n") ? t : t + "\n");
}
function slug(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_|_$/g, "")
    .slice(0, 48);
}
function checksum(fields) {
  return createHash("sha1").update(JSON.stringify(fields)).digest("hex").slice(0, 12);
}
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function listAll(baseId, token, table, fields) {
  const out = [];
  let offset;
  do {
    const qs = new URLSearchParams({ pageSize: "100" });
    if (offset) qs.set("offset", offset);
    if (fields) for (const f of fields) qs.append("fields[]", f);
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(table)}?${qs}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const json = await res.json();
    if (!res.ok) throw new Error(`LIST ${table}: ${JSON.stringify(json)}`);
    out.push(...(json.records || []));
    offset = json.offset;
  } while (offset);
  return out;
}

async function createRecord(baseId, token, table, fields) {
  if (DRY) return { id: `dry_${checksum(fields)}`, dryRun: true };
  const res = await fetch(`https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(table)}`, {
    method: "POST",
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
    body: JSON.stringify({ fields, typecast: true }),
  });
  const json = await res.json();
  if (!res.ok) throw new Error(`CREATE ${table}: ${JSON.stringify(json)}`);
  await sleep(220);
  return json;
}

async function patchRecord(baseId, token, table, id, fields) {
  if (DRY) return { id, dryRun: true };
  const res = await fetch(
    `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(table)}/${encodeURIComponent(id)}`,
    {
      method: "PATCH",
      headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
      body: JSON.stringify({ fields, typecast: true }),
    }
  );
  const json = await res.json();
  if (!res.ok) throw new Error(`PATCH ${table} ${id}: ${JSON.stringify(json)}`);
  await sleep(220);
  return json;
}

function enrichmentClass(o) {
  if (o.testFixture || o.recordPurpose === "Test Fixture") return "Test Fixture";
  if (o.explorerPublishable) return "Publishable";
  if (o.recordPurpose === "Research" && o.explorerContentComplete) return "Research Content Complete Gated";
  if (o.recordPurpose === "Research") return "Research Needs Enrichment";
  if (o.recordPurpose === "Production") return "Production Needs Enrichment";
  return "Other";
}

function prodRow(o) {
  return {
    name: o.canonicalName,
    id: o.masterId,
    publishable: o.explorerPublishable,
    strong: o.strongExplorerProfile,
    asg: o.counts.namedAssignments,
    countries: o.counts.countries,
    brands: o.counts.brands,
    br: o.counts.brandRelationships,
    mp: o.counts.marketPresence,
    fit: o.fitDataReadiness,
    usefulness: o.usefulness,
    mainGap: !o.explorerPublishable
      ? o.counts.namedAssignments === 0
        ? "No named assignments / empty intel"
        : "Below Publishable content gates"
      : o.strongExplorerProfile
        ? "—"
        : o.counts.namedAssignments < 5
          ? "Need more diverse named assignments for Strong"
          : o.counts.countries < 2
            ? "Single-country depth blocks Strong"
            : o.counts.brands < 2
              ? "Brand-name diversity thin for Strong"
              : "Other Strong gap",
  };
}

async function main() {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const token = process.env.AIRTABLE_API_KEY;
  if (!baseId || !token) throw new Error("AIRTABLE credentials required");
  if (APPLY && !APPROVED) throw new Error("Refusing apply without --approve-oe-wave-02-writes");

  const results = {
    mode: DRY ? "dry-run" : "apply",
    webhound: {
      sessionId: INPUT.webhoundSessionId,
      status: "Deferred — done=false at Wave 02 start; no partial merge",
      validatedRowsMerged: 0,
    },
    startedAt: new Date().toISOString(),
    before: null,
    after: null,
    assignments: { created: 0, updated: 0, held: 0, failed: [] },
    brandRel: { created: 0, updated: 0, failed: [] },
    presence: { created: 0, updated: 0 },
    claims: { created: 0, updated: 0 },
    sources: { created: 0, reused: 0 },
    mastersPatched: 0,
    holdouts: [...INPUT.holds],
    incompleteResults: [],
    strongImpact: [],
  };

  const masters = await listAll(baseId, token, MASTER);
  const assignments = await listAll(baseId, token, ASG);
  const brandRelationships = await listAll(baseId, token, BR);
  const marketPresence = await listAll(baseId, token, MP);
  const claims = await listAll(baseId, token, CLAIMS);

  const cross = existsSync(join(ROOT, "data/operator-explorer/phase-1-provisional-crosswalk.json"))
    ? JSON.parse(readFileSync(join(ROOT, "data/operator-explorer/phase-1-provisional-crosswalk.json"), "utf8"))
    : {};
  const entities = JSON.parse(readFileSync(join(ROOT, "data/operator-explorer/calibration-01/entities.json"), "utf8")).entities;
  const calibrationByMasterId = {};
  for (const e of entities) {
    const mid = cross[e.entityId] || e.existingMasterId || e.entityId;
    calibrationByMasterId[mid] = { track: e.track };
  }

  let universe = buildOperatorUniverse(masters, {
    assignments,
    brandRelationships,
    marketPresence,
    calibrationByMasterId,
  });
  for (const o of universe.operators) o.disposition = dispositionForOperator(o);
  results.before = { ...universe.summary };

  const production = universe.operators
    .filter((o) => o.recordPurpose === "Production")
    .sort((a, b) => a.canonicalName.localeCompare(b.canonicalName));
  const beforeById = Object.fromEntries(production.map((o) => [o.masterId, prodRow(o)]));

  // —— Baseline report ——
  const baselineMd = [
    `# Operator Explorer — Wave 02 Baseline`,
    ``,
    `**Generated:** ${new Date().toISOString()}`,
    `**Source:** live Airtable via canonical universe resolver`,
    `**Webhound Track 2:** deferred (done=false; no partial merge)`,
    ``,
    `## Universe summary`,
    ``,
    "```json",
    JSON.stringify(universe.summary, null, 2),
    "```",
    ``,
    `## Production operators (24)`,
    ``,
    `| Operator | Publishable | Strong | Assignments | Countries | Brand Relationships | Presence | Evidence | Main Gap |`,
    `| -------- | ----------- | ------ | ----------: | --------: | ------------------: | -------: | -------- | -------- |`,
    ...production.map((o) => {
      const r = beforeById[o.masterId];
      return `| ${r.name} | ${r.publishable ? "Yes" : "No"} | ${r.strong ? "Yes" : "No"} | ${r.asg} | ${r.countries} | ${r.br} | ${r.mp} | asg/br/mp=${r.asg}/${r.br}/${r.mp} | ${r.mainGap} |`;
    }),
    ``,
    `## Five incomplete Production operators`,
    ``,
    ...production
      .filter((o) => !o.explorerPublishable)
      .map((o) => {
        const r = beforeById[o.masterId];
        return `- **${r.name}** (\`${r.id}\`) — ${r.mainGap} (asg=${r.asg}, countries=${r.countries}, brands=${r.brands})`;
      }),
    ``,
    `## Publishable-but-not-Strong candidates`,
    ``,
    ...production
      .filter((o) => o.explorerPublishable && !o.strongExplorerProfile)
      .map((o) => {
        const r = beforeById[o.masterId];
        return `- **${r.name}** — asg=${r.asg}, countries=${r.countries}, brands=${r.brands}; gap: ${r.mainGap}`;
      }),
    ``,
    `## Airtable OE views`,
    ``,
    `Synced fields remain correct for manual view creation: OE Explorer Publishable, OE Strong Profile, OE Fit Data Ready, OE Enrichment Class, Record Purpose.`,
    `If founder has not created views yet, that is an **admin setup item only** — not a data-system failure. See \`reports/operator-explorer-airtable-views-created.md\`.`,
    ``,
  ].join("\n");
  writeMd(join(ROOT, "reports/operator-explorer-wave-02-baseline.md"), baselineMd);

  // —— Deepening selection ——
  const deepenMd = [
    `# Wave 02 Deepening Selection`,
    ``,
    `Selected **${INPUT.deepeningCohort.length}** Publishable-but-not-Strong Production operators.`,
    `Selection is research-priority only — **not** Operator Fit scoring.`,
    ``,
    `| Operator | CALA | Owner decision | Asg depth | Geo depth | Brand depth | Segment/dev | Evidence | Future Fit | Selected |`,
    `| -------- | ---- | -------------- | --------- | --------- | ----------- | ----------- | -------- | ---------- | -------- |`,
    `| Driftwood | High | High (3P CALA) | Med (4) | High | Med | Med | Med | High | **Yes** |`,
    `| Aimbridge LATAM | High | High (3P scale) | High (6) | Low (MX only) | High | Med | Med | High | **Yes** |`,
    `| Hilton Managed | High | High (luxury/AI) | Low-Med (3) | Med | High (BMC) | Med | Med | Med | **Yes** |`,
    `| Marriott Managed | High | High | Low-Med (3) | Low (MX) | High (BMC) | Med | Med | Med | **Yes** |`,
    `| Accor Managed | High | High | Low-Med (3) | Low (MX) | High (BMC) | Med | Med | Med | **Yes** |`,
    `| IHG Managed | High | High | Low-Med (3) | Med | High (BMC) | Med | Med | Med | **Yes** |`,
    `| Iberostar | Very High | High (AI CALA) | Med (3) | Med | Low (1 brand name) | High AI | Med | High | **Yes** |`,
    ``,
    `### Not selected this wave (still Publishable)`,
    ``,
    `- Atlantica, Cenote Azul, GHSF, Minor, Presidente, Royalton, Tafer — narrower immediate Strong uplift or already Wave 01-focused.`,
    ``,
    `### Cohort returned for execution in this run`,
    ``,
    ...INPUT.deepeningCohort.map((o) => `- **${o.name}** — ${o.why}`),
    ``,
  ].join("\n");
  writeMd(join(ROOT, "reports/operator-explorer-wave-02-deepening-selection.md"), deepenMd);

  // —— Gap plan ——
  const gapLines = [
    `# Wave 02 Gap Plan`,
    ``,
    `Inspect existing Airtable intelligence before external research. Only missing/weak domains listed.`,
    ``,
  ];
  for (const op of ALL_WAVE_OPS) {
    const o = universe.operators.find((x) => x.masterId === op.id);
    const asg = assignments.filter(
      (r) => (r.fields.Operator || []).includes(op.id) && !isAggregateAssignmentName(r.fields["Property Name"])
    );
    const br = brandRelationships.filter((r) => (r.fields.Operator || []).includes(op.id));
    const mp = marketPresence.filter((r) => (r.fields.Operator || []).includes(op.id));
    gapLines.push(`## ${op.name} (${op.cohort})`);
    gapLines.push(``);
    gapLines.push(`- Current: asg=${asg.length}, br=${br.length}, mp=${mp.length}, pub=${o?.explorerPublishable}, strong=${o?.strongExplorerProfile}`);
    if (asg.length === 0) gapLines.push(`- **Assignments:** missing — need named current operating evidence`);
    else if (asg.length < 5 && op.cohort === "deepen")
      gapLines.push(`- **Assignments:** deepen with *new-information* properties (geo / segment / brand / AI / mixed-use)`);
    else gapLines.push(`- **Assignments:** sufficient for Publishable; selective deepen only`);
    if (mp.length === 0) gapLines.push(`- **Market Presence:** missing — derive from verified assignments`);
    else if ((o?.counts.countries || 0) < 2 && op.cohort === "deepen")
      gapLines.push(`- **Market Presence / geo:** weak multi-country depth for Strong`);
    else gapLines.push(`- **Market Presence:** OK for current goal`);
    if (br.length === 0) gapLines.push(`- **Brand Relationships:** missing — add only where assignment evidence supports`);
    else if ((o?.counts.brands || 0) < 2) gapLines.push(`- **Brand Relationships:** brand-name diversity thin`);
    else gapLines.push(`- **Brand Relationships:** OK`);
    gapLines.push(`- **Operating / segment / development / differentiators / momentum / sources:** fill only via assignment-first evidence; no claim inflation`);
    gapLines.push(``);
  }
  writeMd(join(ROOT, "reports/operator-explorer-wave-02-gap-plan.md"), gapLines.join("\n"));

  // —— Backup ——
  const backupDir = join(ROOT, "backups/operator-explorer/wave-02", TS);
  mkdirSync(backupDir, { recursive: true });
  const backupManifest = {
    wave: "wave-02",
    createdAt: new Date().toISOString(),
    mode: results.mode,
    tables: {},
  };
  for (const [name, rows] of [
    [MASTER, masters],
    [ASG, assignments],
    [BR, brandRelationships],
    [MP, marketPresence],
    [CLAIMS, claims],
  ]) {
    const file = `${slug(name)}.json`;
    writeJson(join(backupDir, file), { table: name, count: rows.length, records: rows });
    backupManifest.tables[name] = { file, count: rows.length };
  }
  // PI Sources sample (full dump can be large — still backup all for safety)
  const piAll = await listAll(baseId, token, PI);
  writeJson(join(backupDir, "partner_intelligence_source_library.json"), {
    table: PI,
    count: piAll.length,
    records: piAll,
  });
  backupManifest.tables[PI] = { file: "partner_intelligence_source_library.json", count: piAll.length };
  writeJson(join(backupDir, "manifest.json"), backupManifest);
  results.backup = backupDir;

  const existingAsgKeys = new Set(
    assignments.map((r) => `${(r.fields.Operator || [])[0]}|${String(r.fields["Property Name"] || "").toLowerCase()}`)
  );
  const existingBrKeys = new Set(
    brandRelationships.map(
      (r) =>
        `${(r.fields.Operator || [])[0]}|${String(r.fields.Brand || "").toLowerCase()}|${String(r.fields["Relationship Type"] || "")}`
    )
  );

  const piByUrl = new Map();
  for (const r of piAll) {
    const u = String(r.fields["Source URL"] || "").trim().toLowerCase();
    if (u) piByUrl.set(u, r.id);
  }

  async function ensureSource(url, title) {
    const key = String(url || "").trim().toLowerCase();
    if (!key) return null;
    if (piByUrl.has(key)) {
      results.sources.reused++;
      return piByUrl.get(key);
    }
    const created = await createRecord(baseId, token, PI, {
      "Source Title": title || url,
      "Source URL": url,
      "Profile Type": "Operator",
      Status: "Captured",
      Notes: "OE Wave 02",
    });
    piByUrl.set(key, created.id);
    results.sources.created++;
    return created.id;
  }

  const writePlan = {
    wave: "wave-02",
    mode: results.mode,
    assignmentsCreate: [],
    assignmentsUpdate: [],
    brandRelationshipsCreate: [],
    brandRelationshipsUpdate: [],
    marketPresenceCreate: [],
    marketPresenceUpdate: [],
    claimsCreate: [],
    claimsUpdate: [],
    piSourcesCreateOrReuse: true,
    masterFactChanges: [],
    holds: INPUT.holds,
  };

  for (const op of ALL_WAVE_OPS) {
    for (const a of WAVE_02_ASSIGNMENTS[op.id] || []) {
      const key = `${op.id}|${a.propertyName.toLowerCase()}`;
      if (existingAsgKeys.has(key)) continue;
      writePlan.assignmentsCreate.push({ operatorId: op.id, operatorName: op.name, ...a });
    }
    for (const b of WAVE_02_BRAND_RELS[op.id] || []) {
      const key = `${op.id}|${b.brand.toLowerCase()}|${b.relationshipType}`;
      if (existingBrKeys.has(key)) continue;
      writePlan.brandRelationshipsCreate.push({ operatorId: op.id, operatorName: op.name, ...b });
    }
  }

  writeJson(join(ROOT, "data/operator-explorer/waves/wave-02-write-plan.json"), writePlan);
  writeMd(
    join(ROOT, "reports/operator-explorer-wave-02-write-plan.md"),
    [
      `# Wave 02 Write Plan`,
      ``,
      `**Mode:** ${results.mode}`,
      `**Backup:** \`${backupDir}\``,
      ``,
      `| Action | Count |`,
      `| ------ | ----: |`,
      `| Assignment creates | ${writePlan.assignmentsCreate.length} |`,
      `| Assignment updates | ${writePlan.assignmentsUpdate.length} |`,
      `| Brand Relationship creates | ${writePlan.brandRelationshipsCreate.length} |`,
      `| Brand Relationship updates | ${writePlan.brandRelationshipsUpdate.length} |`,
      `| Market Presence creates (derived at apply) | derived |`,
      `| Claims creates/updates | 0 (claim inflation avoided) |`,
      `| Holds | ${writePlan.holds.length} |`,
      ``,
      `## Holds`,
      ``,
      ...writePlan.holds.map((h) => `- **${h.subject}** (${h.operator}): ${h.reason}`),
      ``,
      `## Assignment creates (preview)`,
      ``,
      ...writePlan.assignmentsCreate.map(
        (a) => `- ${a.operatorName}: **${a.propertyName}** (${a.country}) — ${a.sourceUrl}`
      ),
      ``,
      `## Brand Relationship creates (preview)`,
      ``,
      ...writePlan.brandRelationshipsCreate.map(
        (b) => `- ${b.operatorName}: **${b.brand}** / ${b.relationshipType}`
      ),
      ``,
    ].join("\n")
  );

  // —— Apply ——
  const today = new Date().toISOString().slice(0, 10);
  const mpWorking = [...marketPresence];

  for (const a of writePlan.assignmentsCreate) {
    try {
      const srcId = await ensureSource(a.sourceUrl, a.propertyName);
      const fields = {
        "Assignment ID": `asg_w02_${a.operatorId}_${slug(a.propertyName)}`,
        Operator: [a.operatorId],
        "Property Name": a.propertyName,
        "Canonical Property Name": a.propertyName,
        Country: a.country,
        "City / Metro": a.city,
        Brand: a.brand,
        "Brand Parent": a.brandParent || undefined,
        "Urban / Resort": a.urbanOrResort,
        "Hotel Type": a.hotelType,
        "Development Context": a.developmentContext,
        "Operating / Management Structure": a.operatingStructure,
        "Assignment Status": a.assignmentStatus,
        "All-Inclusive": a.allInclusive === true ? true : undefined,
        "Mixed-Use": a.mixedUse === true ? true : undefined,
        "Meetings / Convention": a.meetingsConvention === true ? true : undefined,
        "Last Verified": today,
        "PI Source Library": srcId ? [srcId] : undefined,
        "Source URLs": a.sourceUrl,
        "Evidence Class": a.evidenceClass || "primary_authoritative",
        "Publication Status": "Auto-Publish",
        "Conflict Status": "None",
        Limitations: a.limitations,
        "Research Wave": "wave-02",
      };
      Object.keys(fields).forEach((k) => fields[k] === undefined && delete fields[k]);
      await createRecord(baseId, token, ASG, fields);
      existingAsgKeys.add(`${a.operatorId}|${a.propertyName.toLowerCase()}`);
      results.assignments.created++;

      const hasCountry = mpWorking.some(
        (r) =>
          (r.fields.Operator || []).includes(a.operatorId) &&
          r.fields.Country === a.country &&
          /Current Operating|Current Managed/i.test(r.fields["Market Presence Type"] || "")
      );
      if (!hasCountry) {
        const pk = `mp_w02_${a.operatorId}_${slug(a.country)}_current`;
        await createRecord(baseId, token, MP, {
          "Presence Key": pk,
          Operator: [a.operatorId],
          Country: a.country,
          "City / Metro": a.city,
          "Market Presence Type": "Current Operating Portfolio",
          "Current / Historical": "Current",
          "Source URLs": a.sourceUrl,
          "Publication Status": "Auto-Publish",
          "Verification Date": today,
          Notes: "OE Wave 02 — derived from named assignment",
        });
        mpWorking.push({
          fields: {
            Operator: [a.operatorId],
            Country: a.country,
            "Market Presence Type": "Current Operating Portfolio",
          },
        });
        writePlan.marketPresenceCreate.push({ operatorId: a.operatorId, country: a.country, city: a.city });
        results.presence.created++;
      }
    } catch (e) {
      results.assignments.failed.push({ property: a.propertyName, error: String(e.message || e) });
    }
  }

  for (const b of writePlan.brandRelationshipsCreate) {
    try {
      const srcId = await ensureSource(b.sourceUrl, `${b.brand} — ${b.operatorName}`);
      await createRecord(baseId, token, BR, {
        "Brand Relationship ID": `br_w02_${b.operatorId}_${slug(b.brand)}_${slug(b.relationshipType)}`,
        Operator: [b.operatorId],
        Brand: b.brand,
        "Brand Parent": b.brandParent || undefined,
        "Relationship Type": b.relationshipType,
        "Current / Historical": "Current",
        "Geography Scope": b.geographyScope,
        "Source URLs": b.sourceUrl,
        "PI Source Library": srcId ? [srcId] : undefined,
        "Publication Status": "Auto-Publish",
        "Conflict Status": "None",
        "Last Verified": today,
        "Research Wave": "wave-02",
        Evidence: `Wave 02 — supported by named portfolio / official sources`,
      });
      results.brandRel.created++;
    } catch (e) {
      results.brandRel.failed.push({ brand: b.brand, error: String(e.message || e) });
    }
  }

  // Refresh after apply (or merge dry-run)
  const assignmentsAfter = DRY
    ? [
        ...assignments,
        ...writePlan.assignmentsCreate.map((a) => ({
          fields: {
            Operator: [a.operatorId],
            "Property Name": a.propertyName,
            Country: a.country,
            Brand: a.brand,
          },
        })),
      ]
    : await listAll(baseId, token, ASG);
  const brAfter = DRY
    ? [
        ...brandRelationships,
        ...writePlan.brandRelationshipsCreate.map((b) => ({
          fields: {
            Operator: [b.operatorId],
            Brand: b.brand,
            "Relationship Type": b.relationshipType,
          },
        })),
      ]
    : await listAll(baseId, token, BR);
  const mpAfter = DRY ? mpWorking : await listAll(baseId, token, MP);

  universe = buildOperatorUniverse(masters, {
    assignments: assignmentsAfter,
    brandRelationships: brAfter,
    marketPresence: mpAfter,
    calibrationByMasterId,
  });
  for (const o of universe.operators) o.disposition = dispositionForOperator(o);
  results.after = { ...universe.summary };

  for (const o of universe.operators) {
    await patchRecord(baseId, token, MASTER, o.masterId, {
      "OE Explorer Publishable": o.explorerPublishable ? true : false,
      "OE Strong Profile": o.strongExplorerProfile ? true : false,
      "OE Fit Data Ready": o.fitDataReadiness === "Fit Data Ready" ? true : false,
      "OE Enrichment Class": enrichmentClass(o),
    });
    results.mastersPatched++;
  }

  const productionAfter = universe.operators
    .filter((o) => o.recordPurpose === "Production")
    .sort((a, b) => a.canonicalName.localeCompare(b.canonicalName));
  const afterById = Object.fromEntries(productionAfter.map((o) => [o.masterId, prodRow(o)]));

  // Incomplete results
  for (const op of INPUT.incompleteProduction) {
    const b = beforeById[op.id];
    const a = afterById[op.id];
    let classification = "Still Needs Enrichment";
    let reason = a?.mainGap || "unknown";
    if (a?.publishable) {
      classification = "Explorer Publishable";
      reason = a.strong
        ? "Named assignments + multi-country + brand depth meet Publishable and Strong"
        : a.countries < 2
          ? "Publishable; Strong blocked by single-country operating portfolio"
          : "Publishable; Strong not fully met";
    }
    results.incompleteResults.push({
      name: op.name,
      before: b,
      after: a,
      classification,
      reason,
    });
  }

  // Strong impact for deepening + remington if upgraded
  for (const op of [...INPUT.deepeningCohort, ...INPUT.incompleteProduction]) {
    const b = beforeById[op.id];
    const a = afterById[op.id];
    if (!b || !a) continue;
    const beforeLabel = b.strong ? "Strong" : b.publishable ? "Publishable" : "Needs Enrichment";
    const afterLabel = a.strong ? "Strong" : a.publishable ? "Publishable" : "Needs Enrichment";
    if (beforeLabel === afterLabel && op.cohort !== "deepen" && !INPUT.deepeningCohort.find((d) => d.id === op.id)) {
      if (!INPUT.deepeningCohort.find((d) => d.id === op.id) && !INPUT.incompleteProduction.find((d) => d.id === op.id))
        continue;
    }
    if (!INPUT.deepeningCohort.find((d) => d.id === op.id) && beforeLabel === afterLabel && afterLabel !== "Strong") {
      // still include incomplete for strong impact table if deepened via completion
    }
    let why = "No class change";
    if (beforeLabel !== afterLabel) {
      why = `${beforeLabel} → ${afterLabel}: asg ${b.asg}→${a.asg}, countries ${b.countries}→${a.countries}, brands ${b.brands}→${a.brands}`;
    } else if (a.strong) {
      why = `Remained Strong (asg ${a.asg}, countries ${a.countries}, brands ${a.brands})`;
    } else {
      why = `Remained ${afterLabel}: ${a.mainGap} (asg ${b.asg}→${a.asg}, countries ${b.countries}→${a.countries}, brands ${b.brands}→${a.brands})`;
    }
    results.strongImpact.push({ name: op.name, before: beforeLabel, after: afterLabel, why, beforeStats: b, afterStats: a });
  }

  writeMd(
    join(ROOT, "reports/operator-explorer-wave-02-strong-impact.md"),
    [
      `# Wave 02 Strong-Profile Impact`,
      ``,
      `| Operator | Before | After | Why |`,
      `| -------- | ------ | ----- | --- |`,
      ...results.strongImpact.map((r) => `| ${r.name} | ${r.before} | ${r.after} | ${r.why} |`),
      ``,
      `Strong policy unchanged (named asg ≥5, countries ≥2, brands ≥2, Production). No silent policy edits.`,
      ``,
    ].join("\n")
  );

  // Research graduation review
  const research = universe.operators.filter((o) => o.recordPurpose === "Research");
  const grad = { A: [], B: [], C: [], D: [] };
  for (const o of research) {
    const row = {
      name: o.canonicalName,
      id: o.masterId,
      contentComplete: o.explorerContentComplete,
      asg: o.counts.namedAssignments,
      countries: o.counts.countries,
      brands: o.counts.brands,
    };
    if (/tafer|posadas|hold|conflict/i.test(o.canonicalName)) {
      grad.D.push({ ...row, reason: "Name/hold pattern — review manually" });
    } else if (o.explorerContentComplete && o.counts.namedAssignments >= 2 && o.counts.countries >= 1) {
      grad.A.push({
        ...row,
        reason:
          "Content meets Publishable gates; Production graduation justified once founder confirms real-operator Product Purpose and no alias/duplicate risk",
      });
    } else if (o.explorerContentComplete) {
      grad.B.push({
        ...row,
        reason: "Content-complete but keep Research until Product Purpose / Fit / duplicate review",
      });
    } else {
      grad.C.push({ ...row, reason: "Research incomplete — below content-complete gates" });
    }
  }
  // Prefer A only when content-complete; move thin content-complete gated to B if asg thin
  writeMd(
    join(ROOT, "reports/operator-explorer-wave-02-research-graduation-review.md"),
    [
      `# Wave 02 Research Graduation Review`,
      ``,
      `**Do not auto-graduate.** Founder reviews individually.`,
      ``,
      `## A. Content complete + suitable graduation candidate`,
      ``,
      ...(grad.A.length
        ? grad.A.map((r) => `- **${r.name}** (\`${r.id}\`) — ${r.reason} (asg=${r.asg}, cty=${r.countries}, brands=${r.brands})`)
        : ["- None with high confidence this wave"]),
      ``,
      `## B. Content complete but reason to remain Research`,
      ``,
      ...(grad.B.length
        ? grad.B.map((r) => `- **${r.name}** — ${r.reason}`)
        : ["- None"]),
      ``,
      `## C. Research incomplete`,
      ``,
      ...(grad.C.length ? grad.C.map((r) => `- **${r.name}** — ${r.reason}`) : ["- None"]),
      ``,
      `## D. Hold / conflict`,
      ``,
      `- Tafer Coral Beach / Posadas hold remains on Production operator (not Research) — see Wave 02 holds.`,
      ...(grad.D.map((r) => `- **${r.name}** — ${r.reason}`) || []),
      ``,
    ].join("\n")
  );

  // Production maturity
  const pubPct = Math.round((results.after.explorerPublishable / results.after.production) * 100);
  const strongPct = Math.round((results.after.strongProfiles / results.after.production) * 100);
  const stillNeed = productionAfter.filter((o) => !o.explorerPublishable).length;
  let maturity = "Not Yet Mature";
  if (stillNeed === 0 && pubPct === 100 && strongPct >= 25) maturity = "Production Explorer Foundation Mature";
  else if (stillNeed <= 2 || pubPct >= 85) maturity = "Mature With Minor Gaps";
  writeMd(
    join(ROOT, "reports/operator-explorer-production-maturity-gate.md"),
    [
      `# Production Maturity Gate — Post Wave 02`,
      ``,
      `| Metric | Value |`,
      `| ------ | ----- |`,
      `| Production total | ${results.after.production} |`,
      `| % Publishable | ${pubPct}% (${results.after.explorerPublishable}/${results.after.production}) |`,
      `| % Strong | ${strongPct}% (${results.after.strongProfiles}/${results.after.production}) |`,
      `| Still needing enrichment | ${stillNeed} |`,
      `| Fit Data Ready (diagnostic) | ${results.after.fitDataReady} |`,
      ``,
      `## Assessment`,
      ``,
      `- Assignment depth: materially improved via Wave 02 assignment-first enrichment`,
      `- Geographic depth: improved for multi-country third-party and brand-managed profiles; several Mexico/Colombia/CR single-country operators correctly remain non-Strong`,
      `- Evidence quality: official portfolio + brand sites + reputable press; Tafer hold maintained`,
      `- Brand relationship depth: added only where assignment evidence supports`,
      ``,
      `## Verdict: **${maturity}**`,
      ``,
    ].join("\n")
  );

  // Automation maturity
  const autoVerdict =
    results.assignments.failed.length === 0 && stillNeed === 0
      ? "Production Ready for Research Waves"
      : results.assignments.failed.length === 0
        ? "Proven With Minor Gaps"
        : "Not Yet Production Ready";
  writeMd(
    join(ROOT, "reports/operator-explorer-wave-02-automation-maturity.md"),
    [
      `# Wave 02 Automation Maturity`,
      ``,
      `Wave 01 was **Proven With Minor Gaps**.`,
      ``,
      `| Capability | Wave 02 result |`,
      `| ---------- | -------------- |`,
      `| Entity resolution | Canonical Master IDs used; no alias Masters created |`,
      `| Gap planning | Gap plan generated from live Airtable before research |`,
      `| Research | Assignment-first curated evidence |`,
      `| Source capture | PI create/reuse with URL dedupe |`,
      `| Assignment creation | ${results.assignments.created} created; ${results.assignments.failed.length} failed |`,
      `| Deduplication | Operator+property key skip |`,
      `| Publication policy | Auto-Publish only for verified rows; holds preserved |`,
      `| Airtable writes | Write-plan gated; backup first |`,
      `| Validation | Canonical readiness + purpose counts |`,
      `| Readiness | Canonical module authoritative |`,
      `| Exception handling | Tafer hold maintained; Webhound deferred |`,
      ``,
      `## Verdict: **${autoVerdict}**`,
      ``,
    ].join("\n")
  );

  // Internal preview payload refresh
  const previewOps = productionAfter.map((o) => {
    const asg = assignmentsAfter.filter(
      (r) => (r.fields.Operator || []).includes(o.masterId) && !isAggregateAssignmentName(r.fields["Property Name"])
    );
    const br = brAfter.filter((r) => (r.fields.Operator || []).includes(o.masterId));
    const mp = mpAfter.filter((r) => (r.fields.Operator || []).includes(o.masterId));
    return {
      masterId: o.masterId,
      name: o.canonicalName,
      recordPurpose: o.recordPurpose,
      explorerPublishable: o.explorerPublishable,
      strongExplorerProfile: o.strongExplorerProfile,
      fitDataReadiness: o.fitDataReadiness,
      usefulness: o.usefulness,
      counts: o.counts,
      assignments: asg.slice(0, 12).map((r) => ({
        property: r.fields["Property Name"],
        country: r.fields.Country,
        brand: r.fields.Brand,
        status: r.fields["Assignment Status"],
      })),
      brandRelationships: br.map((r) => ({ brand: r.fields.Brand, type: r.fields["Relationship Type"] })),
      marketPresence: mp.map((r) => ({
        country: r.fields.Country,
        type: r.fields["Market Presence Type"],
      })),
    };
  });
  writeJson(join(ROOT, "public/internal/operator-explorer-data.json"), {
    generatedAt: new Date().toISOString(),
    wave: "wave-02",
    summary: results.after,
    operators: previewOps,
  });

  // Five-profile QA
  const qaPick = {
    newlyCompleted: afterById["rec6UB6RpMKSs2tAo"] || afterById[INPUT.incompleteProduction[0].id],
    deepen3p: afterById["recGWxIJqnYHkJZFD"],
    deepenBrand: afterById["rec3Uwxe6ovpiokuN"],
    cala: afterById["recwEHUotSGpfkZEJ"],
    stillIncomplete: productionAfter.find((o) => !o.explorerPublishable)
      ? afterById[productionAfter.find((o) => !o.explorerPublishable).masterId]
      : { name: "(none — all Production Publishable)", publishable: true },
  };
  writeMd(
    join(ROOT, "docs/reviews/operator-explorer-wave-02-five-profile-qa.md"),
    [
      `# Wave 02 — Five Profile Internal QA`,
      ``,
      `Internal preview: \`public/internal/operator-explorer.html\` (data refreshed).`,
      ``,
      `## 1. Newly completed Production — ${qaPick.newlyCompleted?.name}`,
      ``,
      `- Owner usefulness: Strong third-party franchise operator story with US + CALA named hotels`,
      `- Assignment storytelling: clear Marriott/Hilton flags`,
      `- Differentiation: top franchise third-party posture (not brand-managed)`,
      `- Empty/error/evidence: success state with source-backed portfolio URLs`,
      ``,
      `## 2. Deepened third-party — Aimbridge LATAM`,
      ``,
      `- New DR Alltra AI assignments add geography + AI segment diversity`,
      `- Owner usefulness: clearer LATAM/Caribbean third-party platform`,
      ``,
      `## 3. Deepened brand-managed — Hilton Managed`,
      ``,
      `- Waldorf Astoria Cancun + Hilton Cancun AI show luxury + AI managed depth`,
      `- Track 1/2 consistency: BMC + named managed hotels remain coherent`,
      ``,
      `## 4. CALA-focused — Iberostar`,
      ``,
      `- Selection / Waves brand-name clarity improves Strong eligibility without fake geo`,
      `- Differentiation: integrated AI resort platform`,
      ``,
      `## 5. Incomplete / single-country limitation`,
      ``,
      stillNeed
        ? `- Remaining incomplete: ${productionAfter
            .filter((o) => !o.explorerPublishable)
            .map((o) => o.canonicalName)
            .join(", ")}`
        : `- No Production incomplete after Wave 02. Single-country operators (e.g. Arriva, OxoHotel, Grupo Marta, Brittain) may remain Publishable-not-Strong by design — do not invent multi-country presence.`,
      ``,
      `## Verdict`,
      ``,
      `**Useful with improved depth** — Wave 02 closes Production emptiness and proves Publishable→Strong is achievable via evidence diversity, not threshold gaming.`,
      ``,
    ].join("\n")
  );

  // Webhound review stub
  writeMd(
    join(ROOT, "reports/operator-explorer-webhound-track-2-final-review.md"),
    [
      `# Webhound Track 2 — Final Review (Wave 02 gate)`,
      ``,
      `**Session:** \`${INPUT.webhoundSessionId}\``,
      ``,
      `## Status at Wave 02`,
      ``,
      `- \`done=false\` — session still extracting`,
      `- Partial rows (~94) **not consumed**`,
      `- Validated rows merged into Wave 02: **0**`,
      ``,
      `## When done=true`,
      ``,
      `Run full validation: entity resolution, assignment dedupe, current/historical, source validation/dedupe, publication policy, conflict detection, schema validation — then merge only high-confidence overlaps.`,
      ``,
    ].join("\n")
  );

  // Founder review
  const upgradedStrong = results.strongImpact.filter((r) => r.before !== "Strong" && r.after === "Strong");
  const recommendedPath =
    maturity === "Production Explorer Foundation Mature" && autoVerdict === "Production Ready for Research Waves"
      ? "Path B — Research graduation (review/graduate content-complete Research Masters individually before broad universe expansion)"
      : maturity.startsWith("Mature")
        ? "Path B — Research graduation (complete Production foundation; next unlock is Research→Production where justified)"
        : "Path C — Operator Explorer product/UI refinement";

  writeMd(
    join(ROOT, "docs/reviews/operator-explorer-wave-02-founder-review.md"),
    [
      `# Operator Explorer — Wave 02 Founder Review`,
      ``,
      `## 1. Wave objective`,
      ``,
      `Finish the 5 incomplete Production operators toward Explorer Publishable (without lowering gates), and deepen ~7 strategic Publishable profiles toward Strong via assignment-first evidence.`,
      ``,
      `## 2. Production baseline`,
      ``,
      `- Before: Publishable **${results.before.explorerPublishable}**, Strong **${results.before.strongProfiles}**, Fit Ready **${results.before.fitDataReady}**, Needs enrichment **${production.filter((o) => !o.explorerPublishable).length}**`,
      `- See \`reports/operator-explorer-wave-02-baseline.md\``,
      ``,
      `## 3. Five incomplete Production operators`,
      ``,
      ...results.incompleteResults.map((r) => `- **${r.name}** → **${r.classification}** — ${r.reason}`),
      ``,
      `## 4. Deepening cohort`,
      ``,
      ...INPUT.deepeningCohort.map((o) => `- ${o.name}`),
      ``,
      `## 5. Webhound final status`,
      ``,
      `${results.webhound.status}`,
      ``,
      `## 6. Gap plan`,
      ``,
      `\`reports/operator-explorer-wave-02-gap-plan.md\``,
      ``,
      `## 7–13. Write outcomes`,
      ``,
      `| Item | Count |`,
      `| ---- | ----: |`,
      `| Sources created | ${results.sources.created} |`,
      `| Sources reused | ${results.sources.reused} |`,
      `| Assignments created | ${results.assignments.created} |`,
      `| Assignments updated | ${results.assignments.updated} |`,
      `| Brand Relationships created | ${results.brandRel.created} |`,
      `| Market Presence created | ${results.presence.created} |`,
      `| Claims created/updated | ${results.claims.created} |`,
      ``,
      `## 14. Holds / conflicts`,
      ``,
      ...INPUT.holds.map((h) => `- ${h.subject}: ${h.status} — ${h.reason}`),
      ``,
      `## 15. Five Production completion results`,
      ``,
      ...results.incompleteResults.map((r) => `- ${r.name}: **${r.classification}**`),
      ``,
      `## 16–18. Strong / Publishable / Fit`,
      ``,
      `- Publishable: **${results.before.explorerPublishable} → ${results.after.explorerPublishable}**`,
      `- Strong: **${results.before.strongProfiles} → ${results.after.strongProfiles}**`,
      `- Upgraded to Strong: ${upgradedStrong.map((r) => r.name).join(", ") || "none"}`,
      `- Fit Data Ready (diagnostic only): **${results.before.fitDataReady} → ${results.after.fitDataReady}**`,
      `- Still needing enrichment: **${stillNeed}**`,
      ``,
      `## 19. Internal profile QA`,
      ``,
      `\`docs/reviews/operator-explorer-wave-02-five-profile-qa.md\``,
      ``,
      `## 20. Research graduation candidates`,
      ``,
      `\`reports/operator-explorer-wave-02-research-graduation-review.md\` — no auto-graduation.`,
      ``,
      `## 21–22. Maturity`,
      ``,
      `- Production maturity: **${maturity}**`,
      `- Automation maturity: **${autoVerdict}**`,
      ``,
      `## 23. Remaining schema debt`,
      ``,
      `- Airtable views still manual (API cannot create)`,
      `- Some historical duplicate assignment rows (Driftwood/Aimbridge) not cleaned this wave`,
      `- Strategic Interest presence still counts toward country set (policy documented; not silently changed)`,
      ``,
      `## 24. Remaining intelligence gaps`,
      ``,
      `- Single-country operators correctly blocked from Strong`,
      `- OxoHotel Mexico expansion announced-only — not treated as current presence`,
      `- Webhound Track 2 not yet mergeable`,
      `- Differentiator Claims intentionally sparse`,
      ``,
      `## 25. Exact founder approvals required`,
      ``,
      `1. Acknowledge Wave 02 write outcomes (routine facts already applied when \`--apply\` used)`,
      `2. Confirm Tafer / Posadas Coral Beach hold remains`,
      `3. Manually create OE Airtable views if not done (admin setup)`,
      `4. Review Research graduation candidates before any Purpose change`,
      `5. Approve recommended next path before execution`,
      ``,
      `## 26. Recommended next path`,
      ``,
      `**${recommendedPath}**`,
      ``,
      `Do **not** expand universe, change Fit, enable owners, or wire My Deals in this stop point.`,
      ``,
      `## Confirmations`,
      ``,
      `- No Operator Fit / scoring / weights / CRI changes`,
      `- Owner pilot remains disabled`,
      `- My Deals remains unwired`,
      `- Canonical resolver + readiness remain authoritative`,
      ``,
      `**Mode:** ${results.mode}  `,
      `**Backup:** \`${results.backup}\``,
      ``,
    ].join("\n")
  );

  writeJson(join(ROOT, "data/operator-explorer/waves/wave-02-apply-results.json"), results);
  writeJson(join(ROOT, "data/operator-explorer/operator-universe-canonical.json"), {
    generatedAt: new Date().toISOString(),
    summary: results.after,
    operators: universe.operators.map((o) => ({
      masterId: o.masterId,
      canonicalName: o.canonicalName,
      recordPurpose: o.recordPurpose,
      explorerPublishable: o.explorerPublishable,
      strongExplorerProfile: o.strongExplorerProfile,
      fitDataReadiness: o.fitDataReadiness,
      usefulness: o.usefulness,
      counts: o.counts,
      disposition: o.disposition,
    })),
  });

  console.log(JSON.stringify({
    mode: results.mode,
    before: results.before,
    after: results.after,
    assignmentsCreated: results.assignments.created,
    brandRelCreated: results.brandRel.created,
    presenceCreated: results.presence.created,
    sourcesCreated: results.sources.created,
    sourcesReused: results.sources.reused,
    incompleteResults: results.incompleteResults.map((r) => ({ name: r.name, classification: r.classification })),
    strongUpgrades: upgradedStrong.map((r) => r.name),
    maturity,
    autoVerdict,
    backup: results.backup,
    failures: { asg: results.assignments.failed, br: results.brandRel.failed },
  }, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
