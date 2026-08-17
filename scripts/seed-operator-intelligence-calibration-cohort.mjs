#!/usr/bin/env node
/**
 * WRITE LOCAL FILES ONLY — seeds calibration cohort JSON from documented public research.
 * Does NOT write Airtable.
 *
 *   node scripts/seed-operator-intelligence-calibration-cohort.mjs
 */
import { writeFileSync, mkdirSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { resolvePublicationDecision } from "../lib/operator-intelligence/publication-policy.js";
import { detectConflictsForOperator } from "../lib/operator-intelligence/conflict-detector.js";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const dir = join(root, "data", "operator-intelligence", "calibration-cohort");
mkdirSync(dir, { recursive: true });

const RESEARCH_DATE = "2026-08-03";
const PROCESS = "calibration-wave-1";

const OPS = {
  arbor: {
    operatorId: "recF5Z87OAqFgndoq",
    operatorName: "Arbor Lodging (CALA)",
    website: "https://arborlodging.com",
    operatorType: "Third-party management / investment",
    parentCompany: "Arbor Lodging Partners",
    chainScales: ["Upper Upscale", "Upscale", "Upper Midscale", "Midscale"],
    airtableActiveCountries: [],
    airtableStructures: ["Full third-party management", "Pre-opening / transition support"],
  },
  cenote: {
    operatorId: "recQ6Cf8O2z0tiqBz",
    operatorName: "Cenote Azul Operadores",
    website: "https://cenoteazul.mx",
    operatorType: "Third-party management",
    parentCompany: null,
    chainScales: ["Midscale", "Upper Midscale", "Upscale"],
    airtableActiveCountries: [
      "Mexico",
      "Costa Rica",
      "Panama",
      "Colombia",
      "Peru",
      "Chile",
      "Dominican Republic",
      "Puerto Rico",
    ],
    airtableStructures: ["Full third-party management", "Franchise support", "Hybrid / project-specific"],
  },
  he: {
    operatorId: "recWPKu5laVZxsvpn",
    operatorName: "Hotel Equities (CALA)",
    website: "https://hotelequities.com/cala.htm",
    operatorType: "Third-party management",
    parentCompany: "Hotel Equities",
    chainScales: ["Luxury", "Upper Upscale", "Upscale", "Independent"],
    ownerReportingLevel: "Institutional reporting",
    airtableActiveCountries: [
      "Mexico",
      "Costa Rica",
      "Dominican Republic",
      "Jamaica",
      "Curaçao",
      "Trinidad & Tobago",
      "Dominica",
      "Grenada",
      "Turks & Caicos",
      "U.S. Virgin Islands",
    ],
    airtableStructures: ["Full third-party management", "Franchise support"],
  },
  ghl: {
    operatorId: "reciI2tYQBfMoMK9G",
    operatorName: "GHL Hoteles (GHL Holding)",
    website: "https://ghlholding.com",
    operatorType: "Third-party / multi-brand management",
    parentCompany: "GHL Holding (Advent International backed)",
    chainScales: ["Upper Upscale", "Upscale", "Upper Midscale", "Midscale"],
    airtableActiveCountries: [],
    airtableStructures: [],
  },
  playa: {
    operatorId: "rec3TUHT9Z4AnFp5P",
    operatorName: "Playa Hotels & Resorts",
    website: "https://www.playaresorts.com/",
    operatorType: "Owner-operator / resort manager (all-inclusive)",
    parentCompany: "Playa Hotels & Resorts N.V. (strategic ownership evolving — Hyatt transaction context)",
    chainScales: ["Luxury", "Upper Upscale", "Upscale"],
    airtableActiveCountries: [],
    airtableStructures: [],
  },
  aimbridge: {
    operatorId: "recGWxIJqnYHkJZFD",
    operatorName: "Aimbridge Hospitality (LATAM)",
    website: "https://aimbridgelatam.com/en/home/",
    operatorType: "Third-party management",
    parentCompany: "Aimbridge Hospitality",
    chainScales: ["Luxury", "Upper Upscale", "Upscale", "Upper Midscale", "Midscale"],
    airtableActiveCountries: [],
    airtableStructures: [],
  },
};

let srcN = 0;
let clmN = 0;
const sources = [];
const claims = [];
const geography = [];
const managementStructures = [];
const experience = [];
const brandRelationships = [];
const comparables = [];
const exceptions = [];

function src(partial) {
  srcN += 1;
  const id = `src_cal_${String(srcN).padStart(3, "0")}`;
  const row = {
    id,
    title: partial.title,
    publisher: partial.publisher,
    url: partial.url,
    sourceType: partial.sourceType,
    authority: partial.authority,
    date: partial.date || null,
    accessedAt: RESEARCH_DATE,
    verificationStatus: partial.verificationStatus || "Reviewed",
    refreshDate: RESEARCH_DATE,
    notes: partial.notes || null,
  };
  sources.push(row);
  return id;
}

function claim(partial) {
  clmN += 1;
  const id = `clm_cal_${String(clmN).padStart(3, "0")}`;
  const row = {
    id,
    operatorId: partial.operatorId,
    operatorName: partial.operatorName,
    claimCategory: partial.claimCategory,
    claimSubject: partial.claimSubject,
    claimPredicate: partial.claimPredicate,
    claimValue: partial.claimValue,
    normalizedValue: partial.normalizedValue ?? partial.claimValue,
    geographicScope: partial.geographicScope || null,
    brandScope: partial.brandScope || null,
    propertyScope: partial.propertyScope || null,
    hotelSegmentScope: partial.hotelSegmentScope || null,
    operatingStructureScope: partial.operatingStructureScope || null,
    effectiveDate: partial.effectiveDate || null,
    expirationOrReviewDate: partial.expirationOrReviewDate || "2027-02-03",
    sourceIds: partial.sourceIds || [],
    sourceType: partial.sourceType || null,
    evidenceClass: partial.evidenceClass,
    verificationStatus: partial.verificationStatus || "Verified",
    publicationClass: partial.publicationClass,
    confidence: partial.confidence || "Moderate",
    conflictStatus: partial.conflictStatus || "None",
    scoringRelevance: partial.scoringRelevance || "High",
    potentialScoreImpact: partial.potentialScoreImpact || "Eligibility",
    publishedDestination: partial.publishedDestination || null,
    currentPublishedValue: partial.currentPublishedValue || null,
    researchDate: RESEARCH_DATE,
    researcherOrProcess: PROCESS,
    notes: partial.notes || null,
    limitations: partial.limitations || null,
    objectiveFact: partial.objectiveFact === true,
    requiresEvidenceLabel: partial.requiresEvidenceLabel === true,
    evidenceLabel: partial.evidenceLabel || null,
    internalOnly: partial.internalOnly === true,
    neverInfer: partial.neverInfer === true,
    sensitive: partial.sensitive === true,
    flags: partial.flags || {},
    fieldState: partial.fieldState || "Present",
  };
  claims.push(row);
  return id;
}

// —— Sources ——
const S = {
  arborPortfolio: src({
    title: "Arbor Lodging Portfolio",
    publisher: "Arbor Lodging",
    url: "https://www.arborlodging.com/portfolio",
    sourceType: "official_operator_portfolio",
    authority: "primary_authoritative",
    date: "2026",
  }),
  arborPlatforms: src({
    title: "Arbor Lodging Platforms — CALA / Mexico City office",
    publisher: "Arbor Lodging",
    url: "https://www.arborlodging.com/platforms",
    sourceType: "official_operator_corporate",
    authority: "primary_authoritative",
  }),
  arborPressHutchinson: src({
    title: "Arbor Lodging expands CALA growth — Hutchinson appointment / Los Cabos project",
    publisher: "EIN Presswire / Arbor",
    url: "https://www.einpresswire.com/article/866025694/arbor-lodging-expands-cala-growth-with-appointment-of-christian-hutchinson-as-director-of-business-development",
    sourceType: "official_operator_press",
    authority: "primary_authoritative",
    date: "2025",
  }),
  heCala: src({
    title: "Hotel Equities CALA page",
    publisher: "Hotel Equities",
    url: "https://www.hotelequities.com/cala.htm",
    sourceType: "official_operator_corporate",
    authority: "primary_authoritative",
    date: "2026",
  }),
  heBlog: src({
    title: "HE expands CALA capabilities with open hotels and in-market platform",
    publisher: "Hotel Equities",
    url: "https://www.hotelequities.com/blog/hotel-equities-expands-caribbean-and-latin-america-cala-capabilities-with-open-hotels-and-in-market-operating-platform/",
    sourceType: "official_operator_press",
    authority: "primary_authoritative",
    date: "2026",
  }),
  heHb: src({
    title: "Southern Exposure: Hotel Equities expands CALA",
    publisher: "Hotel Business",
    url: "https://togo.hotelbusiness.com/article/southern-exposure-hotel-equities-expands-presence-across-caribbean-latin-america/",
    sourceType: "trade_press",
    authority: "reliable_independent",
    date: "2026",
  }),
  ghlHotels: src({
    title: "GHL Hotels — hotels directory",
    publisher: "GHL Hoteles",
    url: "https://www.ghlhoteles.com/en/hotels/",
    sourceType: "official_operator_portfolio",
    authority: "primary_authoritative",
  }),
  sonestaGhl: src({
    title: "Sonesta extends master franchise agreement with GHL through 2034",
    publisher: "Sonesta Newsroom",
    url: "https://newsroom.sonesta.com/franchising/sonesta-extends-master-franchise-agreement-with-ghl-hoteles-through-2034/",
    sourceType: "official_brand_press",
    authority: "primary_authoritative",
    date: "2025-04-29",
  }),
  ghlHit: src({
    title: "GHL Hoteles expands in Panama, Peru",
    publisher: "Hotel Investment Today",
    url: "https://www.hotelinvestmenttoday.com/Deals/Management/GHL-Hoteles-expands-in-Panama-Peru",
    sourceType: "trade_press",
    authority: "reliable_independent",
  }),
  playaSec: src({
    title: "Playa Hotels & Resorts Form 10-K / annual disclosure (portfolio description)",
    publisher: "SEC / Playa",
    url: "https://www.sec.gov/Archives/edgar/data/1692412/000169241225000052/plya-20241231.htm",
    sourceType: "investor_filing",
    authority: "primary_authoritative",
    date: "2024-12-31",
  }),
  playaIr: src({
    title: "Playa investor relations portfolio description",
    publisher: "Playa Resorts IR",
    url: "https://investors.playaresorts.com/2024-02-22-Playa-Hotels-Resorts-N-V-Reports-Fourth-Quarter-and-Full-Year-2023-Results",
    sourceType: "official_operator_press",
    authority: "primary_authoritative",
    date: "2024-02-22",
  }),
  aimAbout: src({
    title: "Aimbridge LATAM About Us",
    publisher: "Aimbridge LATAM",
    url: "https://aimbridgelatam.com/en/about-us/",
    sourceType: "official_operator_corporate",
    authority: "primary_authoritative",
  }),
  aimDir: src({
    title: "Aimbridge LATAM hotel directory",
    publisher: "Aimbridge LATAM",
    url: "https://aimbridgelatam.com/directorio-hoteles/",
    sourceType: "official_operator_portfolio",
    authority: "primary_authoritative",
  }),
  cenoteSite: src({
    title: "Cenote Azul official site (HTTP 503 during calibration fetch)",
    publisher: "Cenote Azul",
    url: "https://cenoteazul.mx",
    sourceType: "official_operator_corporate",
    authority: "primary_authoritative",
    verificationStatus: "Unverified",
    notes: "Site returned 503 during calibration research; queued for re-fetch.",
  }),
};

// —— Arbor ——
geography.push(
  {
    operatorId: OPS.arbor.operatorId,
    country: "United States",
    presenceType: "Current Operating Portfolio",
    evidence: "Portfolio lists current/previous US hotels",
    sourceIds: [S.arborPortfolio],
    limitations: "US-heavy; CALA still early",
  },
  {
    operatorId: OPS.arbor.operatorId,
    country: "Mexico",
    presenceType: "Regional Office or Team",
    evidence: "Mexico City regional office stated on platforms / press",
    sourceIds: [S.arborPlatforms, S.arborPressHutchinson],
    limitations: "Office ≠ managed hotel inventory",
  },
  {
    operatorId: OPS.arbor.operatorId,
    country: "Mexico",
    presenceType: "Active Development",
    evidence: "Los Cabos first LatAm luxury project awarded — details TBD",
    sourceIds: [S.arborPressHutchinson],
    limitations: "Announced/awarded; not confirmed open as Current Managed Property",
  },
  {
    operatorId: OPS.arbor.operatorId,
    country: "Peru",
    presenceType: "Strategic Interest",
    evidence: "BD experience cited across Peru/Costa Rica/Mexico — not operating proof",
    sourceIds: [S.arborPressHutchinson],
    limitations: "Do not treat as Current Managed Property",
  },
  {
    operatorId: OPS.arbor.operatorId,
    country: "Costa Rica",
    presenceType: "Strategic Interest",
    evidence: "BD experience cited — not operating proof",
    sourceIds: [S.arborPressHutchinson],
    limitations: "Do not treat as Current Managed Property",
  }
);
managementStructures.push({
  operatorId: OPS.arbor.operatorId,
  structure: "Third-Party Management",
  status: "Supported",
  applicableGeography: "United States; CALA expansion",
  evidence: "Arbor Lodging Management platform",
  sourceIds: [S.arborPlatforms, S.arborPortfolio],
  limitations: null,
});
for (const [dim, level] of [
  ["urban", "Repeated Demonstrated Experience"],
  ["select_service", "Repeated Demonstrated Experience"],
  ["full_service", "Some Demonstrated Experience"],
  ["resort", "Some Demonstrated Experience"],
  ["new_build", "Some Demonstrated Experience"],
  ["renovation", "Some Demonstrated Experience"],
  ["institutional_ownership", "Operator-Reported Claim"],
  ["cala_experience", "One Documented Example"],
]) {
  experience.push({
    operatorId: OPS.arbor.operatorId,
    dimension: dim,
    level,
    sourceIds: [S.arborPortfolio, S.arborPressHutchinson],
  });
}
for (const brand of ["Marriott", "Hilton", "Hyatt", "IHG"]) {
  brandRelationships.push({
    operatorId: OPS.arbor.operatorId,
    brand,
    relationshipStatus: "Verified Current Relationship",
    geography: "United States (primary)",
    evidence: "Portfolio brands we operate",
    sourceIds: [S.arborPortfolio],
    verificationDate: RESEARCH_DATE,
    limitations: "US portfolio evidence; do not infer CALA brand approval",
  });
}
comparables.push(
  {
    operatorId: OPS.arbor.operatorId,
    propertyName: "AC Hotel Phoenix Downtown",
    city: "Phoenix",
    country: "United States",
    brand: "AC Hotels",
    hotelSegment: "Upscale",
    urbanOrResort: "Urban",
    assetType: "Urban",
    developmentType: "Stabilized / managed",
    whyComparable: "Urban upscale branded third-party operations example",
    comparabilityStrength: "Moderate",
    verificationStatus: "Referenced",
    sourceIds: [S.arborPortfolio],
    limitations: "US not CALA — limited for LatAm-specific deals",
    performanceEvidence: "Performance evidence unavailable or not independently verified.",
  },
  {
    operatorId: OPS.arbor.operatorId,
    propertyName: "Eaglewood Resort & Spa",
    city: "Metro Chicago",
    country: "United States",
    brand: "Independent / resort",
    keyCount: 295,
    urbanOrResort: "Resort",
    assetType: "Resort",
    developmentType: "Acquisition / operations",
    whyComparable: "Full-service resort operations scale",
    comparabilityStrength: "Moderate",
    verificationStatus: "Referenced",
    sourceIds: [S.arborPortfolio, S.arborPressHutchinson],
    limitations: "US resort; not CALA beach AI",
    performanceEvidence: "Performance evidence unavailable or not independently verified.",
  }
);
claim({
  operatorId: OPS.arbor.operatorId,
  operatorName: OPS.arbor.operatorName,
  claimCategory: "geography",
  claimSubject: "mexico_presence",
  claimPredicate: "has_presence_type",
  claimValue: "Regional Office or Team + Active Development (Los Cabos)",
  normalizedValue: "Mexico",
  publicationClass: 1,
  objectiveFact: true,
  evidenceClass: "independently_referenced",
  sourceIds: [S.arborPlatforms, S.arborPressHutchinson],
  scoringRelevance: "High",
  potentialScoreImpact: "Eligibility",
  limitations: "Not Current Managed Property until open",
});
claim({
  operatorId: OPS.arbor.operatorId,
  operatorName: OPS.arbor.operatorName,
  claimCategory: "structure",
  claimSubject: "third_party_management",
  claimPredicate: "supports",
  claimValue: "Third-Party Management",
  publicationClass: 1,
  objectiveFact: true,
  evidenceClass: "independently_referenced",
  sourceIds: [S.arborPlatforms],
  scoringRelevance: "High",
  potentialScoreImpact: "Eligibility",
});

// —— Hotel Equities ——
for (const [country, presence] of [
  ["U.S. Virgin Islands", "Current Managed Property"],
  ["Dominican Republic", "Current Managed Property"],
  ["Jamaica", "Current Managed Property"],
  ["Mexico", "Current Managed Property"],
  ["Costa Rica", "Active Development"],
  ["Turks & Caicos", "Active Development"],
  ["Curaçao", "Active Development"],
  ["Trinidad & Tobago", "Active Development"],
  ["Grenada", "Active Development"],
  ["Dominica", "Active Development"],
]) {
  geography.push({
    operatorId: OPS.he.operatorId,
    country,
    presenceType: presence,
    evidence: "HE CALA official portfolio / press",
    sourceIds: [S.heCala, S.heBlog, S.heHb],
    limitations: presence.includes("Development") ? "Pipeline / under development — not operating inventory" : null,
  });
}
managementStructures.push({
  operatorId: OPS.he.operatorId,
  structure: "Third-Party Management",
  status: "Supported",
  applicableGeography: "CALA",
  evidence: "HE CALA third-party positioning",
  sourceIds: [S.heCala],
});
managementStructures.push({
  operatorId: OPS.he.operatorId,
  structure: "Franchise + Operator",
  status: "Supported With Conditions",
  applicableGeography: "CALA branded hotels",
  evidence: "Branded select-service and lifestyle examples",
  sourceIds: [S.heCala, S.heHb],
  limitations: "Property-scoped brand relationships only",
});
for (const [dim, level] of [
  ["resort", "Repeated Demonstrated Experience"],
  ["urban", "Some Demonstrated Experience"],
  ["lifestyle", "Some Demonstrated Experience"],
  ["select_service", "Some Demonstrated Experience"],
  ["luxury", "Some Demonstrated Experience"],
  ["all_inclusive", "Some Demonstrated Experience"],
  ["new_build", "Some Demonstrated Experience"],
  ["pre_opening", "Some Demonstrated Experience"],
  ["cala_experience", "Repeated Demonstrated Experience"],
  ["mixed_use", "One Documented Example"],
  ["branded_residences", "One Documented Example"],
]) {
  experience.push({ operatorId: OPS.he.operatorId, dimension: dim, level, sourceIds: [S.heCala, S.heBlog] });
}
for (const brand of ["Hilton", "Marriott", "Hyatt", "Independent"]) {
  brandRelationships.push({
    operatorId: OPS.he.operatorId,
    brand,
    relationshipStatus: "Verified Current Relationship",
    geography: "CALA property-scoped",
    evidence: "Named CALA properties on HE site",
    sourceIds: [S.heCala],
    verificationDate: RESEARCH_DATE,
    limitations: "Do not infer global brand approval",
  });
}
comparables.push(
  {
    operatorId: OPS.he.operatorId,
    propertyName: "Hampton by Hilton St. Thomas",
    city: "Charlotte Amalie",
    country: "U.S. Virgin Islands",
    brand: "Hampton by Hilton",
    hotelSegment: "Upper Midscale",
    urbanOrResort: "Urban",
    assetType: "Select service",
    developmentType: "Open / operating",
    whyComparable: "Open CALA branded select-service third-party management",
    comparabilityStrength: "High",
    verificationStatus: "Verified",
    sourceIds: [S.heCala, S.heBlog],
    performanceEvidence: "Performance evidence unavailable or not independently verified.",
  },
  {
    operatorId: OPS.he.operatorId,
    propertyName: "Donoma Las Terrenas Beach Resort & Spa",
    city: "Las Terrenas",
    country: "Dominican Republic",
    brand: "Autograph Collection",
    hotelSegment: "Upper Upscale",
    urbanOrResort: "Resort",
    assetType: "Resort",
    developmentType: "Open / operating",
    whyComparable: "CALA resort soft-brand operations",
    comparabilityStrength: "High",
    verificationStatus: "Verified",
    sourceIds: [S.heCala, S.heHb],
    performanceEvidence: "Performance evidence unavailable or not independently verified.",
  },
  {
    operatorId: OPS.he.operatorId,
    propertyName: "Casas del XVI",
    city: "Santo Domingo",
    country: "Dominican Republic",
    brand: "Vignette Collection",
    hotelSegment: "Upscale",
    urbanOrResort: "Urban",
    assetType: "Lifestyle / boutique",
    developmentType: "Open / operating",
    whyComparable: "Urban lifestyle boutique in CALA",
    comparabilityStrength: "High",
    verificationStatus: "Verified",
    sourceIds: [S.heCala, S.heHb],
    performanceEvidence: "Performance evidence unavailable or not independently verified.",
  },
  {
    operatorId: OPS.he.operatorId,
    propertyName: "Kimpton Tres Rios",
    city: "Playa del Carmen",
    country: "Mexico",
    brand: "Kimpton",
    hotelSegment: "Upper Upscale",
    urbanOrResort: "Resort",
    assetType: "Lifestyle resort",
    developmentType: "Pipeline / development",
    whyComparable: "Mexico lifestyle pipeline — limited until open",
    comparabilityStrength: "Limited",
    verificationStatus: "Referenced",
    sourceIds: [S.heCala],
    limitations: "Not yet operating — Announced / Active Development",
    performanceEvidence: "Performance evidence unavailable or not independently verified.",
  }
);
claim({
  operatorId: OPS.he.operatorId,
  operatorName: OPS.he.operatorName,
  claimCategory: "geography",
  claimSubject: "active_countries",
  claimPredicate: "operates_in",
  claimValue: "Mexico; Dominican Republic; Jamaica; U.S. Virgin Islands (+ pipeline markets)",
  normalizedValue: ["Mexico", "Dominican Republic", "Jamaica", "U.S. Virgin Islands"],
  publicationClass: 1,
  objectiveFact: true,
  evidenceClass: "independently_referenced",
  sourceIds: [S.heCala, S.heBlog, S.heHb],
  scoringRelevance: "High",
  potentialScoreImpact: "Eligibility",
});
claim({
  operatorId: OPS.he.operatorId,
  operatorName: OPS.he.operatorName,
  claimCategory: "structure",
  claimSubject: "third_party_management",
  claimPredicate: "supports",
  claimValue: "Third-Party Management",
  publicationClass: 1,
  objectiveFact: true,
  sourceIds: [S.heCala],
  evidenceClass: "independently_referenced",
  scoringRelevance: "High",
  potentialScoreImpact: "Eligibility",
});

// —— GHL ——
for (const country of ["Colombia", "Peru", "Chile", "Ecuador", "Guatemala"]) {
  geography.push({
    operatorId: OPS.ghl.operatorId,
    country,
    presenceType: "Current Operating Portfolio",
    evidence: "GHL hotels directory and/or Sonesta MFA disclosure",
    sourceIds: [S.ghlHotels, S.sonestaGhl],
  });
}
geography.push({
  operatorId: OPS.ghl.operatorId,
  country: "Panama",
  presenceType: "Current Managed Property",
  evidence: "Radisson Hotel Panama Canal management — HIT",
  sourceIds: [S.ghlHit],
});
geography.push({
  operatorId: OPS.ghl.operatorId,
  country: "Argentina",
  presenceType: "Claimed Capability",
  evidence: "Sonesta MFA geography includes Argentina — operating inventory not fully enumerated on directory fetch",
  sourceIds: [S.sonestaGhl],
  limitations: "Master franchise territory ≠ proven operating portfolio in this calibration",
});
managementStructures.push({
  operatorId: OPS.ghl.operatorId,
  structure: "Third-Party Management",
  status: "Supported",
  sourceIds: [S.sonestaGhl, S.ghlHit],
  applicableGeography: "LatAm",
});
managementStructures.push({
  operatorId: OPS.ghl.operatorId,
  structure: "Franchise + Operator",
  status: "Supported",
  sourceIds: [S.sonestaGhl],
  applicableGeography: "Sonesta MFA markets",
  limitations: "Franchise master agreement is brand-scoped",
});
for (const [dim, level] of [
  ["urban", "Repeated Demonstrated Experience"],
  ["full_service", "Repeated Demonstrated Experience"],
  ["select_service", "Some Demonstrated Experience"],
  ["resort", "Some Demonstrated Experience"],
  ["lifestyle", "Some Demonstrated Experience"],
  ["new_build", "Some Demonstrated Experience"],
  ["conversion", "Some Demonstrated Experience"],
  ["meetings_and_group", "Some Demonstrated Experience"],
  ["cala_experience", "Repeated Demonstrated Experience"],
]) {
  experience.push({ operatorId: OPS.ghl.operatorId, dimension: dim, level, sourceIds: [S.ghlHotels, S.sonestaGhl] });
}
brandRelationships.push({
  operatorId: OPS.ghl.operatorId,
  brand: "Sonesta",
  relationshipStatus: "Verified Current Relationship",
  geography: "Peru, Colombia, Ecuador, Chile, Argentina (MFA)",
  evidence: "Master Franchise Agreement through 2034; 14 open Sonesta hotels cited",
  sourceIds: [S.sonestaGhl],
  verificationDate: "2025-04-29",
  limitations: "Do not infer non-Sonesta global approvals from MFA",
});
for (const brand of ["Hyatt", "Marriott", "Radisson", "Independent"]) {
  brandRelationships.push({
    operatorId: OPS.ghl.operatorId,
    brand,
    relationshipStatus: "Verified Current Relationship",
    geography: "LatAm property-scoped",
    evidence: "GHL directory brand filters / listings",
    sourceIds: [S.ghlHotels],
    verificationDate: RESEARCH_DATE,
    limitations: "Property-scoped only",
  });
}
comparables.push(
  {
    operatorId: OPS.ghl.operatorId,
    propertyName: "Sonesta El Olivar",
    city: "Lima",
    country: "Peru",
    brand: "Sonesta",
    urbanOrResort: "Urban",
    assetType: "Urban full service",
    developmentType: "Operating",
    whyComparable: "Urban branded LatAm management — Peru relevance",
    comparabilityStrength: "High",
    verificationStatus: "Verified",
    sourceIds: [S.sonestaGhl],
    performanceEvidence: "Performance evidence unavailable or not independently verified.",
  },
  {
    operatorId: OPS.ghl.operatorId,
    propertyName: "Radisson Hotel Panama Canal",
    city: "Panama City",
    country: "Panama",
    brand: "Radisson",
    keyCount: 250,
    urbanOrResort: "Urban",
    assetType: "Urban full service / meetings",
    developmentType: "Management takeover",
    whyComparable: "Urban convention-adjacent management assignment",
    comparabilityStrength: "High",
    verificationStatus: "Referenced",
    sourceIds: [S.ghlHit],
    performanceEvidence: "Performance evidence unavailable or not independently verified.",
  },
  {
    operatorId: OPS.ghl.operatorId,
    propertyName: "Holiday Inn Lima Miraflores",
    city: "Lima",
    country: "Peru",
    brand: "Holiday Inn",
    keyCount: 200,
    urbanOrResort: "Urban",
    assetType: "Upscale / upper midscale urban",
    developmentType: "Management takeover",
    whyComparable: "Urban conversion/management transition example",
    comparabilityStrength: "High",
    verificationStatus: "Referenced",
    sourceIds: [S.ghlHit],
    performanceEvidence: "Performance evidence unavailable or not independently verified.",
  }
);
claim({
  operatorId: OPS.ghl.operatorId,
  operatorName: OPS.ghl.operatorName,
  claimCategory: "geography",
  claimSubject: "active_countries",
  claimPredicate: "operates_in",
  claimValue: "Colombia, Peru, Chile, Ecuador, Guatemala, Panama (+)",
  normalizedValue: ["Colombia", "Peru", "Chile", "Ecuador", "Guatemala", "Panama"],
  publicationClass: 1,
  objectiveFact: true,
  sourceIds: [S.ghlHotels, S.sonestaGhl, S.ghlHit],
  evidenceClass: "independently_referenced",
  scoringRelevance: "High",
  potentialScoreImpact: "Eligibility",
});
claim({
  operatorId: OPS.ghl.operatorId,
  operatorName: OPS.ghl.operatorName,
  claimCategory: "experience",
  claimSubject: "conversion",
  claimPredicate: "has_experience",
  claimValue: "Ground-up developments and strategic conversions (Sonesta disclosure)",
  publicationClass: 2,
  requiresEvidenceLabel: true,
  evidenceLabel: "Independently Referenced",
  sourceIds: [S.sonestaGhl],
  evidenceClass: "independently_referenced",
  scoringRelevance: "High",
  potentialScoreImpact: "Alignment",
});

// —— Playa ——
for (const country of ["Mexico", "Jamaica", "Dominican Republic"]) {
  geography.push({
    operatorId: OPS.playa.operatorId,
    country,
    presenceType: "Current Operating Portfolio",
    evidence: "SEC / IR portfolio — all-inclusive resorts owned and/or managed",
    sourceIds: [S.playaSec, S.playaIr],
  });
}
managementStructures.push({
  operatorId: OPS.playa.operatorId,
  structure: "Owner-Operated",
  status: "Supported",
  sourceIds: [S.playaSec],
  applicableGeography: "Mexico, Jamaica, Dominican Republic",
  limitations: "Primarily owner-operator of AI resorts; third-party breadth limited",
});
managementStructures.push({
  operatorId: OPS.playa.operatorId,
  structure: "Third-Party Management",
  status: "Supported With Conditions",
  sourceIds: [S.playaIr],
  applicableGeography: "Managed resorts for third-party owners (subset)",
  limitations: "Confirm deal-by-deal; ownership structure evolving post Hyatt transaction context",
});
for (const [dim, level] of [
  ["resort", "Repeated Demonstrated Experience"],
  ["all_inclusive", "Repeated Demonstrated Experience"],
  ["luxury", "Repeated Demonstrated Experience"],
  ["upper_upscale", "Repeated Demonstrated Experience"],
  ["food_and_beverage_intensive", "Repeated Demonstrated Experience"],
  ["cala_experience", "Repeated Demonstrated Experience"],
  ["urban", "No Evidence Found"],
  ["conversion", "Unknown"],
  ["select_service", "No Evidence Found"],
]) {
  experience.push({ operatorId: OPS.playa.operatorId, dimension: dim, level, sourceIds: [S.playaSec, S.playaIr] });
}
for (const brand of ["Hyatt", "Hilton", "Wyndham", "Marriott"]) {
  brandRelationships.push({
    operatorId: OPS.playa.operatorId,
    brand,
    relationshipStatus: "Verified Current Relationship",
    geography: "Mexico / Jamaica / Dominican Republic AI resorts",
    evidence: "Named brand portfolio in IR/SEC",
    sourceIds: [S.playaSec, S.playaIr],
    verificationDate: RESEARCH_DATE,
    limitations: "All-inclusive brand relationships; ownership/management evolving — refresh required",
  });
}
comparables.push(
  {
    operatorId: OPS.playa.operatorId,
    propertyName: "Hyatt Ziva Cancún",
    city: "Cancún",
    country: "Mexico",
    brand: "Hyatt Ziva",
    urbanOrResort: "Resort",
    assetType: "All-inclusive resort",
    developmentType: "Operating",
    whyComparable: "Large AI leisure resort operations in Mexico",
    comparabilityStrength: "High",
    verificationStatus: "Verified",
    sourceIds: [S.playaIr],
    performanceEvidence: "Performance evidence unavailable or not independently verified.",
  },
  {
    operatorId: OPS.playa.operatorId,
    propertyName: "Hyatt Zilara Cap Cana",
    city: "Cap Cana",
    country: "Dominican Republic",
    brand: "Hyatt Zilara",
    urbanOrResort: "Resort",
    assetType: "All-inclusive adults resort",
    developmentType: "Operating",
    whyComparable: "DR AI luxury leisure",
    comparabilityStrength: "High",
    verificationStatus: "Verified",
    sourceIds: [S.playaIr],
    performanceEvidence: "Performance evidence unavailable or not independently verified.",
  },
  {
    operatorId: OPS.playa.operatorId,
    propertyName: "Hilton Rose Hall Resort & Spa",
    city: "Montego Bay",
    country: "Jamaica",
    brand: "Hilton",
    urbanOrResort: "Resort",
    assetType: "All-inclusive resort",
    developmentType: "Operating",
    whyComparable: "Jamaica AI resort operations",
    comparabilityStrength: "High",
    verificationStatus: "Verified",
    sourceIds: [S.playaIr],
    performanceEvidence: "Performance evidence unavailable or not independently verified.",
  }
);
claim({
  operatorId: OPS.playa.operatorId,
  operatorName: OPS.playa.operatorName,
  claimCategory: "geography",
  claimSubject: "active_countries",
  claimPredicate: "operates_in",
  claimValue: "Mexico, Jamaica, Dominican Republic",
  normalizedValue: ["Mexico", "Jamaica", "Dominican Republic"],
  publicationClass: 1,
  objectiveFact: true,
  sourceIds: [S.playaSec, S.playaIr],
  evidenceClass: "independently_referenced",
  scoringRelevance: "High",
  potentialScoreImpact: "Eligibility",
});
claim({
  operatorId: OPS.playa.operatorId,
  operatorName: OPS.playa.operatorName,
  claimCategory: "performance",
  claimSubject: "financial_performance",
  claimPredicate: "unknown",
  claimValue: "Performance evidence unavailable or not independently verified.",
  publicationClass: 3,
  internalOnly: true,
  sourceIds: [],
  evidenceClass: "unknown",
  scoringRelevance: "None",
  potentialScoreImpact: "None",
  fieldState: "Unknown",
});

// —— Aimbridge ——
geography.push({
  operatorId: OPS.aimbridge.operatorId,
  country: "Mexico",
  presenceType: "Current Operating Portfolio",
  evidence: "Extensive Mexico hotel directory",
  sourceIds: [S.aimDir, S.aimAbout],
});
managementStructures.push({
  operatorId: OPS.aimbridge.operatorId,
  structure: "Third-Party Management",
  status: "Supported",
  sourceIds: [S.aimAbout],
  applicableGeography: "Mexico / LATAM",
});
for (const [dim, level] of [
  ["urban", "Repeated Demonstrated Experience"],
  ["select_service", "Repeated Demonstrated Experience"],
  ["full_service", "Repeated Demonstrated Experience"],
  ["luxury", "Some Demonstrated Experience"],
  ["lifestyle", "Some Demonstrated Experience"],
  ["extended_stay", "Some Demonstrated Experience"],
  ["resort", "Some Demonstrated Experience"],
  ["conversion", "Operator-Reported Claim"],
  ["cala_experience", "Repeated Demonstrated Experience"],
]) {
  experience.push({ operatorId: OPS.aimbridge.operatorId, dimension: dim, level, sourceIds: [S.aimDir, S.aimAbout] });
}
for (const brand of ["Marriott", "Hilton", "IHG", "Wyndham", "Accor", "Hyatt"]) {
  brandRelationships.push({
    operatorId: OPS.aimbridge.operatorId,
    brand,
    relationshipStatus: "Verified Current Relationship",
    geography: "Mexico (primary evidence)",
    evidence: "About Us partnerships + directory flags",
    sourceIds: [S.aimAbout, S.aimDir],
    verificationDate: RESEARCH_DATE,
    limitations: "Property-scoped; no global approval inference",
  });
}
comparables.push(
  {
    operatorId: OPS.aimbridge.operatorId,
    propertyName: "JW Marriott Monterrey Valle",
    city: "Monterrey",
    country: "Mexico",
    brand: "JW Marriott",
    hotelSegment: "Luxury",
    urbanOrResort: "Urban",
    assetType: "Luxury urban",
    developmentType: "Operating",
    whyComparable: "Urban luxury branded management in Mexico",
    comparabilityStrength: "High",
    verificationStatus: "Verified",
    sourceIds: [S.aimDir],
    performanceEvidence: "Performance evidence unavailable or not independently verified.",
  },
  {
    operatorId: OPS.aimbridge.operatorId,
    propertyName: "Aloft Playa del Carmen",
    city: "Playa del Carmen",
    country: "Mexico",
    brand: "Aloft",
    hotelSegment: "Upscale",
    urbanOrResort: "Urban",
    assetType: "Lifestyle select / upscale",
    developmentType: "Operating",
    whyComparable: "Lifestyle urban coastal Mexico",
    comparabilityStrength: "High",
    verificationStatus: "Verified",
    sourceIds: [S.aimDir],
    performanceEvidence: "Performance evidence unavailable or not independently verified.",
  },
  {
    operatorId: OPS.aimbridge.operatorId,
    propertyName: "Staybridge Suites Guadalajara Expo",
    city: "Guadalajara",
    country: "Mexico",
    brand: "Staybridge Suites",
    hotelSegment: "Upscale extended stay",
    urbanOrResort: "Urban",
    assetType: "Extended stay",
    developmentType: "Operating",
    whyComparable: "Extended-stay branded management",
    comparabilityStrength: "Moderate",
    verificationStatus: "Verified",
    sourceIds: [S.aimDir],
    performanceEvidence: "Performance evidence unavailable or not independently verified.",
  }
);
claim({
  operatorId: OPS.aimbridge.operatorId,
  operatorName: OPS.aimbridge.operatorName,
  claimCategory: "geography",
  claimSubject: "active_countries",
  claimPredicate: "operates_in",
  claimValue: "Mexico",
  normalizedValue: ["Mexico"],
  publicationClass: 1,
  objectiveFact: true,
  sourceIds: [S.aimDir, S.aimAbout],
  evidenceClass: "independently_referenced",
  scoringRelevance: "High",
  potentialScoreImpact: "Eligibility",
});

// —— Cenote (public research limited) ——
geography.push({
  operatorId: OPS.cenote.operatorId,
  country: "Mexico",
  presenceType: "Claimed Capability",
  evidence: "Airtable Active Countries + HQ registry (Mérida); official site 503 during calibration",
  sourceIds: [S.cenoteSite],
  limitations: "Pending independent public corroboration of managed properties",
});
managementStructures.push({
  operatorId: OPS.cenote.operatorId,
  structure: "Third-Party Management",
  status: "Supported With Conditions",
  sourceIds: [],
  evidence: "Existing Airtable Commercial structures — treat as Operator Reported until sourced",
  limitations: "No independent public source located in this wave",
});
exceptions.push({
  operatorId: OPS.cenote.operatorId,
  claim: "Active Countries multi-country list",
  existingValue: OPS.cenote.airtableActiveCountries.join(", "),
  proposedValue: "Mexico (Claimed Capability) pending property-level corroboration; other countries Unknown",
  sources: [S.cenoteSite],
  conflictType: "unsupported_current_value",
  potentialScoringImpact: "High",
  publicationClass: 2,
  reasonForEscalation: "Airtable lists 8 countries; public research could not corroborate managed properties; site 503",
  recommendedResolution: "Demote non-Mexico to Unknown/Strategic Interest until sourced",
  reviewer: "Founder / Operator Intelligence lead",
  reviewStatus: "Open",
});
claim({
  operatorId: OPS.cenote.operatorId,
  operatorName: OPS.cenote.operatorName,
  claimCategory: "geography",
  claimSubject: "active_countries_breadth",
  claimPredicate: "unsupported_breadth",
  claimValue: OPS.cenote.airtableActiveCountries.join("|"),
  publicationClass: 2,
  requiresEvidenceLabel: true,
  evidenceLabel: "Operator Reported",
  sourceIds: [S.cenoteSite],
  evidenceClass: "general_claim",
  conflictStatus: "Hard",
  flags: { unsupportedCurrent: true },
  scoringRelevance: "High",
  potentialScoreImpact: "Eligibility",
  limitations: "Exception queue — do not auto-publish multi-country as Current Managed Property",
});
claim({
  operatorId: OPS.cenote.operatorId,
  operatorName: OPS.cenote.operatorName,
  claimCategory: "identity",
  claimSubject: "website",
  claimPredicate: "equals",
  claimValue: "https://cenoteazul.mx",
  publicationClass: 1,
  objectiveFact: true,
  sourceIds: [S.cenoteSite],
  evidenceClass: "portfolio_level",
  verificationStatus: "Unverified",
  scoringRelevance: "Low",
  potentialScoreImpact: "Coverage",
  notes: "URL known from Airtable; live fetch failed 503",
});

// Publication decisions
const publicationDecisions = claims.map((c) => {
  const d = resolvePublicationDecision(c, { sources });
  return { claimId: c.id, operatorId: c.operatorId, ...d };
});

// Extra exception: Arbor Peru strategic interest must not equal current
exceptions.push({
  operatorId: OPS.arbor.operatorId,
  claim: "Peru / Costa Rica BD experience",
  existingValue: "none",
  proposedValue: "Strategic Interest only",
  sources: [S.arborPressHutchinson],
  conflictType: "presence_overclaim_prevention",
  potentialScoringImpact: "High",
  publicationClass: 4,
  reasonForEscalation: "Never-infer: strategic interest ≠ operating presence",
  recommendedResolution: "Keep Strategic Interest; exclude from Active Countries overlay",
  reviewer: "System rule",
  reviewStatus: "Resolved",
});

const operators = Object.values(OPS).map((o) => ({
  ...o,
  researchLimitation:
    o.operatorId === OPS.cenote.operatorId
      ? "Public web footprint for hotel-management identity not independently confirmed; site 503; source queue open"
      : null,
}));

const files = {
  "operators.json": operators,
  "sources.json": sources,
  "claims.json": claims,
  "geography.json": geography,
  "management-structures.json": managementStructures,
  "experience.json": experience,
  "brand-relationships.json": brandRelationships,
  "comparables.json": comparables,
  "exceptions.json": exceptions,
  "publication-decisions.json": publicationDecisions,
};

for (const [name, data] of Object.entries(files)) {
  writeFileSync(join(dir, name), JSON.stringify(data, null, 2), "utf8");
}

writeFileSync(
  join(dir, "README.md"),
  `# Operator Intelligence — Calibration Cohort Dataset

**Generated:** ${RESEARCH_DATE}  
**Process:** ${PROCESS}  
**Mode:** Local only — does **not** write Airtable or change production behavior.

## Cohort

| Operator | ID |
| -------- | -- |
| Arbor Lodging (CALA) | recF5Z87OAqFgndoq |
| Cenote Azul Operadores | recQ6Cf8O2z0tiqBz |
| Hotel Equities (CALA) | recWPKu5laVZxsvpn |
| GHL Hoteles | reciI2tYQBfMoMK9G |
| Playa Hotels & Resorts | rec3TUHT9Z4AnFp5P |
| Aimbridge Hospitality (LATAM) | recGWxIJqnYHkJZFD |

## Important limitations

- Search snippets and AI summaries were **not** used as evidence.
- No property-level financial performance was invented.
- Cenote Azul: public corroboration incomplete; exception open for multi-country Airtable geography.
- Arbor: Mexico office + Los Cabos development ≠ current managed portfolio; Peru/Costa Rica = Strategic Interest only.
- Presence types are distinguished per source policy.

## Regenerate

\`\`\`bash
node scripts/seed-operator-intelligence-calibration-cohort.mjs
\`\`\`
`,
  "utf8"
);

console.log(
  JSON.stringify(
    {
      wrote: dir,
      operators: operators.length,
      sources: sources.length,
      claims: claims.length,
      comparables: comparables.length,
      exceptions: exceptions.length,
      publicationDecisions: publicationDecisions.length,
    },
    null,
    2
  )
);
