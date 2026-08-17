#!/usr/bin/env node
/**
 * Wave 3 research-stage local seed (Argentina-capable). No Airtable Master creates.
 *   node scripts/seed-operator-intelligence-wave-3.mjs
 */
import { writeFileSync, mkdirSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { resolvePublicationDecision } from "../lib/operator-intelligence/publication-policy.js";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const dir = join(root, "data", "operator-intelligence", "wave-3-cohort");
mkdirSync(dir, { recursive: true });
const RESEARCH_DATE = "2026-08-04";

/** Research-stage IDs (not Airtable Master IDs). */
const OPS = {
  alvarez: {
    operatorId: "research_alvarez_arguelles",
    operatorName: "Álvarez Argüelles Hoteles",
    website: "https://www.alvarezarguelles.com",
    operatorType: "Owner-operator + third-party management",
    chainScales: ["Upscale", "Upper Upscale", "Upper Midscale"],
    dealalityMasterId: null,
  },
  tremun: {
    operatorId: "research_tremun",
    operatorName: "Tremun Hoteles",
    website: "https://www.tremunhoteles.com.ar",
    operatorType: "Third-party management / lease variants",
    chainScales: ["Upscale", "Upper Midscale", "Midscale"],
    dealalityMasterId: null,
  },
  aadesa: {
    operatorId: "research_aadesa",
    operatorName: "AADESA",
    website: "https://www.aadesa.com.ar",
    operatorType: "Hotel management & franchising",
    chainScales: ["Upscale", "Upper Midscale"],
    dealalityMasterId: null,
  },
};

const sources = [
  {
    id: "src_w3_aa_001",
    title: "Álvarez Argüelles — Consulting & Management",
    publisher: "Álvarez Argüelles",
    url: "https://www.alvarezarguelles.com/en/consulting-and-management/",
    authority: "operator_marketing",
    dateAccessed: RESEARCH_DATE,
  },
  {
    id: "src_w3_aa_002",
    title: "Álvarez Argüelles — Acerca de (portfolio cities)",
    publisher: "Álvarez Argüelles",
    url: "https://www.alvarezarguelles.com/acerca-de/",
    authority: "primary_authoritative",
    dateAccessed: RESEARCH_DATE,
  },
  {
    id: "src_w3_tr_001",
    title: "Tremun Hoteles — Sobre Tremun (management models)",
    publisher: "Tremun",
    url: "https://www.tremunhoteles.com.ar/en/sobre-tremun.html",
    authority: "primary_authoritative",
    dateAccessed: RESEARCH_DATE,
  },
  {
    id: "src_w3_ad_001",
    title: "AADESA — Hotel management and franchising",
    publisher: "AADESA",
    url: "https://www.aadesa.com.ar/en/index.html",
    authority: "operator_marketing",
    dateAccessed: RESEARCH_DATE,
  },
];

function claim(p) {
  return {
    geographicScope: null,
    brandScope: null,
    conflictStatus: "None",
    researchDate: RESEARCH_DATE,
    researcherOrProcess: "wave-3-argentina",
    objectiveFact: true,
    internalOnly: false,
    neverInfer: false,
    sensitive: false,
    fieldState: "Present",
    ...p,
  };
}

const claims = [
  claim({
    id: "clm_w3_aa_001",
    operatorId: OPS.alvarez.operatorId,
    operatorName: OPS.alvarez.operatorName,
    claimCategory: "geography",
    claimSubject: "argentina_operating",
    claimPredicate: "operates_in",
    claimValue: "Argentina — 12–13 establishments own + third-party across multiple cities",
    normalizedValue: ["Argentina"],
    evidenceClass: "independently_referenced",
    verificationStatus: "Verified",
    publicationClass: 1,
    sourceIds: ["src_w3_aa_001", "src_w3_aa_002"],
    scoringRelevance: "High",
    potentialScoreImpact: "Eligibility",
    limitations: "Operator-published portfolio; property-level deal outreach still required",
  }),
  claim({
    id: "clm_w3_aa_002",
    operatorId: OPS.alvarez.operatorId,
    operatorName: OPS.alvarez.operatorName,
    claimCategory: "structure",
    claimSubject: "third_party_management",
    claimPredicate: "supports",
    claimValue: "Third-party management / consulting",
    normalizedValue: "Third-Party Management",
    evidenceClass: "operator_reported",
    verificationStatus: "Verified",
    publicationClass: 2,
    requiresEvidenceLabel: true,
    sourceIds: ["src_w3_aa_001"],
    scoringRelevance: "High",
    potentialScoreImpact: "Eligibility",
  }),
  claim({
    id: "clm_w3_tr_001",
    operatorId: OPS.tremun.operatorId,
    operatorName: OPS.tremun.operatorName,
    claimCategory: "geography",
    claimSubject: "argentina_operating",
    claimPredicate: "operates_in",
    claimValue: "Argentina — manages own and third-party hotels",
    normalizedValue: ["Argentina"],
    evidenceClass: "independently_referenced",
    verificationStatus: "Verified",
    publicationClass: 1,
    sourceIds: ["src_w3_tr_001"],
    scoringRelevance: "High",
    potentialScoreImpact: "Eligibility",
  }),
  claim({
    id: "clm_w3_tr_002",
    operatorId: OPS.tremun.operatorId,
    operatorName: OPS.tremun.operatorName,
    claimCategory: "structure",
    claimSubject: "management_models",
    claimPredicate: "supports",
    claimValue: "Full management fee; lease / variable rent models",
    normalizedValue: ["Third-Party Management", "Lease"],
    evidenceClass: "independently_referenced",
    verificationStatus: "Verified",
    publicationClass: 1,
    sourceIds: ["src_w3_tr_001"],
    scoringRelevance: "High",
    potentialScoreImpact: "Eligibility",
  }),
  claim({
    id: "clm_w3_ad_001",
    operatorId: OPS.aadesa.operatorId,
    operatorName: OPS.aadesa.operatorName,
    claimCategory: "geography",
    claimSubject: "argentina_base",
    claimPredicate: "operates_in",
    claimValue: "Argentina (Buenos Aires base); LatAm management/franchising",
    normalizedValue: ["Argentina"],
    evidenceClass: "operator_reported",
    verificationStatus: "Referenced",
    publicationClass: 2,
    requiresEvidenceLabel: true,
    sourceIds: ["src_w3_ad_001"],
    scoringRelevance: "High",
    potentialScoreImpact: "Eligibility",
    limitations: "Confirm current managed inventory depth vs franchising",
  }),
  claim({
    id: "clm_w3_perf",
    operatorId: OPS.alvarez.operatorId,
    operatorName: OPS.alvarez.operatorName,
    claimCategory: "performance",
    claimSubject: "metrics",
    claimPredicate: "has_verified_metrics",
    claimValue: "Performance evidence unavailable or not independently verified.",
    publicationClass: 3,
    internalOnly: true,
    evidenceClass: "insufficient",
    verificationStatus: "Unknown",
    sourceIds: [],
    scoringRelevance: "None",
    potentialScoreImpact: "None",
    objectiveFact: false,
  }),
];

const geography = [
  {
    operatorId: OPS.alvarez.operatorId,
    country: "Argentina",
    presenceType: "Current Operating Portfolio",
    sourceIds: ["src_w3_aa_001", "src_w3_aa_002"],
    evidence: "12–13 establishments across AR cities; own + third-party",
  },
  {
    operatorId: OPS.tremun.operatorId,
    country: "Argentina",
    presenceType: "Current Operating Portfolio",
    sourceIds: ["src_w3_tr_001"],
    evidence: "Manages own and third-party hotels in Argentina",
  },
  {
    operatorId: OPS.aadesa.operatorId,
    country: "Argentina",
    presenceType: "Current Managed Property",
    sourceIds: ["src_w3_ad_001"],
    evidence: "BA-based management/franchising; Wyndham Nordelta cited",
    limitations: "Depth of current managed count to be confirmed in outreach",
  },
];

const managementStructures = [
  { operatorId: OPS.alvarez.operatorId, structure: "Third-Party Management", status: "Supported", sourceIds: ["src_w3_aa_001"] },
  { operatorId: OPS.alvarez.operatorId, structure: "Owner-Operated", status: "Supported With Conditions", sourceIds: ["src_w3_aa_002"] },
  { operatorId: OPS.tremun.operatorId, structure: "Third-Party Management", status: "Supported", sourceIds: ["src_w3_tr_001"] },
  { operatorId: OPS.tremun.operatorId, structure: "Lease", status: "Supported", sourceIds: ["src_w3_tr_001"] },
  { operatorId: OPS.aadesa.operatorId, structure: "Third-Party Management", status: "Supported", sourceIds: ["src_w3_ad_001"] },
  { operatorId: OPS.aadesa.operatorId, structure: "Franchise + Operator", status: "Supported", sourceIds: ["src_w3_ad_001"] },
];

const experience = [
  { operatorId: OPS.alvarez.operatorId, dimension: "urban", level: "Repeated Demonstrated Experience", sourceIds: ["src_w3_aa_002"] },
  { operatorId: OPS.alvarez.operatorId, dimension: "leisure", level: "Some Demonstrated Experience", sourceIds: ["src_w3_aa_002"] },
  { operatorId: OPS.alvarez.operatorId, dimension: "resort", level: "Some Demonstrated Experience", sourceIds: ["src_w3_aa_002"] },
  { operatorId: OPS.alvarez.operatorId, dimension: "full_service", level: "Repeated Demonstrated Experience", sourceIds: ["src_w3_aa_001"] },
  { operatorId: OPS.tremun.operatorId, dimension: "urban", level: "Some Demonstrated Experience", sourceIds: ["src_w3_tr_001"] },
  { operatorId: OPS.tremun.operatorId, dimension: "leisure", level: "Some Demonstrated Experience", sourceIds: ["src_w3_tr_001"] },
  { operatorId: OPS.aadesa.operatorId, dimension: "urban", level: "Some Demonstrated Experience", sourceIds: ["src_w3_ad_001"] },
  { operatorId: OPS.aadesa.operatorId, dimension: "select_service", level: "Some Demonstrated Experience", sourceIds: ["src_w3_ad_001"] },
];

const comparables = [
  {
    operatorId: OPS.alvarez.operatorId,
    propertyName: "Álvarez Argüelles Argentina portfolio (multi-city)",
    country: "Argentina",
    urbanOrResort: "Urban + leisure",
    developmentType: "Operating portfolio",
    comparabilityStrength: "High",
    whyComparable: "In-country Argentina operating platform for Deal B geography",
    verificationStatus: "Referenced",
    sourceIds: ["src_w3_aa_002"],
    performanceEvidence: "Performance evidence unavailable or not independently verified.",
  },
  {
    operatorId: OPS.tremun.operatorId,
    propertyName: "Tremun third-party managed hotels (Argentina)",
    country: "Argentina",
    urbanOrResort: "Urban / leisure",
    developmentType: "Operating",
    comparabilityStrength: "Moderate",
    whyComparable: "Explicit third-party management model in Argentina",
    verificationStatus: "Referenced",
    sourceIds: ["src_w3_tr_001"],
    performanceEvidence: "Performance evidence unavailable or not independently verified.",
  },
  {
    operatorId: OPS.aadesa.operatorId,
    propertyName: "Wyndham Nordelta Tigre (cited)",
    city: "Tigre",
    country: "Argentina",
    brand: "Wyndham",
    urbanOrResort: "Urban / suburban",
    comparabilityStrength: "Moderate",
    whyComparable: "Branded Argentina operating example",
    verificationStatus: "Referenced",
    sourceIds: ["src_w3_ad_001"],
    performanceEvidence: "Performance evidence unavailable or not independently verified.",
  },
];

const exceptions = [
  {
    operatorId: "research_wave3",
    claim: "Create Airtable Master records for Wave 3",
    reasonForEscalation: "Research-stage only — Master onboarding not approved this phase",
    reviewStatus: "Blocked pending founder onboarding decision",
    disposition: "Local overlay only",
  },
];

const publicationDecisions = claims.map((c) => {
  const d = resolvePublicationDecision(c, { sources });
  return { claimId: c.id, operatorId: c.operatorId, ...d };
});

const files = {
  "operators.json": Object.values(OPS),
  "sources.json": sources,
  "claims.json": claims,
  "geography.json": geography,
  "management-structures.json": managementStructures,
  "experience.json": experience,
  "comparables.json": comparables,
  "brand-relationships.json": [],
  "exceptions.json": exceptions,
  "publication-decisions.json": publicationDecisions,
};
for (const [n, d] of Object.entries(files)) {
  writeFileSync(join(dir, n), JSON.stringify(d, null, 2));
}
writeFileSync(
  join(dir, "README.md"),
  `# Wave 3 — Argentina research-stage cohort\n\nNo Airtable Master IDs. Local overlay for Deal B diagnosis only.\n`
);
console.log(JSON.stringify({ dir, operators: 3, claims: claims.length, autoPublish: publicationDecisions.filter((d) => /Auto-Publish/i.test(d.status)).length }, null, 2));
