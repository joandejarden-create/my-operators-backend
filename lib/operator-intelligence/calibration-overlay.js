/**
 * Local calibration overlay → Operator Fit v2 prefill merge (non-production).
 * Does not write Airtable; does not alter OAS.
 */

import { readFileSync, existsSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { EVIDENCE_CLASSES } from "../operator-fit/config.js";
import { resolvePublicationDecision, PUBLICATION_DECISION } from "./publication-policy.js";
import { detectConflictsForOperator } from "./conflict-detector.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const DEFAULT_DIR = join(__dirname, "..", "..", "data", "operator-intelligence", "calibration-cohort");
const WAVE2_DIR = join(__dirname, "..", "..", "data", "operator-intelligence", "wave-2-cohort");

function readCohortDir(dir) {
  const read = (name) => {
    const p = join(dir, name);
    if (!existsSync(p)) return [];
    return JSON.parse(readFileSync(p, "utf8"));
  };
  return {
    dir,
    operators: read("operators.json") || [],
    claims: read("claims.json") || [],
    sources: read("sources.json") || [],
    geography: read("geography.json") || [],
    managementStructures: read("management-structures.json") || [],
    experience: read("experience.json") || [],
    brandRelationships: read("brand-relationships.json") || [],
    comparables: read("comparables.json") || [],
    exceptions: read("exceptions.json") || [],
    publicationDecisions: read("publication-decisions.json") || [],
  };
}

function mergeCohorts(...parts) {
  const out = {
    dirs: parts.map((p) => p.dir).filter(Boolean),
    operators: [],
    claims: [],
    sources: [],
    geography: [],
    managementStructures: [],
    experience: [],
    brandRelationships: [],
    comparables: [],
    exceptions: [],
    publicationDecisions: [],
  };
  const seenOp = new Set();
  const seenClaim = new Set();
  const seenSrc = new Set();
  for (const p of parts) {
    for (const o of p.operators || []) {
      if (seenOp.has(o.operatorId)) continue;
      seenOp.add(o.operatorId);
      out.operators.push(o);
    }
    for (const c of p.claims || []) {
      if (seenClaim.has(c.id)) continue;
      seenClaim.add(c.id);
      out.claims.push(c);
    }
    for (const s of p.sources || []) {
      if (seenSrc.has(s.id)) continue;
      seenSrc.add(s.id);
      out.sources.push(s);
    }
    out.geography.push(...(p.geography || []));
    out.managementStructures.push(...(p.managementStructures || []));
    out.experience.push(...(p.experience || []));
    out.brandRelationships.push(...(p.brandRelationships || []));
    out.comparables.push(...(p.comparables || []));
    out.exceptions.push(...(p.exceptions || []));
    out.publicationDecisions.push(...(p.publicationDecisions || []));
  }
  return out;
}

export function loadCalibrationCohort(dir = DEFAULT_DIR) {
  return readCohortDir(dir);
}

/** Calibration + Wave 2 local research overlays (merged). */
export function loadOperatorIntelligenceUniverse(opts = {}) {
  const cal = readCohortDir(opts.calibrationDir || DEFAULT_DIR);
  const w2Path = opts.wave2Dir || WAVE2_DIR;
  const w2 = existsSync(join(w2Path, "operators.json")) ? readCohortDir(w2Path) : readCohortDir(w2Path);
  if (!existsSync(join(w2Path, "operators.json"))) {
    return { ...cal, dirs: [cal.dir] };
  }
  return mergeCohorts(cal, w2);
}

/**
 * Hydrate Fit prefill from Airtable Case Study rows (post-persistence path).
 */
export function hydratePrefillFromCaseStudies(prefill = {}, caseStudyRows = []) {
  const rows = Array.isArray(caseStudyRows) ? caseStudyRows : [];
  if (!rows.length) return { ...prefill };
  const comparables = rows.map((r) => {
    const f = r.fields || r;
    const name = f.property_name || f["Property Name"] || "";
    const strength = f["Comparability Strength"] || "";
    return {
      propertyName: name,
      region: f.region || f.Region || "",
      situation: f.situation || f.Situation || "",
      brand: f.branded_independent || f["Branded / Independent"] || "",
      whyComparable: f["Why Comparable"] || f.owner_relevance || f["Owner Relevance"] || "",
      verified: /High/i.test(strength) || /Verified/i.test(String(f.data_status || "")),
      referenced: true,
      source: "airtable_case_study",
    };
  }).filter((c) => c.propertyName);

  const assetTypes = [];
  const situations = [];
  for (const r of rows) {
    const f = r.fields || r;
    const ht = String(f.hotel_type || f["Hotel Type"] || "");
    const sit = String(f.situation || f.Situation || "");
    const why = String(f["Why Comparable"] || f.owner_relevance || "");
    const blob = `${ht} ${sit} ${why}`;
    if (/resort|leisure/i.test(blob)) assetTypes.push("Resort");
    if (/urban/i.test(blob)) assetTypes.push("Urban");
    if (/select|midscale|limited/i.test(blob)) assetTypes.push("Select Service");
    if (/lifestyle|independent/i.test(blob)) assetTypes.push("Lifestyle");
    if (/mixed.?use|residence/i.test(blob)) assetTypes.push("Mixed-Use");
    if (/full.?service|luxury|upscale/i.test(blob)) assetTypes.push("Full Service");
    if (/conversion|reflag/i.test(blob)) situations.push("Conversion");
    if (/new.?build|opening/i.test(blob)) situations.push("New Build");
    if (/renovation|reposition|turnaround/i.test(blob)) situations.push("Renovation");
  }

  const sources = [
    ...(Array.isArray(prefill.sources) ? prefill.sources : []),
    ...comparables.map((c) => ({
      label: c.propertyName,
      url: null,
      independent: Boolean(c.verified || c.referenced),
    })),
  ];

  const evidenceClasses = [...(prefill.evidenceClasses || [])];
  if (comparables.some((c) => c.verified) && !evidenceClasses.includes(EVIDENCE_CLASSES.VERIFIED_PROJECT)) {
    evidenceClasses.push(EVIDENCE_CLASSES.INDEPENDENT_REFERENCED);
  } else if (comparables.length && !evidenceClasses.length) {
    evidenceClasses.push(EVIDENCE_CLASSES.INDEPENDENT_REFERENCED);
  }

  return {
    ...prefill,
    comparables: comparables.length ? comparables : prefill.comparables,
    sources,
    evidenceClasses: evidenceClasses.length ? evidenceClasses : prefill.evidenceClasses,
    bestFitAssetTypes: [...new Set([...(prefill.bestFitAssetTypes || []), ...assetTypes])],
    operatingSituations: [...new Set([...(prefill.operatingSituations || []), ...situations])],
  };
}

export function buildPrefillOverlayFromCohort(operatorId, cohort) {
  const op = (cohort.operators || []).find((o) => o.operatorId === operatorId);
  if (!op) return null;

  const claims = (cohort.claims || []).filter((c) => c.operatorId === operatorId);
  const sourcesById = Object.fromEntries((cohort.sources || []).map((s) => [s.id, s]));
  const geos = (cohort.geography || []).filter((g) => g.operatorId === operatorId);
  const structures = (cohort.managementStructures || []).filter((s) => s.operatorId === operatorId);
  const comps = (cohort.comparables || []).filter((c) => c.operatorId === operatorId);
  const brands = (cohort.brandRelationships || []).filter((b) => b.operatorId === operatorId);
  const experience = (cohort.experience || []).filter((e) => e.operatorId === operatorId);

  const publishedClaims = [];
  const qualifiedClaims = [];
  const internalClaims = [];
  const rejected = [];

  for (const claim of claims) {
    const decision = resolvePublicationDecision(claim, { sources: cohort.sources });
    const row = { claim, decision };
    if (decision.status === PUBLICATION_DECISION.AUTO_PUBLISH) publishedClaims.push(row);
    else if (decision.status === PUBLICATION_DECISION.PUBLISH_WITH_LABEL) qualifiedClaims.push(row);
    else if (decision.status === PUBLICATION_DECISION.INTERNAL_ONLY) internalClaims.push(row);
    else rejected.push(row);
  }

  // Geography: only strong presence types populate Active Countries for Fit
  const countrySet = new Set();
  const marketPresence = [];
  for (const g of geos) {
    const t = g.presenceType || g.marketPresenceType;
    marketPresence.push({
      country: g.country || (Array.isArray(g.countries) ? g.countries[0] : null),
      presenceType: t,
      evidence: g.evidence || null,
      sourceIds: g.sourceIds || [],
      limitations: g.limitations || null,
      currentOrHistorical: /Historical/i.test(t || "")
        ? "Historical"
        : /Strategic Interest|Claimed Capability|Unknown/i.test(t || "")
          ? "Non-current"
          : "Current",
    });
    if (
      t === "Current Managed Property" ||
      t === "Current Operating Portfolio" ||
      t === "Regional Office or Team"
    ) {
      for (const c of [].concat(g.country || g.countries || [])) countrySet.add(c);
    }
  }

  const structureLabels = [];
  for (const s of structures) {
    if (s.status === "Supported" || s.status === "Supported With Conditions") {
      structureLabels.push(mapStructureToPrefill(s.structure));
    }
  }

  const comparables = comps
    .filter((c) => c.comparabilityStrength !== "Weak")
    .map((c) => ({
      propertyName: c.propertyName,
      region: [c.city, c.country].filter(Boolean).join(", "),
      situation: c.developmentType || c.assetType,
      brand: c.brand,
      keys: c.keyCount,
      verified: c.verificationStatus === "Verified",
      referenced: c.verificationStatus !== "Unverified",
      source: c.sourceIds?.[0] || null,
      whyComparable: c.whyComparable,
    }));

  const sourceRows = [];
  for (const row of [...publishedClaims, ...qualifiedClaims]) {
    for (const sid of row.claim.sourceIds || []) {
      const s = sourcesById[sid];
      if (s) sourceRows.push({ label: s.title, url: s.url, independent: s.authority !== "operator_marketing" });
    }
  }

  const evidenceClasses = [];
  if (comparables.some((c) => c.verified)) evidenceClasses.push(EVIDENCE_CLASSES.VERIFIED_PROJECT);
  else if (comparables.some((c) => c.referenced) || publishedClaims.length) {
    evidenceClasses.push(EVIDENCE_CLASSES.INDEPENDENT_REFERENCED);
  } else if (qualifiedClaims.length) {
    evidenceClasses.push(EVIDENCE_CLASSES.DETAILED_OPERATOR_PROVIDED);
  }

  const assetTypes = [];
  const situations = [];
  let conversion = null;
  let newBuild = null;
  for (const e of experience) {
    const level = e.level || e.classification;
    if (!level || /No Evidence|Unknown|Confirmed Absence/i.test(level)) continue;
    if (/Operator-Reported/i.test(level) && !/Repeated|Some Demonstrated|One Documented/i.test(level)) {
      // qualified only — still usable as soft differentiator for overlay
    }
    const dim = e.dimension || e.experienceDimension;
    if (/urban|resort|mixed-use|lifestyle|select|luxury|full.?service/i.test(dim)) assetTypes.push(titleCase(dim));
    if (/new.?build|conversion|reflag|turnaround|pre-opening|renovation/i.test(dim)) {
      situations.push(titleCase(dim));
      if (/conversion|reflag/i.test(dim)) conversion = level;
      if (/new.?build/i.test(dim)) newBuild = level;
    }
  }

  const brandNames = brands
    .filter((b) => /Verified Current|Announced|Operator-Reported/i.test(b.relationshipStatus || ""))
    .map((b) => b.brand)
    .filter(Boolean);

  const overlay = {
    submission_status: "Active",
    companyName: op.operatorName,
    website: op.website || undefined,
    activeCountries: [...countrySet],
    _calibrationReplaceGeo: true,
    marketPresence,
    marketPresenceRecords: marketPresence,
    marketPresenceType: geos.map((g) => g.presenceType).filter(Boolean),
    managementStructuresSupported: [...new Set(structureLabels.filter(Boolean))],
    _calibrationReplaceStructures: structureLabels.length > 0,
    chainScalesSupported: op.chainScales || undefined,
    brands: brandNames.length ? brandNames : undefined,
    bestFitAssetTypes: [...new Set(assetTypes)],
    operatingSituations: [...new Set(situations)],
    conversionReflagExperience: conversion || undefined,
    newBuildOpeningExperience: newBuild || undefined,
    comparables,
    sources: sourceRows,
    evidenceClasses,
    ownerReportingLevel: op.ownerReportingLevel || undefined,
    _calibrationMeta: {
      operatorId,
      publishedFactCount: publishedClaims.length,
      qualifiedFactCount: qualifiedClaims.length,
      internalClaimCount: internalClaims.length,
      rejectedCount: rejected.length,
      comparableCount: comparables.length,
      sourceCount: sourceRows.length,
    },
  };

  const conflicts = detectConflictsForOperator({
    operatorId,
    operatorName: op.operatorName,
    claims,
    profile: {
      activeCountries: op.airtableActiveCountries || [],
      managementStructuresSupported: op.airtableStructures || [],
      parentCompany: op.parentCompany,
    },
  });

  return {
    operatorId,
    companyName: op.operatorName,
    overlay,
    diagnostics: {
      claimsAdded: claims.length,
      publishedFactsAdded: publishedClaims.length,
      qualifiedFactsAdded: qualifiedClaims.length,
      internalOnly: internalClaims.length,
      conflictsDetected: conflicts.filter((c) => c.conflictType !== "none"),
      exceptions: (cohort.exceptions || []).filter((e) => e.operatorId === operatorId),
    },
  };
}

/**
 * Merge Airtable-style prefill with calibration overlay (overlay wins on provided keys).
 */
export function mergePrefillWithCalibration(basePrefill = {}, overlayBundle) {
  if (!overlayBundle?.overlay) {
    return { prefill: { ...basePrefill }, mode: "airtable_only", diagnostics: null };
  }
  const o = overlayBundle.overlay;
  const merged = { ...basePrefill };
  const assignIf = (key, val) => {
    if (val == null) return;
    if (Array.isArray(val) && val.length === 0) return;
    merged[key] = val;
  };
  // Calibration may intentionally clear overstated Airtable geography/structures.
  if (o._calibrationReplaceGeo) {
    merged.activeCountries = Array.isArray(o.activeCountries) ? o.activeCountries : [];
  } else {
    assignIf("activeCountries", o.activeCountries);
  }
  if (Array.isArray(o.marketPresence) && o.marketPresence.length) {
    merged.marketPresence = o.marketPresence;
    merged.marketPresenceRecords = o.marketPresence;
  } else if (Array.isArray(o.marketPresenceRecords) && o.marketPresenceRecords.length) {
    merged.marketPresence = o.marketPresenceRecords;
    merged.marketPresenceRecords = o.marketPresenceRecords;
  }
  assignIf("marketPresenceType", o.marketPresenceType);
  if (o._calibrationReplaceStructures) {
    merged.managementStructuresSupported = Array.isArray(o.managementStructuresSupported)
      ? o.managementStructuresSupported
      : [];
  } else {
    assignIf("managementStructuresSupported", o.managementStructuresSupported);
  }
  assignIf("chainScalesSupported", o.chainScalesSupported);
  assignIf("brands", o.brands);
  assignIf("bestFitAssetTypes", o.bestFitAssetTypes);
  assignIf("operatingSituations", o.operatingSituations);
  assignIf("conversionReflagExperience", o.conversionReflagExperience);
  assignIf("newBuildOpeningExperience", o.newBuildOpeningExperience);
  assignIf("ownerReportingLevel", o.ownerReportingLevel);
  if (o.comparables?.length) merged.comparables = o.comparables;
  if (o.sources?.length) merged.sources = o.sources;
  if (o.evidenceClasses?.length) merged.evidenceClasses = o.evidenceClasses;
  merged.submission_status = merged.submission_status || "Active";
  return {
    prefill: merged,
    mode: "airtable_plus_calibration",
    diagnostics: overlayBundle.diagnostics,
    calibrationMeta: o._calibrationMeta,
  };
}

function mapStructureToPrefill(structure) {
  const s = String(structure || "");
  if (/third.?party/i.test(s)) return "Full third-party management";
  if (/franchise\s*\+\s*operator|franchise support/i.test(s)) return "Franchise support";
  if (/franchise only/i.test(s)) return "Franchise support";
  if (/owner.?operated/i.test(s)) return "Owner-operated";
  if (/lease/i.test(s)) return "Lease";
  if (/asset management/i.test(s)) return "Asset management";
  if (/hybrid/i.test(s)) return "Hybrid / project-specific";
  return s || null;
}

function titleCase(s) {
  return String(s || "")
    .replace(/[_-]/g, " ")
    .replace(/\b\w/g, (c) => c.toUpperCase());
}
