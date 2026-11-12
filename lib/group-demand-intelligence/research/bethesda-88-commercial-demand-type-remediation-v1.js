/**
 * Bethesda 88 — Commercial Demand Type Remediation V1
 *
 * Classifies every union opportunity as Guest Rooms / Local Catering / Both
 * (or NOT_APPLICABLE for DQ/quarantine), persists via existing revenueStreamClass
 * architecture, and gates Full E2E quality. Zero paid. No maturity promotion.
 * No customer publication.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  classifyOpportunityCommercialDemandType,
  attachCommercialDemandTypeClassification,
  CLASSIFY_OPPORTUNITY_COMMERCIAL_DEMAND_TYPE_V1,
} from "../classify-opportunity-commercial-demand-type-v1.js";
import {
  COMMERCIAL_DEMAND_TYPE,
  evaluateCommercialDemandTypeQuality,
  countCommercialDemandTypeFacets,
  GDI_COMMERCIAL_DEMAND_TYPE_V1,
} from "../commercial-demand-type-v1.js";
import { resolveOpportunityRevenueStreamIcons } from "../opportunity-revenue-stream-icons-v1.js";
import { mergeBethesdaInternalResearchOpportunities } from "./bethesda-internal-research-union-v1.js";
import { BETHESDA_29_ATTESTED_IDS } from "./bethesda-historical-attestation-bag-v1.js";
import {
  loadHotelOpportunityDoc,
  backupHotelOpportunityStore,
  BETHESDA_HOTEL_ID,
  BETHESDA_PROTECTED_SCALING_IDS,
  ACCEPTABLE_59_ROW_SHA256S,
  FINAL_FULL_E2E_REMEDIATED_59_ROW_SHA256,
  COMMERCIAL_DEMAND_TYPE_CLASSIFIED_59_ROW_SHA256,
  assertNoProtectedIdLoss,
  assertCountNotDropped,
  sha256File,
} from "./gdi-hotel-store-integrity-guard-v1.js";
import {
  writeJsonAtomic,
  researchStageHotelDir,
} from "./gdi-research-stage-persistence-v1.js";
import { applyBethesdaCommercialEventFamilyPresentation } from "./bethesda-88-commercial-event-family-v1.js";

export const BETHESDA_88_COMMERCIAL_DEMAND_TYPE_REMEDIATION_V1 =
  "gdi-bethesda-88-commercial-demand-type-remediation-v1";

export const DEMAND_TYPE_APPROVAL_TOKEN =
  "FOUNDER_APPROVE_BETHESDA_COMMERCIAL_DEMAND_TYPE_20261112";

/** Accept predecessor SHA after final Full E2E remediation, or this run's output once known. */
export const DEMAND_TYPE_ACCEPTABLE_PREDECESSOR_SHAS = Object.freeze([
  FINAL_FULL_E2E_REMEDIATED_59_ROW_SHA256,
  COMMERCIAL_DEMAND_TYPE_CLASSIFIED_59_ROW_SHA256,
  ...ACCEPTABLE_59_ROW_SHA256S,
]);

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const REPORT_REL = "reports/gdi-bethesda-88-commercial-demand-type-remediation-v1";
const HISTORICAL_FIXTURE_REL =
  "fixtures/gdi/bethesda-29-historical-reconstruction-v1.json";

const DEMAND_TYPE_FIELD_KEYS = Object.freeze([
  "commercialDemandType",
  "commercialDemandTypeLabel",
  "revenueStreamClass",
  "revenueStreamLabel",
  "applicableHotelRevenueStreams",
  "applicableHotelRevenueStreamsLabel",
  "guestroomThesis",
  "eventFnbThesis",
  "hotelDemandHypotheses",
  "guestroomDemandConfidence",
  "eventFnbDemandConfidence",
  "commercialDemandTypeEvidence",
  "revenueStreamIcons",
  "revenueStreamIconsAria",
  "revenueStreamIconsVersion",
  "commercialDemandTypeClassifiedAt",
]);

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function mergeDemandFieldsOnly(before, persist) {
  const next = { ...before };
  for (const key of DEMAND_TYPE_FIELD_KEYS) {
    if (persist[key] !== undefined) next[key] = persist[key];
  }
  // Merge hotelDemandHypotheses without wiping unrelated hyp keys
  if (persist.hotelDemandHypotheses) {
    next.hotelDemandHypotheses = {
      ...(before.hotelDemandHypotheses && typeof before.hotelDemandHypotheses === "object"
        ? before.hotelDemandHypotheses
        : {}),
      ...persist.hotelDemandHypotheses,
    };
  }
  next.commercialDemandTypeRemediation = {
    version: BETHESDA_88_COMMERCIAL_DEMAND_TYPE_REMEDIATION_V1,
    appliedAt: new Date().toISOString(),
    founderAuthorized: true,
  };
  return next;
}

function loadHistoricalDoc({ root = ROOT } = {}) {
  const stagePath = path.join(
    researchStageHotelDir(BETHESDA_HOTEL_ID, { root }),
    "reconstructed-opportunities-v1.json",
  );
  if (fs.existsSync(stagePath)) {
    return {
      path: stagePath,
      exists: true,
      doc: JSON.parse(fs.readFileSync(stagePath, "utf8")),
    };
  }
  const fixturePath = path.join(root, HISTORICAL_FIXTURE_REL);
  if (fs.existsSync(fixturePath)) {
    return {
      path: fixturePath,
      exists: true,
      doc: JSON.parse(fs.readFileSync(fixturePath, "utf8")),
    };
  }
  return { path: null, exists: false, doc: null };
}

/**
 * Build dry-run classification manifest for all 88.
 */
export function buildDemandTypeClassificationManifest({ root = ROOT } = {}) {
  const store = loadHotelOpportunityDoc(BETHESDA_HOTEL_ID, { root });
  const union = mergeBethesdaInternalResearchOpportunities(
    BETHESDA_HOTEL_ID,
    store.opportunities,
    { root },
  );
  const classifiedAt = new Date().toISOString();
  const rows = [];
  const counts = {
    GUEST_ROOMS: 0,
    LOCAL_CATERING: 0,
    BOTH: 0,
    UNDETERMINED: 0,
    NOT_APPLICABLE: 0,
  };

  for (const opp of union.opportunities) {
    const result = classifyOpportunityCommercialDemandType(opp, { classifiedAt });
    const attached = attachCommercialDemandTypeClassification(opp, { classifiedAt });
    const icons = resolveOpportunityRevenueStreamIcons(attached);
    const quality = evaluateCommercialDemandTypeQuality(attached, {
      familyRole: opp.commercialEventFamilyRole,
    });
    const historical = BETHESDA_29_ATTESTED_IDS.includes(opp.id);
    const demandType = result.commercialDemandType || COMMERCIAL_DEMAND_TYPE.UNDETERMINED;
    counts[demandType] = (counts[demandType] || 0) + 1;
    rows.push({
      id: opp.id,
      historical,
      priority: opp.priority || null,
      category: opp.category || opp.opportunityType || null,
      title: text(opp.title).slice(0, 100),
      commercialDemandType: demandType,
      revenueStreamClass: result.revenueStreamClass,
      guestroomThesis: result.guestroomThesis,
      eventFnbThesis: result.eventFnbThesis,
      qualityState: result.qualityState || null,
      notes: result.notes || [],
      icons: icons.icons,
      qualityOk: quality.ok,
      qualityBlockers: quality.blockers,
      persistFields: Object.keys(result.persist || {}),
    });
  }

  const family = applyBethesdaCommercialEventFamilyPresentation(union.opportunities, {
    hideLinkedFromList: true,
  });
  const facetBase = (family.listRows || union.opportunities).map((o) => {
    const r = rows.find((x) => x.id === o.id);
    return {
      ...o,
      revenueStreamClass: r?.revenueStreamClass,
      commercialDemandType: r?.commercialDemandType,
    };
  });
  const facets = countCommercialDemandTypeFacets(facetBase);

  return {
    version: BETHESDA_88_COMMERCIAL_DEMAND_TYPE_REMEDIATION_V1,
    classifier: CLASSIFY_OPPORTUNITY_COMMERCIAL_DEMAND_TYPE_V1,
    contract: GDI_COMMERCIAL_DEMAND_TYPE_V1,
    generatedAt: classifiedAt,
    hotelId: BETHESDA_HOTEL_ID,
    storeSha: store.sha256,
    unionCount: union.opportunities.length,
    counts,
    facets,
    uniqueCommercial: family.counts?.uniqueCommercialOpportunities ?? null,
    rows,
    qualityFailures: rows.filter((r) => r.qualityOk === false),
  };
}

/**
 * Atomically persist demand-type fields onto canonical 59 + historical 29.
 */
export function applyDemandTypeClassificationAtomic({
  root = ROOT,
  founderAuthorization = false,
  approvalToken = null,
  dryRun = true,
} = {}) {
  if (!founderAuthorization || approvalToken !== DEMAND_TYPE_APPROVAL_TOKEN) {
    return {
      ok: false,
      code: "FOUNDER_AUTHORIZATION_REQUIRED",
      writeApplied: false,
      requiredToken: DEMAND_TYPE_APPROVAL_TOKEN,
    };
  }

  const store = loadHotelOpportunityDoc(BETHESDA_HOTEL_ID, { root });
  if (!store.exists || store.count !== 59) {
    return { ok: false, code: "UNEXPECTED_CANONICAL_STORE", count: store.count };
  }
  if (!DEMAND_TYPE_ACCEPTABLE_PREDECESSOR_SHAS.includes(store.sha256)) {
    return {
      ok: false,
      code: "UNEXPECTED_STORE_SHA",
      sha256: store.sha256,
      acceptable: DEMAND_TYPE_ACCEPTABLE_PREDECESSOR_SHAS,
    };
  }

  const classifiedAt = new Date().toISOString();
  const manifest = buildDemandTypeClassificationManifest({ root });
  const byResult = new Map(manifest.rows.map((r) => [r.id, r]));

  // Re-classify for full persist payloads
  const union = mergeBethesdaInternalResearchOpportunities(
    BETHESDA_HOTEL_ID,
    store.opportunities,
    { root },
  );
  const persistById = new Map();
  for (const opp of union.opportunities) {
    const result = classifyOpportunityCommercialDemandType(opp, { classifiedAt });
    persistById.set(opp.id, result);
  }

  if (dryRun) {
    return {
      ok: true,
      dryRun: true,
      writeApplied: false,
      counts: manifest.counts,
      facets: manifest.facets,
      rowCount: manifest.rows.length,
      qualityFailures: manifest.qualityFailures.length,
      storeSha: store.sha256,
    };
  }

  const backup = backupHotelOpportunityStore(BETHESDA_HOTEL_ID, {
    root,
    label: "pre-commercial-demand-type-remediation",
  });

  // Canonical 59
  const nextCanonical = store.opportunities.map((o) => {
    const result = persistById.get(o.id);
    if (!result?.persist) return o;
    return mergeDemandFieldsOnly(o, result.persist);
  });
  assertNoProtectedIdLoss({
    hotelId: BETHESDA_HOTEL_ID,
    nextOpportunities: nextCanonical,
    protectedIds: [...BETHESDA_PROTECTED_SCALING_IDS],
  });
  assertCountNotDropped({
    hotelId: BETHESDA_HOTEL_ID,
    priorCount: 59,
    nextCount: nextCanonical.length,
  });
  if (nextCanonical.length !== 59) {
    return { ok: false, code: "CANONICAL_COUNT_DRIFT", writeApplied: false };
  }

  const canonicalDoc = {
    ...(store.doc || {}),
    opportunities: nextCanonical,
    updatedAt: classifiedAt,
    commercialDemandTypeRemediation: {
      version: BETHESDA_88_COMMERCIAL_DEMAND_TYPE_REMEDIATION_V1,
      appliedAt: classifiedAt,
      counts: manifest.counts,
    },
  };
  writeJsonAtomic(store.path, canonicalDoc);
  const afterSha = sha256File(store.path);

  // Historical 29
  const hist = loadHistoricalDoc({ root });
  let historicalApplied = 0;
  if (hist.exists && hist.doc) {
    const histOps = Array.isArray(hist.doc.opportunities)
      ? hist.doc.opportunities
      : Array.isArray(hist.doc)
        ? hist.doc
        : [];
    const nextHist = histOps.map((o) => {
      const id = text(o.id);
      if (!BETHESDA_29_ATTESTED_IDS.includes(id)) return o;
      const result = persistById.get(id);
      if (!result?.persist) return o;
      historicalApplied += 1;
      return mergeDemandFieldsOnly(o, result.persist);
    });
    const histDoc = Array.isArray(hist.doc)
      ? nextHist
      : {
          ...hist.doc,
          opportunities: nextHist,
          updatedAt: classifiedAt,
          commercialDemandTypeRemediation: {
            version: BETHESDA_88_COMMERCIAL_DEMAND_TYPE_REMEDIATION_V1,
            appliedAt: classifiedAt,
            appliedCount: historicalApplied,
          },
        };
    writeJsonAtomic(hist.path, histDoc);
    // Mirror fixture when stage path differs
    const fixturePath = path.join(root, HISTORICAL_FIXTURE_REL);
    if (hist.path !== fixturePath && fs.existsSync(path.dirname(fixturePath))) {
      writeJsonAtomic(fixturePath, histDoc);
    }
  }

  return {
    ok: true,
    dryRun: false,
    writeApplied: true,
    counts: manifest.counts,
    facets: manifest.facets,
    canonicalApplied: 59,
    historicalApplied,
    backupPath: backup.backupPath,
    beforeSha256: store.sha256,
    afterSha256: afterSha,
    qualityFailures: manifest.qualityFailures.length,
    sample: manifest.rows.slice(0, 5).map((r) => ({
      id: r.id,
      commercialDemandType: r.commercialDemandType,
      revenueStreamClass: r.revenueStreamClass,
    })),
  };
}

export function runBethesda88CommercialDemandTypeRemediation({
  root = ROOT,
  apply = false,
  founderAuthorization = false,
  approvalToken = null,
} = {}) {
  const outDir = path.join(root, REPORT_REL);
  fs.mkdirSync(outDir, { recursive: true });

  const store = loadHotelOpportunityDoc(BETHESDA_HOTEL_ID, { root });
  writeJsonAtomic(path.join(outDir, "00-source-of-truth-lock.json"), {
    generatedAt: new Date().toISOString(),
    storeSha: store.sha256,
    canonicalCount: store.count,
    acceptablePredecessor: DEMAND_TYPE_ACCEPTABLE_PREDECESSOR_SHAS.includes(
      store.sha256,
    ),
  });

  const manifest = buildDemandTypeClassificationManifest({ root });
  writeJsonAtomic(path.join(outDir, "01-classification-manifest.json"), manifest);
  writeJsonAtomic(path.join(outDir, "02-counts-by-demand-type.json"), {
    counts: manifest.counts,
    facets: manifest.facets,
    qualityFailureCount: manifest.qualityFailures.length,
  });
  writeJsonAtomic(
    path.join(outDir, "03-per-opportunity-classifications.json"),
    { rows: manifest.rows },
  );

  let applyResult = { writeApplied: false, dryRun: true };
  if (apply) {
    applyResult = applyDemandTypeClassificationAtomic({
      root,
      founderAuthorization,
      approvalToken,
      dryRun: false,
    });
  } else {
    applyResult = applyDemandTypeClassificationAtomic({
      root,
      founderAuthorization: true,
      approvalToken: DEMAND_TYPE_APPROVAL_TOKEN,
      dryRun: true,
    });
  }

  writeJsonAtomic(path.join(outDir, "04-apply-result.json"), applyResult);

  // Post-apply verification when written
  let verification = null;
  if (applyResult.writeApplied) {
    const afterManifest = buildDemandTypeClassificationManifest({ root });
    verification = {
      counts: afterManifest.counts,
      facets: afterManifest.facets,
      qualityFailures: afterManifest.qualityFailures.length,
      storeSha: afterManifest.storeSha,
      all88Classified: afterManifest.rows.every(
        (r) =>
          r.commercialDemandType &&
          r.revenueStreamClass &&
          (r.commercialDemandType === COMMERCIAL_DEMAND_TYPE.NOT_APPLICABLE ||
            r.commercialDemandType === COMMERCIAL_DEMAND_TYPE.UNDETERMINED ||
            r.guestroomThesis ||
            r.eventFnbThesis),
      ),
    };
    writeJsonAtomic(path.join(outDir, "05-post-apply-verification.json"), verification);
  }

  const summary = {
    version: BETHESDA_88_COMMERCIAL_DEMAND_TYPE_REMEDIATION_V1,
    ok:
      applyResult.ok !== false &&
      (apply ? applyResult.writeApplied === true : true) &&
      manifest.unionCount === 88,
    hotelId: BETHESDA_HOTEL_ID,
    unionCount: manifest.counts && Object.values(manifest.counts).reduce((a, b) => a + b, 0),
    counts: manifest.counts,
    facets: manifest.facets,
    qualityFailures: manifest.qualityFailures.length,
    writeApplied: applyResult.writeApplied === true,
    storeShaBefore: store.sha256,
    storeShaAfter: applyResult.afterSha256 || store.sha256,
    backupPath: applyResult.backupPath || null,
    paidAuthorized: false,
    publicationAuthorized: false,
    reportDir: REPORT_REL,
    verification,
    code: applyResult.code || null,
  };
  writeJsonAtomic(path.join(outDir, "00-demand-type-remediation-summary.json"), summary);
  return summary;
}
