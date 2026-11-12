/**
 * Bethesda 88 — Commercial Surface Reconciliation V1
 *
 * Fixes the systemic defect where Full E2E reported 88/88 processed while the
 * actual GDI commercial list still showed research themes, DISQUALIFIED events,
 * and duplicate cards. Persists surface dispositions; does not delete research.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  classifyCommercialSurfaceDisposition,
  attachCommercialSurfaceDisposition,
  summarizeCommercialSurfaceBatch,
  filterOpportunitiesByCommercialSurface,
  primaryCommercialForbiddenFixtures,
  COMMERCIAL_SURFACE,
  GDI_COMMERCIAL_SURFACE_DISPOSITION_V1,
  GDI_LIST_SURFACE_VIEW,
} from "../gdi-commercial-surface-disposition-v1.js";
import { applyBethesdaCommercialEventFamilyPresentation } from "./bethesda-88-commercial-event-family-v1.js";
import { mergeBethesdaInternalResearchOpportunities } from "./bethesda-internal-research-union-v1.js";
import { BETHESDA_29_ATTESTED_IDS } from "./bethesda-historical-attestation-bag-v1.js";
import {
  loadHotelOpportunityDoc,
  backupHotelOpportunityStore,
  BETHESDA_HOTEL_ID,
  BETHESDA_PROTECTED_SCALING_IDS,
  ACCEPTABLE_59_ROW_SHA256S,
  COMMERCIAL_DEMAND_TYPE_CLASSIFIED_59_ROW_SHA256,
  assertNoProtectedIdLoss,
  assertCountNotDropped,
  sha256File,
} from "./gdi-hotel-store-integrity-guard-v1.js";
import {
  writeJsonAtomic,
  researchStageHotelDir,
} from "./gdi-research-stage-persistence-v1.js";

export const BETHESDA_88_COMMERCIAL_SURFACE_RECONCILIATION_V1 =
  "gdi-bethesda-88-commercial-surface-reconciliation-v1";

export const SURFACE_RECON_APPROVAL_TOKEN =
  "FOUNDER_APPROVE_BETHESDA_COMMERCIAL_SURFACE_RECON_20261112";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const REPORT_REL = "reports/gdi-bethesda-88-commercial-surface-reconciliation-v1";
const HISTORICAL_FIXTURE_REL =
  "fixtures/gdi/bethesda-29-historical-reconstruction-v1.json";

/** Exact factual / disposition field patches (founder-authorized). */
export const SURFACE_FACTUAL_PATCHES = Object.freeze([
  {
    id: "gdi_opp_marriott_hq_adjacent_corporate_watch",
    fields: {
      title:
        "Market intelligence — corporate / training demand near Marriott HQ corridor (research theme)",
      commercialSurfaceDisposition: COMMERCIAL_SURFACE.MARKET_INTELLIGENCE,
      classification: "MARKET_INTELLIGENCE",
      hotelFitHonesty: "CATEGORY_WATCH_NO_SINGLE_BUYER",
      recommendedSalesAction:
        "Use as a research theme to discover specific HQ-adjacent accounts. Do not treat this card as a Build Account opportunity — no single buyer or verified meeting exists.",
      recommendedNextAction:
        "Research theme only — identify specific accounts before creating a commercial opportunity.",
      salesActionKey: "MONITOR",
      salesAction: "Monitor",
      customerVisible: false,
      salesReady: false,
      primaryCommercialEligible: false,
      hotelOpportunityThesis:
        "Corridor-level market intelligence about possible corporate offsite / training demand near Marriott HQ. Not a specific customer, meeting, or verified demand event — preserve for prospect discovery only.",
    },
    reason: "RESEARCH_THEME_NOT_OPPORTUNITY",
  },
  {
    id: "gdi_opp_2027_nci_rna_biology_symposium_7",
    fields: {
      commercialSurfaceDisposition: COMMERCIAL_SURFACE.RESEARCH_PIPELINE,
      primaryCommercialEligible: false,
      contactResearchStatus: "ORG_ROUTE_ONLY_NAMED_BUYER_UNVERIFIED",
      recommendedSalesAction:
        "Research pipeline — CCR conferences inbox is an org route only; named lodging owner not verified. Continue buyer research before commercial pursuit.",
      salesActionKey: "MONITOR",
    },
    reason: "NO_VERIFIED_NAMED_BUYER",
  },
]);

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function loadHistoricalDoc({ root = ROOT } = {}) {
  const stagePath = path.join(
    researchStageHotelDir(BETHESDA_HOTEL_ID, { root }),
    "reconstructed-opportunities-v1.json",
  );
  if (fs.existsSync(stagePath)) {
    return { path: stagePath, exists: true, doc: JSON.parse(fs.readFileSync(stagePath, "utf8")) };
  }
  const fixturePath = path.join(root, HISTORICAL_FIXTURE_REL);
  if (fs.existsSync(fixturePath)) {
    return { path: fixturePath, exists: true, doc: JSON.parse(fs.readFileSync(fixturePath, "utf8")) };
  }
  return { path: null, exists: false, doc: null };
}

function mergeSurfaceFields(before, patchFields, classification) {
  const next = { ...before };
  for (const [k, v] of Object.entries(patchFields || {})) {
    next[k] = v;
  }
  const attached = attachCommercialSurfaceDisposition({
    ...next,
    commercialSurfaceDisposition:
      patchFields?.commercialSurfaceDisposition ||
      classification.surface ||
      next.commercialSurfaceDisposition,
  });
  next.commercialSurfaceDisposition = attached.commercialSurfaceDisposition;
  next.commercialSurfaceLabel = attached.commercialSurfaceLabel;
  next.primaryCommercialEligible = attached.primaryCommercialEligible;
  next.commercialSurfaceReason = attached.commercialSurfaceReason;
  next.commercialSurfaceReconciliation = {
    version: BETHESDA_88_COMMERCIAL_SURFACE_RECONCILIATION_V1,
    appliedAt: new Date().toISOString(),
    founderAuthorized: true,
  };
  return next;
}

/**
 * Build before/after surface reconciliation for all 88.
 */
export function buildCommercialSurfaceReconciliationManifest({ root = ROOT } = {}) {
  const store = loadHotelOpportunityDoc(BETHESDA_HOTEL_ID, { root });
  const union = mergeBethesdaInternalResearchOpportunities(
    BETHESDA_HOTEL_ID,
    store.opportunities,
    { root },
  );
  const beforeFamily = applyBethesdaCommercialEventFamilyPresentation(
    union.opportunities,
    { hideLinkedFromList: true },
  );
  const beforeListIds = (beforeFamily.opportunities || []).map((o) => o.id);
  const beforeSummary = summarizeCommercialSurfaceBatch(beforeFamily.opportunities);

  const patchById = new Map(SURFACE_FACTUAL_PATCHES.map((p) => [p.id, p]));
  const afterUnion = union.opportunities.map((o) => {
    const patch = patchById.get(o.id);
    const classification = classifyCommercialSurfaceDisposition(
      patch ? { ...o, ...patch.fields } : o,
    );
    if (patch) {
      return mergeSurfaceFields(o, patch.fields, classification);
    }
    return attachCommercialSurfaceDisposition(o);
  });
  const afterFamily = applyBethesdaCommercialEventFamilyPresentation(afterUnion, {
    hideLinkedFromList: true,
  });
  const commercialList = filterOpportunitiesByCommercialSurface(
    afterFamily.opportunities,
    GDI_LIST_SURFACE_VIEW.COMMERCIAL,
  );
  const afterSummary = summarizeCommercialSurfaceBatch(afterFamily.opportunities);
  const afterListIds = commercialList.map((o) => o.id);

  const removedFromCommercial = beforeListIds.filter((id) => !afterListIds.includes(id));
  const removedReasons = removedFromCommercial.map((id) => {
    const row = afterUnion.find((o) => o.id === id) || {};
    const c = classifyCommercialSurfaceDisposition(row);
    return { id, surface: c.surface, reason: c.reason, title: text(row.title).slice(0, 80) };
  });

  const fixtures = primaryCommercialForbiddenFixtures();
  const forbiddenStillVisible = [
    ...fixtures.marketIntelligenceIds,
    ...fixtures.disqualifiedIds,
  ].filter((id) => afterListIds.includes(id));

  // Duplicate families: at most one of each pair on commercial list
  const ifeOnList = fixtures.duplicateFamilyPrimaryOnly.ife.filter((id) =>
    afterListIds.includes(id),
  );
  const gbccOnList = fixtures.duplicateFamilyPrimaryOnly.gbcc.filter((id) =>
    afterListIds.includes(id),
  );

  return {
    version: BETHESDA_88_COMMERCIAL_SURFACE_RECONCILIATION_V1,
    contract: GDI_COMMERCIAL_SURFACE_DISPOSITION_V1,
    generatedAt: new Date().toISOString(),
    storeSha: store.sha256,
    unionCount: union.opportunities.length,
    before: {
      listCount: beforeListIds.length,
      listIds: beforeListIds,
      surfaceSummary: beforeSummary.counts,
    },
    after: {
      familyListCount: afterFamily.opportunities.length,
      commercialListCount: afterListIds.length,
      commercialListIds: afterListIds,
      surfaceSummary: afterSummary.counts,
      primaryCommercial: afterSummary.counts[COMMERCIAL_SURFACE.COMMERCIAL_OPPORTUNITY],
      researchPipeline: afterSummary.counts[COMMERCIAL_SURFACE.RESEARCH_PIPELINE],
      marketIntelligence: afterSummary.counts[COMMERCIAL_SURFACE.MARKET_INTELLIGENCE],
      archiveDisqualified: afterSummary.counts[COMMERCIAL_SURFACE.ARCHIVE_DISQUALIFIED],
      linkedDuplicate: afterSummary.counts[COMMERCIAL_SURFACE.LINKED_DUPLICATE],
      quarantined: afterSummary.counts[COMMERCIAL_SURFACE.QUARANTINED],
    },
    removedFromCommercial,
    removedReasons,
    forbiddenStillVisible,
    duplicateCheck: {
      ifeOnCommercialList: ifeOnList,
      gbccOnCommercialList: gbccOnList,
      ifeOk: ifeOnList.length <= 1,
      gbccOk: gbccOnList.length <= 1,
    },
    rows: afterSummary.rows,
    acceptance: {
      noForbiddenVisible: forbiddenStillVisible.length === 0,
      duplicatesCollapsed: ifeOnList.length <= 1 && gbccOnList.length <= 1,
      marriottExcluded: !afterListIds.includes(
        "gdi_opp_marriott_hq_adjacent_corporate_watch",
      ),
      rawIdsPreserved: union.opportunities.length === 88,
    },
  };
}

export function applyCommercialSurfaceReconciliationAtomic({
  root = ROOT,
  founderAuthorization = false,
  approvalToken = null,
  dryRun = true,
} = {}) {
  if (!founderAuthorization || approvalToken !== SURFACE_RECON_APPROVAL_TOKEN) {
    return {
      ok: false,
      code: "FOUNDER_AUTHORIZATION_REQUIRED",
      writeApplied: false,
      requiredToken: SURFACE_RECON_APPROVAL_TOKEN,
    };
  }
  const store = loadHotelOpportunityDoc(BETHESDA_HOTEL_ID, { root });
  if (!store.exists || store.count !== 59) {
    return { ok: false, code: "UNEXPECTED_CANONICAL_STORE", count: store.count };
  }
  if (!ACCEPTABLE_59_ROW_SHA256S.includes(store.sha256)) {
    return { ok: false, code: "UNEXPECTED_STORE_SHA", sha256: store.sha256 };
  }

  const manifest = buildCommercialSurfaceReconciliationManifest({ root });
  if (dryRun) {
    return {
      ok: true,
      dryRun: true,
      writeApplied: false,
      acceptance: manifest.acceptance,
      after: manifest.after,
      removedCount: manifest.removedFromCommercial.length,
    };
  }

  const backup = backupHotelOpportunityStore(BETHESDA_HOTEL_ID, {
    root,
    label: "pre-commercial-surface-reconciliation",
  });

  const patchById = new Map(SURFACE_FACTUAL_PATCHES.map((p) => [p.id, p]));
  const nextCanonical = store.opportunities.map((o) => {
    const patch = patchById.get(o.id);
    const classification = classifyCommercialSurfaceDisposition(
      patch ? { ...o, ...patch.fields } : o,
    );
    if (patch) return mergeSurfaceFields(o, patch.fields, classification);
    // Stamp disposition on every canonical row without unrelated field changes
    return mergeSurfaceFields(o, {}, classification);
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

  const classifiedAt = new Date().toISOString();
  writeJsonAtomic(store.path, {
    ...(store.doc || {}),
    opportunities: nextCanonical,
    updatedAt: classifiedAt,
    commercialSurfaceReconciliation: {
      version: BETHESDA_88_COMMERCIAL_SURFACE_RECONCILIATION_V1,
      appliedAt: classifiedAt,
      predecessorSha: store.sha256,
    },
  });

  // Historical 29 — stamp dispositions + apply patches when ID present
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
      const patch = patchById.get(id);
      const classification = classifyCommercialSurfaceDisposition(
        patch ? { ...o, ...patch.fields } : o,
      );
      historicalApplied += 1;
      return mergeSurfaceFields(o, patch?.fields || {}, classification);
    });
    const histDoc = Array.isArray(hist.doc)
      ? nextHist
      : {
          ...hist.doc,
          opportunities: nextHist,
          updatedAt: classifiedAt,
          commercialSurfaceReconciliation: {
            version: BETHESDA_88_COMMERCIAL_SURFACE_RECONCILIATION_V1,
            appliedAt: classifiedAt,
          },
        };
    writeJsonAtomic(hist.path, histDoc);
    const fixturePath = path.join(root, HISTORICAL_FIXTURE_REL);
    if (hist.path !== fixturePath && fs.existsSync(path.dirname(fixturePath))) {
      writeJsonAtomic(fixturePath, histDoc);
    }
  }

  return {
    ok: true,
    dryRun: false,
    writeApplied: true,
    backupPath: backup.backupPath,
    beforeSha256: store.sha256,
    afterSha256: sha256File(store.path),
    historicalApplied,
    acceptance: buildCommercialSurfaceReconciliationManifest({ root }).acceptance,
    predecessorAllowlistIncludesDemandType:
      store.sha256 === COMMERCIAL_DEMAND_TYPE_CLASSIFIED_59_ROW_SHA256,
  };
}

export function runBethesda88CommercialSurfaceReconciliation({
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
  });

  const manifest = buildCommercialSurfaceReconciliationManifest({ root });
  writeJsonAtomic(path.join(outDir, "01-surface-reconciliation-manifest.json"), manifest);
  writeJsonAtomic(path.join(outDir, "02-displayed-id-diff.json"), {
    beforeListIds: manifest.before.listIds,
    afterCommercialListIds: manifest.after.commercialListIds,
    removedFromCommercial: manifest.removedReasons,
  });

  let applyResult = { writeApplied: false, dryRun: true };
  if (apply) {
    applyResult = applyCommercialSurfaceReconciliationAtomic({
      root,
      founderAuthorization,
      approvalToken,
      dryRun: false,
    });
  } else {
    applyResult = applyCommercialSurfaceReconciliationAtomic({
      root,
      founderAuthorization: true,
      approvalToken: SURFACE_RECON_APPROVAL_TOKEN,
      dryRun: true,
    });
  }
  writeJsonAtomic(path.join(outDir, "03-apply-result.json"), applyResult);

  let verification = null;
  if (applyResult.writeApplied) {
    verification = buildCommercialSurfaceReconciliationManifest({ root });
    writeJsonAtomic(path.join(outDir, "04-post-apply-verification.json"), {
      acceptance: verification.acceptance,
      after: verification.after,
      forbiddenStillVisible: verification.forbiddenStillVisible,
    });
  }

  const summary = {
    version: BETHESDA_88_COMMERCIAL_SURFACE_RECONCILIATION_V1,
    ok:
      applyResult.ok !== false &&
      (apply ? applyResult.writeApplied === true : true) &&
      manifest.acceptance.noForbiddenVisible &&
      manifest.acceptance.duplicatesCollapsed &&
      manifest.acceptance.marriottExcluded,
    rootCause:
      "Bethesda API skipped filterSalespersonView and had no primary-commercial surface gate; research integrity ≠ commercial list eligibility.",
    answers: {
      whyMarriottPassed:
        "CATEGORY_WATCH_NO_SINGLE_BUYER was treated as contact-exempt honesty, not list exclusion; WATCHLIST + sales-action resolver still projected a pursuit-style pill on the default All list.",
      whyDqRemainedVisible:
        "Bethesda internal-union branch deliberately bypassed filterSalespersonView so DISQUALIFIED rows stayed in the 83-card family-collapsed payload.",
      otherSameDefect:
        "All DISQUALIFIED / destination-outside rows, market-intelligence themes, and uncollapsed IFE/GBCC duplicates.",
      trueCommercialAfter: manifest.after.primaryCommercial,
      researchPipeline: manifest.after.researchPipeline,
      marketIntelligence: manifest.after.marketIntelligence,
      excludedDisqualified: manifest.after.archiveDisqualified,
    },
    beforeListCount: manifest.before.listCount,
    afterCommercialCount: manifest.after.commercialListCount,
    removedCount: manifest.removedFromCommercial.length,
    writeApplied: applyResult.writeApplied === true,
    storeShaBefore: store.sha256,
    storeShaAfter: applyResult.afterSha256 || store.sha256,
    paidAuthorized: false,
    publicationAuthorized: false,
    reportDir: REPORT_REL,
    acceptance: verification?.acceptance || manifest.acceptance,
    code: applyResult.code || null,
  };
  writeJsonAtomic(path.join(outDir, "00-surface-reconciliation-summary.json"), summary);
  return summary;
}
