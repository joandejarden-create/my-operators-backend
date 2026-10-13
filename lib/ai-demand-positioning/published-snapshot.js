/**
 * AI Demand Positioning — Published snapshot storage (pre-computed UI payloads).
 * Raw observation corpora stay internal; production serves published snapshots only.
 */

import { readFileSync, writeFileSync, existsSync, mkdirSync, readdirSync, renameSync, unlinkSync } from "fs";
import { join, dirname } from "path";
import { buildOwnerPayload, ADP_PRODUCT_VERSION } from "./customer/owner-payload.js";
import { buildEvidenceIndex } from "./customer/evidence-index.js";
import { buildScenarioUniverse } from "./prompt-universe/scenario-registry.js";
import { loadAllPeriods, loadPropertyProfile } from "./data-model.js";
import { filterCustomerTrendPeriods } from "./period-eligibility-v1.js";
import { publishExistingHotelAdp } from "./measurement-assurance/claims-anomalies-publish-guard.js";
import { persistCompetitiveHistoryAfterCertification } from "./pipeline/existing-hotel-period-pipeline-v1.js";
import { projectPeriodForV11CoreMetrics } from "./measurement-assurance/v1-1-core-eligibility.js";
import { MEASUREMENT_CONTRACT_V1_1 } from "./measurement-assurance/adp-measurement-contract-v1-1-candidate.js";
import {
  buildTrendsFromCanonicalResolution,
  resolveCanonicalComparablePeriods,
} from "./metrics/canonical-comparable-period-resolver-v1.js";
import { enqueueAiDemandReviewAfterPublication } from "./publication-triggers-ai-demand-review-v1.js";
import { autoArchiveAdpPublishedSnapshot } from "../dealality-report-archive/auto-archive-published-report-v1.js";

const PUBLISHED_DIR = join(process.cwd(), "data/ai-demand-positioning/published");
const PUBLISHED_SEED_DIR = join(process.cwd(), "fixtures/ai-demand-positioning/published");

function propertyDir(propertyId, useSeed = false) {
  return join(useSeed ? PUBLISHED_SEED_DIR : PUBLISHED_DIR, propertyId);
}

function manifestPath(propertyId, useSeed = false) {
  return join(propertyDir(propertyId, useSeed), "manifest.json");
}

function reportPath(propertyId, periodId, useSeed = false) {
  return join(propertyDir(propertyId, useSeed), `report-${periodId}.json`);
}

function evidencePath(propertyId, periodId, useSeed = false) {
  return join(propertyDir(propertyId, useSeed), `evidence-${periodId}.json`);
}

function readJson(path) {
  if (!existsSync(path)) return null;
  return JSON.parse(readFileSync(path, "utf-8"));
}

function writeJson(path, data) {
  mkdirSync(dirname(path), { recursive: true });
  const payload = JSON.stringify(data, null, 2);
  const tmp = `${path}.${process.pid}.tmp`;
  writeFileSync(tmp, payload);
  try {
    renameSync(tmp, path);
  } catch {
    let lastErr = null;
    for (let i = 0; i < 8; i++) {
      try {
        writeFileSync(path, payload);
        lastErr = null;
        break;
      } catch (err) {
        lastErr = err;
        const until = Date.now() + 250 * (i + 1);
        while (Date.now() < until) {
          /* spin */
        }
      }
    }
    try {
      unlinkSync(tmp);
    } catch {
      /* best-effort */
    }
    if (lastErr) throw lastErr;
  }
}

function buildTrends(propertyId, scenarios, periods, { measurementContractVersion = null, currentPeriodId = null } = {}) {
  const profile = loadPropertyProfile(propertyId);
  const resolution = resolveCanonicalComparablePeriods({
    allPeriods: periods,
    scenarios,
    currentPeriodId,
  });
  if (!resolution.periodCount) return undefined;
  const useV11 =
    measurementContractVersion === MEASUREMENT_CONTRACT_V1_1 ||
    measurementContractVersion === "ADP_MEASUREMENT_CONTRACT_V1_1" ||
    (resolution.comparablePeriods || []).some(
      (p) =>
        p?.measurementContractVersionActiveForCorrection === MEASUREMENT_CONTRACT_V1_1 ||
        p?.measurementContractVersion === MEASUREMENT_CONTRACT_V1_1
    );
  return buildTrendsFromCanonicalResolution({
    resolution,
    scenarios,
    propertyProfile: profile,
    measurementContractVersion: useV11 ? MEASUREMENT_CONTRACT_V1_1 : measurementContractVersion,
  });
}

export function buildPublishedSnapshotBundle({
  period,
  profile,
  censusRecordId = null,
  measurementContractVersion = null,
  scenarios: scenariosOverride = null,
}) {
  const scenarios =
    Array.isArray(scenariosOverride) && scenariosOverride.length
      ? scenariosOverride
      : buildScenarioUniverse(profile);
  const useV11 =
    measurementContractVersion === MEASUREMENT_CONTRACT_V1_1 ||
    measurementContractVersion === "ADP_MEASUREMENT_CONTRACT_V1_1" ||
    period?.measurementContractVersionActiveForCorrection === MEASUREMENT_CONTRACT_V1_1;

  const periodForMetrics = useV11 ? projectPeriodForV11CoreMetrics(period) : period;
  const payload = buildOwnerPayload(periodForMetrics, scenarios, profile);
  if (!payload.ok) {
    return { ok: false, error: payload.error, message: payload.message };
  }
  if (useV11) {
    payload.measurementContractVersion = MEASUREMENT_CONTRACT_V1_1;
    payload.correction = period.correction || payload.correction || null;
    payload.reportVersionLabel = "CORRECTED_V1_1";
  }

  const allPeriods = loadAllPeriods(profile.propertyId);
  const trends = buildTrends(profile.propertyId, scenarios, allPeriods, {
    measurementContractVersion: useV11 ? MEASUREMENT_CONTRACT_V1_1 : null,
    currentPeriodId: period.periodId,
  });
  if (trends) payload.trends = trends;

  // Bake Prior Run deltas from the same comparable lineage as Trends (lean deploys).
  try {
    const withPrior = buildOwnerPayload(periodForMetrics, scenarios, profile, {
      allPeriods: filterCustomerTrendPeriods(allPeriods),
    });
    if (withPrior?.executiveMetrics?.currentVsPrior) {
      payload.executiveMetrics = {
        ...(payload.executiveMetrics || {}),
        ...withPrior.executiveMetrics,
        currentVsPrior: withPrior.executiveMetrics.currentVsPrior,
      };
    }
  } catch (err) {
    console.error("[ADP publish] currentVsPrior bake skipped:", err.message);
  }

  // Evidence must use the same measurement version as KPI surfaces
  const evidenceIndex = buildEvidenceIndex(periodForMetrics, scenarios, profile);

  const publishedAt = new Date().toISOString();
  const report = {
    ok: true,
    publishedAt,
    productVersion: ADP_PRODUCT_VERSION,
    propertyId: profile.propertyId,
    periodId: period.periodId,
    censusRecordId: censusRecordId || null,
    measurementContractVersion: useV11 ? MEASUREMENT_CONTRACT_V1_1 : "ADP_MEASUREMENT_CONTRACT_V1",
    reportEdition: useV11 ? "CORRECTED_V1_1" : "ORIGINAL_V1",
    payload,
  };

  const manifest = {
    propertyId: profile.propertyId,
    propertyName: profile.name,
    city: profile.city,
    state: profile.state,
    market: profile.market || null,
    latestPeriodId: period.periodId,
    latestPublishedAt: publishedAt,
    productVersion: ADP_PRODUCT_VERSION,
    censusRecordId: censusRecordId || null,
    publishStatus: "Live",
    reportFile: `report-${period.periodId}.json`,
    evidenceFile: `evidence-${period.periodId}.json`,
    demandCaptureRate: payload.demandCapture?.overallRate ?? null,
    providerCount: payload.period?.providerCount ?? null,
    measurementContractVersion: useV11 ? MEASUREMENT_CONTRACT_V1_1 : "ADP_MEASUREMENT_CONTRACT_V1",
    reportEdition: useV11 ? "CORRECTED_V1_1" : "ORIGINAL_V1",
  };

  return {
    ok: true,
    manifest,
    report,
    evidenceIndex,
    summary: {
      propertyId: profile.propertyId,
      periodId: period.periodId,
      demandCapture: payload.demandCapture?.display || null,
      payloadBytes: JSON.stringify(payload).length,
      evidenceBytes: JSON.stringify(evidenceIndex).length,
    },
  };
}

/**
 * Persist a published snapshot bundle.
 *
 * Official customer publication requires CERTIFIED (or CERTIFIED_WITH_DISCLOSURES)
 * when enforceCertification is active. Escape hatch: allowUncertifiedPublish=true
 * (legacy migration / internal research only — never for new official baselines).
 */
export function savePublishedSnapshotBundle(bundle, options = {}) {
  const seed = options.seed === true;
  if (!bundle?.ok) {
    throw new Error(bundle?.message || "Cannot save invalid published snapshot bundle");
  }

  const status =
    bundle.certificationRecord?.certificationStatus ||
    bundle.certificationRecord?.status ||
    bundle.manifest?.certificationStatus ||
    null;

  const enforce =
    options.requireCertification === true ||
    options.officialCustomerPublish === true ||
    process.env.ADP_REQUIRE_CERTIFICATION_BEFORE_PUBLISH === "1";

  const allowedOfficial =
    status === "CERTIFIED" || status === "CERTIFIED_WITH_DISCLOSURES";

  if (enforce && options.allowUncertifiedPublish !== true && !allowedOfficial) {
    const err = new Error(
      `ADP_CERTIFICATION_REQUIRED: cannot publish official customer snapshot with certificationStatus=${status || "MISSING"}`
    );
    err.code = "ADP_CERTIFICATION_REQUIRED";
    err.certificationStatus = status;
    throw err;
  }

  const dir = propertyDir(bundle.manifest.propertyId, seed);
  mkdirSync(dir, { recursive: true });

  const reportFile = reportPath(bundle.manifest.propertyId, bundle.manifest.latestPeriodId, seed);
  const evidenceFile = evidencePath(bundle.manifest.propertyId, bundle.manifest.latestPeriodId, seed);
  const manifestFile = manifestPath(bundle.manifest.propertyId, seed);

  // Attach certification metadata when provided on bundle
  const report = bundle.certificationRecord
    ? { ...bundle.report, _certification: bundle.certificationRecord }
    : bundle.report;
  const manifest = bundle.certificationRecord
    ? {
        ...bundle.manifest,
        certificationStatus: bundle.certificationRecord.certificationStatus,
        assuranceVersion: bundle.certificationRecord.assuranceVersion,
        certificationTimestamp: bundle.certificationRecord.certificationTimestamp,
        globalCertificationEngineVersion:
          bundle.certificationRecord.engineVersion ||
          bundle.certificationRecord.assuranceVersion ||
          null,
      }
    : bundle.manifest;

  writeJson(reportFile, report);
  writeJson(evidenceFile, bundle.evidenceIndex);
  writeJson(manifestFile, manifest);

  return {
    ok: true,
    dir,
    manifestFile,
    reportFile,
    evidenceFile,
    seed,
    certificationStatus: status,
  };
}

/**
 * Hard publish path for Existing Hotel ADP.
 * Refuses unless certificationStatus === CERTIFIED (or governed disclosures / emergency override).
 * When certificationRecord is omitted, runs the global certifyAdpPeriod engine first.
 */
export async function publishExistingHotelAdpSnapshot(bundle, certificationRecord, options = {}) {
  let record = certificationRecord;
  if (!record && options.skipAutoCertify !== true) {
    const { certifyAdpPeriod } = await import("./certification/certify-adp-period-v1.js");
    const propertyId = bundle?.manifest?.propertyId;
    const periodId = bundle?.manifest?.latestPeriodId;
    const cert = await certifyAdpPeriod(
      { propertyId, periodId },
      {
        trigger: "publishExistingHotelAdpSnapshot",
        writeManifest: true,
        forceOfficialCertification: true,
      }
    );
    record = {
      certificationStatus: cert.publishCertificationStatus || cert.status,
      status: cert.status,
      assuranceVersion: cert.assuranceVersion || cert.engineVersion,
      certificationTimestamp: cert.certificationTimestamp,
      engineVersion: cert.engineVersion,
      hardFailures: cert.hardFailures,
      reviewFlags: cert.reviewFlags,
      manifest: cert.manifest,
    };
  }

  const gate = publishExistingHotelAdp(bundle, record, options);
  if (!gate.ok) {
    return gate;
  }
  const enriched = {
    ...bundle,
    certificationRecord: {
      ...record,
      publishGuard: gate.reason || "CERTIFIED",
      override: gate.override || null,
    },
  };
  const saved = savePublishedSnapshotBundle(enriched, {
    seed: options.seed === true,
    officialCustomerPublish: true,
    allowUncertifiedPublish: options.allowUncertifiedPublish === true,
  });

  let competitiveHistory = null;
  if (options.skipCompetitiveHistory !== true) {
    try {
      const propertyId = bundle.manifest.propertyId;
      const periodId = bundle.manifest.latestPeriodId;
      const profile = loadPropertyProfile(propertyId);
      const period = (loadAllPeriods(propertyId) || []).find((p) => p.periodId === periodId);
      if (profile && period) {
        const scenarios = buildScenarioUniverse(profile);
        competitiveHistory = persistCompetitiveHistoryAfterCertification({
          period,
          scenarios,
          propertyProfile: profile,
          certificationStatus: certificationRecord.certificationStatus,
          write: true,
        });
      }
    } catch (err) {
      console.error("[ADP publish] competitive history persist skipped:", err.message);
      competitiveHistory = { ok: false, reason: err.message };
    }
  }

  const propertyId = bundle.manifest.propertyId;
  const publishedReport =
    (bundle.report && typeof bundle.report === "object" && bundle.report.payload) ||
    bundle.report ||
    null;
  let reportArchive = null;
  try {
    reportArchive = autoArchiveAdpPublishedSnapshot({
      propertyId,
      hotelId:
        publishedReport?.property?.censusRecordId ||
        bundle.manifest?.censusRecordId ||
        null,
      hotelName: publishedReport?.property?.name || null,
      report: publishedReport,
      manifest: bundle.manifest,
      reportClass:
        String(publishedReport?.period?.status || "").toUpperCase() || "CURRENT",
    });
  } catch (err) {
    console.error("[ADP publish] report archive skipped:", err?.message || err);
    reportArchive = { ok: false, reason: String(err?.message || err) };
  }

  // Shared post-certification dropdown registration (listPropertyProfiles filter).
  let dropdownRegistration = null;
  try {
    const { registerCertifiedAdpPropertyForCustomerDropdown } = await import(
      "./client-readiness/register-certified-adp-dropdown-v1.js"
    );
    dropdownRegistration = registerCertifiedAdpPropertyForCustomerDropdown(propertyId, {
      certificationStatus: certificationRecord?.certificationStatus || record?.certificationStatus,
    });
  } catch (err) {
    console.error("[ADP publish] dropdown registration skipped:", err?.message || err);
    dropdownRegistration = { ok: false, reason: String(err?.message || err) };
  }

  return {
    ok: true,
    ...saved,
    certificationStatus: certificationRecord.certificationStatus,
    competitiveHistory,
    reportArchive,
    dropdownRegistration,
    aiDemandReview: enqueueAiDemandReviewAfterPublication(propertyId, {
      generatedBy: "publishExistingHotelAdpSnapshot",
    }),
  };
}

function loadManifest(propertyId) {
  const runtime = readJson(manifestPath(propertyId, false));
  if (runtime) return { manifest: runtime, seed: false };
  const seeded = readJson(manifestPath(propertyId, true));
  if (seeded) return { manifest: seeded, seed: true };
  return null;
}

export function loadPublishedReport(propertyId) {
  const loaded = loadManifest(propertyId);
  if (!loaded) return null;

  const { manifest, seed } = loaded;
  const reportFile = join(propertyDir(propertyId, seed), manifest.reportFile || `report-${manifest.latestPeriodId}.json`);
  const report = readJson(reportFile);
  if (!report) return null;
  // Nested wrapper (Bethesda / Cambridge style) OR flat published report (CALA style).
  if (report.payload && typeof report.payload === "object") return report.payload;
  if (report.executiveRead || report.demandCapture || report.intentPresenceIndex || report.property) {
    return report;
  }
  return null;
}

export function loadPublishedEvidenceIndex(propertyId, periodId = null) {
  const loaded = loadManifest(propertyId);
  if (!loaded) return null;

  const { manifest, seed } = loaded;
  const targetPeriodId = periodId || manifest.latestPeriodId;
  const evidenceFile = join(
    propertyDir(propertyId, seed),
    manifest.evidenceFile || `evidence-${targetPeriodId}.json`
  );
  return readJson(evidenceFile);
}

export function listPublishedPropertyIds() {
  const ids = new Set();
  for (const root of [PUBLISHED_DIR, PUBLISHED_SEED_DIR]) {
    if (!existsSync(root)) continue;
    for (const entry of readdirSync(root, { withFileTypes: true })) {
      if (entry.isDirectory()) ids.add(entry.name);
    }
  }
  return [...ids];
}

export function loadPublishedManifest(propertyId) {
  return loadManifest(propertyId)?.manifest || null;
}
