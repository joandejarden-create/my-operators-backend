/**
 * AI Demand Positioning — Published snapshot storage (pre-computed UI payloads).
 * Raw observation corpora stay internal; production serves published snapshots only.
 */

import { readFileSync, writeFileSync, existsSync, mkdirSync, readdirSync, renameSync, unlinkSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
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
import {
  assertPublishedFsMatchesRegistry,
  recordPublishedCurrent,
  sha256File,
} from "./published-current-registry-v1.js";
import { archivePublishedPdfOnPublish } from "./full-adp-e2e-pdf/adp-published-pdf-history-v1.js";
import { resolveFullAdpE2ePdfArtifact } from "./full-adp-e2e-pdf/adp-full-e2e-pdf-artifact-store-v1.js";
import {
  isAdpDurablePublishedStoreEnabled,
  shouldReadFromDurableStore,
  shouldWriteToDurableStore,
  allowEphemeralPublishedFallback,
  resolveAdpDurablePublishedMode,
} from "./durable-publication/resolve-durable-root-v1.js";
import { createFilesystemDurablePublishedStore } from "./durable-publication/filesystem-durable-store-v1.js";
import { commitDurableAdpPublication } from "./durable-publication/publication-transaction-v1.js";
import { bumpAdminPublicationCoverageGeneration } from "./monthly-review/admin/admin-publication-coverage-generation-v1.js";
import { invalidateLiveVerifyForProperty } from "./share/adp-external-share-live-verify-ledger-v1.js";

const REPO_ROOT = join(dirname(fileURLToPath(import.meta.url)), "../..");
const PUBLISHED_DIR = join(REPO_ROOT, "data/ai-demand-positioning/published");
/** Real-hotel seed manifests only — never store synthetic IDs here. */
const PUBLISHED_SEED_DIR = join(REPO_ROOT, "fixtures/ai-demand-positioning/published");
/**
 * Isolated synthetic / smoke-test published seeds (e.g. adp_smoke_test_synthetic).
 * Not scanned by operational listPublishedPropertyIds() unless includeSyntheticFixtures.
 */
const PUBLISHED_TEST_ONLY_SEED_DIR = join(
  REPO_ROOT,
  "fixtures/ai-demand-positioning/test-only/published"
);

export function getAdpPublishedTestOnlySeedDir() {
  return PUBLISHED_TEST_ONLY_SEED_DIR;
}

/**
 * Inline synthetic detector — mirrors adp-operational-registry-hygiene-v1
 * without importing it (avoids circular init with data-model / published paths).
 * Keep patterns in sync with isAdpSyntheticOrTestPropertyId.
 */
function isSyntheticPublishedPropertyId(propertyId) {
  const s = String(propertyId || "").trim();
  if (!s) return false;
  if (s === "adp_smoke_test_synthetic") return true;
  if (/_smoke_test_/i.test(s)) return true;
  if (/_synthetic$/i.test(s)) return true;
  if (/^adp_test_/i.test(s)) return true;
  // Durable-storage / unit-test leftovers (e.g. adp_durable_wire_hotel).
  if (/^adp_durable_/i.test(s)) return true;
  if (/_fixture_/i.test(s) || /_fixture$/i.test(s)) return true;
  if (/_wire_hotel$/i.test(s)) return true;
  return false;
}

function propertyDir(propertyId, useSeed = false) {
  if (!useSeed) return join(PUBLISHED_DIR, propertyId);
  // Synthetic smoke fixtures write/load only under test-only — never operational seed.
  if (isSyntheticPublishedPropertyId(propertyId)) {
    return join(PUBLISHED_TEST_ONLY_SEED_DIR, propertyId);
  }
  return join(PUBLISHED_SEED_DIR, propertyId);
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

  // Prefer durable store when cutover mode allows writes (warm|prefer|required).
  let durableTxn = null;
  if (
    seed !== true &&
    options.skipDurableStore !== true &&
    shouldWriteToDurableStore(options.env || process.env)
  ) {
    durableTxn = commitDurableAdpPublication({
      propertyId: manifest.propertyId,
      periodId: manifest.latestPeriodId,
      report,
      evidence: bundle.evidenceIndex,
      manifest,
      sourceStagedVersionId: manifest.sourceStagedVersionId || options.sourceStagedVersionId || null,
      skipStagedValidation: options.skipStagedValidation === true,
      requireStagedVersionId: options.requireStagedVersionId === true,
      provenance: options.registryProvenance || "savePublishedSnapshotBundle",
      env: options.env || process.env,
      root: options.durableRoot,
      store: options.durableStore,
    });
    if (!durableTxn.ok) {
      const err = new Error(durableTxn.message || "Durable publication transaction failed");
      err.code = durableTxn.code || "ADP_DURABLE_PUBLICATION_FAILED";
      err.durableTxn = durableTxn;
      throw err;
    }
    // Optional mirror into ephemeral published/ for local tooling — never the SoT.
    if (options.mirrorToEphemeralPublished !== false) {
      try {
        writeJson(reportFile, report);
        writeJson(evidenceFile, bundle.evidenceIndex);
        writeJson(manifestFile, manifest);
      } catch (err) {
        console.error("[ADP publish] ephemeral mirror skipped:", err?.message || err);
      }
    }

    // Compatibility: keep static deploy-pack registry aligned with durable current.
    // Stale static must never become a competing authority under prefer|required, but
    // syncing prevents Admin/ephemeral paths from seeing lagging deploy-pack pointers.
    let registryWrite = {
      ok: true,
      durable: true,
      current: durableTxn.current,
      staticSync: null,
    };
    if (
      options.skipPublishedCurrentRegistry !== true &&
      options.skipStaticRegistrySync !== true
    ) {
      try {
        const current = durableTxn.current || {};
        registryWrite.staticSync = recordPublishedCurrent(
          {
            propertyId: manifest.propertyId,
            periodId: manifest.latestPeriodId,
            reportFile: current.reportFile || `report-${manifest.latestPeriodId}.json`,
            evidenceFile:
              current.evidenceFile || `evidence-${manifest.latestPeriodId}.json`,
            reportSha256: current.reportSha256 || null,
            evidenceSha256: current.evidenceSha256 ?? null,
            reportBytes: current.reportBytes ?? null,
            latestPublishedAt:
              current.latestPublishedAt || manifest.latestPublishedAt || null,
            sourceStagedVersionId:
              current.sourceStagedVersionId ||
              manifest.sourceStagedVersionId ||
              null,
            considerationRate: current.considerationRate ?? null,
            publishStatus: current.publishStatus || manifest.publishStatus || "Live",
            provenance:
              options.registryProvenance ||
              "savePublishedSnapshotBundle_static_sync_after_durable",
          },
          options.env || process.env
        );
      } catch (err) {
        console.error(
          "[ADP publish] static deploy-pack registry sync after durable failed:",
          err?.message || err
        );
        registryWrite.staticSync = {
          ok: false,
          reason: String(err?.message || err),
        };
        if (options.requirePublishedCurrentRegistry === true) {
          throw err;
        }
      }
    }

    // Fail closed: publication identity change invalidates prior Verified Customer Access.
    try {
      invalidateLiveVerifyForProperty(
        manifest.propertyId,
        `durable_publish:${manifest.latestPeriodId}`,
        options.env || process.env
      );
    } catch (err) {
      console.error(
        "[ADP publish] live-verify ledger invalidate skipped:",
        err?.message || err
      );
    }

    return {
      ok: true,
      dir: durableTxn.durableRoot,
      manifestFile: null,
      reportFile: null,
      evidenceFile: null,
      seed,
      certificationStatus: status,
      durableTxn,
      registryWrite,
    };
  }

  writeJson(reportFile, report);
  writeJson(evidenceFile, bundle.evidenceIndex);
  writeJson(manifestFile, manifest);

  // Legacy registry pointer (deploy-pack coupled) — used only when durable store is off.
  let registryWrite = null;
  if (seed !== true && options.skipPublishedCurrentRegistry !== true) {
    try {
      const cons =
        report?.payload?.executiveMetrics?.considerationRate?.rate ??
        report?.executiveMetrics?.considerationRate?.rate ??
        manifest?.considerationRate ??
        null;
      registryWrite = recordPublishedCurrent({
        propertyId: manifest.propertyId,
        periodId: manifest.latestPeriodId,
        reportFile: `report-${manifest.latestPeriodId}.json`,
        evidenceFile: `evidence-${manifest.latestPeriodId}.json`,
        reportSha256: sha256File(reportFile),
        evidenceSha256: existsSync(evidenceFile) ? sha256File(evidenceFile) : null,
        reportBytes: readFileSync(reportFile).length,
        latestPublishedAt: manifest.latestPublishedAt || null,
        sourceStagedVersionId: manifest.sourceStagedVersionId || null,
        considerationRate: cons,
        publishStatus: manifest.publishStatus || "Live",
        provenance: options.registryProvenance || "savePublishedSnapshotBundle",
      });
    } catch (err) {
      console.error(
        "[ADP publish] published-current registry update failed:",
        err?.message || err
      );
      registryWrite = { ok: false, reason: String(err?.message || err) };
      if (options.requirePublishedCurrentRegistry === true) {
        throw err;
      }
    }
  }

  let coverageGeneration = null;
  try {
    coverageGeneration = bumpAdminPublicationCoverageGeneration({
      propertyId: manifest.propertyId,
      periodId: manifest.latestPeriodId,
      reason: "savePublishedSnapshotBundle",
    });
  } catch (err) {
    console.error(
      "[ADP publish] admin coverage generation bump failed:",
      err?.message || err
    );
    coverageGeneration = { ok: false, reason: String(err?.message || err) };
  }

  return {
    ok: true,
    dir,
    manifestFile,
    reportFile,
    evidenceFile,
    seed,
    certificationStatus: status,
    registryWrite,
    durableTxn: null,
    coverageGeneration,
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
          certificationStatus:
            record?.certificationStatus ||
            certificationRecord?.certificationStatus ||
            null,
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

  // Published PDF History: demote prior Current → Historical; archive immutable PDF bytes
  let publishedPdfHistory = null;
  try {
    const periodId = bundle.manifest?.latestPeriodId || null;
    const stagedVersionId =
      bundle.manifest?.sourceStagedVersionId ||
      options.sourceStagedVersionId ||
      null;
    const art = resolveFullAdpE2ePdfArtifact(propertyId, {
      expectedStagedVersionId: stagedVersionId || undefined,
    });
    const pdfSha =
      art?.pdfSha256 ||
      options.pdfSha256 ||
      null;
    if (periodId && pdfSha) {
      publishedPdfHistory = archivePublishedPdfOnPublish({
        propertyId,
        periodId,
        stagedVersionId: art?.stagedVersionId || stagedVersionId,
        pdfSha256: pdfSha,
        pdfSourcePath: art?.pdfAbsolutePath || null,
        publishedAt: new Date().toISOString(),
        authorizationRecord: {
          actor: options.actor || "publishExistingHotelAdpSnapshot",
          certificationStatus:
            record?.certificationStatus ||
            certificationRecord?.certificationStatus ||
            null,
        },
        dryRun: options.dryRun === true || options.skipPublishedPdfHistory === true,
      });
    } else {
      publishedPdfHistory = {
        ok: false,
        reason: "full_adp_pdf_or_period_missing_for_history",
        periodId,
        pdfSha256: pdfSha,
      };
    }
  } catch (err) {
    console.error(
      "[ADP publish] published PDF history archive skipped:",
      err?.message || err
    );
    publishedPdfHistory = { ok: false, reason: String(err?.message || err) };
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
    certificationStatus:
      record?.certificationStatus ||
      certificationRecord?.certificationStatus ||
      saved?.certificationStatus ||
      null,
    competitiveHistory,
    reportArchive,
    publishedPdfHistory,
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

function loadFromDurableStore(propertyId, env = process.env) {
  if (!shouldReadFromDurableStore(env)) return null;
  const store = createFilesystemDurablePublishedStore({ env });
  return store;
}

export function loadPublishedReport(propertyId) {
  const env = process.env;
  const durable = loadFromDurableStore(propertyId, env);
  if (durable) {
    const entry = durable.getCurrentEntry(propertyId);
    if (entry) {
      const resolved = durable.readPublishedReport(propertyId);
      if (!resolved) {
        if (!allowEphemeralPublishedFallback(env)) return null;
        // fall through to ephemeral
      } else {
        const report = resolved.report;
        if (report?.payload && typeof report.payload === "object") return report.payload;
        if (
          report?.executiveRead ||
          report?.demandCapture ||
          report?.intentPresenceIndex ||
          report?.property
        ) {
          return report;
        }
        return resolved.payload || null;
      }
    } else if (!allowEphemeralPublishedFallback(env)) {
      return null;
    }
  }

  const loaded = loadManifest(propertyId);
  if (!loaded) {
    // Registry says published but FS missing → explicit integrity failure (no silent null
    // that Admin/share layers reclassify as "not published" / older fallback).
    assertPublishedFsMatchesRegistry({ propertyId, manifest: null, reportFilePath: null });
    return null;
  }

  const { manifest, seed } = loaded;
  const reportFile = join(propertyDir(propertyId, seed), manifest.reportFile || `report-${manifest.latestPeriodId}.json`);
  assertPublishedFsMatchesRegistry({
    propertyId,
    manifest,
    reportFilePath: reportFile,
    evidenceFilePath: join(
      propertyDir(propertyId, seed),
      manifest.evidenceFile || `evidence-${manifest.latestPeriodId}.json`
    ),
  });
  const report = readJson(reportFile);
  if (!report) {
    assertPublishedFsMatchesRegistry({
      propertyId,
      manifest,
      reportFilePath: reportFile,
    });
    return null;
  }
  // Nested wrapper (Bethesda / Cambridge style) OR flat published report (CALA style).
  if (report.payload && typeof report.payload === "object") return report.payload;
  if (report.executiveRead || report.demandCapture || report.intentPresenceIndex || report.property) {
    return report;
  }
  return null;
}

export function loadPublishedEvidenceIndex(propertyId, periodId = null) {
  const env = process.env;
  const durable = loadFromDurableStore(propertyId, env);
  if (durable) {
    const entry = durable.getCurrentEntry(propertyId);
    if (entry) {
      const targetPeriodId = periodId || entry.periodId;
      const ePath = durable.evidencePath(propertyId, targetPeriodId);
      if (existsSync(ePath)) return JSON.parse(readFileSync(ePath, "utf8"));
      if (!allowEphemeralPublishedFallback(env)) return null;
    } else if (!allowEphemeralPublishedFallback(env)) {
      return null;
    }
  }

  const loaded = loadManifest(propertyId);
  if (!loaded) return null;

  const { manifest, seed } = loaded;
  const targetPeriodId = periodId || manifest.latestPeriodId;
  const evidenceFile = join(
    propertyDir(propertyId, seed),
    manifest.evidenceFile || `evidence-${targetPeriodId}.json`
  );
  if (!periodId || periodId === manifest.latestPeriodId) {
    assertPublishedFsMatchesRegistry({
      propertyId,
      manifest,
      reportFilePath: join(
        propertyDir(propertyId, seed),
        manifest.reportFile || `report-${manifest.latestPeriodId}.json`
      ),
      evidenceFilePath: evidenceFile,
    });
  }
  return readJson(evidenceFile);
}

/**
 * List published ADP property IDs (runtime data/ + optional seed fixtures).
 *
 * Operational default:
 *   - Scans `data/.../published` + `fixtures/.../published` (real hotels only)
 *   - Never scans `fixtures/.../test-only/published`
 *   - Always strips synthetic IDs even if they were wrongly placed in operational dirs
 *
 * Pass `{ includeSyntheticFixtures: true }` for isolated publish-guard / unit tests only
 * (also scans the test-only published seed directory).
 *
 * @param {{ includeSyntheticFixtures?: boolean, includeSeedFixtures?: boolean }} [opts]
 * @returns {string[]}
 */
export function listPublishedPropertyIds(opts = {}) {
  const includeSynthetic = opts.includeSyntheticFixtures === true;
  const includeSeed = opts.includeSeedFixtures !== false;
  const ids = new Set();

  const env = opts.env || process.env;
  if (shouldReadFromDurableStore(env) || shouldWriteToDurableStore(env)) {
    try {
      const store = createFilesystemDurablePublishedStore({ env });
      for (const row of store.readRegistry().properties || []) {
        if (row?.propertyId) ids.add(row.propertyId);
      }
    } catch (err) {
      console.error("[ADP] durable listPublishedPropertyIds failed:", err?.message || err);
    }
  }

  const roots = [PUBLISHED_DIR];
  if (includeSeed) roots.push(PUBLISHED_SEED_DIR);
  // Test-only synthetic seeds are never part of operational discovery.
  if (includeSynthetic) roots.push(PUBLISHED_TEST_ONLY_SEED_DIR);
  for (const root of roots) {
    if (!existsSync(root)) continue;
    for (const entry of readdirSync(root, { withFileTypes: true })) {
      if (entry.isDirectory()) ids.add(entry.name);
    }
  }
  if (includeSynthetic) return [...ids];
  return [...ids].filter((id) => !isSyntheticPublishedPropertyId(id));
}

export function loadPublishedManifest(propertyId) {
  const env = process.env;
  const durable = loadFromDurableStore(propertyId, env);
  if (durable) {
    const entry = durable.getCurrentEntry(propertyId);
    if (entry) return durable.readPublishedManifest(propertyId);
    if (!allowEphemeralPublishedFallback(env)) return null;
  }

  const loaded = loadManifest(propertyId);
  if (!loaded) {
    assertPublishedFsMatchesRegistry({ propertyId, manifest: null, reportFilePath: null });
    return null;
  }
  const { manifest, seed } = loaded;
  assertPublishedFsMatchesRegistry({
    propertyId,
    manifest,
    reportFilePath: join(
      propertyDir(propertyId, seed),
      manifest.reportFile || `report-${manifest.latestPeriodId}.json`
    ),
  });
  return manifest;
}
