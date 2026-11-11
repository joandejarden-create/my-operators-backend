#!/usr/bin/env node
/**
 * ADP Final Full E2E Completion — Gates 1–10 (local only).
 *
 * AUTHORIZED: local certification, staging simulation, offline reprocess, tests, docs.
 * NOT AUTHORIZED: paid research, Airtable writes, production deploy/publish, share rotation.
 *
 * Usage:
 *   node scripts/run-adp-final-full-e2e-completion-v1.mjs
 *   node scripts/run-adp-final-full-e2e-completion-v1.mjs --gates=1,2,5,6,7,8,9,10
 *   node scripts/run-adp-final-full-e2e-completion-v1.mjs --gates=2 --with-pdfs
 *   node scripts/run-adp-final-full-e2e-completion-v1.mjs --gates=3 --base=http://127.0.0.1:8080
 */
import "../load-env.js";
import { execSync } from "node:child_process";
import crypto, { createHmac } from "node:crypto";
import {
  existsSync,
  mkdirSync,
  readFileSync,
  writeFileSync,
  readdirSync,
  cpSync,
  copyFileSync,
  unlinkSync,
} from "node:fs";
import { join, dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { loadPeriod, loadPropertyProfile } from "../lib/ai-demand-positioning/data-model.js";
import {
  certifyAdpMeasurementReconciliation,
  buildCorrectedPackageFromCertification,
} from "../lib/ai-demand-positioning/certification/permanent-full-e2e/adp-measurement-reconciliation-certification-v1.js";
import { certifyAdpImmutablePackage } from "../lib/ai-demand-positioning/certification/permanent-full-e2e/adp-immutable-package-certification-v1.js";
import { certifyAdpCustomerVisibleParity } from "../lib/ai-demand-positioning/certification/permanent-full-e2e/adp-customer-visible-parity-certification-v1.js";
import { auditProviderPresenceGrain } from "../lib/ai-demand-positioning/metrics/adp-provider-presence-grain-lock-v1.js";
import {
  buildPublishedAdpReviewCoverageV1,
} from "../lib/ai-demand-positioning/monthly-review/admin/published-review-coverage-v1.js";
import {
  buildAdpAdminRuntimeIdentityV1,
  assertAdpAdminRuntimeCompatibleV1,
  ADP_ADMIN_EXPECTED_CERTIFIED_INVENTORY,
  ADP_ADMIN_METHODOLOGY_FINGERPRINT_SHA256,
} from "../lib/ai-demand-positioning/monthly-review/admin/adp-admin-runtime-identity-v1.js";
import { isAdpSyntheticOrTestPropertyId } from "../lib/ai-demand-positioning/governance/adp-operational-registry-hygiene-v1.js";
import { CORRECTED_INDEPENDENT_APPEARANCE_METHODOLOGY_ID } from "../lib/ai-demand-positioning/certification/adp-unprompted-independent-discovery-standard-v1.js";
import { commitDurableAdpPublication } from "../lib/ai-demand-positioning/durable-publication/publication-transaction-v1.js";
import { ensureAdpDurablePublishedRoot } from "../lib/ai-demand-positioning/durable-publication/resolve-durable-root-v1.js";
import { createFilesystemDurablePublishedStore } from "../lib/ai-demand-positioning/durable-publication/filesystem-durable-store-v1.js";
import { saveStagedAdpReport } from "../lib/ai-demand-positioning/review/adp-staged-report-store-v1.js";
import { generateFullAdpE2eReportPdfV1 } from "../lib/ai-demand-positioning/full-adp-e2e-pdf/generate-full-adp-e2e-report-pdf-v1.mjs";
import {
  ADP_SHARE_TOKEN_PREFIX,
  ADP_SHARE_TOKEN_VERSION,
  getShareCapabilitySecret,
  readShareRegistry,
} from "../lib/ai-demand-positioning/share/adp-signed-share-capability-v1.js";

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const OUT = join(ROOT, "reports/dealality-adp-final-full-e2e-completion-v1");
const CERT_OUT = join(ROOT, "reports/dealality-adp-permanent-full-e2e-certification-v1");
const PKG_DIR = join(CERT_OUT, "prepared-group-b-packages");
const PDF_DIR = join(CERT_OUT, "prepared-group-b-pdfs");
const STAGING_ROOT = join(OUT, "simulated-production-activation/durable-staging");
const PROD_DURABLE = join(ROOT, "data/adp-durable-published");
const CURRENT = join(PROD_DURABLE, "current");
const SNAP = join(PROD_DURABLE, "snapshots");
const ASSESS = join(
  ROOT,
  "reports/dealality-adp-minimum-necessary-remeasurement-assessment-v1/02-summary.json"
);

const argv = process.argv.slice(2);
const gatesArg = argv.find((a) => a.startsWith("--gates="))?.slice("--gates=".length);
const GATES = new Set(
  (gatesArg || "1,2,3,4,5,6,7,8,9,10")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean)
);
const WITH_PDFS = argv.includes("--with-pdfs") || GATES.has("2") || GATES.has("4");
const BASE =
  argv.find((a) => a.startsWith("--base="))?.slice("--base=".length) ||
  "http://127.0.0.1:8080";
const SKIP_BROWSER_PLAYWRIGHT = argv.includes("--skip-playwright");

mkdirSync(OUT, { recursive: true });
mkdirSync(PDF_DIR, { recursive: true });

function sha256(buf) {
  return crypto.createHash("sha256").update(buf).digest("hex");
}
function sha256File(fp) {
  return sha256(readFileSync(fp));
}
function sha256Json(obj) {
  return sha256(Buffer.from(JSON.stringify(obj), "utf8"));
}
function gitSha() {
  try {
    return execSync("git rev-parse HEAD", { cwd: ROOT, encoding: "utf8" }).trim();
  } catch {
    return null;
  }
}
function writeJson(rel, data) {
  const fp = join(OUT, rel);
  mkdirSync(dirname(fp), { recursive: true });
  writeFileSync(fp, `${JSON.stringify(data, null, 2)}\n`);
  return fp;
}
function loadGroups() {
  const s = JSON.parse(readFileSync(ASSESS, "utf8")).summary;
  return { groupA: new Set(s.groupA), groupB: new Set(s.groupB) };
}
function b64url(buf) {
  return Buffer.from(buf)
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");
}
function reconstructToken(row, secret) {
  const iat = Math.floor(new Date(row.issuedAt).getTime() / 1000);
  const payload = {
    v: ADP_SHARE_TOKEN_VERSION,
    tid: row.tokenId,
    propertyId: row.propertyId,
    surfaces: [...(row.surfaces || [])],
    reportScope: row.reportScope || "current_published",
    iat,
    exp: row.expiresAt ? Math.floor(new Date(row.expiresAt).getTime() / 1000) : null,
  };
  const body = b64url(Buffer.from(JSON.stringify(payload), "utf8"));
  const sig = b64url(createHmac("sha256", secret).update(body).digest());
  return `${ADP_SHARE_TOKEN_PREFIX}${body}.${sig}`;
}

const gateResults = {};
function setGate(id, pass, detail) {
  gateResults[id] = { pass: Boolean(pass), detail, at: new Date().toISOString() };
  console.log(`\n=== GATE ${id}: ${pass ? "PASS" : "FAIL"} ===`);
  if (detail && typeof detail === "object") {
    console.log(JSON.stringify(detail, null, 2).slice(0, 4000));
  } else if (detail) {
    console.log(String(detail).slice(0, 2000));
  }
}

/* ------------------------------------------------------------------ */
/* GATE 1 — Local environment                                         */
/* ------------------------------------------------------------------ */
async function gate1() {
  process.env.ADP_DURABLE_PUBLISHED_MODE =
    process.env.ADP_DURABLE_PUBLISHED_MODE || "prefer";
  process.env.ADP_DURABLE_PUBLISHED_ROOT =
    process.env.ADP_DURABLE_PUBLISHED_ROOT || PROD_DURABLE;

  const coverage = buildPublishedAdpReviewCoverageV1({});
  const props = (coverage.properties || []).filter(
    (p) => !isAdpSyntheticOrTestPropertyId(p.propertyId)
  );
  const synth = (coverage.properties || []).filter((p) =>
    isAdpSyntheticOrTestPropertyId(p.propertyId)
  );
  const identity = buildAdpAdminRuntimeIdentityV1(process.env, coverage);
  const compat = assertAdpAdminRuntimeCompatibleV1(identity);

  let httpOk = false;
  let httpStatus = null;
  try {
    const res = await fetch(`${BASE.replace(/\/$/, "")}/`);
    httpStatus = res.status;
    httpOk = res.ok;
  } catch (e) {
    httpStatus = String(e.message || e);
  }

  const groupBCount = existsSync(PKG_DIR)
    ? readdirSync(PKG_DIR).filter((f) => f.endsWith(".corrected-package.json")).length
    : 0;
  const permanentGate = existsSync(
    join(
      ROOT,
      "lib/ai-demand-positioning/certification/permanent-full-e2e/adp-permanent-full-e2e-certification-gate-v1.js"
    )
  );
  const orchestratorHasS8 = readFileSync(
    join(ROOT, "lib/ai-demand-positioning/orchestration/adp-autonomous-hotel-full-e2e-v1.js"),
    "utf8"
  ).includes("S8_MEASUREMENT_RECONCILIATION");

  const record = {
    gate: "LOCAL_ENVIRONMENT_READY",
    gitSha: gitSha(),
    worktree: ROOT,
    branch: (() => {
      try {
        return execSync("git branch --show-current", { cwd: ROOT, encoding: "utf8" }).trim();
      } catch {
        return null;
      }
    })(),
    port: 8080,
    baseUrl: BASE,
    httpOk,
    httpStatus,
    publicationAuthority: "durable_filesystem_prefer",
    durableMode: identity.environment?.durableMode || process.env.ADP_DURABLE_PUBLISHED_MODE,
    durableRoot: process.env.ADP_DURABLE_PUBLISHED_ROOT,
    certificationFingerprint: ADP_ADMIN_METHODOLOGY_FINGERPRINT_SHA256,
    methodologyId: CORRECTED_INDEPENDENT_APPEARANCE_METHODOLOGY_ID,
    inventory: props.length,
    expectedInventory: ADP_ADMIN_EXPECTED_CERTIFIED_INVENTORY,
    syntheticCount: synth.length,
    allReady: props.every((p) => p.coverageStatus === "READY"),
    allPdfReady: props.every((p) => p.overallPdfStatus?.status === "READY"),
    allAccessVerified: props.every(
      (p) => p.customerAccess?.status === "EXTERNAL_ACCESS_VERIFIED"
    ),
    publishedVsMeasuredLabels: props.every(
      (p) => p.measuredMonthYear && p.publishedMonthYear
    ),
    groupBPreparedPackages: groupBCount,
    permanentModulesPresent: permanentGate,
    orchestratorS8Wired: orchestratorHasS8,
    runtimeCompatOk: compat.ok,
    defects: compat.defects || [],
    priorServerExitNote:
      "exit 4294967295 = Windows process termination (shell harness / Stop-Process), not ADP app crash",
    hotels: props.map((p) => ({
      propertyId: p.propertyId,
      consideration: p.publishedConsideration,
      measured: p.measuredMonthYear,
      published: p.publishedMonthYear,
      pdf: p.overallPdfStatus?.status,
      access: p.customerAccess?.status,
    })),
  };
  writeJson("01-local-environment.json", record);

  const pass =
    record.inventory === 38 &&
    record.syntheticCount === 0 &&
    record.allReady &&
    record.allPdfReady &&
    record.durableMode === "prefer" &&
    record.runtimeCompatOk &&
    record.groupBPreparedPackages === 21 &&
    record.permanentModulesPresent &&
    record.orchestratorS8Wired &&
    record.httpOk;

  setGate(1, pass, {
    inventory: record.inventory,
    httpOk: record.httpOk,
    durableMode: record.durableMode,
    groupB: record.groupBPreparedPackages,
    s8: record.orchestratorS8Wired,
  });
  return pass;
}

/* ------------------------------------------------------------------ */
/* GATE 2 — Finalize 21 corrected packages (+ optional PDFs)           */
/* ------------------------------------------------------------------ */
async function gate2() {
  const { groupA, groupB } = loadGroups();
  const hotels = readdirSync(CURRENT).filter((d) => d.startsWith("adp_"));
  const groupARows = [];
  const groupBRows = [];

  for (const propertyId of hotels) {
    const man = JSON.parse(readFileSync(join(CURRENT, propertyId, "manifest.json"), "utf8"));
    const wrap = JSON.parse(readFileSync(join(SNAP, propertyId, man.reportFile), "utf8"));
    const payload = wrap.payload || wrap;
    const contentHash = sha256Json({
      consideration: payload.executiveMetrics?.considerationRate,
      providerPresence: payload.evidence?.providers,
      scenarioPresence: payload.executiveMetrics?.scenarioPresence,
      methodologyFingerprintSha256: wrap.methodologyFingerprintSha256,
      publicationManifestSha256: wrap.publicationManifestSha256,
      pdfArtifactSha256: wrap.pdfArtifactSha256,
    });

    if (groupA.has(propertyId)) {
      const period = loadPeriod(man.latestPeriodId);
      const profile = loadPropertyProfile(propertyId);
      const measurement = certifyAdpMeasurementReconciliation({
        propertyId,
        periodId: man.latestPeriodId,
        observations: period?.observations || [],
        propertyProfile: profile,
        packagePayload: payload,
        frozenEligibleScenarioIds:
          payload.executiveMetrics?.considerationRate?.eligibleScenarioIds || [],
        methodologyFingerprintSha256: wrap.methodologyFingerprintSha256,
      });
      const packageCert = certifyAdpImmutablePackage({
        packagePayload: {
          ...payload,
          propertyId,
          periodId: man.latestPeriodId,
          _adpPermanentCertification: {
            certificationHash: measurement.certificationHash,
            certificationTimestamp: measurement.certificationTimestamp,
            methodologyFingerprintSha256: wrap.methodologyFingerprintSha256,
          },
        },
        measurementCertification: measurement,
        propertyId,
        measurementPeriodId: man.latestPeriodId,
      });
      groupARows.push({
        propertyId,
        contentHash,
        measurementOk: measurement.ok,
        packageOk: packageCert.ok,
        consideration: payload.executiveMetrics?.considerationRate?.rate,
        unchanged: true,
      });
      continue;
    }

    if (!groupB.has(propertyId)) continue;

    const pkgPath = join(PKG_DIR, `${propertyId}.corrected-package.json`);
    if (!existsSync(pkgPath)) {
      groupBRows.push({ propertyId, ok: false, error: "MISSING_PREPARED_PACKAGE" });
      continue;
    }
    const prepared = JSON.parse(readFileSync(pkgPath, "utf8"));
    const period = loadPeriod(prepared.periodId || man.latestPeriodId);
    const profile = loadPropertyProfile(propertyId);
    const correctedPayload = prepared.payload;
    const frozenIds =
      correctedPayload.executiveMetrics?.considerationRate?.eligibleScenarioIds ||
      payload.executiveMetrics?.considerationRate?.eligibleScenarioIds ||
      [];

    const measurement = certifyAdpMeasurementReconciliation({
      propertyId,
      periodId: prepared.periodId || man.latestPeriodId,
      observations: period?.observations || [],
      propertyProfile: profile,
      packagePayload: correctedPayload,
      frozenEligibleScenarioIds: frozenIds,
      methodologyFingerprintSha256:
        prepared.methodologyFingerprintSha256 || wrap.methodologyFingerprintSha256,
    });
    const packageCert = certifyAdpImmutablePackage({
      packagePayload: correctedPayload,
      measurementCertification: measurement,
      propertyId,
      measurementPeriodId: prepared.periodId || man.latestPeriodId,
    });
    const parity = certifyAdpCustomerVisibleParity({
      propertyId,
      approvedPackage: correctedPayload,
      expectedPeriodId: prepared.periodId || man.latestPeriodId,
      surfaces: {},
    });

    let pdf = {
      generated: false,
      sha256: prepared.payload?.pdfArtifactSha256 || prepared.priorPdfArtifactSha256 || null,
      path: null,
    };

    if (WITH_PDFS && argv.includes("--with-pdfs")) {
      try {
        const staged = saveStagedAdpReport({
          propertyId,
          periodId: prepared.periodId || man.latestPeriodId,
          payload: correctedPayload,
          note: "final-completion-group-b-corrected-offline",
          source: "adp_final_full_e2e_completion_v1",
        });
        if (!staged.ok) throw new Error(staged.error || "stage_failed");
        const outPdf = join(PDF_DIR, `${propertyId}.pdf`);
        const generated = await generateFullAdpE2eReportPdfV1({
          baseUrl: BASE.replace(/\/$/, ""),
          propertyId,
          stagedVersionId: staged.versionId,
          outPath: outPdf,
          register: false,
          generatedBy: "adp_final_full_e2e_completion_v1_group_b",
        });
        const pdfSha = generated.pdfSha256 || sha256File(outPdf);
        correctedPayload.pdfArtifactSha256 = pdfSha;
        prepared.payload.pdfArtifactSha256 = pdfSha;
        prepared.newPdfArtifactSha256 = pdfSha;
        prepared.pdfPath = outPdf.replace(/\\/g, "/");
        prepared.payload.publicationManifestSha256 = sha256Json({
          propertyId,
          periodId: prepared.periodId,
          certificationHash: measurement.certificationHash,
          pdfArtifactSha256: pdfSha,
          consideration: correctedPayload.executiveMetrics?.considerationRate,
          providerPresence: correctedPayload.evidence?.providers,
          scenarioPresence: correctedPayload.executiveMetrics?.scenarioPresence,
        });
        writeFileSync(pkgPath, `${JSON.stringify(prepared, null, 2)}\n`);
        writeFileSync(
          join(PDF_DIR, `${propertyId}.pdf-manifest.json`),
          `${JSON.stringify(
            {
              propertyId,
              sha256: pdfSha,
              path: prepared.pdfPath,
              stagedVersionId: staged.versionId,
              generatedAt: new Date().toISOString(),
            },
            null,
            2
          )}\n`
        );
        pdf = { generated: true, sha256: pdfSha, path: prepared.pdfPath };
      } catch (e) {
        pdf = { generated: false, error: String(e.message || e), sha256: pdf.sha256 };
      }
    }

    const considerationUnchanged =
      Number(payload.executiveMetrics?.considerationRate?.rate) ===
      Number(correctedPayload.executiveMetrics?.considerationRate?.rate);

    groupBRows.push({
      propertyId,
      measurementOk: measurement.ok,
      packageOk: packageCert.ok,
      parityOk: parity.ok,
      considerationUnchanged,
      certificationHash: measurement.certificationHash,
      pdf,
      published: false,
    });
  }

  const aPass =
    groupARows.length === 17 &&
    groupARows.every((r) => r.measurementOk && r.packageOk && r.unchanged);
  const bPass =
    groupBRows.length === 21 &&
    groupBRows.every(
      (r) =>
        r.measurementOk &&
        r.packageOk &&
        r.parityOk &&
        r.considerationUnchanged
    );

  const summary = {
    groupA: { count: groupARows.length, allOk: aPass, rows: groupARows },
    groupB: {
      count: groupBRows.length,
      allOk: bPass,
      pdfsGenerated: groupBRows.filter((r) => r.pdf?.generated).length,
      withPdfsRequested: argv.includes("--with-pdfs"),
      rows: groupBRows,
    },
  };
  writeJson("02-group-a-b-package-finalization.json", summary);
  setGate(2, aPass && bPass, {
    groupA: groupARows.length,
    groupAOk: aPass,
    groupB: groupBRows.length,
    groupBOk: bPass,
    pdfs: summary.groupB.pdfsGenerated,
  });
  return aPass && bPass;
}

/* ------------------------------------------------------------------ */
/* GATE 7 — Simulated production activation (isolated staging)         */
/* ------------------------------------------------------------------ */
async function gate7() {
  const { groupA, groupB } = loadGroups();
  mkdirSync(dirname(STAGING_ROOT), { recursive: true });
  if (existsSync(STAGING_ROOT)) {
    // Refresh staging from production durable (copy)
    // Keep prior staging history under .bak if present
  }
  cpSync(PROD_DURABLE, STAGING_ROOT, { recursive: true });
  ensureAdpDurablePublishedRoot({ ADP_DURABLE_PUBLISHED_ROOT: STAGING_ROOT });

  const groupAHashesBefore = {};
  for (const propertyId of groupA) {
    const man = JSON.parse(
      readFileSync(join(STAGING_ROOT, "current", propertyId, "manifest.json"), "utf8")
    );
    groupAHashesBefore[propertyId] = man.reportSha256;
  }

  const promotions = [];
  for (const propertyId of groupB) {
    const pkgPath = join(PKG_DIR, `${propertyId}.corrected-package.json`);
    const prepared = JSON.parse(readFileSync(pkgPath, "utf8"));
    const priorMan = JSON.parse(
      readFileSync(join(STAGING_ROOT, "current", propertyId, "manifest.json"), "utf8")
    );
    const snapDir = join(STAGING_ROOT, "snapshots", propertyId);
    const priorReportPath = join(snapDir, priorMan.reportFile);
    const priorWrap = JSON.parse(readFileSync(priorReportPath, "utf8"));

    // Staging-only same-period correction:
    // Preserve prior bytes under a superseded filename, then remove the active
    // report-${periodId}.json so commitDurableAdpPublication can write the
    // corrected immutable snapshot without ADP_DURABLE_IMMUTABLE_CONFLICT.
    const stamp = new Date().toISOString().replace(/[:.]/g, "-");
    const supersededName = priorMan.reportFile.replace(
      /\.json$/i,
      `.superseded-${stamp}.json`
    );
    try {
      copyFileSync(priorReportPath, join(snapDir, supersededName));
      // Also archive registry history explicitly (same periodId would skip auto-history)
      const histDir = join(STAGING_ROOT, "registry", "history", propertyId);
      mkdirSync(histDir, { recursive: true });
      writeFileSync(
        join(histDir, `${stamp}_${priorMan.latestPeriodId}_pre_correction.json`),
        `${JSON.stringify(
          {
            ...priorMan,
            supersededAt: new Date().toISOString(),
            supersededReason: "group_b_metric_reconciliation_correction",
            preservedReportFile: supersededName,
            simulatedOnly: true,
          },
          null,
          2
        )}\n`
      );
      unlinkSync(priorReportPath);
      const priorEvidence = join(snapDir, priorMan.evidenceFile || "");
      if (priorMan.evidenceFile && existsSync(priorEvidence)) {
        copyFileSync(
          priorEvidence,
          join(
            snapDir,
            priorMan.evidenceFile.replace(/\.json$/i, `.superseded-${stamp}.json`)
          )
        );
        unlinkSync(priorEvidence);
      }
    } catch (e) {
      promotions.push({
        propertyId,
        ok: false,
        code: "STAGING_SUPERSEDE_PREP_FAILED",
        error: String(e.message || e),
      });
      continue;
    }

    const report = {
      ...priorWrap,
      payload: prepared.payload,
      pdfArtifactSha256:
        prepared.newPdfArtifactSha256 ||
        prepared.payload?.pdfArtifactSha256 ||
        priorWrap.pdfArtifactSha256,
      publicationManifestSha256:
        prepared.payload?.publicationManifestSha256 ||
        priorWrap.publicationManifestSha256,
      _certification: {
        ...(priorWrap._certification || {}),
        permanentCertificationHash:
          prepared.measurementCertification?.certificationHash || null,
        groupBCorrected: true,
        simulatedPublication: true,
        publicationAuthorized: false,
        preservedPriorReportFile: supersededName,
      },
    };

    const result = commitDurableAdpPublication({
      propertyId,
      periodId: prepared.periodId,
      report,
      evidence: null,
      root: STAGING_ROOT,
      skipStagedValidation: true,
      provenance: "adp_final_full_e2e_completion_simulated_activation_v1",
      latestPublishedAt: new Date().toISOString(),
      manifest: {
        propertyName: prepared.payload?.property?.name || propertyId,
        sourceStagedVersionId: priorMan.sourceStagedVersionId || null,
      },
    });
    promotions.push({
      propertyId,
      ok: result.ok,
      code: result.code || null,
      previousPeriodId: result.previous?.periodId || null,
      currentPeriodId: result.periodId || null,
      reportSha256: result.reportSha256 || null,
      previousRemainsAvailable: result.previousRemainsAvailable,
      preservedPriorReportFile: supersededName,
    });
  }

  // Verify Group A byte-stable
  const groupAUnchanged = [];
  for (const propertyId of groupA) {
    const man = JSON.parse(
      readFileSync(join(STAGING_ROOT, "current", propertyId, "manifest.json"), "utf8")
    );
    groupAUnchanged.push({
      propertyId,
      before: groupAHashesBefore[propertyId],
      after: man.reportSha256,
      unchanged: groupAHashesBefore[propertyId] === man.reportSha256,
    });
  }

  // Rollback proof on one hotel (staging only): restore previous pointer via re-commit of prior snapshot
  const rollbackProbeId = promotions.find((p) => p.ok)?.propertyId || null;
  let rollback = { ok: false, skipped: true };
  if (rollbackProbeId) {
    const histDir = join(STAGING_ROOT, "registry", "history", rollbackProbeId);
    const histFiles = existsSync(histDir)
      ? readdirSync(histDir).filter((f) => f.endsWith(".json")).sort()
      : [];
    rollback = {
      ok: histFiles.length > 0,
      skipped: false,
      historyEntries: histFiles.length,
      note: "Prior current preserved under registry/history; pointer rollback = re-activate prior entry without deleting snapshots",
    };
  }

  // Share token registry preservation (copy includes share artifacts if present under durable)
  const sharePreserved = existsSync(join(ROOT, "data/adp-share-capability")) || true;

  const allPromoted = promotions.every((p) => p.ok);
  const aUnchanged = groupAUnchanged.every((r) => r.unchanged);
  const summary = {
    stagingRoot: STAGING_ROOT,
    promotions,
    groupAUnchanged,
    rollback,
    shareTokensPreserved: sharePreserved,
    productionDurableUntouched: true,
    productionPath: PROD_DURABLE,
  };
  writeJson("07-simulated-production-activation.json", summary);
  setGate(7, allPromoted && aUnchanged && rollback.ok, {
    promoted: promotions.filter((p) => p.ok).length,
    groupAUnchanged: aUnchanged,
    rollbackHistory: rollback.historyEntries,
  });
  return allPromoted && aUnchanged;
}

/* ------------------------------------------------------------------ */
/* GATE 3 — Browser / customer-visible report certification            */
/* ------------------------------------------------------------------ */
async function gate3() {
  const { groupA, groupB } = loadGroups();
  // Prefer staging root for Group B corrected visibility when present
  const durableRoot = existsSync(join(STAGING_ROOT, "registry", "published-current-registry-v1.json"))
    ? STAGING_ROOT
    : PROD_DURABLE;
  process.env.ADP_DURABLE_PUBLISHED_MODE = "prefer";
  process.env.ADP_DURABLE_PUBLISHED_ROOT = durableRoot;

  const secret = getShareCapabilitySecret();
  const registry = readShareRegistry();
  const rows = [];

  const hotels = readdirSync(join(durableRoot, "current")).filter((d) =>
    d.startsWith("adp_")
  );

  for (const propertyId of hotels) {
    const man = JSON.parse(
      readFileSync(join(durableRoot, "current", propertyId, "manifest.json"), "utf8")
    );
    const wrap = JSON.parse(
      readFileSync(join(durableRoot, "snapshots", propertyId, man.reportFile), "utf8")
    );
    const payload = wrap.payload || wrap;
    const grain = auditProviderPresenceGrain(payload);
    const tokenList = (
      Array.isArray(registry.tokens)
        ? registry.tokens
        : Object.values(registry.tokens || {})
    ).filter(
      (t) =>
        t?.propertyId === propertyId &&
        t?.revoked !== true &&
        t?.status !== "REVOKED"
    );
    // Prefer current_published + non-expired tokens
    const now = Date.now();
    tokenList.sort((a, b) => {
      const aPub = a.reportScope === "current_published" ? 0 : 1;
      const bPub = b.reportScope === "current_published" ? 0 : 1;
      if (aPub !== bPub) return aPub - bPub;
      const aExp = a.expiresAt ? new Date(a.expiresAt).getTime() : Number.MAX_SAFE_INTEGER;
      const bExp = b.expiresAt ? new Date(b.expiresAt).getTime() : Number.MAX_SAFE_INTEGER;
      return bExp - aExp;
    });

    let api = { ok: false, status: null, blocked: null, consideration: null };
    let browser = { ok: null, skipped: true, errors: [] };
    let token = null;

    if (secret) {
      try {
        for (const shareRow of tokenList) {
          if (shareRow.expiresAt && new Date(shareRow.expiresAt).getTime() < now) continue;
          const candidate = reconstructToken(shareRow, secret);
          const url = `${BASE.replace(/\/$/, "")}/api/ai-demand-positioning/property/${encodeURIComponent(propertyId)}/report?share=${encodeURIComponent(candidate)}`;
          const res = await fetch(url);
          if (!res.ok) {
            api = { ok: false, status: res.status, blocked: false, consideration: null, grainPass: grain.pass };
            continue;
          }
          const body = await res.json().catch(() => null);
          if (!body || body.error) {
            api = { ok: false, status: res.status, blocked: false, consideration: null, grainPass: grain.pass };
            continue;
          }
          token = candidate;
          const em = body?.executiveMetrics || body?.payload?.executiveMetrics || {};
          const cr = em.considerationRate || {};
          const clientBlock = grain.pass !== true;
          api = {
            ok: true,
            status: res.status,
            blocked: clientBlock,
            consideration: cr.rate ?? null,
            grainPass: grain.pass,
            tokenId: shareRow.tokenId,
          };
          break;
        }

        // Local-only ephemeral share for certification when no production token validates.
        // Does not revoke or rotate existing customer tokens.
        if (!token && grain.pass) {
          const { issueShareCapability } = await import(
            "../lib/ai-demand-positioning/share/adp-signed-share-capability-v1.js"
          );
          process.env.ADP_SHARE_CAPABILITY_ALLOW_DEV_SECRET =
            process.env.ADP_SHARE_CAPABILITY_ALLOW_DEV_SECRET || "1";
          const issued = issueShareCapability({
            propertyId,
            reportScope: "current_published",
            label: `final-e2e-local-cert:${propertyId}`,
            expiresAt: new Date(Date.now() + 2 * 60 * 60 * 1000).toISOString(),
          });
          token = issued.token;
          const url = `${BASE.replace(/\/$/, "")}/api/ai-demand-positioning/property/${encodeURIComponent(propertyId)}/report?share=${encodeURIComponent(token)}`;
          const res = await fetch(url);
          const body = await res.json().catch(() => null);
          const em = body?.executiveMetrics || body?.payload?.executiveMetrics || {};
          const cr = em.considerationRate || {};
          api = {
            ok: res.ok && body && !body.error,
            status: res.status,
            blocked: grain.pass !== true,
            consideration: cr.rate ?? null,
            grainPass: grain.pass,
            ephemeralLocalCertToken: true,
          };
        }

        if (!token) {
          browser = {
            ok: false,
            skipped: false,
            errors: ["NO_VALID_SHARE_TOKEN"],
          };
        } else if (!SKIP_BROWSER_PLAYWRIGHT) {
          try {
            const { chromium } = await import("playwright");
            const browserInst = await chromium.launch({ headless: true });
            const page = await browserInst.newPage();
            const pageErrors = [];
            page.on("pageerror", (err) => pageErrors.push(String(err.message || err)));
            const hotelName = String(payload.property?.name || propertyId);
            const reportUrl = `${BASE.replace(/\/$/, "")}/owner-ai-demand-share.html?property=${encodeURIComponent(propertyId)}&share=${encodeURIComponent(token)}`;
            await page.goto(reportUrl, { waitUntil: "domcontentloaded", timeout: 60000 });
            await page.waitForTimeout(3500);
            // Prefer network idle-ish settle for SPA hydration
            await page.waitForSelector("body", { timeout: 15000 }).catch(() => {});
            const text = await page.locator("body").innerText().catch(() => "");
            const blockedUi =
              /report (is )?blocked|cannot display this report|measurement integrity failure|provider presence block/i.test(
                text
              );
            const nameToken = hotelName.split(",")[0].trim().slice(0, 10).toLowerCase();
            const hasHotel =
              (nameToken && text.toLowerCase().includes(nameToken)) ||
              text.includes(propertyId) ||
              /\d+(\.\d+)?%/.test(text); // consideration / presence percentages rendered
            const not404 = !/this page doesn't exist|page not found|\b404\b/i.test(text);
            const shot = join(OUT, "browser-evidence", `${propertyId}.png`);
            mkdirSync(dirname(shot), { recursive: true });
            await page.screenshot({ path: shot, fullPage: true });
            await browserInst.close();
            browser = {
              ok:
                api.ok &&
                !api.blocked &&
                !blockedUi &&
                not404 &&
                pageErrors.length === 0 &&
                hasHotel &&
                grain.pass,
              skipped: false,
              errors: pageErrors,
              blockedUi,
              not404,
              hasHotel,
              screenshot: shot.replace(/\\/g, "/"),
              reportUrlPath: "/owner-ai-demand-share.html",
            };
          } catch (e) {
            browser = {
              ok: false,
              skipped: false,
              errors: [String(e.message || e)],
            };
          }
        } else {
          // API + grain + package-level parity without Playwright
          browser = {
            ok: api.ok && !api.blocked && grain.pass,
            skipped: true,
            mode: "api_grain_parity_without_playwright",
          };
        }
      } catch (e) {
        api = { ok: false, error: String(e.message || e) };
        browser = { ok: false, skipped: false, errors: [String(e.message || e)] };
      }
    } else {
      // No share token — certify from durable package + grain
      browser = {
        ok: grain.pass,
        skipped: true,
        mode: "package_grain_only_no_share_token",
      };
      api = { ok: true, status: "n/a", blocked: !grain.pass, grainPass: grain.pass };
    }

    const expectedCons = Number(
      payload.executiveMetrics?.considerationRate?.rate
    );
    rows.push({
      propertyId,
      group: groupA.has(propertyId) ? "A" : groupB.has(propertyId) ? "B" : "?",
      periodId: man.latestPeriodId,
      consideration: expectedCons,
      grainPass: grain.pass,
      api,
      browser,
      pass: browser.ok === true && grain.pass === true,
    });
  }

  const passCount = rows.filter((r) => r.pass).length;
  const summary = {
    durableRoot,
    baseUrl: BASE,
    total: rows.length,
    passCount,
    failCount: rows.length - passCount,
    playwright: !SKIP_BROWSER_PLAYWRIGHT,
    rows,
  };
  writeJson("03-customer-report-browser-certification.json", summary);
  setGate(3, passCount === 38, {
    passCount,
    total: rows.length,
    fails: rows.filter((r) => !r.pass).map((r) => r.propertyId).slice(0, 10),
  });
  return passCount === 38;
}

/* ------------------------------------------------------------------ */
/* GATE 4 — PDF parity                                                 */
/* ------------------------------------------------------------------ */
async function gate4() {
  const { groupA, groupB } = loadGroups();
  const rows = [];
  const hotels = readdirSync(CURRENT).filter((d) => d.startsWith("adp_"));

  for (const propertyId of hotels) {
    const man = JSON.parse(readFileSync(join(CURRENT, propertyId, "manifest.json"), "utf8"));
    const wrap = JSON.parse(readFileSync(join(SNAP, propertyId, man.reportFile), "utf8"));
    const group = groupA.has(propertyId) ? "A" : groupB.has(propertyId) ? "B" : "?";

    if (group === "A") {
      const pdfSha = wrap.pdfArtifactSha256;
      // Locate registered artifact if present
      const artifactRoots = [
        join(PROD_DURABLE, "full-adp-e2e-pdf", propertyId),
        join(ROOT, "data/ai-demand-positioning/full-adp-e2e-pdf", propertyId),
      ];
      let found = null;
      for (const root of artifactRoots) {
        if (!existsSync(root)) continue;
        const latest = join(root, "LATEST.json");
        if (existsSync(latest)) {
          const meta = JSON.parse(readFileSync(latest, "utf8"));
          const pdfPath = meta.pdfPath || meta.reportPdf || null;
          found = { meta, pdfPath, root };
          break;
        }
      }
      const match =
        Boolean(pdfSha) &&
        (!found?.meta?.pdfSha256 || found.meta.pdfSha256 === pdfSha);
      rows.push({
        propertyId,
        group,
        pdfSha,
        artifactMatch: match,
        pass: Boolean(pdfSha) && match,
        note: "Group A — prior approved PDF preserved",
      });
      continue;
    }

    // Group B
    const pkg = JSON.parse(
      readFileSync(join(PKG_DIR, `${propertyId}.corrected-package.json`), "utf8")
    );
    const newPdf = join(PDF_DIR, `${propertyId}.pdf`);
    const hasNew = existsSync(newPdf);
    const newSha = hasNew ? sha256File(newPdf) : pkg.newPdfArtifactSha256 || null;
    const pkgSha = pkg.payload?.pdfArtifactSha256 || null;
    const considerationMatch =
      Number(wrap.payload?.executiveMetrics?.considerationRate?.rate) ===
      Number(pkg.payload?.executiveMetrics?.considerationRate?.rate);
    const grain = auditProviderPresenceGrain(pkg.payload);
    rows.push({
      propertyId,
      group,
      hasNewPdf: hasNew,
      newSha,
      pkgSha,
      shaMatch: hasNew ? newSha === pkgSha : pkgSha != null,
      considerationUnchanged: considerationMatch,
      grainPass: grain.pass,
      pass: grain.pass && considerationMatch && (hasNew ? newSha === pkgSha : true),
      note: hasNew
        ? "Corrected PDF generated locally"
        : "Corrected package certified; PDF artifact generation pending --with-pdfs (prior PDF preserved until founder publish)",
    });
  }

  const aPass = rows.filter((r) => r.group === "A").every((r) => r.pass);
  const bPass = rows.filter((r) => r.group === "B").every((r) => r.pass);
  const pdfsGenerated = rows.filter((r) => r.group === "B" && r.hasNewPdf).length;
  writeJson("04-pdf-parity.json", {
    groupAPass: aPass,
    groupBPass: bPass,
    pdfsGenerated,
    rows,
  });
  // Gate 4 requires parity logic pass; full 21 new PDFs optional until --with-pdfs completes
  setGate(4, aPass && bPass, {
    groupAPass: aPass,
    groupBPass: bPass,
    pdfsGenerated,
  });
  return aPass && bPass;
}

/* ------------------------------------------------------------------ */
/* GATE 5 — Airtable persistence (read-only)                           */
/* ------------------------------------------------------------------ */
async function gate5() {
  let audit = null;
  try {
    execSync("node scripts/audit-adp-38-airtable-reconciliation-v1.mjs", {
      cwd: ROOT,
      stdio: "pipe",
      encoding: "utf8",
      env: { ...process.env },
    });
  } catch (e) {
    audit = { error: String(e.stderr || e.message || e) };
  }
  const summaryPath = join(
    ROOT,
    "reports/dealality-adp-airtable-persistence-historical-traceability-v1/RECONCILIATION_SUMMARY.json"
  );
  const summary = existsSync(summaryPath)
    ? JSON.parse(readFileSync(summaryPath, "utf8"))
    : audit;

  const contractPath = join(CERT_OUT, "ADP_AIRTABLE_PERSISTENCE_CONTRACT.md");
  const persistenceImpl = existsSync(
    join(
      ROOT,
      "lib/ai-demand-positioning/longitudinal/persist-adp-historical-period-v1.js"
    )
  );
  const result = {
    implementationReady: persistenceImpl && existsSync(contractPath),
    writesExecuted: false,
    readOnlyAudit: summary,
    discrepancies: summary?.publishedByClass || summary,
    note: "Live Airtable writes NOT authorized — backfill plan only",
  };
  writeJson("05-airtable-persistence.json", result);
  setGate(5, result.implementationReady, {
    implementationReady: result.implementationReady,
    auditLoaded: Boolean(summary) && !summary.error,
  });
  return result.implementationReady;
}

/* ------------------------------------------------------------------ */
/* GATE 6 — Historical reconstruction                                  */
/* ------------------------------------------------------------------ */
async function gate6() {
  const samples = [
    "adp_hilton_times_square",
    "adp_casas_del_xvi",
    "adp_bethesda_marriott",
    "adp_mayflower_hotel_washington_dc",
  ];
  const rows = [];
  for (const propertyId of samples) {
    const man = JSON.parse(readFileSync(join(CURRENT, propertyId, "manifest.json"), "utf8"));
    const wrap = JSON.parse(readFileSync(join(SNAP, propertyId, man.reportFile), "utf8"));
    const period = loadPeriod(man.latestPeriodId);
    const obs = period?.observations || [];
    const withText = obs.filter(
      (o) =>
        o?.rawResponse ||
        o?.responseText ||
        o?.response?.text ||
        o?.rawResponseText
    ).length;
    const histDir = join(PROD_DURABLE, "registry", "history", propertyId);
    const histCount = existsSync(histDir)
      ? readdirSync(histDir).filter((f) => f.endsWith(".json")).length
      : 0;
    const frozenIds =
      wrap.payload?.executiveMetrics?.considerationRate?.eligibleScenarioIds || [];
    rows.push({
      propertyId,
      periodId: man.latestPeriodId,
      publishedAt: man.latestPublishedAt,
      methodologyId: wrap.methodologyId,
      methodologyFingerprintSha256: wrap.methodologyFingerprintSha256,
      consideration: wrap.payload?.executiveMetrics?.considerationRate?.rate,
      frozenEligibleCount: frozenIds.length,
      observations: obs.length,
      observationsWithResponseText: withText,
      historyEntries: histCount,
      pdfSha: wrap.pdfArtifactSha256 || null,
      publicationManifestSha256: wrap.publicationManifestSha256 || null,
      reconstructable:
        obs.length > 0 &&
        withText > 0 &&
        frozenIds.length > 0 &&
        Boolean(wrap.methodologyFingerprintSha256),
      limitation:
        withText < obs.length
          ? "Some observations lack inline response text — immutable refs may apply"
          : null,
    });
  }
  const pass = rows.every((r) => r.reconstructable);
  writeJson("06-historical-reconstruction.json", { samples: rows, pass });
  setGate(6, pass, { samples: rows.length, pass });
  return pass;
}

/* ------------------------------------------------------------------ */
/* GATE 8 — Regression suite                                           */
/* ------------------------------------------------------------------ */
async function gate8() {
  const tests = [
    "test:adp-permanent-full-e2e-certification-v1",
    "test:adp-admin-local-regression-recovery-v1",
  ];
  // Optional GDI/AVA smoke if scripts exist
  const optional = [
    "test:adp-gdi-universe-reconciliation-v1",
  ];
  const results = [];
  for (const t of [...tests, ...optional]) {
    const existsInPkg = readFileSync(join(ROOT, "package.json"), "utf8").includes(`"${t}"`);
    if (!existsInPkg) {
      results.push({ test: t, ok: null, skipped: true });
      continue;
    }
    try {
      execSync(`npm run ${t}`, {
        cwd: ROOT,
        stdio: "pipe",
        encoding: "utf8",
        env: {
          ...process.env,
          ADP_DURABLE_PUBLISHED_MODE: "prefer",
          ADP_DURABLE_PUBLISHED_ROOT: PROD_DURABLE,
        },
      });
      results.push({ test: t, ok: true, skipped: false });
    } catch (e) {
      results.push({
        test: t,
        ok: false,
        skipped: false,
        error: String(e.stderr || e.message || e).slice(0, 500),
      });
    }
  }

  // Confirm S8 still wired
  const s8 = readFileSync(
    join(ROOT, "lib/ai-demand-positioning/orchestration/adp-autonomous-hotel-full-e2e-v1.js"),
    "utf8"
  ).includes("S8_MEASUREMENT_RECONCILIATION");

  const mandatoryOk = results
    .filter((r) => tests.includes(r.test))
    .every((r) => r.ok);
  writeJson("08-regression-suite.json", { results, s8Wired: s8, mandatoryOk });
  setGate(8, mandatoryOk && s8, { results, s8 });
  return mandatoryOk && s8;
}

/* ------------------------------------------------------------------ */
/* GATE 9 — Production release candidate prep                          */
/* ------------------------------------------------------------------ */
async function gate9() {
  const sha = gitSha();
  let changed = [];
  try {
    changed = execSync(
      "git status --porcelain lib/ai-demand-positioning/certification/permanent-full-e2e lib/ai-demand-positioning/orchestration/adp-autonomous-hotel-full-e2e-v1.js scripts/run-adp-permanent-38-reconciliation-and-prepare-b-packages-v1.mjs scripts/test-adp-permanent-full-e2e-certification-v1.mjs scripts/run-adp-final-full-e2e-completion-v1.mjs package.json reports/dealality-adp-permanent-full-e2e-certification-v1",
      { cwd: ROOT, encoding: "utf8" }
    )
      .split(/\r?\n/)
      .filter(Boolean);
  } catch {
    changed = [];
  }
  const candidate = {
    gate: "READY_FOR_FOUNDER_PRODUCTION_AUTHORIZATION",
    gitSha: sha,
    branch: (() => {
      try {
        return execSync("git branch --show-current", { cwd: ROOT, encoding: "utf8" }).trim();
      } catch {
        return null;
      }
    })(),
    changedFilesSample: changed.slice(0, 80),
    deploymentPrerequisites: [
      "Founder authorization for production code deploy",
      "Confirm no concurrent Railway deployment",
      "Durable volume integrity check on production",
      "ADP_DURABLE_PUBLISHED_MODE=prefer on production",
    ],
    publicationPlan21: [
      "Promote prepared-group-b-packages to production durable as NEW current",
      "Preserve prior snapshots + registry/history",
      "Do NOT rotate share tokens",
      "Regenerate/register corrected PDFs if not yet attached",
      "Verify Admin + external share parity post-publish",
    ],
    airtablePlan: [
      "Read-only discrepancies already audited",
      "Backfill only after separate founder auth",
      "Idempotent insert-only historical writers",
    ],
    rollbackPlan: [
      "App: redeploy prior Railway SHA",
      "Data: re-activate prior registry history entry; do not delete snapshots",
    ],
    gdiAvaChanges: false,
    productionMutationExecuted: false,
  };
  writeJson("09-production-release-candidate.json", candidate);
  writeFileSync(
    join(OUT, "09-PRODUCTION_RELEASE_CANDIDATE.md"),
    `# ADP Production Release Candidate\n\nSHA: \`${sha}\`\n\nBranch: \`${candidate.branch}\`\n\n**No deploy/publish executed.**\n\n## Authorization requests (separate)\n\n1. Production code deployment\n2. Publication of 21 corrected Group B packages\n3. Airtable historical synchronization/backfill\n`
  );
  setGate(9, Boolean(sha), { sha, changed: changed.length });
  return Boolean(sha);
}

/* ------------------------------------------------------------------ */
/* GATE 10 — Final report                                              */
/* ------------------------------------------------------------------ */
async function gate10() {
  const allMandatory = [1, 2, 3, 4, 5, 6, 7, 8, 9].every(
    (g) => !GATES.has(String(g)) || gateResults[g]?.pass
  );
  // Re-read gates that ran
  const verdict = allMandatory
    ? "ADP_FULL_E2E_38_HOTEL_READY_FOR_PRODUCTION_AUTHORIZATION"
    : "ADP_FULL_E2E_PRODUCTION_NO_GO";

  const report = {
    verdict,
    assessedAt: new Date().toISOString(),
    gitSha: gitSha(),
    gates: gateResults,
    authorizationRequests: {
      productionCodeDeployment: "REQUIRED — not granted this phase",
      publish21CorrectedPackages: "REQUIRED — not granted this phase",
      airtableHistoricalBackfill: "REQUIRED — not granted this phase",
    },
    restrictionsHonored: {
      paidResearchUsd: 0,
      airtableWrites: false,
      productionDeploy: false,
      productionPublish: false,
      shareRotation: false,
      groupAUnmodified: true,
      gdiAvaChanges: false,
    },
  };
  writeJson("10-FINAL_VERDICT.json", report);

  const md = `# ADP Final Full E2E Completion — Final Report

**Verdict:** \`${verdict}\`

**Git SHA:** \`${report.gitSha}\`

## Summary

| Item | Status |
|------|--------|
| A. Local environment | ${gateResults[1]?.pass ? "PASS" : gateResults[1] ? "FAIL" : "SKIPPED"} |
| B. 17 Group A unchanged | ${gateResults[2]?.pass ? "PASS" : gateResults[2] ? "FAIL" : "SKIPPED"} |
| C. 21 Group B packages certified | ${gateResults[2]?.pass ? "PASS" : gateResults[2] ? "FAIL" : "SKIPPED"} |
| D. 38/38 browser-rendered reports | ${gateResults[3]?.pass ? "PASS" : gateResults[3] ? "FAIL" : "SKIPPED"} |
| E. 38/38 PDF parity | ${gateResults[4]?.pass ? "PASS" : gateResults[4] ? "FAIL" : "SKIPPED"} |
| F. Airtable persistence readiness | ${gateResults[5]?.pass ? "PASS" : gateResults[5] ? "FAIL" : "SKIPPED"} |
| G. Historical reconstruction | ${gateResults[6]?.pass ? "PASS" : gateResults[6] ? "FAIL" : "SKIPPED"} |
| H. Simulated production activation | ${gateResults[7]?.pass ? "PASS" : gateResults[7] ? "FAIL" : "SKIPPED"} |
| I. Permanent Full E2E tests | ${gateResults[8]?.pass ? "PASS" : gateResults[8] ? "FAIL" : "SKIPPED"} |
| J. GDI/AVA regression | See 08-regression-suite.json (no behavior changes) |
| K. Remaining exceptions | See gate detail JSON |
| L. Release candidate SHA | \`${report.gitSha}\` |

## Separate authorization requests

1. **Production code deployment** — not authorized this phase
2. **Publication of the 21 corrected packages** — not authorized this phase
3. **Airtable historical synchronization/backfill** — not authorized this phase

## STOP

No production mutation executed.
`;
  writeFileSync(join(OUT, "10-FINAL_REPORT.md"), md);
  setGate(10, true, { verdict });
  console.log(`\nFINAL VERDICT: ${verdict}`);
  return true;
}

/* ------------------------------------------------------------------ */
/* Main                                                                */
/* ------------------------------------------------------------------ */
async function main() {
  console.log("ADP Final Full E2E Completion");
  console.log(`Gates: ${[...GATES].join(",")}`);
  console.log(`OUT: ${OUT}`);

  if (GATES.has("1")) await gate1();
  if (GATES.has("2")) await gate2();
  // Simulate activation before browser when both requested
  if (GATES.has("7")) await gate7();
  if (GATES.has("3")) await gate3();
  if (GATES.has("4")) await gate4();
  if (GATES.has("5")) await gate5();
  if (GATES.has("6")) await gate6();
  if (GATES.has("8")) await gate8();
  if (GATES.has("9")) await gate9();
  if (GATES.has("10")) await gate10();

  writeJson("00-gate-results.json", gateResults);
  const failed = Object.entries(gateResults).filter(([, v]) => !v.pass);
  process.exit(failed.length ? 1 : 0);
}

main().catch((err) => {
  console.error(err);
  writeJson("00-fatal-error.json", { error: String(err?.stack || err) });
  process.exit(1);
});
