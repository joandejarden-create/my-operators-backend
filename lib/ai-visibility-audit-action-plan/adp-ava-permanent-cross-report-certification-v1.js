/**
 * Permanent ADP ↔ AVA cross-report certification.
 *
 * One authoritative ADP measurement per approved reporting period must be
 * consistently represented across customer-facing Dealality products.
 *
 * Contract: ADP_AVA_PERMANENT_CROSS_REPORT_CERTIFICATION_V1
 *
 * Extends ADP_AVA_REFERENCE_CONTRACT_V1 (lean pin integrity) with
 * customer-facing approved/published package resolution and joint-delivery gates.
 *
 * Does NOT remeasure ADP, publish, deploy, or rotate share tokens.
 */
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  ADP_AVA_REFERENCE_CONTRACT,
  loadAuthoritativeAdpReference,
  reconcileAvaPayloadAgainstAdp,
  listCertifiedAdpUniverse,
  ZERO_VERDICTS,
} from "./adp-ava-reference-contract-v1.js";

export const ADP_AVA_PERMANENT_CROSS_REPORT_CERTIFICATION =
  "ADP_AVA_PERMANENT_CROSS_REPORT_CERTIFICATION_V1";

export const ADP_AVA_CERTIFICATION_STATUS = Object.freeze({
  CERTIFIED_MATCH: "CERTIFIED_MATCH",
  CERTIFIED_HISTORICAL_REFERENCE: "CERTIFIED_HISTORICAL_REFERENCE",
  RECONCILIATION_REQUIRED: "RECONCILIATION_REQUIRED",
  CERTIFICATION_BLOCKED: "CERTIFICATION_BLOCKED",
});

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const MAIN_ROOT = path.resolve(ROOT, "..", "my-operators-backend");

const APPROVED_PKG_DIRS = [
  path.join(ROOT, "reports/dealality-adp-single-version-publication-contract-v1/approved-packages"),
  path.join(
    MAIN_ROOT,
    "reports/dealality-adp-single-version-publication-contract-v1/approved-packages"
  ),
];

const PUBLISHED_REGISTRY_CANDIDATES = [
  path.join(
    MAIN_ROOT,
    "reports/dealality-adp-single-version-publication-contract-v1/production-activation/durable-staging-prefer/registry/published-current-registry-v1.json"
  ),
  path.join(ROOT, "config/ai-demand-positioning/adp-published-current-registry-v1.json"),
  path.join(MAIN_ROOT, "config/ai-demand-positioning/adp-published-current-registry-v1.json"),
];

const SNAPSHOT_ROOTS = [
  path.join(
    MAIN_ROOT,
    "reports/dealality-adp-single-version-publication-contract-v1/production-activation/durable-staging-prefer/snapshots"
  ),
  path.join(
    ROOT,
    "reports/dealality-adp-single-version-publication-contract-v1/production-activation/durable-staging-prefer/snapshots"
  ),
];

function sha256Text(text) {
  return crypto.createHash("sha256").update(String(text ?? ""), "utf8").digest("hex");
}

function sha256File(abs) {
  return crypto.createHash("sha256").update(fs.readFileSync(abs)).digest("hex");
}

function readJson(abs) {
  return JSON.parse(fs.readFileSync(abs, "utf8"));
}

function exists(p) {
  return p && fs.existsSync(p);
}

function normalizePct(v) {
  if (v == null || v === "") return null;
  if (typeof v === "number" && Number.isFinite(v)) return Math.round(v * 10) / 10;
  const m = String(v).match(/(-?\d+(?:\.\d+)?)\s*%?/);
  if (!m) return null;
  return Math.round(Number(m[1]) * 10) / 10;
}

function pctClose(a, b, tol = 0.55) {
  if (a == null || b == null) return false;
  return Math.abs(Number(a) - Number(b)) <= tol;
}

function firstExisting(...candidates) {
  for (const c of candidates) {
    if (exists(c)) return c;
  }
  return null;
}

/**
 * Load latest approved ADP canonical report package for a property.
 */
export function loadApprovedAdpPackage(propertyId) {
  const id = String(propertyId || "").trim();
  for (const dir of APPROVED_PKG_DIRS) {
    const abs = path.join(dir, `${id}.approved-package.json`);
    if (!exists(abs)) continue;
    const pkg = readJson(abs);
    return {
      ok: true,
      propertyId: id,
      absPath: abs,
      relativePath: path.relative(ROOT, abs).replace(/\\/g, "/"),
      packageSha256: sha256File(abs),
      packageId: `${id}:${pkg.measurementPeriodId}`,
      periodId: pkg.measurementPeriodId || pkg.periodId || null,
      methodologyId: pkg.methodologyId || pkg.certifiedMeasurement?.methodologyId || null,
      methodologyFingerprintSha256:
        pkg.methodologyFingerprintSha256 ||
        pkg.certifiedMeasurement?.methodologyFingerprintSha256 ||
        null,
      contentFingerprintSha256: pkg.contentFingerprintSha256 || null,
      pdfArtifactSha256: pkg.pdfArtifactSha256 || null,
      pdfPath: pkg.pdfPath || null,
      reportContentVersion: pkg.reportContentVersion || null,
      reportTemplateVersion: pkg.reportTemplateVersion || null,
      lifecycleStatus: pkg.lifecycleStatus || null,
      approvedAt: pkg.approvedAt || null,
      publishedAt: pkg.publishedAt || null,
      certifiedMeasurement: pkg.certifiedMeasurement || null,
      certificationStamp: pkg.certificationStamp || null,
      raw: pkg,
    };
  }
  return { ok: false, propertyId: id, error: "APPROVED_PACKAGE_MISSING" };
}

/**
 * Load published-current registry entry (customer-live ADP pointer).
 */
export function loadPublishedCurrentAdp(propertyId) {
  const id = String(propertyId || "").trim();
  for (const regPath of PUBLISHED_REGISTRY_CANDIDATES) {
    if (!exists(regPath)) continue;
    const reg = readJson(regPath);
    const props = reg.properties || reg.entries || [];
    const hit = props.find((p) => p.propertyId === id);
    if (!hit) continue;
    return {
      ok: true,
      propertyId: id,
      registryPath: regPath,
      periodId: hit.periodId || hit.latestPeriodId || null,
      considerationRate: normalizePct(hit.considerationRate),
      reportSha256: hit.reportSha256 || null,
      evidenceSha256: hit.evidenceSha256 || null,
      publishStatus: hit.publishStatus || null,
      latestPublishedAt: hit.latestPublishedAt || null,
      methodologyFingerprintSha256: hit.methodologyFingerprintSha256 || null,
      pdfArtifactSha256: hit.pdfArtifactSha256 || null,
      provenance: hit.provenance || null,
      entry: hit,
    };
  }
  return { ok: false, propertyId: id, error: "PUBLISHED_CURRENT_MISSING" };
}

/**
 * Load customer-facing ADP report snapshot for a period (when available).
 */
export function loadAdpCustomerReportSnapshot(propertyId, periodId) {
  const id = String(propertyId || "").trim();
  const period = String(periodId || "").trim();
  if (!id || !period) return { ok: false, error: "MISSING_IDS" };
  for (const root of SNAPSHOT_ROOTS) {
    const abs = path.join(root, id, `report-${period}.json`);
    if (!exists(abs)) continue;
    const raw = readJson(abs);
    const payload = raw.payload || raw;
    const cons = payload.executiveMetrics?.considerationRate || {};
    const scen = payload.executiveMetrics?.scenarioPresence || {};
    const rg = payload.realityGap || {};
    const peer =
      payload.executiveMetrics?.certifiedPeerPosition ||
      payload.brandPortfolioPosition ||
      payload.certifiedPeerPosition ||
      null;
    let realityRate = null;
    if (rg.recognizedCount != null && rg.totalAttributes) {
      realityRate =
        Math.round((Number(rg.recognizedCount) / Number(rg.totalAttributes)) * 1000) / 10;
    } else if (rg.coverageRate != null) {
      realityRate = normalizePct(rg.coverageRate);
    }
    return {
      ok: true,
      absPath: abs,
      sha256: sha256File(abs),
      periodId: payload.period?.periodId || period,
      metrics: {
        aiConsideration: {
          rate: normalizePct(cons.rate),
          numerator: cons.presentObservations ?? null,
          denominator: cons.comparableObservations ?? null,
          methodologyId: cons.methodologyId || null,
        },
        scenarioPresence: {
          rate: normalizePct(scen.rate),
          numerator: scen.capturedScenarios ?? null,
          denominator: scen.eligibleScenarios ?? null,
          methodologyId: scen.methodologyId || null,
        },
        realityCoverage: {
          rate: realityRate,
          numerator: rg.recognizedCount ?? null,
          denominator: rg.totalAttributes ?? null,
        },
        peerPosition: {
          value: peer?.value || peer?.display || null,
          meta: peer?.meta || null,
        },
      },
      methodologyFingerprintSha256:
        payload.measurementMethodology?.methodologyFingerprintSha256 || null,
      realityGap: rg,
      payload,
    };
  }
  return { ok: false, propertyId: id, periodId: period, error: "SNAPSHOT_MISSING" };
}

/**
 * Resolve which ADP package the customer would receive alongside AVA.
 *
 * Default selection policy (joint delivery):
 *   1. Live published-current entry when present
 *   2. Else latest approved package
 *
 * Override with options.intendedDelivery = "approved" | "published" | periodId
 */
export function resolveCustomerFacingAdpForAvaDelivery(propertyId, options = {}) {
  const approved = loadApprovedAdpPackage(propertyId);
  const published = loadPublishedCurrentAdp(propertyId);
  const avaPin = loadAuthoritativeAdpReference(propertyId);

  let selectedSource = null;
  let selectedPeriodId = null;
  let selectedMetrics = null;
  let selectedFingerprint = null;
  let selectedPackageId = null;
  let selectedPdfSha = null;
  let selectedPdfPath = null;
  const intended = String(options.intendedDelivery || "published-prefer").trim();

  if (intended === "approved" && approved.ok) {
    selectedSource = "APPROVED_PACKAGE";
    selectedPeriodId = approved.periodId;
    selectedFingerprint = approved.methodologyFingerprintSha256;
    selectedPackageId = approved.packageId;
    selectedPdfSha = approved.pdfArtifactSha256;
    selectedPdfPath = approved.pdfPath;
  } else if (
    (intended === "published" || intended === "published-prefer") &&
    published.ok &&
    published.periodId
  ) {
    selectedSource = "PUBLISHED_CURRENT";
    selectedPeriodId = published.periodId;
    selectedFingerprint = published.methodologyFingerprintSha256;
    selectedPackageId = `${propertyId}:${published.periodId}`;
    selectedPdfSha = published.pdfArtifactSha256;
  } else if (approved.ok) {
    selectedSource = "APPROVED_PACKAGE_FALLBACK";
    selectedPeriodId = approved.periodId;
    selectedFingerprint = approved.methodologyFingerprintSha256;
    selectedPackageId = approved.packageId;
    selectedPdfSha = approved.pdfArtifactSha256;
    selectedPdfPath = approved.pdfPath;
  } else if (/^adp_period_/i.test(intended)) {
    selectedSource = "EXPLICIT_PERIOD";
    selectedPeriodId = intended;
    selectedPackageId = `${propertyId}:${intended}`;
  }

  const snap = selectedPeriodId
    ? loadAdpCustomerReportSnapshot(propertyId, selectedPeriodId)
    : { ok: false };

  if (snap.ok) {
    selectedMetrics = snap.metrics;
    selectedFingerprint =
      selectedFingerprint || snap.methodologyFingerprintSha256 || null;
  } else if (
    approved.ok &&
    approved.periodId === selectedPeriodId &&
    approved.certifiedMeasurement
  ) {
    selectedMetrics = {
      aiConsideration: {
        rate: normalizePct(approved.certifiedMeasurement.considerationRate),
        numerator: approved.certifiedMeasurement.presentObservations ?? null,
        denominator: approved.certifiedMeasurement.comparableObservations ?? null,
        methodologyId: approved.certifiedMeasurement.methodologyId || null,
      },
      scenarioPresence: null,
      realityCoverage: null,
      peerPosition: null,
    };
  } else if (published.ok && published.periodId === selectedPeriodId) {
    selectedMetrics = {
      aiConsideration: {
        rate: published.considerationRate,
        numerator: null,
        denominator: null,
        methodologyId: null,
      },
      scenarioPresence: null,
      realityCoverage: null,
      peerPosition: null,
    };
  }

  // When published period equals approved period, enrich from approved package
  if (
    approved.ok &&
    approved.periodId === selectedPeriodId &&
    selectedMetrics?.aiConsideration &&
    selectedMetrics.aiConsideration.numerator == null &&
    approved.certifiedMeasurement
  ) {
    selectedMetrics.aiConsideration = {
      rate: normalizePct(approved.certifiedMeasurement.considerationRate),
      numerator: approved.certifiedMeasurement.presentObservations ?? null,
      denominator: approved.certifiedMeasurement.comparableObservations ?? null,
      methodologyId: approved.certifiedMeasurement.methodologyId || null,
    };
    selectedFingerprint =
      selectedFingerprint || approved.methodologyFingerprintSha256 || null;
    selectedPdfPath = selectedPdfPath || approved.pdfPath;
    selectedPdfSha = selectedPdfSha || approved.pdfArtifactSha256;
  }

  return {
    contract: ADP_AVA_PERMANENT_CROSS_REPORT_CERTIFICATION,
    propertyId,
    approved,
    published,
    avaPin: avaPin.ok
      ? {
          ok: true,
          periodId: avaPin.periodId,
          baselineSha256: avaPin.baselineSha256,
          sourceSha256: avaPin.sourceSha256,
          metrics: avaPin.metrics,
          methodologyId: avaPin.metrics?.aiConsideration?.methodologyId || null,
        }
      : { ok: false, error: avaPin.error },
    selected: {
      source: selectedSource,
      periodId: selectedPeriodId,
      packageId: selectedPackageId,
      methodologyFingerprintSha256: selectedFingerprint,
      metrics: selectedMetrics,
      pdfArtifactSha256: selectedPdfSha,
      pdfPath: selectedPdfPath,
      snapshotOk: snap.ok === true,
      snapshotPath: snap.absPath || null,
      snapshotSha256: snap.sha256 || null,
    },
    alignment: {
      avaEqualsSelected:
        avaPin.ok && selectedPeriodId && avaPin.periodId === selectedPeriodId,
      avaEqualsApproved:
        avaPin.ok && approved.ok && avaPin.periodId === approved.periodId,
      avaEqualsPublished:
        avaPin.ok && published.ok && avaPin.periodId === published.periodId,
      approvedEqualsPublished:
        approved.ok &&
        published.ok &&
        approved.periodId === published.periodId,
      newerApprovedThanAva:
        avaPin.ok &&
        approved.ok &&
        approved.periodId &&
        avaPin.periodId &&
        approved.periodId !== avaPin.periodId,
      newerApprovedThanPublished:
        approved.ok &&
        published.ok &&
        approved.periodId &&
        published.periodId &&
        approved.periodId !== published.periodId,
    },
  };
}

function extractAvaKpis(payload) {
  const out = {
    aiConsideration: null,
    scenarioPresence: null,
    peerPosition: null,
    realityCoverage: null,
  };
  for (const kpi of payload?.kpis || []) {
    const label = String(kpi.label || "");
    if (/AI Consideration/i.test(label)) {
      out.aiConsideration = {
        rate: normalizePct(kpi.value),
        meta: kpi.meta || null,
      };
    } else if (/Scenario Presence/i.test(label)) {
      out.scenarioPresence = {
        rate: normalizePct(kpi.value),
        meta: kpi.meta || null,
      };
    } else if (/Peer Position|Certified Peer/i.test(label)) {
      out.peerPosition = {
        value: String(kpi.value || "").replace(/\s+/g, " ").trim(),
        meta: kpi.meta || null,
      };
    } else if (/Reality Coverage/i.test(label)) {
      out.realityCoverage = { rate: normalizePct(kpi.value) };
    }
  }
  return out;
}

function periodDisclosedInPayload(payload, periodId) {
  if (!periodId) return false;
  const reported =
    payload?.reporting?.adpPeriodId ||
    payload?.adpPeriodId ||
    payload?.crossReportIdentity?.adpPeriodId ||
    null;
  if (reported && reported === periodId) return true;
  const text = JSON.stringify(payload || {});
  return text.includes(periodId);
}

async function extractPdfMetricMentions(pdfPath, expectedRates = []) {
  if (!exists(pdfPath)) {
    return { ok: false, error: "PDF_MISSING" };
  }
  try {
    const pdfParse = (await import("pdf-parse")).default;
    const data = await pdfParse(fs.readFileSync(pdfPath));
    const text = String(data.text || "");
    const found = [];
    for (const rate of expectedRates) {
      if (rate == null) continue;
      const variants = [
        String(rate),
        Number(rate).toFixed(1),
        `${rate}%`,
        `${Number(rate).toFixed(1)}%`,
      ];
      const hit = variants.some((v) => text.includes(v));
      found.push({ rate, present: hit });
    }
    return {
      ok: true,
      sha256: sha256File(pdfPath),
      pageCount: data.numpages || null,
      mentions: found,
      allExpectedPresent: found.every((f) => f.present),
    };
  } catch (err) {
    return { ok: false, error: err.message || String(err) };
  }
}

/**
 * Build durable cross-report identity binding for an AVA payload.
 */
export function buildAvaAdpCrossReportIdentity(payload, resolution, certification) {
  const avaPin = resolution.avaPin;
  const selected = resolution.selected;
  return {
    contract: ADP_AVA_PERMANENT_CROSS_REPORT_CERTIFICATION,
    propertyId: payload?.property?.propertyId || resolution.propertyId,
    hotelName: payload?.property?.name || null,
    adpPeriodId: avaPin?.periodId || null,
    approvedAdpPackageId: resolution.approved?.packageId || null,
    selectedCustomerFacingAdpPackageId: selected?.packageId || null,
    selectedCustomerFacingAdpPeriodId: selected?.periodId || null,
    adpMeasurementFingerprint:
      selected?.methodologyFingerprintSha256 ||
      resolution.approved?.methodologyFingerprintSha256 ||
      null,
    adpMethodologyVersion:
      selected?.metrics?.aiConsideration?.methodologyId ||
      resolution.approved?.methodologyId ||
      avaPin?.methodologyId ||
      null,
    adpCustomerReportVersion:
      resolution.approved?.reportContentVersion || null,
    avaReportVersion:
      payload?.contractVersion ||
      payload?.reporting?.templateVersion ||
      "AI_VISIBILITY_AUDIT_ACTION_PLAN_PDF_V1",
    avaBaselineSha256: avaPin?.baselineSha256 || null,
    avaSourceSha256: avaPin?.sourceSha256 || null,
    certificationStatus: certification?.status || null,
    certifiedAt: certification?.certifiedAt || null,
    certificationId: certification?.certificationId || null,
    artifactHashes: certification?.artifactHashes || null,
  };
}

/**
 * Certify one AVA payload against the customer-facing ADP package selected
 * for joint delivery.
 */
export async function certifyAvaAgainstCustomerFacingAdp(payload, options = {}) {
  const propertyId = payload?.property?.propertyId;
  const defects = [];
  const metricChecks = [];
  const certifiedAt = new Date().toISOString();

  if (!propertyId) {
    return {
      ok: false,
      status: ADP_AVA_CERTIFICATION_STATUS.CERTIFICATION_BLOCKED,
      defects: [{ type: "MISSING_PROPERTY_ID", severity: "CRITICAL" }],
      certifiedAt,
    };
  }

  const resolution = resolveCustomerFacingAdpForAvaDelivery(propertyId, options);
  const leanParity = reconcileAvaPayloadAgainstAdp(payload, {
    checkApprovedPackageDrift: true,
  });

  if (!resolution.avaPin?.ok) {
    defects.push({
      type: "AVA_ADP_PIN_MISSING",
      severity: "CRITICAL",
      message: resolution.avaPin?.error || "lean pin missing",
    });
  }
  if (!resolution.selected?.periodId) {
    defects.push({
      type: "CUSTOMER_FACING_ADP_MISSING",
      severity: "CRITICAL",
      message: "No approved or published ADP package resolved for delivery",
    });
  }
  if (!leanParity.ok) {
    for (const d of leanParity.defects || []) {
      defects.push({
        type: `LEAN_PIN_${d.type}`,
        severity: d.severity || "HIGH",
        detail: d,
      });
    }
  }

  const avaKpis = extractAvaKpis(payload);
  const selected = resolution.selected;
  const avaPeriod = resolution.avaPin?.periodId || null;
  const selectedPeriod = selected?.periodId || null;
  const periodsMatch = !!(avaPeriod && selectedPeriod && avaPeriod === selectedPeriod);
  const disclosed = periodDisclosedInPayload(payload, avaPeriod);

  // Metric parity vs selected customer-facing ADP (when same period)
  const compareMetric = (kind, avaRate, adpRate, extra = {}) => {
    if (avaRate == null || adpRate == null) {
      metricChecks.push({
        kind,
        ava: avaRate,
        adp: adpRate,
        match: null,
        skipped: true,
        ...extra,
      });
      return;
    }
    const match = pctClose(avaRate, adpRate);
    metricChecks.push({ kind, ava: avaRate, adp: adpRate, match, ...extra });
    if (periodsMatch && !match) {
      defects.push({
        type: "CUSTOMER_FACING_METRIC_MISMATCH",
        severity: "CRITICAL",
        metric: kind,
        ava: avaRate,
        adp: adpRate,
      });
    }
  };

  if (selected?.metrics?.aiConsideration?.rate != null) {
    compareMetric(
      "AI_CONSIDERATION",
      avaKpis.aiConsideration?.rate,
      selected.metrics.aiConsideration.rate,
      {
        numerator: selected.metrics.aiConsideration.numerator,
        denominator: selected.metrics.aiConsideration.denominator,
      }
    );
  }
  if (selected?.metrics?.scenarioPresence?.rate != null) {
    compareMetric(
      "SCENARIO_PRESENCE",
      avaKpis.scenarioPresence?.rate,
      selected.metrics.scenarioPresence.rate,
      {
        numerator: selected.metrics.scenarioPresence.numerator,
        denominator: selected.metrics.scenarioPresence.denominator,
      }
    );
  }
  if (selected?.metrics?.realityCoverage?.rate != null && avaKpis.realityCoverage?.rate != null) {
    compareMetric(
      "REALITY_COVERAGE",
      avaKpis.realityCoverage.rate,
      selected.metrics.realityCoverage.rate
    );
  }

  // Zero integrity from lean pin reconciliation (authoritative measurement evidence)
  const zeroIssues = (leanParity.zeros || []).filter(
    (z) =>
      z.zeroVerdict === ZERO_VERDICTS.UNSUPPORTED_ZERO ||
      z.zeroVerdict === ZERO_VERDICTS.METRIC_MISMATCH ||
      z.zeroVerdict === ZERO_VERDICTS.NOT_MEASURED
  );
  for (const z of zeroIssues) {
    defects.push({
      type: "ZERO_INTEGRITY_FAILURE",
      severity: "HIGH",
      label: z.label,
      verdict: z.zeroVerdict,
      reason: z.zeroReason,
    });
  }

  // Rendered output checks (best-effort when PDFs exist)
  let renderedParity = { ok: null, ava: null, adp: null };
  const avaPdfCandidates = [
    options.avaPdfPath,
    payload?.reporting?.pdfPath,
    path.join(
      ROOT,
      "reports/dealality-ai-visibility-audit-action-plan-pilot-v1/portfolio-v4-local-staging",
      String(payload?.property?.name || "")
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, "-")
        .replace(/^-|-$/g, ""),
      `Dealality - AI Visibility Audit & Action Plan V4 - ${payload?.property?.name} - NOVEMBER2026.pdf`
    ),
  ].filter(Boolean);

  // Prefer known slug from options
  if (options.avaPdfPath) {
    avaPdfCandidates.unshift(options.avaPdfPath);
  }
  const avaPdf = firstExisting(...avaPdfCandidates);
  const adpPdf = firstExisting(selected?.pdfPath, resolution.approved?.pdfPath);

  const expectedAvaRates = [
    avaKpis.aiConsideration?.rate,
    avaKpis.scenarioPresence?.rate,
  ].filter((x) => x != null);
  const expectedAdpRates = [
    selected?.metrics?.aiConsideration?.rate,
    selected?.metrics?.scenarioPresence?.rate,
  ].filter((x) => x != null);

  if (avaPdf) {
    renderedParity.ava = await extractPdfMetricMentions(avaPdf, expectedAvaRates);
    if (
      renderedParity.ava.ok &&
      periodsMatch &&
      renderedParity.ava.allExpectedPresent === false
    ) {
      defects.push({
        type: "AVA_PDF_VALUE_MISSING",
        severity: "HIGH",
        message: "AVA PDF does not contain expected KPI percentages from payload",
        mentions: renderedParity.ava.mentions,
      });
    }
  }
  if (adpPdf && periodsMatch) {
    renderedParity.adp = await extractPdfMetricMentions(adpPdf, expectedAdpRates);
  }
  renderedParity.ok =
    (!renderedParity.ava || renderedParity.ava.ok !== false) &&
    (!periodsMatch ||
      !renderedParity.ava ||
      renderedParity.ava.allExpectedPresent !== false);

  // Status decision
  let status;
  const critical = defects.filter((d) => d.severity === "CRITICAL");
  const blockingZero = defects.some((d) => d.type === "ZERO_INTEGRITY_FAILURE");
  const metricMismatch = defects.some((d) => d.type === "CUSTOMER_FACING_METRIC_MISMATCH");

  if (
    critical.length ||
    blockingZero ||
    metricMismatch ||
    !resolution.selected?.periodId ||
    !resolution.avaPin?.ok
  ) {
    if (
      !resolution.selected?.periodId ||
      !resolution.avaPin?.ok ||
      critical.some((d) =>
        /MISSING_PROPERTY|AVA_ADP_PIN_MISSING|CUSTOMER_FACING_ADP_MISSING|LEAN_PIN_ADP_BASELINE/i.test(
          d.type
        )
      )
    ) {
      status = ADP_AVA_CERTIFICATION_STATUS.CERTIFICATION_BLOCKED;
    } else if (periodsMatch && (metricMismatch || blockingZero)) {
      status = ADP_AVA_CERTIFICATION_STATUS.CERTIFICATION_BLOCKED;
    } else {
      status = ADP_AVA_CERTIFICATION_STATUS.RECONCILIATION_REQUIRED;
    }
  } else if (periodsMatch && leanParity.ok) {
    status = ADP_AVA_CERTIFICATION_STATUS.CERTIFIED_MATCH;
  } else if (!periodsMatch && leanParity.ok && disclosed) {
    // Accurate historical pin; not current-period match with selected delivery package
    status = ADP_AVA_CERTIFICATION_STATUS.CERTIFIED_HISTORICAL_REFERENCE;
    // Joint current-period delivery still requires reconciliation
    if (options.requireCurrentPeriodMatch === true) {
      status = ADP_AVA_CERTIFICATION_STATUS.RECONCILIATION_REQUIRED;
      defects.push({
        type: "HISTORICAL_PIN_NOT_VALID_FOR_CURRENT_JOINT_DELIVERY",
        severity: "HIGH",
        message: `AVA pin ${avaPeriod} ≠ selected customer ADP ${selectedPeriod}`,
      });
    }
  } else if (!periodsMatch && leanParity.ok && !disclosed) {
    status = ADP_AVA_CERTIFICATION_STATUS.RECONCILIATION_REQUIRED;
    defects.push({
      type: "PERIOD_NOT_DISCLOSED",
      severity: "HIGH",
      message: `AVA uses ${avaPeriod} while customer ADP is ${selectedPeriod}; period must be disclosed`,
    });
  } else {
    status = ADP_AVA_CERTIFICATION_STATUS.RECONCILIATION_REQUIRED;
  }

  // When historical is accurate but newer approved/published differs — flag for joint delivery
  if (
    status === ADP_AVA_CERTIFICATION_STATUS.CERTIFIED_HISTORICAL_REFERENCE &&
    resolution.alignment.newerApprovedThanAva
  ) {
    defects.push({
      type: "NEWER_APPROVED_ADP_EXISTS",
      severity: "MEDIUM",
      message: "Informational: newer approved ADP package exists; re-pin required before current-period joint delivery",
      avaPeriod,
      approvedPeriod: resolution.approved?.periodId,
      selectedPeriod,
    });
  }

  const artifactHashes = {
    avaPayloadSha256: sha256Text(JSON.stringify(payload)),
    avaBaselineSha256: resolution.avaPin?.baselineSha256 || null,
    avaSourceSha256: resolution.avaPin?.sourceSha256 || null,
    approvedPackageSha256: resolution.approved?.packageSha256 || null,
    selectedSnapshotSha256: selected?.snapshotSha256 || null,
    avaPdfSha256: renderedParity.ava?.sha256 || null,
    adpPdfSha256: renderedParity.adp?.sha256 || null,
  };

  const certificationId = sha256Text(
    [
      ADP_AVA_PERMANENT_CROSS_REPORT_CERTIFICATION,
      propertyId,
      avaPeriod,
      selectedPeriod,
      status,
      artifactHashes.avaPayloadSha256,
      artifactHashes.approvedPackageSha256,
      certifiedAt,
    ].join("|")
  );

  const result = {
    ok:
      status === ADP_AVA_CERTIFICATION_STATUS.CERTIFIED_MATCH ||
      (status === ADP_AVA_CERTIFICATION_STATUS.CERTIFIED_HISTORICAL_REFERENCE &&
        options.requireCurrentPeriodMatch !== true),
    currentPeriodMatch: status === ADP_AVA_CERTIFICATION_STATUS.CERTIFIED_MATCH,
    status,
    contract: ADP_AVA_PERMANENT_CROSS_REPORT_CERTIFICATION,
    parentContract: ADP_AVA_REFERENCE_CONTRACT,
    propertyId,
    hotelName: payload?.property?.name || null,
    certifiedAt,
    certificationId,
    resolution,
    leanParitySummary: leanParity.summary || null,
    metricChecks,
    zeroAudit: leanParity.zeros || [],
    renderedParity,
    defects,
    artifactHashes,
    jointPublicationAllowed:
      status === ADP_AVA_CERTIFICATION_STATUS.CERTIFIED_MATCH && defects.filter((d) => d.severity === "CRITICAL" || d.severity === "HIGH").length === 0,
  };

  result.crossReportIdentity = buildAvaAdpCrossReportIdentity(payload, resolution, result);
  return result;
}

/**
 * Gate helper — blocks CUSTOMER_READY / publication when certification fails.
 */
export function assertAvaAdpCrossReportCertificationForPublication(certification, opts = {}) {
  const requireCurrent = opts.requireCurrentPeriodMatch !== false;
  if (!certification) {
    const err = new Error("ADP_AVA_CROSS_REPORT_CERTIFICATION_MISSING");
    err.code = "ADP_AVA_CROSS_REPORT_CERTIFICATION_MISSING";
    throw err;
  }
  if (certification.status === ADP_AVA_CERTIFICATION_STATUS.CERTIFICATION_BLOCKED) {
    const err = new Error("ADP_AVA_CROSS_REPORT_CERTIFICATION_BLOCKED");
    err.code = "ADP_AVA_CROSS_REPORT_CERTIFICATION_BLOCKED";
    err.certification = certification;
    throw err;
  }
  if (certification.status === ADP_AVA_CERTIFICATION_STATUS.RECONCILIATION_REQUIRED) {
    const err = new Error("ADP_AVA_CROSS_REPORT_RECONCILIATION_REQUIRED");
    err.code = "ADP_AVA_CROSS_REPORT_RECONCILIATION_REQUIRED";
    err.certification = certification;
    throw err;
  }
  if (
    requireCurrent &&
    certification.status !== ADP_AVA_CERTIFICATION_STATUS.CERTIFIED_MATCH
  ) {
    const err = new Error("ADP_AVA_CURRENT_PERIOD_MATCH_REQUIRED");
    err.code = "ADP_AVA_CURRENT_PERIOD_MATCH_REQUIRED";
    err.certification = certification;
    throw err;
  }
  if (
    opts.expectedArtifactHashes &&
    certification.artifactHashes
  ) {
    for (const [k, v] of Object.entries(opts.expectedArtifactHashes)) {
      if (v && certification.artifactHashes[k] && certification.artifactHashes[k] !== v) {
        const err = new Error("ADP_AVA_CERTIFICATION_STALE_ARTIFACT_HASH");
        err.code = "ADP_AVA_CERTIFICATION_STALE_ARTIFACT_HASH";
        err.key = k;
        throw err;
      }
    }
  }
  return true;
}

/**
 * Detect CQA defects from permanent cross-report certification.
 */
export function detectAdpAvaPermanentCertificationDefects(payload, options = {}) {
  // Sync-compatible wrapper: run lean checks immediately; full async cert is for portfolio/E2E
  const propertyId = payload?.property?.propertyId;
  if (!propertyId) {
    return [
      {
        type: "ADP_AVA_PARITY_BLOCK",
        severity: "CRITICAL",
        section: "adp-cross-report",
        message: "Missing propertyId for permanent ADP↔AVA certification",
      },
    ];
  }
  const resolution = resolveCustomerFacingAdpForAvaDelivery(propertyId, options);
  const lean = reconcileAvaPayloadAgainstAdp(payload);
  const defects = [];
  if (!lean.ok) {
    defects.push({
      type: "ADP_AVA_PARITY_BLOCK",
      severity: "HIGH",
      section: "adp-cross-report",
      message: "Lean pin reconciliation failed — AVA must match its certified ADP pin",
      detail: lean.defects,
    });
  }
  if (!resolution.selected?.periodId) {
    defects.push({
      type: "ADP_AVA_PARITY_BLOCK",
      severity: "CRITICAL",
      section: "adp-cross-report",
      message: "No customer-facing ADP package resolved",
    });
  }
  const avaPeriod = resolution.avaPin?.periodId;
  const selectedPeriod = resolution.selected?.periodId;
  if (
    avaPeriod &&
    selectedPeriod &&
    avaPeriod !== selectedPeriod &&
    !periodDisclosedInPayload(payload, avaPeriod)
  ) {
    defects.push({
      type: "ADP_AVA_PARITY_BLOCK",
      severity: "HIGH",
      section: "adp-cross-report",
      message: `AVA period ${avaPeriod} differs from customer ADP ${selectedPeriod} without disclosure`,
    });
  }
  if (
    options.requireCurrentPeriodMatch === true &&
    avaPeriod &&
    selectedPeriod &&
    avaPeriod !== selectedPeriod
  ) {
    defects.push({
      type: "ADP_AVA_PARITY_BLOCK",
      severity: "HIGH",
      section: "adp-cross-report",
      message: "Current-period joint delivery requires AVA re-pin to selected customer ADP package",
    });
  }
  return defects;
}

export { listCertifiedAdpUniverse, ZERO_VERDICTS };
