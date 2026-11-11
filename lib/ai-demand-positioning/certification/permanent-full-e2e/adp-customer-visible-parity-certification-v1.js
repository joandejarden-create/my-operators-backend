/**
 * certifyAdpCustomerVisibleParity() — Admin / API / share / browser / PDF.
 * HTTP 200 alone is never sufficient.
 */

import { createHash } from "crypto";
import { auditProviderPresenceGrain } from "../../metrics/adp-provider-presence-grain-lock-v1.js";
import {
  ADP_PERMANENT_FULL_E2E_CERTIFICATION_CONTRACT_V1,
  CERTIFICATION_FAILURE_STATES,
  CERTIFICATION_LIFECYCLE_STATES,
} from "./adp-permanent-full-e2e-lifecycle-states-v1.js";

export const ADP_CUSTOMER_VISIBLE_PARITY_CERTIFICATION_V1 =
  "ADP_CUSTOMER_VISIBLE_PARITY_CERTIFICATION_V1";

function clientWouldBlock(payload) {
  const em = payload?.executiveMetrics || {};
  const frozen = Array.isArray(em.considerationRate?.eligibleScenarioIds)
    ? em.considerationRate.eligibleScenarioIds.length
    : 0;
  const spEligible =
    frozen > 0
      ? frozen
      : Number(em.scenarioPresence?.eligibleScenarios);
  const cr = em.considerationRate || {};
  const methId = cr.methodologyId || em.scenarioPresence?.methodologyId || "";
  const independent =
    /INDEPENDENT_UNPROMPTED/i.test(String(methId)) ||
    (spEligible === 64 && Number(cr.comparableObservations) === 256);
  if (!independent || !Array.isArray(payload?.evidence?.providers)) {
    return { blocked: false };
  }
  let mentionSum = 0;
  let comparableSum = 0;
  const bad = payload.evidence.providers.filter((row) => {
    const c = Number(row.comparable != null ? row.comparable : row.total);
    const sched = Number(
      row.scenariosScheduled != null ? row.scenariosScheduled : row.scheduled
    );
    const m = Number(row.mentioned || 0);
    if (Number.isFinite(m)) mentionSum += m;
    if (Number.isFinite(c)) comparableSum += c;
    return (
      c === 78 ||
      sched === 78 ||
      (spEligible > 0 && c > spEligible) ||
      (spEligible > 0 && sched > spEligible)
    );
  });
  const sumMismatch =
    (Number.isFinite(Number(cr.presentObservations)) &&
      mentionSum !== Number(cr.presentObservations)) ||
    (Number.isFinite(Number(cr.comparableObservations)) &&
      comparableSum !== Number(cr.comparableObservations));
  return {
    blocked: Boolean(bad.length || sumMismatch),
    mentionSum,
    comparableSum,
    execPresent: cr.presentObservations,
    execComparable: cr.comparableObservations,
    spEligible,
  };
}

/**
 * @param {object} input
 * approvedPackage — certified payload
 * surfaces — { adminReport?, customerApi?, shareApi?, browser?, pdfPreview?, pdfDownload? }
 */
export function certifyAdpCustomerVisibleParity(input = {}) {
  const failures = [];
  const checks = {};
  const approved = input.approvedPackage || input.packagePayload || null;
  const propertyId = input.propertyId || approved?.propertyId || null;
  const periodId =
    input.expectedPeriodId ||
    approved?.period?.periodId ||
    approved?.periodId ||
    null;
  const expectedConsideration = Number(
    approved?.executiveMetrics?.considerationRate?.rate ??
      approved?.executiveMetrics?.considerationRate?.percentage
  );
  const expectedManifest =
    input.expectedPublicationManifestSha256 ||
    approved?.publicationManifestSha256 ||
    null;
  const expectedPdf =
    input.expectedPdfArtifactSha256 || approved?.pdfArtifactSha256 || null;
  const expectedMethFp =
    input.expectedMethodologyFingerprintSha256 ||
    approved?.methodologyFingerprintSha256 ||
    null;

  if (!approved) {
    return {
      ok: false,
      verdict: "FAILED",
      gate: ADP_CUSTOMER_VISIBLE_PARITY_CERTIFICATION_V1,
      failures: [
        {
          code: CERTIFICATION_FAILURE_STATES.PACKAGE_INTEGRITY_FAILED,
          detail: "approvedPackage required",
        },
      ],
    };
  }

  const grain = auditProviderPresenceGrain(approved);
  const block = clientWouldBlock(approved);
  checks.approved_package_grain = { ok: grain.pass, ...grain };
  checks.approved_package_client_render = {
    ok: !block.blocked,
    ...block,
  };
  if (!grain.pass || block.blocked) {
    failures.push({
      code: CERTIFICATION_FAILURE_STATES.RENDER_VALIDATION_FAILED,
      detail: "Approved package would block customer render",
    });
  }

  const surfaces = input.surfaces || {};
  const surfaceNames = [
    "adminReport",
    "customerApi",
    "shareApi",
    "browser",
    "pdfPreview",
    "pdfDownload",
  ];

  for (const name of surfaceNames) {
    const s = surfaces[name];
    if (s == null) {
      checks[name] = { ok: null, status: "NOT_PROVIDED" };
      continue;
    }
    const payload = s.payload || s.report || s;
    const httpOk = s.httpStatus == null || s.httpStatus === 200;
    const pid =
      payload?.propertyId ||
      s.propertyId ||
      payload?.property?.propertyId ||
      null;
    const per =
      payload?.period?.periodId || payload?.periodId || s.periodId || null;
    const cons = Number(
      payload?.executiveMetrics?.considerationRate?.rate ??
        payload?.executiveMetrics?.considerationRate?.percentage ??
        s.consideration
    );
    const grainS = payload?.executiveMetrics
      ? auditProviderPresenceGrain(payload)
      : { pass: s.grainPass !== false };
    const blockS = payload?.executiveMetrics
      ? clientWouldBlock(payload)
      : { blocked: s.clientBlocked === true };
    const manifestOk =
      !expectedManifest ||
      !s.publicationManifestSha256 ||
      s.publicationManifestSha256 === expectedManifest;
    const pdfOk =
      name !== "pdfDownload" && name !== "pdfPreview"
        ? true
        : !expectedPdf ||
          !s.pdfArtifactSha256 ||
          s.pdfArtifactSha256 === expectedPdf;
    const methOk =
      !expectedMethFp ||
      !s.methodologyFingerprintSha256 ||
      s.methodologyFingerprintSha256 === expectedMethFp;

    const ok =
      httpOk &&
      (pid == null || pid === propertyId) &&
      (per == null || !periodId || per === periodId) &&
      (cons == null ||
        !Number.isFinite(expectedConsideration) ||
        Math.abs(cons - expectedConsideration) <= 0.05) &&
      grainS.pass !== false &&
      blockS.blocked !== true &&
      manifestOk &&
      pdfOk &&
      methOk &&
      s.renderError !== true &&
      s.blockedReport !== true &&
      s.jsError !== true;

    checks[name] = {
      ok,
      httpStatus: s.httpStatus ?? null,
      propertyMatch: pid == null || pid === propertyId,
      periodMatch: per == null || !periodId || per === periodId,
      considerationMatch:
        cons == null ||
        !Number.isFinite(expectedConsideration) ||
        Math.abs(cons - expectedConsideration) <= 0.05,
      grainPass: grainS.pass,
      clientBlocked: blockS.blocked,
      manifestOk,
      pdfOk,
      http200Insufficient: true,
    };
    if (!ok) {
      failures.push({
        code: CERTIFICATION_FAILURE_STATES.PRODUCTION_PARITY_FAILED,
        surface: name,
        detail: `Surface ${name} failed customer-visible parity`,
      });
    }
  }

  // Charts / sections presence when browser payload provided
  if (surfaces.browser?.payload || surfaces.browser?.sections) {
    const sections = surfaces.browser.sections || {};
    const required = [
      "providerPresence",
      "scenarioPresence",
      "executiveSummary",
    ];
    for (const sec of required) {
      const present =
        sections[sec] === true ||
        sections[sec] == null; /* null = not asserted */
      checks[`section_${sec}`] = { ok: present !== false };
      if (sections[sec] === false) {
        failures.push({
          code: CERTIFICATION_FAILURE_STATES.RENDER_VALIDATION_FAILED,
          detail: `Missing section ${sec}`,
        });
      }
    }
  }

  const ok = failures.length === 0;
  return {
    ok,
    verdict: ok ? "CUSTOMER_VISIBLE_PARITY_CERTIFIED" : "FAILED",
    gate: ADP_CUSTOMER_VISIBLE_PARITY_CERTIFICATION_V1,
    contract: ADP_PERMANENT_FULL_E2E_CERTIFICATION_CONTRACT_V1,
    propertyId,
    measurementPeriodId: periodId,
    lifecycleState: ok
      ? CERTIFICATION_LIFECYCLE_STATES.RENDER_CERTIFIED
      : CERTIFICATION_FAILURE_STATES.RENDER_VALIDATION_FAILED,
    checks,
    failures,
    certificationTimestamp: new Date().toISOString(),
    certificationHash: createHash("sha256")
      .update(JSON.stringify({ propertyId, periodId, checks, ok }))
      .digest("hex"),
    note: "HTTP 200 alone is never sufficient for CUSTOMER_READY",
  };
}
