#!/usr/bin/env node
/**
 * Permanent ADP↔AVA cross-report certification — 38-hotel portfolio.
 *
 *   node scripts/ava-adp-permanent-cross-report-certification-v1.mjs
 *
 * Default intended delivery = published-current (what the customer gets for ADP).
 * Does not publish, deploy, or modify ADP measurements.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  ADP_AVA_PERMANENT_CROSS_REPORT_CERTIFICATION,
  ADP_AVA_CERTIFICATION_STATUS,
  certifyAvaAgainstCustomerFacingAdp,
  listCertifiedAdpUniverse,
  resolveCustomerFacingAdpForAvaDelivery,
} from "../lib/ai-visibility-audit-action-plan/adp-ava-permanent-cross-report-certification-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const PAYLOAD_DIR = path.join(ROOT, "lib/ai-visibility-audit-action-plan/payloads");
const REPORT = path.join(
  ROOT,
  "reports/dealality-ai-visibility-audit-action-plan-pilot-v1"
);
const STAGE = path.join(REPORT, "portfolio-v4-local-staging");
const MANIFEST = path.join(REPORT, "AVA_38_HOTEL_FINAL_MANIFEST.json");

function payloadFileFor(propertyId, manifestHotels) {
  const hit = (manifestHotels || []).find((h) => h.propertyId === propertyId);
  if (hit?.file) return hit.file;
  for (const f of fs.readdirSync(PAYLOAD_DIR).filter((x) => x.endsWith("-v1.json"))) {
    try {
      const p = JSON.parse(fs.readFileSync(path.join(PAYLOAD_DIR, f), "utf8"));
      if (p.property?.propertyId === propertyId) return f;
    } catch {
      /* skip */
    }
  }
  return null;
}

function resolveAvaPdf(file, hotelName) {
  const slug = String(file || "").replace(/-v1\.json$/i, "");
  const candidates = [
    path.join(
      STAGE,
      slug,
      `Dealality - AI Visibility Audit & Action Plan V4 - ${hotelName} - NOVEMBER2026.pdf`
    ),
    path.join(REPORT, slug, "pdf-staged-v4"),
  ];
  for (const c of candidates) {
    if (fs.existsSync(c) && c.endsWith(".pdf")) return c;
    if (fs.existsSync(c) && fs.statSync(c).isDirectory()) {
      const pdf = fs.readdirSync(c).find((f) => f.toLowerCase().endsWith(".pdf"));
      if (pdf) return path.join(c, pdf);
    }
  }
  // glob stage dir
  const stageDir = path.join(STAGE, slug);
  if (fs.existsSync(stageDir)) {
    const pdf = fs.readdirSync(stageDir).find((f) => f.toLowerCase().endsWith(".pdf"));
    if (pdf) return path.join(stageDir, pdf);
  }
  return null;
}

async function main() {
  fs.mkdirSync(REPORT, { recursive: true });
  const universe = listCertifiedAdpUniverse();
  const manifest = fs.existsSync(MANIFEST)
    ? JSON.parse(fs.readFileSync(MANIFEST, "utf8"))
    : { hotels: [] };

  const rows = [];
  const drift17 = [];

  for (const h of universe) {
    const file = payloadFileFor(h.propertyId, manifest.hotels);
    if (!file) {
      rows.push({
        propertyId: h.propertyId,
        hotelName: h.name,
        status: ADP_AVA_CERTIFICATION_STATUS.CERTIFICATION_BLOCKED,
        error: "AVA_PAYLOAD_MISSING",
      });
      continue;
    }
    const payload = JSON.parse(fs.readFileSync(path.join(PAYLOAD_DIR, file), "utf8"));
    const avaPdfPath = resolveAvaPdf(file, payload.property?.name || h.name);
    const cert = await certifyAvaAgainstCustomerFacingAdp(payload, {
      intendedDelivery: "published-prefer",
      avaPdfPath,
      requireCurrentPeriodMatch: false,
    });
    const res = cert.resolution;
    const row = {
      propertyId: cert.propertyId,
      hotelName: cert.hotelName,
      file,
      status: cert.status,
      currentPeriodMatch: cert.currentPeriodMatch,
      jointPublicationAllowed: cert.jointPublicationAllowed,
      certificationId: cert.certificationId,
      avaPeriodId: res.avaPin?.periodId || null,
      approvedPeriodId: res.approved?.periodId || null,
      publishedPeriodId: res.published?.periodId || null,
      selectedSource: res.selected?.source || null,
      selectedPeriodId: res.selected?.periodId || null,
      selectedPackageId: res.selected?.packageId || null,
      methodologyFingerprintSha256:
        res.selected?.methodologyFingerprintSha256 || null,
      alignment: res.alignment,
      metricChecks: cert.metricChecks,
      refsChecked: cert.leanParitySummary?.totalRefs ?? 0,
      matchingRefs: cert.leanParitySummary?.matchingRefs ?? 0,
      measuredZeros: cert.leanParitySummary?.measuredZeros ?? 0,
      unsupportedZeros: cert.leanParitySummary?.unsupportedZeros ?? 0,
      renderedOk: cert.renderedParity?.ok,
      defectTypes: [...new Set((cert.defects || []).map((d) => d.type))],
      defects: cert.defects,
      artifactHashes: cert.artifactHashes,
      crossReportIdentity: cert.crossReportIdentity,
    };
    rows.push(row);

    if (res.alignment?.newerApprovedThanAva) {
      const avaCons = res.avaPin?.metrics?.aiConsideration?.rate ?? null;
      const approvedCons =
        res.approved?.certifiedMeasurement?.considerationRate ?? null;
      const publishedCons = res.published?.considerationRate ?? null;
      const selectedCons = res.selected?.metrics?.aiConsideration?.rate ?? null;
      drift17.push({
        propertyId: h.propertyId,
        hotelName: h.name,
        avaPeriodId: res.avaPin?.periodId,
        approvedPeriodId: res.approved?.periodId,
        publishedPeriodId: res.published?.periodId,
        selectedForDelivery: res.selected?.periodId,
        selectedSource: res.selected?.source,
        fingerprintsMatch:
          !!res.avaPin?.baselineSha256 &&
          res.approved?.methodologyFingerprintSha256 != null
            ? null
            : null,
        avaConsideration: avaCons,
        approvedConsideration: approvedCons,
        publishedConsideration: publishedCons,
        selectedConsideration: selectedCons,
        numbersDifferFromApproved:
          avaCons != null &&
          approvedCons != null &&
          Math.abs(Number(avaCons) - Number(approvedCons)) > 0.55,
        numbersDifferFromPublished:
          avaCons != null &&
          publishedCons != null &&
          Math.abs(Number(avaCons) - Number(publishedCons)) > 0.55,
        avaRequiresRepinForApprovedJointDelivery: true,
        avaRequiresRepinForPublishedJointDelivery:
          res.avaPin?.periodId !== res.published?.periodId,
        certificationStatus: cert.status,
        recommendation:
          res.avaPin?.periodId === res.published?.periodId
            ? "CERTIFIED vs live published ADP; re-pin only before delivering the newer approved package with AVA"
            : "RECONCILE before joint delivery with current customer ADP",
      });
    }

    console.log(
      `${cert.status.padEnd(32)} ${h.name} ava=${String(res.avaPin?.periodId || "").slice(-14)} sel=${String(res.selected?.periodId || "").slice(-14)}`
    );
  }

  const counts = {
    CERTIFIED_MATCH: rows.filter(
      (r) => r.status === ADP_AVA_CERTIFICATION_STATUS.CERTIFIED_MATCH
    ).length,
    CERTIFIED_HISTORICAL_REFERENCE: rows.filter(
      (r) => r.status === ADP_AVA_CERTIFICATION_STATUS.CERTIFIED_HISTORICAL_REFERENCE
    ).length,
    RECONCILIATION_REQUIRED: rows.filter(
      (r) => r.status === ADP_AVA_CERTIFICATION_STATUS.RECONCILIATION_REQUIRED
    ).length,
    CERTIFICATION_BLOCKED: rows.filter(
      (r) => r.status === ADP_AVA_CERTIFICATION_STATUS.CERTIFICATION_BLOCKED
    ).length,
  };

  const blocked = rows.filter(
    (r) => r.status === ADP_AVA_CERTIFICATION_STATUS.CERTIFICATION_BLOCKED
  );
  const systemOk =
    rows.length === 38 &&
    blocked.length === 0 &&
    counts.CERTIFIED_MATCH +
      counts.CERTIFIED_HISTORICAL_REFERENCE +
      counts.RECONCILIATION_REQUIRED ===
      38;

  const summary = {
    contract: ADP_AVA_PERMANENT_CROSS_REPORT_CERTIFICATION,
    generatedAt: new Date().toISOString(),
    intendedDeliveryPolicy: "published-prefer",
    hotelCount: rows.length,
    ...counts,
    driftHotelCount: drift17.length,
    totalRefsChecked: rows.reduce((n, r) => n + (r.refsChecked || 0), 0),
    jointPublicationEligible: rows.filter((r) => r.jointPublicationAllowed).length,
    valuesCorrectedThisRun: 0,
    pdfsRegeneratedThisRun: 0,
    remainingCustomerFacingDiscrepancies: rows.filter(
      (r) =>
        r.status === ADP_AVA_CERTIFICATION_STATUS.RECONCILIATION_REQUIRED ||
        r.status === ADP_AVA_CERTIFICATION_STATUS.CERTIFICATION_BLOCKED
    ).length,
    verdict: systemOk
      ? "ADP_AVA_PERMANENT_CROSS_REPORT_CERTIFICATION_COMPLETE"
      : "ADP_AVA_CERTIFICATION_BLOCKED_WITH_EXACT_REASONS",
    blockingHotels: blocked.map((r) => ({
      propertyId: r.propertyId,
      hotelName: r.hotelName,
      defectTypes: r.defectTypes,
      error: r.error || null,
    })),
  };

  const out = {
    summary,
    matrix: rows,
    seventeenHotelDriftAnalysis: drift17,
  };

  fs.writeFileSync(
    path.join(REPORT, "AVA_ADP_PERMANENT_CROSS_REPORT_CERTIFICATION_MATRIX.json"),
    `${JSON.stringify(out, null, 2)}\n`
  );

  const md = `# Permanent ADP ↔ AVA Cross-Report Certification

**Verdict:** \`${summary.verdict}\`

**Contract:** \`${ADP_AVA_PERMANENT_CROSS_REPORT_CERTIFICATION}\`

Generated: ${summary.generatedAt}

## Policy

- Selected customer-facing ADP for joint delivery = **published-current** when Live, else latest **approved** package.
- \`CERTIFIED_MATCH\` = AVA pin equals selected customer ADP period and metrics agree (current-period parity).
- \`CERTIFIED_HISTORICAL_REFERENCE\` = AVA accurately reflects an older certified pin with period disclosure; not valid for current-period joint delivery without re-pin.
- \`RECONCILIATION_REQUIRED\` = period/package drift or missing disclosure before joint delivery.
- \`CERTIFICATION_BLOCKED\` = missing identity/package or confirmed integrity failure.

## Portfolio

| Status | Count |
|---|---:|
| CERTIFIED_MATCH | ${counts.CERTIFIED_MATCH} |
| CERTIFIED_HISTORICAL_REFERENCE | ${counts.CERTIFIED_HISTORICAL_REFERENCE} |
| RECONCILIATION_REQUIRED | ${counts.RECONCILIATION_REQUIRED} |
| CERTIFICATION_BLOCKED | ${counts.CERTIFICATION_BLOCKED} |
| Hotels checked | ${summary.hotelCount} |
| Joint-publication eligible (current match) | ${summary.jointPublicationEligible} |
| 17-hotel newer-approved drift set | ${summary.driftHotelCount} |
| ADP refs checked (lean pin) | ${summary.totalRefsChecked} |

## Seventeen-hotel drift (newer approved than AVA pin)

${
  drift17.length
    ? drift17
        .map(
          (d) =>
            `- **${d.hotelName}**: AVA \`${String(d.avaPeriodId).slice(-20)}\` (${d.avaConsideration}%) · approved \`${String(d.approvedPeriodId).slice(-20)}\` (${d.approvedConsideration}%) · published \`${String(d.publishedPeriodId || "").slice(-20)}\` (${d.publishedConsideration}%) · status=\`${d.certificationStatus}\` · ${d.recommendation}`
        )
        .join("\n")
    : "_None._"
}

## Blocking hotels

${
  summary.blockingHotels.length
    ? summary.blockingHotels
        .map((b) => `- ${b.hotelName}: ${(b.defectTypes || []).join(", ") || b.error}`)
        .join("\n")
    : "_None._"
}

## Enforcement

- Full E2E stage: \`ADP_AVA_PERMANENT_CROSS_REPORT_CERTIFICATION\`
- Publication gate: \`assertAvaAdpCrossReportCertificationForPublication\`
- Lifecycle: CUSTOMER_READY / PUBLISHED require current-period \`CERTIFIED_MATCH\` when joint ADP+AVA delivery is intended
`;

  fs.writeFileSync(
    path.join(REPORT, "AVA_ADP_PERMANENT_CROSS_REPORT_CERTIFICATION.md"),
    `${md}\n`
  );

  fs.writeFileSync(
    path.join(REPORT, "AVA_ADP_17_HOTEL_APPROVED_PACKAGE_DRIFT_ANALYSIS.json"),
    `${JSON.stringify({ generatedAt: summary.generatedAt, hotels: drift17 }, null, 2)}\n`
  );

  console.log(JSON.stringify(summary, null, 2));
  if (summary.verdict !== "ADP_AVA_PERMANENT_CROSS_REPORT_CERTIFICATION_COMPLETE") {
    process.exit(2);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
