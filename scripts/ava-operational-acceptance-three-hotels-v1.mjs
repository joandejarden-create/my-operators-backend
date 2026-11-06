#!/usr/bin/env node
/**
 * Three-hotel operational acceptance for permanent AVA runner.
 * No paid research. No production Airtable writes. No customer publication.
 */
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";
import { runPermanentAvaAudit } from "../lib/ai-visibility-audit-action-plan/ava-permanent-audit-runner-v1.js";
import {
  loadAuditRun,
  listAuditRunsForProperty,
} from "../lib/ai-visibility-audit-action-plan/ava-audit-run-archive-v1.js";
import { compareAvaAuditRuns } from "../lib/ai-visibility-audit-action-plan/ava-audit-history-compare-v1.js";
import { sha256File } from "../lib/ai-visibility-audit-action-plan/ava-audit-execution-contract-v1.js";
const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT_DIR = path.join(ROOT, "reports/ava-permanent-audit-runner-v1");
fs.mkdirSync(OUT_DIR, { recursive: true });

async function runHotelA() {
  const r = await runPermanentAvaAudit("adp_spice_island_beach_resort", {
    mode: "certify-only",
  });
  return {
    hotel: "Spice Island Beach Resort",
    propertyId: "adp_spice_island_beach_resort",
    purpose: "current-period CERTIFIED_MATCH candidate",
    ok: r.ok === true,
    exitCode: r.exitCode,
    auditRunId: r.auditRunId,
    crossReportStatus: r.crossReport?.status,
    jointPublicationAllowed: r.crossReport?.jointPublicationAllowed === true,
    certificationStatus: r.identity?.certificationStatus,
    adpPeriodId: r.identity?.adpPeriodId,
    adpPackageId: r.identity?.adpPackageId,
    adpFingerprint: r.identity?.adpCertificationFingerprint,
    pdfSha256: r.pdfSha256,
    contentSha256: r.contentSha256,
    archiveDir: r.archiveDir,
    adminRegistration: r.adminRegistration,
    airtableSync: r.airtableSync,
    customerQuality: r.customerQuality,
    defects: r.crossReport?.defects || [],
    lifecycle: r.lifecycle,
  };
}

async function runHotelB() {
  // After 17-hotel current-period refresh, Cambridge is pinned to current ADP
  // (…20261105215424_e93d82). Historical measured-zero handling is covered by
  // permanent-runner regression 5b + AC Coruña versioning/compare below.
  const r = await runPermanentAvaAudit("adp_cambridge_beaches_bermuda", {
    mode: "certify-only",
  });
  const periodAligned =
    Boolean(r.identity?.adpPeriodId) &&
    r.identity?.adpPeriodId === r.inputManifest?.adp?.selectedPeriodId;
  const isCurrentMatch =
    r.crossReport?.status === "CERTIFIED_MATCH" && periodAligned;
  const wronglyShownAsCurrentMatch =
    r.crossReport?.status === "CERTIFIED_MATCH" && !periodAligned;
  return {
    hotel: "Cambridge Beaches Resort & Spa",
    propertyId: "adp_cambridge_beaches_bermuda",
    purpose: "current-period CERTIFIED_MATCH after ADP re-pin",
    ok:
      r.ok === true &&
      isCurrentMatch &&
      r.crossReport?.jointPublicationAllowed === true &&
      !wronglyShownAsCurrentMatch,
    exitCode: r.exitCode,
    auditRunId: r.auditRunId,
    crossReportStatus: r.crossReport?.status,
    certificationStatus: r.identity?.certificationStatus,
    jointPublicationAllowed: r.crossReport?.jointPublicationAllowed === true,
    avaPeriodId: r.identity?.adpPeriodId,
    selectedCustomerPeriodId: r.inputManifest?.adp?.selectedPeriodId,
    historicalExplicit: false,
    currentPeriodCertifiedMatch: isCurrentMatch,
    notSilentCurrentMatch: !wronglyShownAsCurrentMatch,
    pdfSha256: r.pdfSha256,
    archiveDir: r.archiveDir,
    adminRegistration: r.adminRegistration,
    defects: r.crossReport?.defects || [],
  };
}

async function runHotelC() {
  const propertyId = "adp_ac_hotel_a_coruna";
  const v1 = await runPermanentAvaAudit(propertyId, { mode: "certify-only" });
  // Controlled fixture difference for compare: clone archived payload, tweak one finding title priority in a NEW run via reproduce+mutate path
  // Instead: second certify-only produces distinct run id with same content (versioning), then compare documents no invented research diffs.
  const v2 = await runPermanentAvaAudit(propertyId, { mode: "certify-only" });

  const loaded1 = loadAuditRun(v1.auditRunId);
  const loaded2 = loadAuditRun(v2.auditRunId);
  const runs = listAuditRunsForProperty(propertyId);
  const latest = runs.find((r) => r.isLatest);

  // Controlled compare fixture: write a sibling summary-only mutated payload compare using compare engine after patching a copy in memory is not archived.
  // Create a controlled third archive? Spec says use controlled fixtures — mutate v2 payload priority in a temp compare by saving isn't allowed overwrite.
  // Approach: compare v1 vs v2 (may be identical findings) AND run a synthetic compare fixture file for engine capability proof.
  const cmp = compareAvaAuditRuns(v1.auditRunId, v2.auditRunId);

  const syntheticDir = path.join(OUT_DIR, "fixtures");
  fs.mkdirSync(syntheticDir, { recursive: true });
  const earlierSyn = structuredClone(loaded1.payload);
  const laterSyn = structuredClone(loaded1.payload);
  const actions = laterSyn.customerActionRegister?.actions || laterSyn.actions || [];
  if (actions[0]) {
    actions[0] = {
      ...actions[0],
      priority: actions[0].priority === "HIGH" ? "MEDIUM" : "HIGH",
      stableActionId: actions[0].actionId || actions[0].stableActionId || "action_0",
    };
  }
  laterSyn.priorityFindings = [
    ...(laterSyn.priorityFindings || laterSyn.findings || []),
    {
      stableFindingId: "SYNTHETIC_NEW_FINDING_OA_V1",
      title: "Controlled fixture finding for compare engine",
      priority: "LOW",
      status: "OBSERVED",
      body: "Synthetic finding injected only into compare fixture — not a live research claim.",
    },
  ];
  // Save synthetic runs into isolated fixture archive roots via env
  const fixtureEnv = {
    ...process.env,
    AVA_AUDIT_RUNS_ROOT: path.join(OUT_DIR, "fixture-audit-runs"),
  };
  const { saveImmutableAuditRun } = await import(
    "../lib/ai-visibility-audit-action-plan/ava-audit-run-archive-v1.js"
  );
  const { buildAuditExecutionIdentity, newAuditRunId } = await import(
    "../lib/ai-visibility-audit-action-plan/ava-audit-execution-contract-v1.js"
  );
  const eId = newAuditRunId(propertyId);
  const lId = newAuditRunId(propertyId);
  const eIdent = buildAuditExecutionIdentity({
    propertyId,
    hotelName: "AC Hotel A Coruña",
    auditRunId: eId,
    certificationStatus: "HISTORICAL_REFERENCE",
    executionStatus: "AUDIT_CERTIFIED",
  });
  const lIdent = buildAuditExecutionIdentity({
    propertyId,
    hotelName: "AC Hotel A Coruña",
    auditRunId: lId,
    certificationStatus: "HISTORICAL_REFERENCE",
    executionStatus: "AUDIT_CERTIFIED",
    previousVersionRef: eId,
  });
  // copy pdfs
  saveImmutableAuditRun({
    identity: eIdent,
    inputManifest: { mode: "fixture", note: "compare-engine-earlier" },
    payload: earlierSyn,
    certification: { status: "FIXTURE" },
    summary: {},
    pdfAbsPath: loaded1.pdfPath,
    env: fixtureEnv,
  });
  saveImmutableAuditRun({
    identity: lIdent,
    inputManifest: { mode: "fixture", note: "compare-engine-later" },
    payload: laterSyn,
    certification: { status: "FIXTURE" },
    summary: {},
    pdfAbsPath: loaded2.pdfPath,
    env: fixtureEnv,
  });
  const fixtureCmp = compareAvaAuditRuns(eId, lId, fixtureEnv);

  const pdf1Sha = loaded1.pdfPath ? sha256File(loaded1.pdfPath) : null;
  const pdf2Sha = loaded2.pdfPath ? sha256File(loaded2.pdfPath) : null;

  return {
    hotel: "AC Hotel A Coruña",
    propertyId,
    purpose: "versioning + historical comparison",
    ok:
      v1.auditRunId !== v2.auditRunId &&
      loaded1.ok &&
      loaded2.ok &&
      pdf1Sha === loaded1.identity.pdfSha256 &&
      pdf2Sha === loaded2.identity.pdfSha256 &&
      latest?.auditRunId === v2.auditRunId &&
      fs.existsSync(loaded1.pdfPath) &&
      fs.existsSync(loaded2.pdfPath),
    version1: {
      auditRunId: v1.auditRunId,
      pdfSha256: v1.pdfSha256,
      contentSha256: v1.contentSha256,
      crossReportStatus: v1.crossReport?.status,
      certificationStatus: v1.identity?.certificationStatus,
      archiveDir: v1.archiveDir,
      verifiedSha: pdf1Sha,
    },
    version2: {
      auditRunId: v2.auditRunId,
      pdfSha256: v2.pdfSha256,
      contentSha256: v2.contentSha256,
      crossReportStatus: v2.crossReport?.status,
      certificationStatus: v2.identity?.certificationStatus,
      archiveDir: v2.archiveDir,
      verifiedSha: pdf2Sha,
      isLatest: latest?.auditRunId === v2.auditRunId,
    },
    naturalCompare: cmp,
    controlledFixtureCompare: {
      earlierAuditRunId: eId,
      laterAuditRunId: lId,
      result: fixtureCmp,
      note: "Synthetic finding/priority change — not live research invention on production payloads",
    },
  };
}

async function reproduceProbe(auditRunId) {
  const original = loadAuditRun(auditRunId);
  const repro = await runPermanentAvaAudit(null, {
    mode: "reproduce",
    auditRunId,
  });
  let pdfMeta = null;
  if (original.pdfPath && repro.reproducedPdfPath) {
    const a = fs.readFileSync(original.pdfPath);
    const b = fs.readFileSync(repro.reproducedPdfPath);
    const extract = (buf) => {
      const s = buf.toString("latin1");
      const creation = (s.match(/\/CreationDate\s*\(([^)]*)\)/) || [])[1] || null;
      const mod = (s.match(/\/ModDate\s*\(([^)]*)\)/) || [])[1] || null;
      const producer = (s.match(/\/Producer\s*\(([^)]*)\)/) || [])[1] || null;
      return { creation, mod, producer, bytes: buf.length };
    };
    pdfMeta = {
      original: extract(a),
      reproduced: extract(b),
      firstDiffOffset: (() => {
        const n = Math.min(a.length, b.length);
        for (let i = 0; i < n; i++) if (a[i] !== b[i]) return i;
        return a.length === b.length ? null : Math.min(a.length, b.length);
      })(),
    };
  }
  let classification = "REPRODUCTION_FAILED";
  if (repro.semanticContentParity && repro.byteIdenticalPdf) {
    classification = "BYTE_IDENTICAL_REPRODUCTION";
  } else if (repro.semanticContentParity) {
    classification = "SEMANTICALLY_IDENTICAL_REPRODUCTION";
  }
  return {
    sourceAuditRunId: auditRunId,
    classification,
    contentMatch: repro.contentSha256?.match === true,
    pdfMatch: repro.pdfSha256?.match === true,
    note: repro.note,
    pdfMeta,
    inputFingerprints: {
      originalContentSha256: original.identity?.contentSha256,
      reproducedContentSha256: repro.contentSha256?.reproduced,
      originalAdpPackage: original.identity?.adpPackageId,
      originalAdpPeriod: original.identity?.adpPeriodId,
    },
  };
}

async function adminApiProbe(propertyId, auditRunId) {
  // Direct archive integrity for Admin contract: permanent PDF endpoint must serve exact bytes
  const loaded = loadAuditRun(auditRunId);
  if (!loaded.ok || !loaded.pdfPath) {
    return { ok: false, error: "ARCHIVE_MISSING" };
  }
  const expected = loaded.identity.pdfSha256;
  const actual = sha256File(loaded.pdfPath);
  const runs = listAuditRunsForProperty(propertyId);
  return {
    ok: expected === actual,
    propertyId,
    auditRunId,
    listedRuns: runs.length,
    latestRunId: runs.find((r) => r.isLatest)?.auditRunId || null,
    pdfSha256Expected: expected,
    pdfSha256Actual: actual,
    channelContract: "permanent-archive exact bytes; no regenerate",
  };
}

const report = {
  contract: "AVA_OPERATIONAL_ACCEPTANCE_THREE_HOTELS_V1",
  generatedAt: new Date().toISOString(),
  hotels: {},
  reproduction: null,
  admin: {},
};

console.log("Hotel A — Spice Island…");
report.hotels.spice = await runHotelA();
console.log("Hotel B — Cambridge…");
report.hotels.cambridge = await runHotelB();
console.log("Hotel C — AC Coruña…");
report.hotels.acCoruna = await runHotelC();
console.log("Reproduction probe (Spice)…");
report.reproduction = await reproduceProbe(report.hotels.spice.auditRunId);
console.log("Admin archive probes…");
report.admin.spice = await adminApiProbe(
  "adp_spice_island_beach_resort",
  report.hotels.spice.auditRunId
);
report.admin.cambridge = await adminApiProbe(
  "adp_cambridge_beaches_bermuda",
  report.hotels.cambridge.auditRunId
);
report.admin.acV1 = await adminApiProbe(
  "adp_ac_hotel_a_coruna",
  report.hotels.acCoruna.version1.auditRunId
);
report.admin.acV2 = await adminApiProbe(
  "adp_ac_hotel_a_coruna",
  report.hotels.acCoruna.version2.auditRunId
);

report.summary = {
  spiceCertifiedMatch: report.hotels.spice.crossReportStatus === "CERTIFIED_MATCH",
  cambridgeCurrentPeriodMatch:
    report.hotels.cambridge.currentPeriodCertifiedMatch === true &&
    report.hotels.cambridge.ok === true,
  // Retained for manifest compatibility; Cambridge is no longer a historical pin.
  cambridgeHistorical: false,
  acVersioning: report.hotels.acCoruna.ok === true,
  reproductionClass: report.reproduction.classification,
  adminPdfIntegrity:
    report.admin.spice.ok &&
    report.admin.cambridge.ok &&
    report.admin.acV1.ok &&
    report.admin.acV2.ok,
};

const out = path.join(OUT_DIR, "THREE_HOTEL_OPERATIONAL_ACCEPTANCE.json");
fs.writeFileSync(out, `${JSON.stringify(report, null, 2)}\n`);
console.log(JSON.stringify({ out, summary: report.summary }, null, 2));
process.exit(
  report.summary.spiceCertifiedMatch &&
    report.summary.cambridgeCurrentPeriodMatch &&
    report.summary.acVersioning &&
    report.summary.adminPdfIntegrity
    ? 0
    : 3
);
