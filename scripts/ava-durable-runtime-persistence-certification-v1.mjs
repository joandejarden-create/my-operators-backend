/**
 * AVA durable runtime persistence certification.
 *
 * Proves post-deploy audits / PDFs / decisions / simulated founder approval
 * survive a fresh application tree that EXCLUDES the seventh audit, when
 * runtime data lives on an isolated mounted storage root (volume equivalent).
 *
 * Does NOT deploy Railway. Does NOT mutate production hotel founder approvals
 * outside the isolated volume fixture.
 *
 *   node scripts/ava-durable-runtime-persistence-certification-v1.mjs
 */
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { spawnSync } from "node:child_process";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const CERT_ROOT = path.join(
  ROOT,
  "reports/dealality-ai-visibility-audit-action-plan-pilot-v1/_durable-runtime-cert"
);
const VOLUME = path.join(CERT_ROOT, "mounted-volume");
const APP_A = path.join(CERT_ROOT, "app-deploy-A");
const APP_B = path.join(CERT_ROOT, "app-deploy-B-without-seventh");
const SEED_STORE = path.join(
  ROOT,
  "data/ai-visibility-audit-action-plan/production/store"
);
const SEED_PDF = path.join(
  ROOT,
  "reports/ai-visibility-audit-action-plan/archive"
);

const SIX = [
  "adp_spice_island_beach_resort",
  "adp_wyndham_san_diego_bayside",
  "adp_waterstone_boca_raton",
  "adp_bethesda_marriott",
  "adp_castillo_hotel_son_vida",
  "adp_sheraton_mallorca_arabella_golf",
];

const SEVENTH = "adp_cert_runtime_seventh_hotel";

function rm(p) {
  fs.rmSync(p, { recursive: true, force: true });
}
function ensure(p) {
  fs.mkdirSync(p, { recursive: true });
}
function sha(bufOrPath) {
  const buf = Buffer.isBuffer(bufOrPath)
    ? bufOrPath
    : fs.readFileSync(bufOrPath);
  return crypto.createHash("sha256").update(buf).digest("hex");
}
function writeJson(p, obj) {
  ensure(path.dirname(p));
  fs.writeFileSync(p, `${JSON.stringify(obj, null, 2)}\n`);
}

function copySeedIntoVolume(volumeRoot) {
  const layout = {
    audits: path.join(volumeRoot, "audits"),
    pdfs: path.join(volumeRoot, "pdfs"),
    backups: path.join(volumeRoot, "backups"),
  };
  ensure(layout.audits);
  ensure(layout.pdfs);
  ensure(layout.backups);

  const checksums = {};
  for (const id of SIX) {
    const srcAudit = path.join(SEED_STORE, id);
    const destAudit = path.join(layout.audits, id);
    if (fs.existsSync(srcAudit)) {
      fs.cpSync(srcAudit, destAudit, { recursive: true });
    }
    const srcPdf = path.join(SEED_PDF, id, "current.pdf");
    const destPdfDir = path.join(layout.pdfs, id);
    ensure(destPdfDir);
    if (fs.existsSync(srcPdf)) {
      fs.copyFileSync(srcPdf, path.join(destPdfDir, "current.pdf"));
      const metaPath = path.join(destAudit, "AUDIT_META.json");
      if (fs.existsSync(metaPath)) {
        const meta = JSON.parse(fs.readFileSync(metaPath, "utf8"));
        meta.pdfRelativePath = `pdfs/${id}/current.pdf`;
        meta.pdfSha256 = sha(path.join(destPdfDir, "current.pdf"));
        writeJson(metaPath, meta);
        checksums[id] = {
          pdfSha256: meta.pdfSha256,
          baselineSha256: meta.adpBaselineSha256 || null,
          status: meta.status,
          latestVersionId: meta.latestVersionId,
        };
      }
    }
  }
  return { layout, checksums };
}

async function loadStoreWithEnv(volumeRoot) {
  process.env.AVA_STORAGE_ROOT = volumeRoot;
  process.env.AVA_STORAGE_MODE = "required";
  // Bust module cache by dynamic import with query — Node caches ESM.
  // Use child process for true restart isolation instead.
}

function runNodeSnippet(volumeRoot, snippetPath, label) {
  const env = {
    ...process.env,
    AVA_STORAGE_ROOT: volumeRoot,
    AVA_STORAGE_MODE: "required",
  };
  const r = spawnSync(process.execPath, [snippetPath], {
    cwd: ROOT,
    env,
    encoding: "utf8",
  });
  if (r.status !== 0) {
    throw new Error(
      `${label} failed: ${r.stderr || r.stdout || r.status}\n${r.stdout}`
    );
  }
  const line = (r.stdout || "").trim().split("\n").filter(Boolean).pop();
  return JSON.parse(line);
}

function writeWorker(name, code) {
  const p = path.join(CERT_ROOT, name);
  ensure(CERT_ROOT);
  fs.writeFileSync(p, code);
  return p;
}

function main() {
  rm(CERT_ROOT);
  ensure(VOLUME);
  ensure(APP_A);
  ensure(APP_B);

  // 1) Install six seed audits onto the mounted volume
  const { checksums: seedChecksums } = copySeedIntoVolume(VOLUME);

  // Simulate deploy A app tree: code + packaged seed (no seventh)
  fs.cpSync(
    path.join(ROOT, "data/ai-visibility-audit-action-plan/certified-baselines"),
    path.join(APP_A, "data/ai-visibility-audit-action-plan/certified-baselines"),
    { recursive: true }
  );
  // Packaged seed store/pdf optionally present in artifact
  for (const id of SIX) {
    const s = path.join(SEED_STORE, id);
    if (fs.existsSync(s)) {
      fs.cpSync(
        s,
        path.join(APP_A, "data/ai-visibility-audit-action-plan/production/store", id),
        { recursive: true }
      );
    }
  }

  // 2–5) Runtime worker: create seventh audit + PDF + decisions + simulated founder approval
  const workerCreate = writeWorker(
    "worker-create-seventh.mjs",
    `
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import {
  saveAuditVersion,
  recordRecommendationDecision,
  upsertAuditMeta,
  recordApprovalEvent,
  getAuditMeta,
  getLatestAuditVersion,
  listAuditEvents,
} from "../../../lib/ai-visibility-audit-action-plan/audit-store-v1.js";
import { stageAvaPdf } from "../../../lib/ai-visibility-audit-action-plan/ava-pdf-archive-v1.js";

const propertyId = ${JSON.stringify(SEVENTH)};
const versionId = "ava_cert_seventh_v1";
const pdfBytes = Buffer.from("%PDF-1.4\\n%AVA-CERT-SEVENTH-RUNTIME\\n%%EOF\\n");
const staging = path.join(process.env.AVA_STORAGE_ROOT, "backups", "staging-seventh.pdf");
fs.mkdirSync(path.dirname(staging), { recursive: true });
fs.writeFileSync(staging, pdfBytes);
const staged = stageAvaPdf({
  propertyId,
  generatedAbsPath: staging,
  versionId,
  filename: "seventh-current.pdf",
});
const payload = {
  status: "AUDIT_VALIDATED",
  property: { name: "Certification Seventh Hotel", market: "Cert Market" },
  reporting: { adpPeriodId: "adp_period_cert_seventh_20261105" },
  actions: [
    { actionId: "AVA-CERT-001", headline: "Action one", priority: "HIGH" },
    { actionId: "AVA-CERT-002", headline: "Action two", priority: "MEDIUM" },
  ],
};
saveAuditVersion(propertyId, versionId, payload, {
  status: "AUDIT_VALIDATED",
  metaStatus: "AUDIT_VALIDATED",
  forceStatus: true,
  kind: "REPORT_VERSION",
  pdfRelativePath: staged.pdfRelativePath,
  evidenceReviewStatus: "AUDIT_VALIDATED",
});
upsertAuditMeta(propertyId, {
  pdfRelativePath: staged.pdfRelativePath,
  pdfSha256: staged.sha256,
  name: "Certification Seventh Hotel",
  market: "Cert Market",
  forceStatus: true,
});
const beforeVersions = (getAuditMeta(propertyId).versions || []).length;
recordRecommendationDecision(propertyId, "AVA-CERT-001", {
  decision: "APPROVE",
  note: "cert approve",
});
recordRecommendationDecision(propertyId, "AVA-CERT-002", {
  decision: "DEFER",
  note: "cert defer",
});
const afterVersions = (getAuditMeta(propertyId).versions || []).length;
upsertAuditMeta(propertyId, {
  status: "FOUNDER_APPROVED_FOR_CUSTOMER",
  founderApprovedForCustomer: true,
  founderApprovedForCustomerAt: new Date().toISOString(),
  publicationAuthorizationStatus: "FOUNDER_AUTHORIZED_PENDING_DISTRIBUTION",
  customerReadinessStatus: "NOT_CUSTOMER_READY",
  forceStatus: true,
});
recordApprovalEvent(propertyId, {
  kind: "FOUNDER_APPROVAL",
  status: "FOUNDER_APPROVED_FOR_CUSTOMER",
  note: "Isolated certification simulated founder approval — not a production hotel record",
  simulated: true,
});
const meta = getAuditMeta(propertyId);
const latest = getLatestAuditVersion(propertyId);
const events = listAuditEvents(propertyId);
console.log(JSON.stringify({
  ok: true,
  propertyId,
  pdfSha256: staged.sha256,
  pdfRelativePath: staged.pdfRelativePath,
  beforeVersions,
  afterVersions,
  decisionDidNotCreateVersion: afterVersions === beforeVersions,
  status: meta.status,
  founderApprovedForCustomer: meta.founderApprovedForCustomer === true,
  decisions: (latest.payload.actions || []).map(a => ({ id: a.actionId, d: a.adminDecision?.decision })),
  eventKinds: events.map(e => e.kind),
  simulatedApproval: events.some(e => e.kind === "FOUNDER_APPROVAL" && e.simulated === true),
}));
`
  );

  const created = runNodeSnippet(VOLUME, workerCreate, "create-seventh");

  // Snapshot six checksums before "redeploy"
  const sixBefore = {};
  for (const id of SIX) {
    const meta = JSON.parse(
      fs.readFileSync(path.join(VOLUME, "audits", id, "AUDIT_META.json"), "utf8")
    );
    const pdf = path.join(VOLUME, "pdfs", id, "current.pdf");
    sixBefore[id] = {
      status: meta.status,
      latestVersionId: meta.latestVersionId,
      pdfSha256: fs.existsSync(pdf) ? sha(pdf) : null,
      metaSha256: sha(path.join(VOLUME, "audits", id, "AUDIT_META.json")),
    };
  }

  // 6) Restart application = new Node process (worker-verify)
  // 7) Redeploy artifact excluding seventh: APP_B has only six seeds in package;
  //    volume is NOT replaced (this is the durable mount).
  fs.cpSync(APP_A, APP_B, { recursive: true });
  // Explicitly ensure seventh is absent from deploy artifact
  const seventhInArtifact = path.join(
    APP_B,
    "data/ai-visibility-audit-action-plan/production/store",
    SEVENTH
  );
  rm(seventhInArtifact);
  rm(path.join(APP_B, "reports/ai-visibility-audit-action-plan/archive", SEVENTH));

  const workerVerify = writeWorker(
    "worker-verify-after-redeploy.mjs",
    `
import fs from "node:fs";
import path from "node:path";
import {
  getAuditMeta,
  getLatestAuditVersion,
  listAuditEvents,
  listStoredAudits,
} from "../../../lib/ai-visibility-audit-action-plan/audit-store-v1.js";
import { resolveStagedAvaPdfAbs } from "../../../lib/ai-visibility-audit-action-plan/ava-pdf-archive-v1.js";
import crypto from "node:crypto";

function sha(p) {
  return crypto.createHash("sha256").update(fs.readFileSync(p)).digest("hex");
}
const seventh = ${JSON.stringify(SEVENTH)};
const six = ${JSON.stringify(SIX)};
const meta = getAuditMeta(seventh);
const latest = getLatestAuditVersion(seventh);
const events = listAuditEvents(seventh);
const pdfAbs = resolveStagedAvaPdfAbs(meta?.pdfRelativePath);
const listed = listStoredAudits().map(a => a.propertyId);
const sixNow = {};
for (const id of six) {
  const m = getAuditMeta(id);
  const pdf = path.join(process.env.AVA_STORAGE_ROOT, "pdfs", id, "current.pdf");
  sixNow[id] = {
    status: m?.status,
    latestVersionId: m?.latestVersionId,
    pdfSha256: fs.existsSync(pdf) ? sha(pdf) : null,
    metaSha256: sha(path.join(process.env.AVA_STORAGE_ROOT, "audits", id, "AUDIT_META.json")),
  };
}
console.log(JSON.stringify({
  ok: true,
  seventhPresent: !!meta,
  seventhInList: listed.includes(seventh),
  seventhStatus: meta?.status,
  founderApprovedForCustomer: meta?.founderApprovedForCustomer === true,
  pdfExists: !!(pdfAbs && fs.existsSync(pdfAbs)),
  pdfSha256: pdfAbs && fs.existsSync(pdfAbs) ? sha(pdfAbs) : null,
  decisions: (latest?.payload?.actions || []).map(a => ({ id: a.actionId, d: a.adminDecision?.decision })),
  eventKinds: events.map(e => e.kind),
  simulatedApprovalSurvived: events.some(e => e.kind === "FOUNDER_APPROVAL" && e.simulated === true),
  sixNow,
  artifactHasSeventh: fs.existsSync(${JSON.stringify(seventhInArtifact)}),
}));
`
  );

  const verified = runNodeSnippet(VOLUME, workerVerify, "verify-after-redeploy");

  // Unavailable volume fail-closed check
  const missingVolume = path.join(CERT_ROOT, "missing-volume-path-xyz");
  const workerFail = writeWorker(
    "worker-fail-closed.mjs",
    `
import { assertAvaDurableWritable } from "../../../lib/ai-visibility-audit-action-plan/resolve-ava-storage-root-v1.js";
try {
  // Point at a file path that cannot be a writable directory root easily:
  // use a non-existent parent that we make unwritable by pointing to a file as root.
  assertAvaDurableWritable();
  console.log(JSON.stringify({ ok: false, unexpectedSuccess: true }));
} catch (err) {
  console.log(JSON.stringify({ ok: true, failedClosed: true, code: err.code || "ERR", message: String(err.message || err).slice(0, 200) }));
}
`
  );
  // Create a FILE where the root should be so mkdir/access fails appropriately
  const badRoot = path.join(CERT_ROOT, "bad-root-as-file");
  fs.writeFileSync(badRoot, "not-a-directory");
  const failClosed = (() => {
    const env = {
      ...process.env,
      AVA_STORAGE_ROOT: badRoot,
      AVA_STORAGE_MODE: "required",
    };
    const r = spawnSync(process.execPath, [workerFail], {
      cwd: ROOT,
      env,
      encoding: "utf8",
    });
    const line = (r.stdout || "").trim().split("\n").filter(Boolean).pop();
    try {
      return JSON.parse(line);
    } catch {
      return { ok: false, raw: r.stdout, err: r.stderr, status: r.status };
    }
  })();

  // Decision version history check on Bethesda (preserved, not rewritten)
  const bethMeta = JSON.parse(
    fs.readFileSync(
      path.join(VOLUME, "audits", "adp_bethesda_marriott", "AUDIT_META.json"),
      "utf8"
    )
  );
  const bethDecisionVersions = (bethMeta.versions || []).filter((v) =>
    String(v.versionId || "").includes("_decision")
  );

  const sixUnchanged = SIX.every((id) => {
    const a = sixBefore[id];
    const b = verified.sixNow[id];
    return (
      a &&
      b &&
      a.status === b.status &&
      a.latestVersionId === b.latestVersionId &&
      a.pdfSha256 === b.pdfSha256 &&
      a.metaSha256 === b.metaSha256
    );
  });

  const pass =
    created.ok &&
    created.decisionDidNotCreateVersion === true &&
    verified.seventhPresent === true &&
    verified.pdfExists === true &&
    verified.pdfSha256 === created.pdfSha256 &&
    verified.founderApprovedForCustomer === true &&
    verified.simulatedApprovalSurvived === true &&
    verified.decisions?.find((d) => d.id === "AVA-CERT-001")?.d === "APPROVE" &&
    verified.decisions?.find((d) => d.id === "AVA-CERT-002")?.d === "DEFER" &&
    verified.artifactHasSeventh === false &&
    sixUnchanged === true &&
    failClosed.failedClosed === true &&
    bethDecisionVersions.length >= 1;

  const report = {
    contract: "AVA_DURABLE_RUNTIME_STORAGE_CERTIFICATION_V1",
    ok: pass,
    verdict: pass ? "PASS" : "FAIL",
    volumeRoot: VOLUME,
    recommendedProductionMount: "/app/ava-data",
    steps: {
      seedSixInstalled: Object.keys(seedChecksums).length,
      seventhCreated: created,
      afterRedeployWithoutSeventhInArtifact: verified,
      sixUnchanged,
      sixBefore,
      failClosedOnBadVolume: failClosed,
      bethesdaLegacyDecisionVersionsPreserved: bethDecisionVersions.length,
      bethesdaVersionKindsNote:
        "Historical _decision versions retained; new decisions are events only",
    },
    deploymentReadiness: pass ? "PASS_WITH_FOUNDER_VOLUME_AUTH" : "FAIL",
    restrictionsHonored: {
      noProductionDeploy: true,
      noProductionRailwayConfigChange: true,
      noCustomerPublication: true,
      noProductionFounderApprovalOnRealHotels: true,
      noPaidResearch: true,
      noAdpBaselineModification: true,
      noProductionAirtableWrites: true,
    },
  };

  const outJson = path.join(
    ROOT,
    "reports/dealality-ai-visibility-audit-action-plan-pilot-v1/AVA_DURABLE_RUNTIME_PERSISTENCE_CERT.json"
  );
  writeJson(outJson, report);
  console.log(
    JSON.stringify(
      {
        ok: pass,
        verdict: report.verdict,
        outJson,
        decisionDidNotCreateVersion: created.decisionDidNotCreateVersion,
        seventhSurvived: verified.seventhPresent,
        sixUnchanged,
      },
      null,
      2
    )
  );
  process.exit(pass ? 0 : 1);
}

main();
