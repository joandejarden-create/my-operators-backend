/**
 * Prove whether runtime AVA writes survive a fresh Railway-style redeploy
 * when they are NOT included in the new upload artifact.
 *
 * Does not deploy. Writes only under a disposable probe directory.
 *
 *   node scripts/ava-runtime-redeploy-survival-probe-v1.mjs
 */
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const PROBE = path.join(
  ROOT,
  "reports/dealality-ai-visibility-audit-action-plan-pilot-v1/_runtime-redeploy-survival-probe"
);

function rm(p) {
  fs.rmSync(p, { recursive: true, force: true });
}
function ensure(p) {
  fs.mkdirSync(p, { recursive: true });
}
function sha(buf) {
  return crypto.createHash("sha256").update(buf).digest("hex");
}
function writeJson(p, obj) {
  ensure(path.dirname(p));
  fs.writeFileSync(p, `${JSON.stringify(obj, null, 2)}\n`);
}

/**
 * Simulate Railway CLI replace:
 * - Start from prior packaged artifact (deploy A upload)
 * - Application later writes runtime records onto the live filesystem
 * - Deploy B uploads ONLY the new packaged artifact (without those runtime writes)
 * - Entire service filesystem is replaced by Deploy B tree
 */
function main() {
  rm(PROBE);
  const deployA = path.join(PROBE, "deploy-A-upload-artifact");
  const liveFs = path.join(PROBE, "live-railway-filesystem");
  const deployB = path.join(PROBE, "deploy-B-upload-artifact");

  // --- Deploy A packaged artifact (what we intentionally ship) ---
  const packagedStore = path.join(
    deployA,
    "data/ai-visibility-audit-action-plan/production/store/adp_spice_island_beach_resort"
  );
  const packagedPdf = path.join(
    deployA,
    "reports/ai-visibility-audit-action-plan/archive/adp_spice_island_beach_resort/current.pdf"
  );
  writeJson(path.join(packagedStore, "AUDIT_META.json"), {
    propertyId: "adp_spice_island_beach_resort",
    status: "FOUNDER_REVIEWED_STAGED",
    customerReadinessStatus: "NOT_CUSTOMER_READY",
    publicationAuthorizationStatus: "NOT_AUTHORIZED",
    latestVersionId: "packaged_v1",
  });
  ensure(path.dirname(packagedPdf));
  fs.writeFileSync(packagedPdf, Buffer.from("%PDF-PACKAGED-SPICE%"));

  // Live filesystem after Deploy A = copy of upload artifact
  fs.cpSync(deployA, liveFs, { recursive: true });

  // --- Runtime (post-deploy) activity on live filesystem ---
  const runtimeProperty = "adp_runtime_probe_hotel";
  const runtimeStore = path.join(
    liveFs,
    "data/ai-visibility-audit-action-plan/production/store",
    runtimeProperty
  );
  const runtimeVersionId = "ava_runtime_" + Date.now().toString(36);
  const runtimePdfRel =
    "reports/ai-visibility-audit-action-plan/archive/" +
    runtimeProperty +
    "/current.pdf";
  const runtimePdfAbs = path.join(liveFs, runtimePdfRel);
  const runtimePdfBytes = Buffer.from("%PDF-RUNTIME-NEW-AUDIT%");
  const runtimePdfSha = sha(runtimePdfBytes);

  writeJson(path.join(runtimeStore, "AUDIT_META.json"), {
    propertyId: runtimeProperty,
    name: "Runtime Probe Hotel",
    status: "FOUNDER_APPROVED_FOR_CUSTOMER",
    evidenceReviewStatus: "AUDIT_VALIDATED",
    customerReadinessStatus: "NOT_CUSTOMER_READY",
    publicationAuthorizationStatus: "FOUNDER_AUTHORIZED_PENDING_DISTRIBUTION",
    founderApprovedForCustomer: true,
    founderApprovedForCustomerAt: new Date().toISOString(),
    latestVersionId: runtimeVersionId,
    acceptedRecommendationCount: 2,
    pdfRelativePath: runtimePdfRel,
    pdfSha256: runtimePdfSha,
    versions: [{ versionId: runtimeVersionId, status: "FOUNDER_APPROVED_FOR_CUSTOMER" }],
    adminDecisions: [
      { actionId: "AVA-PROBE-001", decision: "APPROVE", at: new Date().toISOString() },
      { actionId: "AVA-PROBE-002", decision: "DEFER", at: new Date().toISOString() },
    ],
  });
  writeJson(path.join(runtimeStore, "versions", `${runtimeVersionId}.json`), {
    versionId: runtimeVersionId,
    propertyId: runtimeProperty,
    status: "FOUNDER_APPROVED_FOR_CUSTOMER",
    pdfRelativePath: runtimePdfRel,
    payload: {
      actions: [
        { actionId: "AVA-PROBE-001", adminDecision: { decision: "APPROVE" } },
        { actionId: "AVA-PROBE-002", adminDecision: { decision: "DEFER" } },
      ],
    },
  });
  ensure(path.dirname(runtimePdfAbs));
  fs.writeFileSync(runtimePdfAbs, runtimePdfBytes);

  const beforeRedeploy = {
    runtimeMetaExists: fs.existsSync(path.join(runtimeStore, "AUDIT_META.json")),
    runtimePdfExists: fs.existsSync(runtimePdfAbs),
    runtimePdfSha,
    founderApprovedForCustomer: true,
    decisionCount: 2,
  };

  // --- Deploy B packaged artifact: intentionally OMIT runtime hotel ---
  // (fresh deploy from laptop/CI that never pulled runtime writes back)
  fs.cpSync(deployA, deployB, { recursive: true });
  // Optionally refresh packaged spice only — still no runtime hotel
  writeJson(
    path.join(
      deployB,
      "data/ai-visibility-audit-action-plan/production/store/adp_spice_island_beach_resort/AUDIT_META.json"
    ),
    {
      propertyId: "adp_spice_island_beach_resort",
      status: "FOUNDER_REVIEWED_STAGED",
      note: "redeployed packaged artifact without runtime probe hotel",
    }
  );

  // Railway replace: live filesystem becomes Deploy B upload tree
  rm(liveFs);
  fs.cpSync(deployB, liveFs, { recursive: true });

  const afterMeta = path.join(
    liveFs,
    "data/ai-visibility-audit-action-plan/production/store",
    runtimeProperty,
    "AUDIT_META.json"
  );
  const afterPdf = path.join(liveFs, runtimePdfRel);
  const spiceStillThere = fs.existsSync(
    path.join(
      liveFs,
      "data/ai-visibility-audit-action-plan/production/store/adp_spice_island_beach_resort/AUDIT_META.json"
    )
  );

  const afterRedeploy = {
    runtimeMetaExists: fs.existsSync(afterMeta),
    runtimePdfExists: fs.existsSync(afterPdf),
    packagedSpiceSurvivedBecauseIncludedInArtifact: spiceStillThere,
  };

  const survivesWithoutArtifactInclusion =
    afterRedeploy.runtimeMetaExists && afterRedeploy.runtimePdfExists;

  const architectureFact =
    "Dealality production deploys via Railway CLI working-tree upload that replaces the entire service filesystem (docs/production-deploy/CURRENT_PRODUCTION_DEPLOY_ARCHITECTURE.md). AVA writes to local paths under data/.../production/store and reports/.../archive with no volume mount and no external object store.";

  const result = {
    ok: true,
    probe: "AVA_RUNTIME_REDEPLOY_SURVIVAL_V1",
    question:
      "Do newly generated audits/PDFs/decisions/founder approvals survive a fresh Railway deploy when NOT included in the new deployment artifact?",
    verdict: survivesWithoutArtifactInclusion ? "PASS" : "FAIL",
    beforeRedeploy,
    afterRedeploy,
    durableStoreToday: {
      mechanism: "application_local_filesystem_paths_uploaded_with_deploy",
      storeRoot: "data/ai-visibility-audit-action-plan/production/store",
      pdfArchiveRoot: "reports/ai-visibility-audit-action-plan/archive",
      externalDurableBackend: null,
      railwayVolumeConfigured: false,
      survivesRedeployWithoutReupload: false,
    },
    recoveryToday: {
      howRecoveredIfLost: "ONLY if the runtime files were pulled back into the deploy workstation/git artifact before the next railway up",
      automaticRecovery: false,
    },
    architectureFact,
    smallestDurableFix: {
      recommendation: "RAILWAY_VOLUME_MOUNT",
      mountPath: "/app/data/ai-visibility-audit-action-plan/production",
      alsoMountOrSymlinkPdfs: "/app/reports/ai-visibility-audit-action-plan/archive",
      whySmallest:
        "Keeps existing file-store API unchanged; only Railway service config + path ensure. No new S3/Airtable schema. Packaged lean baselines can stay in the deploy artifact; runtime store/PDFs live on the volume.",
      alternative:
        "Object storage (R2/S3) for PDFs + JSON metas — larger change, better multi-instance sharing later.",
      requiredBeforeProdEnablementOfRuntimeWrites: true,
    },
  };

  const outPath = path.join(
    ROOT,
    "reports/dealality-ai-visibility-audit-action-plan-pilot-v1/AVA_RUNTIME_REDEPLOY_SURVIVAL_PROBE.json"
  );
  writeJson(outPath, result);
  console.log(JSON.stringify(result, null, 2));
  // Probe always exits 0; verdict field carries PASS/FAIL for the durability question.
}

main();
