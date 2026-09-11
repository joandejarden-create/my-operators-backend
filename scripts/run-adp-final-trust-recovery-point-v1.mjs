#!/usr/bin/env node
/**
 * Freeze ADP_FINAL_TRUST_RECOVERY_POINT before final trust closure / deploy.
 *
 *   node scripts/run-adp-final-trust-recovery-point-v1.mjs
 */

import { createHash } from "crypto";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "fs";
import { join } from "path";
import { execSync } from "child_process";
import {
  listPublishedPropertyIds,
  loadPublishedManifest,
  loadPublishedReport,
} from "../lib/ai-demand-positioning/published-snapshot.js";
import {
  ADP_FINAL_TRUST_RECOVERY_POINT_ID,
} from "../lib/ai-demand-positioning/governance/adp-final-trust-recovery-point-v1.js";
import {
  RENDERER_ASSET_VERSION,
  EVIDENCE_CONTRACT_VERSION,
} from "../lib/ai-demand-positioning/governance/adp-production-output-baseline-v1.js";
import { BPP_CUSTOMER_PUBLICATION_VERSION } from "../lib/ai-demand-positioning/brand-portfolio/bpp-publication-meta-v1.js";
import { DISPLACEMENT_EVIDENCE_RESOLVER_VERSION } from "../lib/ai-demand-positioning/customer/resolve-displacement-evidence-v1.js";

const ROOT = process.cwd();
const OUT = join(ROOT, "reports/ai-demand-positioning/ADP_FINAL_TRUST_RECOVERY_POINT.json");

function git(cmd) {
  try {
    return execSync(cmd, { cwd: ROOT, encoding: "utf8" }).trim();
  } catch {
    return null;
  }
}

function sha256File(path) {
  if (!existsSync(path)) return null;
  return createHash("sha256").update(readFileSync(path)).digest("hex");
}

function sha256Json(obj) {
  return createHash("sha256").update(JSON.stringify(obj)).digest("hex");
}

const publishedIds = listPublishedPropertyIds();
const editions = {};
for (const propertyId of publishedIds) {
  const man = loadPublishedManifest(propertyId);
  const report = loadPublishedReport(propertyId);
  const er = report?.executiveRead || report?.payload?.executiveRead || {};
  const reportPath = man?.reportFile
    ? join(ROOT, "data/ai-demand-positioning/published", propertyId, man.reportFile)
    : null;
  editions[propertyId] = {
    periodId: man?.latestPeriodId || null,
    reportEdition: man?.reportEdition || report?.reportEdition || null,
    executiveReadEditionId: man?.executiveReadEditionId || null,
    compositionVersion: er.compositionVersion || null,
    compositionHash: er.compositionHash || null,
    snapshotHash: reportPath ? sha256File(reportPath) : null,
    topObservedAlternative:
      report?.competitiveSet?.topObservedAlternative ||
      report?.payload?.competitiveSet?.topObservedAlternative ||
      null,
  };
}

const shareRegPath = join(ROOT, "config/client-share/adp-share-registry/active-tokens.json");
const bppPackPath = join(ROOT, "config/client-share/bpp-customer-published-v1.json");
const evidenceIndexCandidates = [
  join(ROOT, "data/ai-demand-positioning/evidence-index/index.json"),
  join(ROOT, "config/client-share/adp-evidence-index.json"),
];

const recovery = {
  id: ADP_FINAL_TRUST_RECOVERY_POINT_ID,
  gate: "ADP_FINAL_TRUST_RECOVERY_POINT",
  frozenAt: new Date().toISOString(),
  git: {
    branch: git("git branch --show-current"),
    head: git("git rev-parse HEAD"),
    headShort: git("git rev-parse --short HEAD"),
  },
  rendererAssetVersion: RENDERER_ASSET_VERSION,
  evidenceContractVersion: EVIDENCE_CONTRACT_VERSION,
  bppPublicationVersion: BPP_CUSTOMER_PUBLICATION_VERSION,
  displacementResolverVersion: DISPLACEMENT_EVIDENCE_RESOLVER_VERSION,
  shareRegistryHash: sha256File(shareRegPath),
  bppPackHash: sha256File(bppPackPath),
  evidenceIndexHash: evidenceIndexCandidates.map(sha256File).find(Boolean) || null,
  publishedUniverseCount: publishedIds.length,
  publishedPropertyIds: publishedIds,
  editions,
  editionsHash: sha256Json(editions),
};

mkdirSync(join(ROOT, "reports/ai-demand-positioning"), { recursive: true });
writeFileSync(OUT, JSON.stringify(recovery, null, 2) + "\n");
console.log(
  JSON.stringify(
    {
      ok: true,
      path: OUT,
      id: recovery.id,
      git: recovery.git,
      publishedUniverseCount: recovery.publishedUniverseCount,
      shareRegistryHash: recovery.shareRegistryHash?.slice(0, 16),
      bppPackHash: recovery.bppPackHash?.slice(0, 16),
      editionsHash: recovery.editionsHash?.slice(0, 16),
    },
    null,
    2
  )
);
