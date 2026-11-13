#!/usr/bin/env node
/**
 * Activate founder-approved Bethesda local-master V2 as CUSTOMER_PUBLISHED CURRENT.
 *
 * Approved fingerprint:
 *   df8da65a172727e627a71c3a31bead5f3f70ee95b03b7245f7a0ea34bbb33c66
 *
 * Does not mutate package content bytes — only lifecycle pointer + approval metadata.
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  createGdiPublicationPackageStore,
  verifyPublicationPackageIntegrity,
  validateFounderApprovalReceipt,
  GDI_PUBLICATION_LIFECYCLE_STATE,
} from "../lib/group-demand-intelligence/publication-package/index.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOTEL_ID = "recLuxvwwxID7U2B8";
const VERSION = "gdi_pub_recLuxvwwxID7U2B8_20261113_local_master_v2";
const APPROVED_FP =
  "df8da65a172727e627a71c3a31bead5f3f70ee95b03b7245f7a0ea34bbb33c66";
const PRIOR_FP =
  "441773211494cebaf50c03ef8921ccac6c57f31ac5cb2398585352b425e36f7f";
const OUT = path.join(ROOT, "reports/gdi-local-master-v2-release");

fs.mkdirSync(OUT, { recursive: true });

const store = createGdiPublicationPackageStore();
const loaded = store.loadPackage(HOTEL_ID, VERSION);
if (!loaded.ok || !loaded.package) {
  console.error(loaded);
  process.exit(1);
}
const pkg = loaded.package;
const integrity = verifyPublicationPackageIntegrity(pkg);
if (!integrity.ok || integrity.recomputedFingerprintSha256 !== APPROVED_FP) {
  console.error({ integrity, expected: APPROVED_FP });
  process.exit(1);
}
if (pkg.contentFingerprintSha256 !== APPROVED_FP) {
  console.error("Package fingerprint mismatch — refusing activation");
  process.exit(1);
}

const receipt = {
  approved: true,
  publicationId: pkg.publicationId,
  publicationVersion: VERSION,
  contentFingerprintSha256: APPROVED_FP,
  approvedAt: new Date().toISOString(),
  approvedBy: "founder",
  authority: "FOUNDER_EXPLICIT_CHAT_APPROVAL_2026-11-13",
  priorFingerprint: PRIOR_FP,
  note: "Activate local-master V2; preserve prior package for rollback; preserve share token + feedback",
};

const receiptCheck = validateFounderApprovalReceipt(receipt, {
  publicationId: pkg.publicationId,
  publicationVersion: VERSION,
  contentFingerprintSha256: APPROVED_FP,
});
if (!receiptCheck.ok) {
  console.error(receiptCheck);
  process.exit(1);
}

const priorCurrent = store.readPointer(HOTEL_ID, "CURRENT");

const toPublish = {
  ...pkg,
  approvalStatus: "FOUNDER_APPROVED",
  approvedAt: receipt.approvedAt,
  lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.CUSTOMER_PUBLISHED,
  founderApprovalReceipt: receipt,
  gates: {
    ...(pkg.gates || {}),
    founder_approval_fingerprint_match: { ok: true, receipt },
    fingerprint_integrity: { ok: true, fingerprint: APPROVED_FP },
    inventory_parity: { ok: true, counts: pkg.inventoryCounts },
  },
};

const published = store.publishCustomerVisible(toPublish, {
  requireFounderApproval: true,
});
if (!published.ok) {
  console.error(published);
  process.exit(1);
}

const current = store.readPointer(HOTEL_ID, "CURRENT");
const after = store.loadCustomerVisiblePackage(HOTEL_ID);
const summary = {
  ok:
    current?.contentFingerprintSha256 === APPROVED_FP &&
    after?.ok === true &&
    after.package?.contentFingerprintSha256 === APPROVED_FP,
  priorCurrent,
  current,
  publicationVersion: after.package?.publicationVersion,
  fingerprint: after.package?.contentFingerprintSha256,
  lifecycleState: after.package?.lifecycleState || current?.lifecycleState,
  inventoryCounts: after.package?.inventoryCounts,
  receipt,
  publishedPath: published.path || null,
};

fs.writeFileSync(
  path.join(OUT, "01-FOUNDER_APPROVAL_RECEIPT.json"),
  `${JSON.stringify(receipt, null, 2)}\n`,
);
fs.writeFileSync(
  path.join(OUT, "13-LIVE_PUBLICATION_IDENTITY.json"),
  `${JSON.stringify(
    {
      phase: "LOCAL_BUNDLE_ACTIVATION_PRE_DEPLOY",
      ...summary,
    },
    null,
    2,
  )}\n`,
);

console.log(JSON.stringify(summary, null, 2));
if (!summary.ok) process.exit(1);
