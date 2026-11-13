#!/usr/bin/env node
/**
 * Rebuild local-master candidate from the FROZEN snapshot (not a newer live capture).
 * Supersedes STAGED pointer only. Does NOT touch CURRENT / CUSTOMER_PUBLISHED.
 *
 * Reason: v1 fingerprint b45106b7… omitted scalingDrawerModel / segmentOverviewModel
 * from PUBLICATION_CONTENT_FIELDS, leaving 12 drawer models out of the package.
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  buildCustomerPublicationPackage,
  verifyPublicationPackageIntegrity,
  createGdiPublicationPackageStore,
  GDI_PUBLICATION_LIFECYCLE_STATE,
} from "../lib/group-demand-intelligence/publication-package/index.js";
import { attachCanonicalCustomerPresentationContract } from "../lib/group-demand-intelligence/presentation/gdi-canonical-customer-presentation-contract-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/gdi-local-master-publication-v1");
const SNAP = path.join(
  OUT,
  "local-master-snapshot/local-live-opportunities.json",
);
const HOTEL_ID = "recLuxvwwxID7U2B8";
const HOTEL_NAME = "Bethesda Marriott";
const PRIOR_FP =
  "441773211494cebaf50c03ef8921ccac6c57f31ac5cb2398585352b425e36f7f";
const SUPERSEDED_V1_FP =
  "b45106b7cd82e92e41423a698ac739bb2d67db6ab2bcf12850ef6b7f5d2a0165";
const VERSION = `gdi_pub_${HOTEL_ID}_20261113_local_master_v2`;

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}
function actionLabel(o = {}) {
  const plan = o.tileBadgePlan;
  if (plan && typeof plan === "object" && plan.salesActionPillLabel) {
    return text(plan.salesActionPillLabel);
  }
  return text(o.salesActionPillLabel || o.salesActivationLabel);
}
function actionKey(o = {}) {
  const plan = o.tileBadgePlan;
  if (plan && typeof plan === "object" && plan.salesActionKey) {
    return text(plan.salesActionKey);
  }
  return text(o.salesActionKey || o.salesActivationKey);
}
function normalize(o) {
  const row = { ...o };
  const key = actionKey(o);
  const label = actionLabel(o);
  if (key) row.salesActionKey = key;
  if (label) row.salesActionPillLabel = label;
  const preview = o.tileContactPreview || {};
  if (!row.primaryContact || typeof row.primaryContact !== "object") {
    row.primaryContact = {};
  }
  if (preview.name || preview.displayName) {
    row.primaryContact = {
      ...row.primaryContact,
      name: preview.name || preview.displayName || row.primaryContact.name,
      role:
        preview.displayRole ||
        preview.role ||
        row.primaryContact.role ||
        row.primaryContact.title,
      title:
        preview.displayRole ||
        preview.role ||
        row.primaryContact.title ||
        row.primaryContact.role,
    };
  }
  return attachCanonicalCustomerPresentationContract(row, {
    hotelId: HOTEL_ID,
    listScope: "LOCAL_MASTER_FROZEN_SNAPSHOT",
  });
}

function drawerPresence(o) {
  return {
    shared: Boolean(o.sharedTenSectionDrawer),
    events: Boolean(o.eventsOpportunityOverview),
    scaling: Boolean(o.scalingDrawerModel),
    eventsDrawer: Boolean(o.eventsDrawerModel),
    segment: Boolean(o.segmentOverviewModel),
    brief: Boolean(o.commercialIntelligenceBrief),
  };
}

const frozen = JSON.parse(fs.readFileSync(SNAP, "utf8"));
if (!Array.isArray(frozen.opportunities) || frozen.opportunities.length !== 67) {
  throw new Error(`Frozen snapshot invalid count=${frozen.opportunities?.length}`);
}

const packageOps = frozen.opportunities.map(normalize);
const candidate = buildCustomerPublicationPackage({
  hotelId: HOTEL_ID,
  hotelName: HOTEL_NAME,
  opportunities: packageOps,
  publicationVersion: VERSION,
  lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.CUSTOMER_RENDERED,
  approvalStatus: "CANDIDATE_AWAITING_FOUNDER_APPROVAL",
  applicationCompatibilityVersion: "gdi-customer-app-compat-v1",
  notes:
    "v2 from frozen local-master snapshot. Adds scalingDrawerModel/segmentOverviewModel allowlist. Supersedes staged v1 b45106b7… Does not publish.",
});

const integrity = verifyPublicationPackageIntegrity(candidate);
if (!integrity.ok) {
  console.error(integrity);
  process.exit(1);
}

const store = createGdiPublicationPackageStore();
const written = store.writePackage(candidate);
if (!written.ok) {
  console.error(written);
  process.exit(1);
}
const staged = store.stageForProduction({
  ...candidate,
  lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.PRODUCTION_STAGED,
  gates: {
    integrity: { ok: true },
    frozenSnapshotSource: { ok: true, path: "local-master-snapshot/local-live-opportunities.json" },
    supersedes: { fingerprint: SUPERSEDED_V1_FP, reason: "missing_scalingDrawerModel_allowlist" },
    founderApproval: { ok: false, required: true },
  },
});

const current = JSON.parse(
  fs.readFileSync(
    path.join(
      ROOT,
      "lib/group-demand-intelligence/bundled-publication-packages",
      HOTEL_ID,
      "CURRENT.json",
    ),
    "utf8",
  ),
);
if (current.contentFingerprintSha256 !== PRIOR_FP) {
  throw new Error(`CURRENT drifted: ${current.contentFingerprintSha256}`);
}

const pBy = new Map(candidate.opportunities.map((o) => [String(o.id), o]));
const drawerDiffs = [];
const actionDiffs = [];
for (const f of frozen.opportunities) {
  const p = pBy.get(String(f.id));
  const ld = drawerPresence(f);
  const pd = drawerPresence(p);
  if (JSON.stringify(ld) !== JSON.stringify(pd)) {
    drawerDiffs.push({ id: f.id, local: ld, pkg: pd });
  }
  if (actionLabel(f) !== actionLabel(p)) {
    actionDiffs.push({
      id: f.id,
      local: actionLabel(f),
      pkg: actionLabel(p),
    });
  }
}

const newFp = candidate.contentFingerprintSha256;
const summary = {
  ok:
    integrity.ok &&
    written.ok &&
    staged.ok === true &&
    current.contentFingerprintSha256 === PRIOR_FP &&
    drawerDiffs.length === 0 &&
    actionDiffs.length === 0,
  verdict: "CANDIDATE_V2_STAGED_AWAITING_FOUNDER_FINGERPRINT_APPROVAL",
  clientReady: false,
  published: false,
  priorFingerprint: PRIOR_FP,
  supersededV1Fingerprint: SUPERSEDED_V1_FP,
  candidateFingerprint: newFp,
  publicationVersion: VERSION,
  inventoryCounts: candidate.inventoryCounts,
  drawerDiffs: drawerDiffs.length,
  actionDiffs: actionDiffs.length,
  storePath: written.path,
  currentUnchanged: current.contentFingerprintSha256 === PRIOR_FP,
};

fs.writeFileSync(
  path.join(OUT, "08-NEW_PUBLICATION_PACKAGE.json"),
  `${JSON.stringify(
    {
      publicationId: candidate.publicationId,
      publicationVersion: VERSION,
      contentFingerprintSha256: newFp,
      priorFingerprint: PRIOR_FP,
      supersededV1Fingerprint: SUPERSEDED_V1_FP,
      opportunityCount: candidate.opportunityCount,
      inventoryCounts: candidate.inventoryCounts,
      lifecycleState: candidate.lifecycleState,
      approvalStatus: candidate.approvalStatus,
      storePath: written.path,
      staged: staged.ok === true,
      published: false,
      currentPointerUnchanged: true,
      integrity,
      drawerPresenceParity: { ok: drawerDiffs.length === 0, diffs: drawerDiffs },
    },
    null,
    2,
  )}\n`,
);
fs.writeFileSync(
  path.join(OUT, "09-PUBLICATION_FINGERPRINT.json"),
  `${JSON.stringify(
    {
      prior: PRIOR_FP,
      supersededV1: SUPERSEDED_V1_FP,
      candidate: newFp,
      changedFromPrior: newFp !== PRIOR_FP,
      changedFromV1: newFp !== SUPERSEDED_V1_FP,
      founderApprovalRequired: true,
      liveCustomerPointer: "UNCHANGED_PRIOR",
      instruction:
        "Approve this exact candidate fingerprint before CURRENT switch. v1 b45106b7… is superseded for staging only; CURRENT remains 441773…",
    },
    null,
    2,
  )}\n`,
);
fs.writeFileSync(
  path.join(OUT, "16-V2-DRAWER-ALLOWLIST-CORRECTION.json"),
  `${JSON.stringify(
    {
      reason:
        "v1 package omitted scalingDrawerModel/segmentOverviewModel from PUBLICATION_CONTENT_FIELDS",
      frozenSource: "local-master-snapshot/local-live-opportunities.json",
      serverBuildShaFrozen: frozen.serverBuildSha || null,
      v1Fingerprint: SUPERSEDED_V1_FP,
      v2Fingerprint: newFp,
      drawerDiffsAfterV2: drawerDiffs,
      actionDiffsAfterV2: actionDiffs,
      fieldsAddedToAllowlist: [
        "scalingDrawerModel",
        "segmentOverviewModel",
        "researchToDrawerV2",
        "approvedCustomerFacingDetail",
      ],
      v1PackageRetainedOnDisk: true,
      currentStillPrior: true,
    },
    null,
    2,
  )}\n`,
);

console.log(JSON.stringify(summary, null, 2));
if (!summary.ok) process.exit(1);
