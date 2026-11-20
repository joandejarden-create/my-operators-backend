#!/usr/bin/env node
/**
 * Restage Sheraton Guadalajara STAGED package with verified public contact
 * channels for already-included opportunities only. Does not change
 * publication eligibility or promote CURRENT.
 *
 *   node scripts/gdi-sheraton-public-contact-channels-restage-v1.mjs
 *   node scripts/gdi-sheraton-public-contact-channels-restage-v1.mjs --apply
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  createGdiPublicationPackageStore,
  verifyPublicationPackageIntegrity,
  fingerprintPublicationContent,
  GDI_PUBLICATION_LIFECYCLE_STATE,
} from "../lib/group-demand-intelligence/publication-package/index.js";
import { buildContactChannelDisplayState } from "../lib/group-demand-intelligence/presentation/gdi-public-contact-channel-hierarchy-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOTEL = "recCAGqkERSXgiku4";
const APPLY = process.argv.includes("--apply");
const OUT = path.join(
  ROOT,
  "reports/gdi-public-contact-discovery-2-v1/publication-refresh",
);
const LOCAL_OPP = path.join(
  ROOT,
  "data/group-demand-intelligence/hotels",
  HOTEL,
  "opportunities.json",
);

const PATCH_IDS = Object.freeze([
  "disc_b4661c17bd92", // Maritza
  "paid3_0a6b19883953", // Alejandra + Paulina
  "paid3_565ce4588e6d", // EUROGUSS Gissella (already authorized in staged)
]);

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function loadLocalMap() {
  const doc = JSON.parse(fs.readFileSync(LOCAL_OPP, "utf8"));
  const map = new Map();
  for (const o of doc.opportunities || []) map.set(o.id, o);
  return map;
}

function patchContactFields(stagedOpp, localOpp) {
  if (!localOpp?.primaryContact) return { changed: false, opp: stagedOpp };
  const before = buildContactChannelDisplayState(stagedOpp);
  const pc = {
    ...(stagedOpp.primaryContact || {}),
    ...Object.fromEntries(
      Object.entries(localOpp.primaryContact || {}).filter(
        ([, v]) => v != null && v !== "",
      ),
    ),
  };
  // Never allow Surfe-derived into package
  pc.surfeDerived = false;
  const next = {
    ...stagedOpp,
    primaryContact: pc,
    contactEmail: localOpp.contactEmail || pc.email || stagedOpp.contactEmail,
    contactPhone: localOpp.contactPhone || pc.phone || stagedOpp.contactPhone,
    contactDataOrigin: "PUBLIC_SOURCE",
  };
  if (localOpp.secondaryContact) {
    next.secondaryContact = {
      ...(stagedOpp.secondaryContact || {}),
      ...localOpp.secondaryContact,
      surfeDerived: false,
    };
  }
  if (Array.isArray(localOpp.discoveredPeerContacts)) {
    next.discoveredPeerContacts = localOpp.discoveredPeerContacts;
  }
  const after = buildContactChannelDisplayState(next);
  const changed =
    text(before.publicDirectEmail) !== text(after.publicDirectEmail) ||
    text(before.publicDirectPhone) !== text(after.publicDirectPhone) ||
    text(before.surfeCtaLabel) !== text(after.surfeCtaLabel) ||
    text(stagedOpp.secondaryContact?.email) !==
      text(next.secondaryContact?.email);
  return { changed, opp: next, before, after };
}

const store = createGdiPublicationPackageStore({
  root: path.join(
    ROOT,
    "lib/group-demand-intelligence/bundled-publication-packages",
  ),
});

const stagedPtr = store.readPointer(HOTEL, "STAGED");
if (!stagedPtr?.publicationVersion) {
  console.error("NO_STAGED_POINTER");
  process.exit(1);
}

const SOURCE_VERSION = stagedPtr.publicationVersion;
const NEW_VERSION = `gdi_pub_${HOTEL}_20261120_public_contact_channels_v1`;
const PRIOR_FP = stagedPtr.contentFingerprintSha256;

const loaded = store.loadPackage(HOTEL, SOURCE_VERSION);
if (!loaded.ok) {
  console.error(JSON.stringify(loaded, null, 2));
  process.exit(1);
}

const localMap = loadLocalMap();
const pkg = JSON.parse(JSON.stringify(loaded.package));
const patches = [];
let patchedCount = 0;

for (const id of PATCH_IDS) {
  const idx = (pkg.opportunities || []).findIndex((o) => o.id === id);
  if (idx < 0) {
    patches.push({ id, status: "NOT_IN_STAGED_SKIPPED" });
    continue;
  }
  const local = localMap.get(id);
  if (!local) {
    patches.push({ id, status: "NO_LOCAL_OPP" });
    continue;
  }
  const result = patchContactFields(pkg.opportunities[idx], local);
  if (result.changed) {
    pkg.opportunities[idx] = result.opp;
    patchedCount += 1;
  }
  patches.push({
    id,
    status: result.changed ? "PATCHED" : "UNCHANGED",
    before: {
      email: result.before?.publicDirectEmail || null,
      phone: result.before?.publicDirectPhone || null,
      surfeCta: result.before?.surfeCtaLabel || null,
    },
    after: {
      email: result.after?.publicDirectEmail || null,
      phone: result.after?.publicDirectPhone || null,
      surfeCta: result.after?.surfeCtaLabel || null,
      secondaryEmail: result.opp?.secondaryContact?.email || null,
    },
  });
}

pkg.publicationVersion = NEW_VERSION;
pkg.publicationId = `gdi_pubpkg_${HOTEL}_public_contact_channels_v1`;
pkg.lifecycleState = GDI_PUBLICATION_LIFECYCLE_STATE.PRODUCTION_STAGED;
pkg.approvalStatus = "PENDING_FOUNDER_APPROVAL";
pkg.approvedAt = null;
pkg.priorContentFingerprintSha256 = PRIOR_FP;
pkg.createdAt = new Date().toISOString();
pkg.remediationNote =
  "Public Contact Discovery 2.0 — patch verified public business channels onto already-staged opportunities; eligibility unchanged; CURRENT not promoted.";

const fp = fingerprintPublicationContent({
  hotelId: pkg.hotelId,
  publicationVersion: pkg.publicationVersion,
  presentationSchemaVersion: pkg.presentationSchemaVersion,
  applicationCompatibilityVersion: pkg.applicationCompatibilityVersion,
  sortOrder: pkg.sortOrder || (pkg.opportunities || []).map((o) => o.id),
  opportunities: pkg.opportunities || [],
});
pkg.contentFingerprintSha256 = fp.contentFingerprintSha256;
pkg.opportunityCount = fp.opportunityCount;

const integrity = verifyPublicationPackageIntegrity(pkg);
if (!integrity.ok) {
  console.error(JSON.stringify({ ok: false, integrity }, null, 2));
  process.exit(1);
}

let written = { ok: true, dryRun: !APPLY };
if (APPLY) {
  written = store.writePackage(pkg);
  if (!written.ok) {
    console.error(JSON.stringify(written, null, 2));
    process.exit(1);
  }
  store.writePointer(HOTEL, "STAGED", {
    publicationId: pkg.publicationId,
    publicationVersion: NEW_VERSION,
    contentFingerprintSha256: pkg.contentFingerprintSha256,
    lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.PRODUCTION_STAGED,
    founderReviewLabel: "STAGED_FOR_FOUNDER_REVIEW",
  });
}

const currentPtr = store.readPointer(HOTEL, "CURRENT");
fs.mkdirSync(OUT, { recursive: true });
const report = {
  ok: true,
  apply: APPLY,
  hotelId: HOTEL,
  sourceVersion: SOURCE_VERSION,
  newVersion: NEW_VERSION,
  priorFingerprint: PRIOR_FP,
  newFingerprint: pkg.contentFingerprintSha256,
  opportunityCount: pkg.opportunityCount,
  patchedCount,
  patches,
  currentPointer: currentPtr
    ? {
        publicationVersion: currentPtr.publicationVersion,
        withheld: currentPtr.withheld === true,
        lifecycleState: currentPtr.lifecycleState,
      }
    : null,
  eligibilityUnchanged: true,
  currentNotPromoted: true,
  written,
};
fs.writeFileSync(
  path.join(OUT, APPLY ? "APPLY.json" : "DRY_RUN.json"),
  `${JSON.stringify(report, null, 2)}\n`,
);
console.log(JSON.stringify(report, null, 2));
