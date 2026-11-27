#!/usr/bin/env node
/**
 * Explicit CURRENT promotion for a reviewed STAGED package version.
 * DryRun default. Live requires FOUNDER_GO AUTHORIZE_GDI_CURRENT_PROMOTION …
 *
 *   node scripts/gdi/run-gdi-promote-current-v1.mjs --hotel-id <rec> --version <ver> [--apply]
 */
import { createGdiPublicationPackageStore } from "../../lib/group-demand-intelligence/publication-package/index.js";

function arg(name, fallback = "") {
  const i = process.argv.indexOf(name);
  return i >= 0 ? String(process.argv[i + 1] || "").trim() : fallback;
}
function flag(name) {
  return process.argv.includes(name);
}

const hotelId = arg("--hotel-id");
const version = arg("--version");
const apply = flag("--apply");
const auth = arg("--auth");
const expectedAuth = `FOUNDER_GO AUTHORIZE_GDI_CURRENT_PROMOTION ${hotelId} ${version}`;

if (!hotelId || !version) {
  console.error("Usage: --hotel-id <rec> --version <publicationVersion> [--apply --auth \"…\"]");
  process.exit(2);
}
if (apply && auth !== expectedAuth) {
  console.log(
    JSON.stringify(
      { ok: false, error: "AUTH_REQUIRED", expected: expectedAuth },
      null,
      2,
    ),
  );
  process.exit(2);
}

const store = createGdiPublicationPackageStore({});
const staged = store.readPointer(hotelId, "STAGED");
const currentBefore = store.readPointer(hotelId, "CURRENT");
const loaded = store.loadPackage(hotelId, version);

if (!loaded.ok) {
  console.log(JSON.stringify({ ok: false, error: "PACKAGE_LOAD_FAILED", version }, null, 2));
  process.exit(3);
}
if ((staged?.publicationVersion || null) !== version) {
  console.log(
    JSON.stringify(
      {
        ok: false,
        error: "VERSION_NOT_ACTIVE_STAGED",
        stagedVersion: staged?.publicationVersion || null,
        requested: version,
      },
      null,
      2,
    ),
  );
  process.exit(4);
}

const pkg = {
  ...loaded.package,
  approvalStatus: "FOUNDER_APPROVED",
  approvedAt: new Date().toISOString(),
};

if (!apply) {
  console.log(
    JSON.stringify(
      {
        ok: true,
        dryRun: true,
        activated: false,
        hotelId,
        publicationVersion: version,
        opportunityCount:
          pkg.opportunityCount || (pkg.opportunities || []).length,
        priorCurrent: currentBefore?.publicationVersion || null,
        note: "DryRun — CURRENT unchanged",
      },
      null,
      2,
    ),
  );
  process.exit(0);
}

const published = store.publishCustomerVisible(pkg, {
  requireFounderApproval: true,
});
if (!published.ok) {
  console.log(
    JSON.stringify(
      { ok: false, error: published.error || "PUBLISH_FAILED", published },
      null,
      2,
    ),
  );
  process.exit(5);
}

const currentAfter = store.readPointer(hotelId, "CURRENT");
console.log(
  JSON.stringify(
    {
      ok: true,
      dryRun: false,
      activated: true,
      hotelId,
      publicationVersion: version,
      currentVersion: currentAfter?.publicationVersion || null,
      opportunityCount:
        pkg.opportunityCount || (pkg.opportunities || []).length,
    },
    null,
    2,
  ),
);
