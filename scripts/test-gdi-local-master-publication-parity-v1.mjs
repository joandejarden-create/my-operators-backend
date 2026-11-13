#!/usr/bin/env node
/**
 * Parity gate: frozen local master live DTOs vs staged candidate package.
 * Does NOT require CURRENT switch. Fails if candidate drifts from master snapshot.
 *
 *   npm run test:gdi-local-master-publication-parity
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { buildCanonicalCustomerPresentationContract } from "../lib/group-demand-intelligence/presentation/gdi-canonical-customer-presentation-contract-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/gdi-local-master-publication-v1");
const SNAP = path.join(OUT, "local-master-snapshot/local-live-opportunities.json");
const META = path.join(OUT, "08-NEW_PUBLICATION_PACKAGE.json");
const FP = path.join(OUT, "09-PUBLICATION_FINGERPRINT.json");
const PRIOR =
  "441773211494cebaf50c03ef8921ccac6c57f31ac5cb2398585352b425e36f7f";

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

function contactRole(o = {}) {
  const preview = o.tileContactPreview || {};
  const pc = o.primaryContact || {};
  return text(
    preview.displayRole ||
      preview.role ||
      o.displayRole ||
      o.opportunityRole ||
      pc.role ||
      pc.title,
  );
}

function contactName(o = {}) {
  const preview = o.tileContactPreview || {};
  const pc = o.primaryContact || {};
  return text(
    preview.name || preview.displayName || pc.name || pc.fullName || o.contactName,
  );
}

assert.ok(fs.existsSync(SNAP), "local master snapshot missing — run gdi:local-master-publication-v1 first");
assert.ok(fs.existsSync(META), "candidate package meta missing");
assert.ok(fs.existsSync(FP), "fingerprint receipt missing");

const live = JSON.parse(fs.readFileSync(SNAP, "utf8"));
const meta = JSON.parse(fs.readFileSync(META, "utf8"));
const fpDoc = JSON.parse(fs.readFileSync(FP, "utf8"));

assert.equal(live.opportunities?.length, 67, "local master must be 67");
assert.equal(meta.published, false, "must not auto-publish");
assert.equal(fpDoc.liveCustomerPointer, "UNCHANGED_PRIOR");
assert.equal(fpDoc.prior, PRIOR);
assert.ok(fpDoc.candidate && fpDoc.candidate !== PRIOR, "candidate fingerprint required");
assert.equal(meta.currentPointerUnchanged, true);
assert.equal(meta.contentFingerprintSha256, fpDoc.candidate);

const storePath = meta.storePath;
assert.ok(storePath && fs.existsSync(storePath), "staged package file missing");
const pkg = JSON.parse(fs.readFileSync(storePath, "utf8"));
assert.equal(pkg.contentFingerprintSha256, fpDoc.candidate);
assert.equal(pkg.opportunityCount, 67);

const byId = new Map(pkg.opportunities.map((o) => [String(o.id), o]));
const mismatches = [];
for (const local of live.opportunities) {
  const id = String(local.id);
  const row = byId.get(id);
  if (!row) {
    mismatches.push({ id, reason: "MISSING_IN_PACKAGE" });
    continue;
  }
  const la = actionLabel(local);
  const pa = actionLabel(row);
  const lr = contactRole(local);
  const pr = contactRole(row);
  const ln = contactName(local);
  const pn = contactName(row);
  if (la !== pa || lr !== pr || ln !== pn) {
    mismatches.push({
      id,
      local: { action: la, role: lr, name: ln },
      package: { action: pa, role: pr, name: pn },
    });
  }
  // Contract attach sanity
  const c = buildCanonicalCustomerPresentationContract(row, {
    hotelId: "recLuxvwwxID7U2B8",
  });
  assert.ok(c.commercialAction?.pillLabel || c.display?.title);
}

const current = JSON.parse(
  fs.readFileSync(
    path.join(
      ROOT,
      "lib/group-demand-intelligence/bundled-publication-packages/recLuxvwwxID7U2B8/CURRENT.json",
    ),
    "utf8",
  ),
);
assert.equal(
  current.contentFingerprintSha256,
  PRIOR,
  "CURRENT must remain prior package until founder approval",
);

const result = {
  ok: mismatches.length === 0,
  candidateFingerprint: fpDoc.candidate,
  priorFingerprint: PRIOR,
  mismatchCount: mismatches.length,
  mismatches: mismatches.slice(0, 40),
  currentPointer: current.contentFingerprintSha256,
  published: false,
  clientReady: false,
};
fs.writeFileSync(
  path.join(OUT, "10-LOCAL_STAGING_BROWSER_PARITY.json"),
  `${JSON.stringify({ ...JSON.parse(fs.readFileSync(path.join(OUT, "10-LOCAL_STAGING_BROWSER_PARITY.json"), "utf8")), testGate: result }, null, 2)}\n`,
);

console.log(JSON.stringify(result, null, 2));
assert.equal(mismatches.length, 0, `local master vs candidate mismatches: ${mismatches.length}`);
