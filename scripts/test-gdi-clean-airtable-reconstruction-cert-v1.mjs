#!/usr/bin/env node
/**
 * Offline regression for gdi-clean-airtable-reconstruction-cert-v1:
 *  - fixture source never certifies PASS (live Airtable read required)
 *  - isolated package written under <report-dir>/_isolated only
 *  - STAGED/CURRENT pointers + package bytes unchanged
 *  - missing STAGED baseline => BLOCKED
 *  - unknown arg / protected report dir fail closed
 *  - pure comparison: contact field parity + membership + authority gaps are reported
 *
 * No network, no Airtable calls, no spend.
 */
import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  buildReconstruction,
  evaluateReconstruction,
  run,
  CERT_STATUS,
  DEFAULT_HOTEL_ID,
  DURABLE_STORES,
} from "./gdi-clean-airtable-reconstruction-cert-v1.mjs";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const PKG_ROOT = path.join(ROOT, "lib/group-demand-intelligence/bundled-publication-packages");

function sha(file) {
  return fs.existsSync(file)
    ? crypto.createHash("sha256").update(fs.readFileSync(file)).digest("hex")
    : null;
}

// ---- pure comparison -------------------------------------------------------
const staged = [
  {
    id: "o1",
    organizationName: "Org One",
    title: "T1",
    priority: "MEDIUM_PRIORITY",
    primaryContact: { name: "Ann Lee", title: "VP Events", email: "ann@one.com", phone: "619-555-0100", linkedinUrl: "https://linkedin.com/in/ann" },
  },
  {
    id: "o2",
    organizationName: "Org Two",
    title: "T2",
    priority: "WATCHLIST",
    primaryContact: { name: "Bo Kim", title: "Planner", email: "bo@two.com" },
  },
];
const airtable = [
  { ...staged[0], customerVisible: true },
  { ...staged[1], primaryContact: { name: "Bo Kim", title: "Planner" }, customerVisible: true },
  { id: "o3", organizationName: "Extra", title: "T3", priority: "WATCHLIST", customerVisible: true, primaryContact: null },
  { id: "o4", organizationName: "Hidden", title: "T4", customerVisible: false },
];
const rec = buildReconstruction({ airtableOpps: airtable, stagedOpps: staged, findymailLedger: [], hotelId: "recTEST" });
assert.deepEqual(rec.ids.missingInAirtable, []);
assert.deepEqual(rec.ids.extraBeyondStaged, ["o3"], "extra visible rows beyond STAGED flagged; hidden excluded");
assert.equal(rec.contactSummary.email.MATCH, 1);
assert.equal(rec.contactSummary.email.STAGED_ONLY, 1, "staged-only email detected");
assert.equal(rec.contactSummary.authority.airtableRawPresent, 0);
assert.equal(rec.contactSummary.authority.reconstructedInMemoryPresent, 2, "authority annotated in memory");
const ev = evaluateReconstruction(rec, { liveAirtableRead: true });
assert.equal(ev.status, CERT_STATUS.PARTIAL);
const codes = ev.missingPersistenceCorrections.map((c) => c.code);
for (const code of [
  "INVENTORY_MEMBERSHIP_NOT_PERSISTED",
  "CONTACT_EMAIL_NOT_RECOVERABLE_FROM_AIRTABLE",
  "BUYING_AUTHORITY_ANNOTATION_NOT_PERSISTED_IN_AIRTABLE",
]) {
  assert.ok(codes.includes(code), `correction ${code}`);
}
const fixtureEval = evaluateReconstruction(rec, { liveAirtableRead: false });
assert.ok(fixtureEval.missingPersistenceCorrections.some((c) => c.code === "LIVE_AIRTABLE_READ_NOT_PERFORMED"));

assert.ok(DURABLE_STORES.length >= 4);
for (const needle of [/Airtable — Group Demand Opportunities/, /Findymail/, /publication packages/, /Research Opportunities/]) {
  assert.ok(DURABLE_STORES.some((s) => needle.test(s.store)), `durable store ${needle}`);
}

// ---- end-to-end on fixture (real STAGED baseline, read-only) ---------------
const pointerFile = path.join(PKG_ROOT, DEFAULT_HOTEL_ID, "STAGED.json");
const currentFile = path.join(PKG_ROOT, DEFAULT_HOTEL_ID, "CURRENT.json");
assert.ok(fs.existsSync(pointerFile), "Wyndham STAGED pointer exists");
const pointer = JSON.parse(fs.readFileSync(pointerFile, "utf8"));
const pkgFile = path.join(PKG_ROOT, DEFAULT_HOTEL_ID, pointer.publicationVersion, "package.json");
const stagedPkg = JSON.parse(fs.readFileSync(pkgFile, "utf8"));
const before = [sha(pointerFile), sha(currentFile), sha(pkgFile)];

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "gdi-recon-cert-"));
const origLog = console.log;
try {
  const fixtureFile = path.join(tmp, "airtable-fixture.json");
  fs.writeFileSync(
    fixtureFile,
    JSON.stringify({
      opportunities: [
        ...stagedPkg.opportunities.map((o) => ({ ...o, customerVisible: true })),
        { id: "extra_fixture_row", hotelId: DEFAULT_HOTEL_ID, title: "Extra", customerVisible: true },
      ],
    }),
  );
  const reportDir = path.join(tmp, "report");
  console.log = () => {};
  const out = await run(["--airtable-fixture", fixtureFile, "--report-dir", reportDir]);
  console.log = origLog;
  assert.equal(out.status, CERT_STATUS.PARTIAL, "fixture source never PASS");
  assert.equal(out.certification.liveAirtableRead, false);
  assert.equal(out.certification.usedStagedAsSource, false);
  assert.equal(out.certification.comparisonBaseline.pointersAndPackageUnchanged, true);
  assert.equal(out.certification.guarantees.stagedOverwrite, false);
  assert.equal(out.certification.idParity.stagedIdsMissingInAirtable, 0);
  assert.equal(out.certification.idParity.reconstructedCandidatesBeyondStaged, 1);
  assert.ok(fs.existsSync(path.join(reportDir, "00-CERTIFICATION.json")));
  assert.ok(fs.existsSync(path.join(reportDir, "05-MISSING_PERSISTENCE_CORRECTIONS.json")));
  const isolatedFile = path.join(ROOT, out.certification.isolatedPackage.file);
  assert.ok(isolatedFile.startsWith(path.join(reportDir, "_isolated")) || fs.existsSync(path.join(reportDir, "_isolated")));
  assert.ok(fs.existsSync(path.join(reportDir, "_isolated", DEFAULT_HOTEL_ID)), "isolated package under _isolated");

  // BLOCKED when comparison baseline is missing
  console.log = () => {};
  const blocked = await run([
    "--hotel-id",
    "recDOESNOTEXIST000",
    "--airtable-fixture",
    fixtureFile,
    "--report-dir",
    path.join(tmp, "blocked"),
  ]);
  console.log = origLog;
  assert.equal(blocked.status, CERT_STATUS.BLOCKED);
  assert.equal(blocked.report.blockedReason, "STAGED_POINTER_MISSING");

  // Fail closed: unknown argument; isolated output inside a publication-package root
  await assert.rejects(() => run(["--bogus"]), /Unknown argument/);
  console.log = () => {};
  await assert.rejects(
    () =>
      run([
        "--airtable-fixture",
        fixtureFile,
        "--report-dir",
        path.join(PKG_ROOT, "__never_write_here__"),
      ]),
    /Refusing isolated reconstruction inside a publication-package root/,
  );
  console.log = origLog;
  assert.ok(!fs.existsSync(path.join(PKG_ROOT, "__never_write_here__", "_isolated")));
} finally {
  console.log = origLog;
  fs.rmSync(tmp, { recursive: true, force: true });
}

assert.deepEqual([sha(pointerFile), sha(currentFile), sha(pkgFile)], before, "STAGED/CURRENT bytes unchanged");
console.log("OK test:gdi-clean-airtable-reconstruction-cert-v1");
