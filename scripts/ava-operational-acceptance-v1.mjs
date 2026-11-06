#!/usr/bin/env node
/**
 * AVA Permanent Audit Runner — Operational Acceptance Gate
 *
 * npm run ava:operational-acceptance
 *
 * Distinguishes per-gate pass/fail/blocked. Never upgrades mocked Airtable
 * to AIRTABLE_PERSISTENCE_PASS.
 */
import fs from "node:fs";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT_DIR = path.join(ROOT, "reports/ava-permanent-audit-runner-v1");
fs.mkdirSync(OUT_DIR, { recursive: true });

function runNode(script, args = []) {
  const r = spawnSync(process.execPath, [script, ...args], {
    cwd: ROOT,
    encoding: "utf8",
    timeout: 600000,
  });
  return {
    status: r.status,
    stdout: r.stdout || "",
    stderr: r.stderr || "",
    ok: r.status === 0,
  };
}

const gates = {
  AUDIT_EXECUTION_PASS: { result: "PENDING", evidence: null },
  ADP_AVA_CERTIFICATION_PASS: { result: "PENDING", evidence: null },
  PDF_QUALITY_PASS: { result: "PENDING", evidence: null },
  ARCHIVE_INTEGRITY_PASS: { result: "PENDING", evidence: null },
  AIRTABLE_PERSISTENCE_PASS: { result: "PENDING", evidence: null },
  ADMIN_HISTORY_PASS: { result: "PENDING", evidence: null },
  HISTORICAL_COMPARISON_PASS: { result: "PENDING", evidence: null },
  REPRODUCTION_PASS: { result: "PENDING", evidence: null },
  REGRESSION_PASS: { result: "PENDING", evidence: null },
};

console.log("== regression ==");
const regression = runNode("scripts/test-ava-permanent-audit-runner-v1.mjs");
gates.REGRESSION_PASS = {
  result: regression.ok ? "PASS" : "FAIL",
  evidence: regression.ok
    ? "test-ava-permanent-audit-runner-v1.mjs PASS"
    : (regression.stdout + regression.stderr).slice(-800),
};

console.log("== three-hotel acceptance ==");
const three = runNode("scripts/ava-operational-acceptance-three-hotels-v1.mjs");
const threePath = path.join(OUT_DIR, "THREE_HOTEL_OPERATIONAL_ACCEPTANCE.json");
const threeJson = fs.existsSync(threePath)
  ? JSON.parse(fs.readFileSync(threePath, "utf8"))
  : null;

gates.AUDIT_EXECUTION_PASS = {
  result:
    threeJson?.summary?.spiceCertifiedMatch &&
    threeJson?.summary?.cambridgeHistorical &&
    threeJson?.summary?.acVersioning
      ? "PASS"
      : "FAIL",
  evidence: threeJson?.summary || three.stderr.slice(-400),
};

gates.ADP_AVA_CERTIFICATION_PASS = {
  result:
    threeJson?.hotels?.spice?.crossReportStatus === "CERTIFIED_MATCH" &&
    threeJson?.hotels?.cambridge?.crossReportStatus ===
      "CERTIFIED_HISTORICAL_REFERENCE"
      ? "PASS"
      : "FAIL",
  evidence: {
    spice: threeJson?.hotels?.spice?.crossReportStatus,
    cambridge: threeJson?.hotels?.cambridge?.crossReportStatus,
    ac: threeJson?.hotels?.acCoruna?.version2?.crossReportStatus,
  },
};

gates.PDF_QUALITY_PASS = {
  result:
    threeJson?.hotels?.spice?.customerQuality?.certified === true &&
    threeJson?.hotels?.spice?.pdfSha256
      ? "PASS"
      : "FAIL",
  evidence: {
    spiceCqa: threeJson?.hotels?.spice?.customerQuality,
    pdfSha256: threeJson?.hotels?.spice?.pdfSha256,
  },
};

gates.ARCHIVE_INTEGRITY_PASS = {
  result: threeJson?.summary?.adminPdfIntegrity ? "PASS" : "FAIL",
  evidence: threeJson?.admin || null,
};

gates.HISTORICAL_COMPARISON_PASS = {
  result:
    threeJson?.hotels?.acCoruna?.naturalCompare?.ok === true &&
    threeJson?.hotels?.acCoruna?.controlledFixtureCompare?.result?.ok === true &&
    (threeJson?.hotels?.acCoruna?.controlledFixtureCompare?.result?.newFindings
      ?.length || 0) >= 1
      ? "PASS"
      : "FAIL",
  evidence: {
    natural: threeJson?.hotels?.acCoruna?.naturalCompare,
    fixtureNewFindings:
      threeJson?.hotels?.acCoruna?.controlledFixtureCompare?.result?.newFindings,
  },
};

gates.REPRODUCTION_PASS = {
  result:
    threeJson?.reproduction?.classification === "BYTE_IDENTICAL_REPRODUCTION" ||
    threeJson?.reproduction?.classification ===
      "SEMANTICALLY_IDENTICAL_REPRODUCTION"
      ? "PASS"
      : "FAIL",
  evidence: threeJson?.reproduction || null,
  note: "Pre-stamp archives may be SEMANTICALLY_IDENTICAL; new stamps enable BYTE_IDENTICAL",
};

console.log("== admin API ==");
const admin = runNode("scripts/ava-operational-acceptance-admin-api-v1.mjs");
const adminPath = path.join(OUT_DIR, "ADMIN_HISTORY_API_VERIFICATION.json");
const adminJson = fs.existsSync(adminPath)
  ? JSON.parse(fs.readFileSync(adminPath, "utf8"))
  : null;
gates.ADMIN_HISTORY_PASS = {
  result: admin.ok && adminJson?.ok ? "PASS" : "FAIL",
  evidence: adminJson || admin.stderr.slice(-400),
};

console.log("== airtable schema readonly ==");
runNode("scripts/ava-airtable-schema-reconcile-readonly-v1.mjs");
const schemaPath = path.join(OUT_DIR, "AIRTABLE_SCHEMA_RECONCILIATION_READONLY.json");
const schemaJson = fs.existsSync(schemaPath)
  ? JSON.parse(fs.readFileSync(schemaPath, "utf8"))
  : null;

console.log("== live ADP Airtable persistence ==");
const livePers = runNode("scripts/ava-adp-airtable-live-persistence-cert-v1.mjs");
const livePath = path.join(OUT_DIR, "ADP_AIRTABLE_LIVE_PERSISTENCE_CERT.json");
const liveJson = fs.existsSync(livePath)
  ? JSON.parse(fs.readFileSync(livePath, "utf8"))
  : null;

const livePass = liveJson?.overall === "AIRTABLE_PERSISTENCE_PASS";
gates.AIRTABLE_PERSISTENCE_PASS = {
  result: livePass ? "PASS" : livePers.ok === false ? "FAIL" : "BLOCKED",
  evidence: {
    overall: liveJson?.overall || null,
    gates: liveJson?.gates || null,
    mapping: liveJson?.mapping || null,
    base: liveJson?.base || null,
    idempotency: liveJson?.idempotency
      ? {
          ok: liveJson.idempotency.ok,
          snapshotRecordCount: liveJson.idempotency.snapshotRecordCount,
          secondWasPatch: liveJson.idempotency.secondWasPatch,
        }
      : null,
    schemaLegacyNote: schemaJson?.verdict || null,
    note: "Live PASS requires write + read-back + idempotency on existing ADP tables (not local mirror).",
  },
};

// All FAIL gates block acceptance (including live Airtable persistence).
const blockingFails = Object.entries(gates).filter(([, v]) => v.result === "FAIL");
const blocked = Object.entries(gates).filter(([, v]) => v.result === "BLOCKED");
const schemaApproval =
  liveJson?.overall === "AIRTABLE_SCHEMA_AUTHORIZATION_REQUIRED" ||
  liveJson?.gates?.AIRTABLE_SCHEMA_COMPATIBLE === "AIRTABLE_SCHEMA_AUTHORIZATION_REQUIRED";

let overall;
let statusToken;
if (schemaApproval && !blockingFails.length) {
  overall = "OPERATIONAL_ACCEPTANCE_CONDITIONAL";
  statusToken = "AVA_EXISTING_ADP_AIRTABLE_SCHEMA_APPROVAL_REQUIRED";
} else if (blockingFails.length) {
  overall = "OPERATIONAL_ACCEPTANCE_FAIL";
  statusToken = "AVA_PERMANENT_AUDIT_RUNNER_OPERATIONAL_ACCEPTANCE_FAIL";
} else if (blocked.length) {
  overall = "OPERATIONAL_ACCEPTANCE_CONDITIONAL";
  statusToken = "AVA_PERMANENT_AUDIT_RUNNER_OPERATIONAL_ACCEPTANCE_CONDITIONAL";
} else {
  overall = "OPERATIONAL_ACCEPTANCE_PASS";
  statusToken = "AVA_PERMANENT_AUDIT_RUNNER_OPERATIONAL_ACCEPTANCE_PASS";
}

const manifest = {
  contract: "AVA_OPERATIONAL_ACCEPTANCE_MANIFEST_V1",
  generatedAt: new Date().toISOString(),
  overall,
  statusToken,
  gates,
  blockers: blocked.map(([k, v]) => ({ gate: k, ...v })),
  failures: blockingFails.map(([k, v]) => ({ gate: k, ...v })),
  artifacts: {
    threeHotel: threePath,
    adminApi: adminPath,
    schema: schemaPath,
    liveAirtable: livePath,
  },
  git: {
    branch: "fix/ava-v4-customer-quality-remediation-v1",
    note: "Uncommitted permanent-runner work in ava-v4-quality worktree; intended for canonical my-operators-backend",
  },
};

const manifestPath = path.join(OUT_DIR, "OPERATIONAL_ACCEPTANCE_MANIFEST.json");
fs.writeFileSync(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`);
console.log(JSON.stringify({ overall, statusToken, manifestPath, gates: Object.fromEntries(Object.entries(gates).map(([k,v])=>[k,v.result])) }, null, 2));
process.exit(overall === "OPERATIONAL_ACCEPTANCE_FAIL" ? 3 : 0);
