#!/usr/bin/env node
/**
 * Certify BE_LOCAL_PRODUCT_PARITY_GATE:
 * - Valid matching Curio/Ascend candidate evidence PASSes
 * - Defective fixtures BLOCK (missing tabs, stale assets, missing images,
 *   invalid KPIs, altered content, stale/wrong hash)
 * - Publication orchestrator refuses writeback without PASS
 *
 * No Airtable writes.
 */
import assert from "assert";
import { mkdtempSync, mkdirSync, writeFileSync, rmSync, readFileSync, existsSync } from "fs";
import { join } from "path";
import { tmpdir } from "os";
import {
  BE_LOCAL_PRODUCT_PARITY_GATE,
  BE_LOCAL_PRODUCT_PARITY_GATE_V1,
  PARITY_STATUSES,
  REQUIRED_ASSET_PATHS,
  buildLocalProductParityEvidence,
  buildValidParityEvidenceFixture,
  evaluateLocalProductParityGate,
  persistLocalProductParityEvidence,
  hashCandidateBlocks,
} from "../lib/brand-explorer/be-local-product-parity-gate-v1.js";
import { BE_CANONICAL_TAB_IDS } from "../lib/brand-explorer/be-airtable-integration-sot-v1.js";
import {
  PUBLICATION_MODES,
  PUBLICATION_STEPS,
  runFounderApprovalPublication,
  validatePublicationGates,
  applyWritesGated,
  hashPresentationBlocks,
} from "../lib/brand-explorer/be-founder-approval-publication-v1.js";

const root = process.cwd();
const fixtureDir = join(root, "fixtures", "be-local-product-parity-gate-v1");
mkdirSync(fixtureDir, { recursive: true });

const CURIO = "receQkxgjlezsc1xg";
const ASCEND = "reclkgOzvAcBheUSo";
const APP_COMMIT = "2cd4d72e29e34324effdbe160f18d395234215d3";
const NOW = Date.parse("2026-11-20T00:00:00.000Z");
const FRESH = "2026-11-19T23:30:00.000Z";

const curioBlocks = [
  { slotKey: "overview.proof.1", title: "Proof", body: "Curio body", sort: 1 },
  { slotKey: "commercial.channels.1", title: "Channels", body: "Summary", sort: 1 },
];
const curioHash = hashCandidateBlocks(curioBlocks);
const ascendBlocks = [
  { slotKey: "overview.proof.1", title: "Proof", body: "Ascend body", sort: 1 },
  { slotKey: "loyalty.benefit.1", title: "Benefit", body: "Choice Privileges", sort: 1 },
];
const ascendHash = hashCandidateBlocks(ascendBlocks);

assert.equal(BE_LOCAL_PRODUCT_PARITY_GATE, "BE_LOCAL_PRODUCT_PARITY_GATE");
assert.equal(BE_LOCAL_PRODUCT_PARITY_GATE_V1, "BE_LOCAL_PRODUCT_PARITY_GATE_V1");
assert.ok(PUBLICATION_STEPS.includes("LOCAL_PRODUCT_PARITY"));
assert.equal(BE_CANONICAL_TAB_IDS.length, 10);

// --- Valid Curio PASS ---
const curioPass = buildValidParityEvidenceFixture({
  brandId: CURIO,
  candidateHash: curioHash,
  applicationCommit: APP_COMMIT,
  previewCommit: APP_COMMIT,
  validatedAt: FRESH,
});
assert.equal(curioPass.status, PARITY_STATUSES.PASS);
assert.equal(curioPass.ok, true);
assert.equal(curioPass.tenTabPassCount, 10);
writeFileSync(join(fixtureDir, "curio-valid-pass.json"), JSON.stringify(curioPass, null, 2));

const curioEval = evaluateLocalProductParityGate({
  expectedCandidateHash: curioHash,
  expectedApplicationCommit: APP_COMMIT,
  report: curioPass,
  nowMs: NOW,
});
assert.equal(curioEval.ok, true, JSON.stringify(curioEval.failures));

// --- Valid Ascend PASS ---
const ascendPass = buildValidParityEvidenceFixture({
  brandId: ASCEND,
  candidateHash: ascendHash,
  applicationCommit: APP_COMMIT,
  previewCommit: APP_COMMIT,
  validatedAt: FRESH,
});
assert.equal(ascendPass.ok, true);
writeFileSync(join(fixtureDir, "ascend-valid-pass.json"), JSON.stringify(ascendPass, null, 2));
assert.equal(
  evaluateLocalProductParityGate({
    expectedCandidateHash: ascendHash,
    expectedApplicationCommit: APP_COMMIT,
    report: ascendPass,
    nowMs: NOW,
  }).ok,
  true,
);

function blockFixture(name, mutate) {
  const base = buildValidParityEvidenceFixture({
    brandId: CURIO,
    candidateHash: curioHash,
    applicationCommit: APP_COMMIT,
    validatedAt: FRESH,
  });
  mutate(base);
  // Force rebuild via evaluate path that reevaluates tabs/assets/content
  const rebuilt = buildLocalProductParityEvidence(base);
  writeFileSync(join(fixtureDir, name), JSON.stringify(rebuilt, null, 2));
  const ev = evaluateLocalProductParityGate({
    expectedCandidateHash: curioHash,
    expectedApplicationCommit: APP_COMMIT,
    report: rebuilt,
    nowMs: NOW,
  });
  assert.equal(ev.ok, false, `${name} must BLOCK`);
  assert.equal(ev.status, PARITY_STATUSES.BLOCK);
  return { rebuilt, ev };
}

// Missing tabs
blockFixture("defect-missing-tabs.json", (b) => {
  delete b.tabs["atelier-insight"];
  delete b.tabs["atelier-materials"];
  b.tenTabPassCount = 8;
});

// Stale / mismatched assets
blockFixture("defect-stale-assets.json", (b) => {
  b.assets[REQUIRED_ASSET_PATHS[0]] = {
    match: false,
    appSha: "old",
    previewSha: "new",
  };
});

// Missing images on a tab
blockFixture("defect-missing-images.json", (b) => {
  b.tabs["atelier-materials"] = {
    pass: false,
    failures: ["IMAGES_DIFFER_OR_MISSING"],
    missingImages: true,
  };
});

// Invalid KPIs
blockFixture("defect-invalid-kpis.json", (b) => {
  b.tabs["atelier-commercial"] = {
    pass: false,
    failures: ["KPIS_DIFFER_OR_INVALID"],
    invalidKpis: true,
  };
});

// Altered content
blockFixture("defect-altered-content.json", (b) => {
  b.content.alteredUnexpectedly = true;
  b.content.materialContentLost = true;
});

// Wrong candidate hash (stale report for another candidate)
{
  const wrong = buildValidParityEvidenceFixture({
    brandId: CURIO,
    candidateHash: "f".repeat(64),
    applicationCommit: APP_COMMIT,
    validatedAt: FRESH,
  });
  writeFileSync(join(fixtureDir, "defect-wrong-candidate-hash.json"), JSON.stringify(wrong, null, 2));
  const ev = evaluateLocalProductParityGate({
    expectedCandidateHash: curioHash,
    expectedApplicationCommit: APP_COMMIT,
    report: wrong,
    nowMs: NOW,
  });
  assert.equal(ev.ok, false);
  assert.ok(ev.failures.some((f) => f.code === "PARITY_REPORT_WRONG_CANDIDATE_HASH"));
}

// Stale timestamp
{
  const stale = buildValidParityEvidenceFixture({
    brandId: CURIO,
    candidateHash: curioHash,
    applicationCommit: APP_COMMIT,
    validatedAt: "2026-11-18T00:00:00.000Z",
  });
  writeFileSync(join(fixtureDir, "defect-stale-report.json"), JSON.stringify(stale, null, 2));
  const ev = evaluateLocalProductParityGate({
    expectedCandidateHash: curioHash,
    expectedApplicationCommit: APP_COMMIT,
    report: stale,
    nowMs: NOW,
    maxAgeMs: 6 * 60 * 60 * 1000,
  });
  assert.equal(ev.ok, false);
  assert.ok(ev.failures.some((f) => f.code === "PARITY_REPORT_STALE"));
}

// Missing report
{
  const ev = evaluateLocalProductParityGate({
    expectedCandidateHash: curioHash,
    expectedApplicationCommit: APP_COMMIT,
  });
  assert.equal(ev.ok, false);
  assert.ok(ev.failures.some((f) => f.code === "LOCAL_PRODUCT_PARITY_REQUIRED"));
}

// Persist evidence into audit store (tmp)
const tmp = mkdtempSync(join(tmpdir(), "be-parity-"));
const persisted = persistLocalProductParityEvidence({ root: tmp, evidence: curioPass });
assert.ok(existsSync(persisted.path));
assert.ok(persisted.relativePath.includes("local-product-parity"));

// Publication orchestrator: without parity → BLOCKED_GATES, 0 writes
mkdirSync(join(tmp, "configs"), { recursive: true });
writeFileSync(
  join(tmp, "configs", "brand-explorer-founder-publication-activation-v1.json"),
  JSON.stringify({
    contract: "BE_FOUNDER_PUBLICATION_ACTIVATION_V1",
    productionWritesAuthorized: false,
    automaticPublicationEnabled: false,
  }),
);

const proposed = curioBlocks;
const dryBlocked = runFounderApprovalPublication({
  root: tmp,
  mode: PUBLICATION_MODES.DRY_RUN,
  brandId: CURIO,
  candidateVersion: "parity-gate-cert",
  currentBlocks: [],
  proposedBlocks: proposed,
  approveAllChanged: true,
  brandDecision: { decision: "approve_and_publish" },
  gate1: { founderApproved: true },
  gate2: { founderApproved: true, status: "APPROVED" },
  scorecard: { founderApprovalStatus: "FOUNDER_APPROVED", priorityBlocked: 0, researchGapCount: 0 },
  applicationCommit: APP_COMMIT,
  persistSnapshot: false,
  persistAudit: true,
});
assert.equal(dryBlocked.airtableWrites, 0);
assert.equal(dryBlocked.publicationApplied, false);
assert.equal(dryBlocked.status, "BLOCKED_GATES");
assert.equal(dryBlocked.localProductParityGate.ok, false);
assert.ok(dryBlocked.steps.some((s) => s.step === "LOCAL_PRODUCT_PARITY"));

// With matching PASS parity → gates can clear parity; still no Airtable writes without activation
const dryParityPass = runFounderApprovalPublication({
  root: tmp,
  mode: PUBLICATION_MODES.PRODUCTION,
  brandId: CURIO,
  candidateVersion: "parity-gate-cert",
  currentBlocks: [],
  proposedBlocks: proposed,
  approveAllChanged: true,
  brandDecision: { decision: "approve_and_publish" },
  gate1: { founderApproved: true },
  gate2: { founderApproved: true, status: "APPROVED" },
  scorecard: { founderApprovalStatus: "FOUNDER_APPROVED", priorityBlocked: 0, researchGapCount: 0 },
  applicationCommit: APP_COMMIT,
  localProductParity: {
    ...curioPass,
    candidateHash: hashPresentationBlocks(proposed),
  },
  persistSnapshot: false,
  persistAudit: true,
  nowMs: NOW,
});
assert.equal(dryParityPass.airtableWrites, 0);
assert.equal(dryParityPass.publicationApplied, false);
assert.equal(dryParityPass.localProductParityGate.ok, true);

// applyWritesGated refuses without localParityOk===true (even if activation+env would allow writes)
const prevEnv = process.env.BE_FOUNDER_PUBLICATION_PRODUCTION_WRITES;
process.env.BE_FOUNDER_PUBLICATION_PRODUCTION_WRITES = "1";
writeFileSync(
  join(tmp, "configs", "brand-explorer-founder-publication-activation-v1.json"),
  JSON.stringify({
    contract: "BE_FOUNDER_PUBLICATION_ACTIVATION_V1",
    productionWritesAuthorized: true,
    automaticPublicationEnabled: false,
  }),
);
const refused = applyWritesGated({
  mode: PUBLICATION_MODES.PRODUCTION,
  root: tmp,
  activation: {
    productionWritesAuthorized: true,
    automaticPublicationEnabled: false,
  },
  gatesOk: true,
  concurrencyOk: true,
  localParityOk: false,
  writePlan: { operations: [{ op: "UPDATE" }] },
});
assert.equal(refused.applied, false);
assert.ok(String(refused.reason).includes("BE_LOCAL_PRODUCT_PARITY_GATE"));
if (prevEnv === undefined) delete process.env.BE_FOUNDER_PUBLICATION_PRODUCTION_WRITES;
else process.env.BE_FOUNDER_PUBLICATION_PRODUCTION_WRITES = prevEnv;
writeFileSync(
  join(tmp, "configs", "brand-explorer-founder-publication-activation-v1.json"),
  JSON.stringify({
    contract: "BE_FOUNDER_PUBLICATION_ACTIVATION_V1",
    productionWritesAuthorized: false,
    automaticPublicationEnabled: false,
  }),
);

// validatePublicationGates lists LOCAL_PRODUCT_PARITY_REQUIRED
const vg = validatePublicationGates({
  brandId: CURIO,
  gate1: { founderApproved: true },
  gate2: { founderApproved: true },
  brandDecision: { decision: "approve_and_publish" },
  scorecard: { founderApprovalStatus: "FOUNDER_APPROVED", priorityBlocked: 0 },
  mode: PUBLICATION_MODES.DRY_RUN,
});
assert.equal(vg.ok, false);
assert.ok(vg.failures.some((f) => f.code === "LOCAL_PRODUCT_PARITY_REQUIRED"));

// Contract file present
const contractPath = join(root, "configs", "brand-explorer-local-product-parity-gate-contract-v1.json");
assert.ok(existsSync(contractPath));
const contract = JSON.parse(readFileSync(contractPath, "utf8"));
assert.equal(contract.gate, BE_LOCAL_PRODUCT_PARITY_GATE);

rmSync(tmp, { recursive: true, force: true });

console.log(
  JSON.stringify(
    {
      ok: true,
      gate: BE_LOCAL_PRODUCT_PARITY_GATE,
      curioPass: true,
      ascendPass: true,
      defectiveFixturesBlocked: [
        "missing-tabs",
        "stale-assets",
        "missing-images",
        "invalid-kpis",
        "altered-content",
        "wrong-candidate-hash",
        "stale-report",
      ],
      publicationBlockedWithoutParity: true,
      airtableWrites: 0,
      fixtureDir,
    },
    null,
    2,
  ),
);
