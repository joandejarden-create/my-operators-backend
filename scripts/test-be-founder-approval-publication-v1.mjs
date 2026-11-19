#!/usr/bin/env node
/**
 * Gate: founder approval → publication workflow (dry-run / isolated).
 * Asserts production writes stay disabled and Curio/Ascend gates fail closed.
 */
import assert from "assert";
import { mkdtempSync, rmSync, writeFileSync, mkdirSync, readFileSync } from "fs";
import { join } from "path";
import { tmpdir } from "os";
import {
  BE_FOUNDER_APPROVAL_PUBLICATION_V1,
  PUBLICATION_MODES,
  hashPresentationBlocks,
  lockCandidateVersion,
  validatePublicationGates,
  buildAirtableWritePlan,
  verifyIdentityAndConcurrency,
  runFounderApprovalPublication,
  productionWritesEnabled,
  loadActivationConfig,
} from "../lib/brand-explorer/be-founder-approval-publication-v1.js";

const root = process.cwd();
const tmp = mkdtempSync(join(tmpdir(), "be-pub-"));
mkdirSync(join(tmp, "configs"), { recursive: true });
mkdirSync(join(tmp, "reports", "brand-explorer", "comparison-studio"), { recursive: true });
writeFileSync(
  join(tmp, "configs", "brand-explorer-founder-publication-activation-v1.json"),
  JSON.stringify({
    contract: "BE_FOUNDER_PUBLICATION_ACTIVATION_V1",
    productionWritesAuthorized: false,
    automaticPublicationEnabled: false,
  }),
);

const currentBlocks = [
  { recordId: "recAAAAAAAAAAAAAA", slotKey: "overview.proof.1", title: "Old", body: "A", sort: 1 },
  { recordId: "recBBBBBBBBBBBBBB", slotKey: "loyalty.benefit.1", title: "Keep", body: "Same", sort: 1 },
];
const proposedBlocks = [
  { slotKey: "overview.proof.1", title: "New", body: "B", sort: 1 },
  { slotKey: "loyalty.benefit.1", title: "Keep", body: "Same", sort: 1 },
  { slotKey: "materials.gallery.1", title: "Photo", body: "https://example.com/a.jpg", sort: 1, imageUrl: "https://example.com/a.jpg" },
];

assert.equal(BE_FOUNDER_APPROVAL_PUBLICATION_V1, "BE_FOUNDER_APPROVAL_PUBLICATION_V1");

const lock = lockCandidateVersion({
  brandId: "receQkxgjlezsc1xg",
  candidateVersion: "phase2h-component-parity",
  proposedBlocks,
});
assert.equal(lock.ok, true);
assert.equal(lock.contentHash.length, 64);
assert.equal(lock.contentHash, hashPresentationBlocks(proposedBlocks));

const gatesFail = validatePublicationGates({
  brandId: "receQkxgjlezsc1xg",
  gate1: { founderApproved: false },
  gate2: { status: "BLOCKED", founderApproved: false },
  brandDecision: { decision: "defer" },
  scorecard: { founderApprovalStatus: "NOT_FOUNDER_APPROVED" },
  activation: loadActivationConfig(tmp),
  mode: PUBLICATION_MODES.DRY_RUN,
});
assert.equal(gatesFail.ok, false);
assert.ok(gatesFail.failures.some((f) => f.code === "GATE1_NOT_APPROVED"));
assert.ok(gatesFail.failures.some((f) => f.code === "GATE2_BLOCKED"));
assert.ok(gatesFail.failures.some((f) => f.code === "FOUNDER_BRAND_NOT_APPROVED"));

const plan = buildAirtableWritePlan({
  brandId: "receQkxgjlezsc1xg",
  currentBlocks,
  proposedBlocks,
  approvedSlotKeys: ["overview.proof.1", "materials.gallery.1"],
});
assert.equal(plan.counts.update, 1);
assert.equal(plan.counts.create, 1);
assert.equal(plan.counts.skippedNotApproved, 0);
assert.ok(plan.operations.every((o) => o.table === "Brand Setup - Brand Explorer Presentation"));
assert.ok(plan.excludedTables.includes("Hotel Property Census"));

const concurrent = verifyIdentityAndConcurrency({
  brandId: "receQkxgjlezsc1xg",
  baselineContentHash: "aaa",
  liveContentHash: "bbb",
});
assert.equal(concurrent.ok, false);
assert.ok(concurrent.failures.some((f) => f.code === "CONCURRENT_MODIFICATION"));

const identityOk = verifyIdentityAndConcurrency({
  brandId: "receQkxgjlezsc1xg",
  baselineContentHash: "abc",
  liveContentHash: "abc",
});
assert.equal(identityOk.ok, true);

assert.equal(productionWritesEnabled({ root: tmp }), false);

const dry = runFounderApprovalPublication({
  root: tmp,
  mode: PUBLICATION_MODES.DRY_RUN,
  brandId: "receQkxgjlezsc1xg",
  brandSlug: "curio-collection",
  brandName: "Curio Collection by Hilton",
  candidateVersion: "phase2h-component-parity",
  currentBlocks,
  proposedBlocks,
  approvedSlotKeys: ["overview.proof.1", "materials.gallery.1"],
  brandDecision: { decision: "approve_and_publish" },
  gate1: { founderApproved: false },
  gate2: { status: "BLOCKED", founderApproved: false },
  scorecard: {
    founderApprovalStatus: "NOT_FOUNDER_APPROVED",
    priorityBlocked: 0,
    researchGapCount: 11,
  },
  persistSnapshot: true,
  persistAudit: true,
});
assert.equal(dry.status, "BLOCKED_GATES");
assert.equal(dry.airtableWrites, 0);
assert.equal(dry.publicationApplied, false);
assert.equal(dry.customerPromoted, false);
assert.equal(dry.productionWritesEnabled, false);
assert.ok(dry.steps.length >= 10);
assert.ok(dry.auditRelativePath);

// Even with approve decision + fake gate pass, production writer remains inert without activation
const wouldPassGates = runFounderApprovalPublication({
  root: tmp,
  mode: PUBLICATION_MODES.PRODUCTION,
  brandId: "receQkxgjlezsc1xg",
  candidateVersion: "phase2h-component-parity",
  currentBlocks,
  proposedBlocks,
  approveAllChanged: true,
  brandDecision: { decision: "approve_and_publish" },
  gate1: { founderApproved: true },
  gate2: { founderApproved: true, status: "APPROVED" },
  scorecard: { founderApprovalStatus: "FOUNDER_APPROVED", priorityBlocked: 0, researchGapCount: 0 },
  persistSnapshot: false,
  persistAudit: true,
});
assert.equal(wouldPassGates.airtableWrites, 0);
assert.equal(wouldPassGates.publicationApplied, false);
const applyStep = wouldPassGates.steps.find((s) => s.step === "APPLY_WRITES");
assert.ok(applyStep);
assert.equal(applyStep.applied, false);

// Repo activation file must remain unauthorized
const repoActivation = loadActivationConfig(root);
assert.equal(repoActivation.productionWritesAuthorized, false);

rmSync(tmp, { recursive: true, force: true });
console.log(
  JSON.stringify(
    {
      ok: true,
      contract: BE_FOUNDER_APPROVAL_PUBLICATION_V1,
      dryRunStatus: dry.status,
      airtableWrites: 0,
      productionWritesAuthorized: false,
      curioAscend: "NOT_APPROVED",
    },
    null,
    2
  )
);
