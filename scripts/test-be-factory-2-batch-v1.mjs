#!/usr/bin/env node
/**
 * Factory 2.0 unit gates — staging, approval tokens, protected skip, dashboard.
 */
import { mkdirSync, rmSync, writeFileSync, existsSync } from "fs";
import { join } from "path";
import { fileURLToPath } from "url";
import { dirname } from "path";
import {
  FACTORY_2_STATUSES,
  PROTECTED_BRAND_IDS,
  BE_FACTORY_2_0_BATCH_V1,
} from "../lib/brand-explorer/factory-2/be-factory-2-contracts-v1.js";
import {
  saveQueue,
  saveBrandStage,
  loadBrandStage,
  buildPortfolioDashboard,
} from "../lib/brand-explorer/factory-2/be-factory-2-staging-store-v1.js";
import {
  recordBatchApprovals,
  detectIncrementalChange,
} from "../lib/brand-explorer/factory-2/be-factory-2-batch-approval-v1.js";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..", ".tmp-factory-2-test");
let failed = 0;

function assert(cond, msg) {
  if (!cond) {
    failed += 1;
    console.error("FAIL:", msg);
  } else {
    console.log("OK:", msg);
  }
}

function hashFn(blocks) {
  return JSON.stringify(blocks || []).length + "-h";
}

function setup() {
  if (existsSync(ROOT)) rmSync(ROOT, { recursive: true, force: true });
  mkdirSync(ROOT, { recursive: true });
  // Minimal package root marker
  writeFileSync(join(ROOT, "package.json"), "{}\n");
}

function main() {
  setup();

  const queue = {
    contract: BE_FACTORY_2_0_BATCH_V1,
    brands: [
      {
        brandId: "recTESTREADY001",
        brandName: "Test Ready",
        parentCompany: "Test Co",
        priority: 1,
        status: FACTORY_2_STATUSES.QUEUED,
      },
      {
        brandId: PROTECTED_BRAND_IDS[0],
        brandName: "Curio",
        parentCompany: "IHG",
        priority: 9999,
        status: FACTORY_2_STATUSES.SKIPPED_PROTECTED,
      },
      {
        brandId: "recTESTGAP002",
        brandName: "Test Gap",
        parentCompany: "Test Co",
        priority: 2,
        status: FACTORY_2_STATUSES.NEEDS_REMEDIATION,
      },
    ],
  };
  saveQueue(ROOT, queue);

  saveBrandStage(ROOT, "recTESTREADY001", {
    contract: BE_FACTORY_2_0_BATCH_V1,
    brandId: "recTESTREADY001",
    brandName: "Test Ready",
    factoryStatus: FACTORY_2_STATUSES.READY_FOR_REVIEW,
    approvalStatus: "NOT_APPROVED",
    candidateHash: "abc123hash",
    assembledHash: "assembled456",
    certification: { gaps: [], tenTabQualityScore: 88 },
    writePlan: { counts: { update: 2, create: 1, deactivate: 0 }, mutatingOps: 3 },
    researchSpend: { totalUsd: 0 },
  });

  saveBrandStage(ROOT, "recTESTGAP002", {
    contract: BE_FACTORY_2_0_BATCH_V1,
    brandId: "recTESTGAP002",
    brandName: "Test Gap",
    factoryStatus: FACTORY_2_STATUSES.NEEDS_REMEDIATION,
    approvalStatus: "NOT_APPROVED",
    candidateHash: "gapHash",
    certification: { gaps: ["loyalty.proof missing"], tenTabQualityScore: 40 },
    writePlan: { mutatingOps: 0 },
    researchSpend: { totalUsd: 0 },
  });

  const dash = buildPortfolioDashboard(ROOT);
  assert(dash.summary.totalQueued === 3, "dashboard counts queued brands");
  assert(dash.summary.readyForReview === 1, "one READY_FOR_REVIEW");
  assert(dash.summary.needsRemediation === 1, "one NEEDS_REMEDIATION");
  assert(dash.summary.skippedProtected === 1, "one SKIPPED_PROTECTED");

  // Wrong hash rejected
  const bad = recordBatchApprovals(ROOT, {
    approvedBy: "test@dealality.com",
    selections: [{ brandId: "recTESTREADY001", candidateHash: "WRONG" }],
  });
  assert(!bad.ok && bad.failedCount === 1, "hash mismatch refuses approval");

  // Remediation cannot approve
  const rem = recordBatchApprovals(ROOT, {
    approvedBy: "test@dealality.com",
    selections: [{ brandId: "recTESTGAP002", candidateHash: "gapHash" }],
  });
  assert(!rem.ok, "NEEDS_REMEDIATION cannot be approved");

  // Protected refused
  const prot = recordBatchApprovals(ROOT, {
    approvedBy: "test@dealality.com",
    selections: [{ brandId: PROTECTED_BRAND_IDS[0], candidateHash: "x" }],
  });
  assert(!prot.ok, "protected brand approval refused");

  // Exact candidate approval succeeds — no writeback flag
  const good = recordBatchApprovals(ROOT, {
    approvedBy: "test@dealality.com",
    selections: [{ brandId: "recTESTREADY001", candidateHash: "abc123hash" }],
  });
  assert(good.ok && good.approvedCount === 1, "exact candidate approval recorded");
  assert(good.productionWritebackExecuted === undefined || good.productionWritebackExecuted === false, "no production write");
  const stage = loadBrandStage(ROOT, "recTESTREADY001");
  assert(
    stage.factoryStatus === FACTORY_2_STATUSES.APPROVED_FOR_WRITEBACK ||
      stage.factoryStatus === FACTORY_2_STATUSES.APPROVED,
    "stage marked APPROVED_FOR_WRITEBACK"
  );
  assert(stage.writebackAuthorized === false, "writeback still false after approval record");
  assert(stage.approval?.tokenId?.startsWith("be-f2-appr-"), "per-candidate token minted");

  // Incremental change detection
  const delta = detectIncrementalChange(
    { candidateHash: "h1" },
    [{ slotKey: "a" }],
    hashFn
  );
  assert(delta.materialChange === true || delta.unchanged === false, "incremental detector runs");
  const same = detectIncrementalChange({ candidateHash: hashFn([{ a: 1 }]) }, [{ a: 1 }], hashFn);
  assert(same.unchanged === true, "unchanged when hashes match");

  rmSync(ROOT, { recursive: true, force: true });

  if (failed) {
    console.error(`\n${failed} assertion(s) failed`);
    process.exit(1);
  }
  console.log("\nAll Factory 2.0 batch tests passed");
}

main();
