#!/usr/bin/env node
/**
 * Master queue reconcile gates — authorized batches survive Active/Live-only cohorts.
 */
import { mkdirSync, rmSync, writeFileSync, existsSync, readFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import {
  FACTORY_2_STATUSES,
  BE_FACTORY_2_0_BATCH_V1,
  AUTOGRAPH_BRAND_ID,
} from "../lib/brand-explorer/factory-2/be-factory-2-contracts-v1.js";
import {
  AUTHORIZED_FACTORY_2_BATCHES,
  authorizedBrandCount,
  authorizedBatchCount,
} from "../lib/brand-explorer/factory-2/be-factory-2-authorized-batches-v1.js";
import {
  saveQueue,
  saveBrandStage,
  loadQueue,
  buildPortfolioDashboard,
} from "../lib/brand-explorer/factory-2/be-factory-2-staging-store-v1.js";
import {
  reconcileMasterQueue,
  upsertQueueBrandFromDepthResult,
} from "../lib/brand-explorer/factory-2/be-factory-2-master-queue-reconcile-v1.js";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..", ".tmp-factory-2-master-queue");
let failed = 0;

function assert(cond, msg) {
  if (!cond) {
    failed += 1;
    console.error("FAIL:", msg);
  } else {
    console.log("OK:", msg);
  }
}

function setup() {
  if (existsSync(ROOT)) rmSync(ROOT, { recursive: true, force: true });
  mkdirSync(ROOT, { recursive: true });
  writeFileSync(join(ROOT, "package.json"), "{}\n");
}

function main() {
  setup();

  assert(authorizedBatchCount() >= 7, "at least 7 authorized batches registered");
  assert(authorizedBrandCount() >= 50, "authorized brand slots cover Batches 1–7 + Autograph");

  // Simulate Active/Live-only queue (6 brands) — the failure mode the founder reported
  saveQueue(ROOT, {
    contract: BE_FACTORY_2_0_BATCH_V1,
    universe: { activeLiveCount: 6 },
    brands: [
      {
        brandId: AUTOGRAPH_BRAND_ID,
        brandName: "Autograph Collection",
        status: FACTORY_2_STATUSES.READY_FOR_REVIEW,
        priority: 1,
      },
      {
        brandId: "recWPEvxBQxVVzSq3",
        brandName: "Radisson Blu by Choice",
        status: FACTORY_2_STATUSES.QUEUED,
        priority: 2,
      },
      {
        brandId: "recOzH5iAE1xEjyD0",
        brandName: "Comfort Inn & Suites",
        status: FACTORY_2_STATUSES.QUEUED,
        priority: 3,
      },
      {
        brandId: "rec9aZp7GHtzUEg0c",
        brandName: "AC Hotels by Marriott",
        status: FACTORY_2_STATUSES.NEEDS_REMEDIATION,
        priority: 4,
        batchId: "NAMED_BATCH_FIVE",
      },
      {
        brandId: "recahVIW4aCx0Ao84",
        brandName: "Moxy Hotels",
        status: FACTORY_2_STATUSES.QUEUED,
        priority: 5,
      },
      {
        brandId: "recDwzv86TWnz2gGB",
        brandName: "Vignette Collection",
        status: FACTORY_2_STATUSES.QUEUED,
        priority: 6,
      },
    ],
  });

  // Staged Draft brand from Batch 5 — missing from Active/Live queue
  const jw = AUTHORIZED_FACTORY_2_BATCHES.find((b) => b.batchId === "BATCH_5").brands[0];
  saveBrandStage(ROOT, jw.brandId, {
    contract: BE_FACTORY_2_0_BATCH_V1,
    brandId: jw.brandId,
    brandName: jw.brandName,
    factoryStatus: FACTORY_2_STATUSES.NEEDS_REMEDIATION,
    candidateHash: "jw-hash-test",
    batchTag: "BATCH_5",
    researchSpend: { totalUsd: 0 },
    writePlan: { mutatingOps: 100 },
  });

  const { queue, dashboard } = reconcileMasterQueue({ repoRoot: ROOT });
  assert(queue.brands.length >= authorizedBrandCount(), "master queue >= authorized brand count");
  assert(
    queue.brands.some((b) => b.brandId === jw.brandId),
    "Batch 5 JW Marriott present after reconcile"
  );
  assert(
    queue.brands.filter((b) => (b.batchIds || []).includes("BATCH_7") || b.batchId === "BATCH_7")
      .length === 10,
    "all 10 Batch 7 brands registered"
  );
  assert(
    dashboard.summary.totalBrandsInMasterQueue >= authorizedBrandCount(),
    "dashboard master queue total accounts for authorized batches"
  );
  assert(
    (dashboard.nextTenScheduled || []).length === 10,
    "next 10 scheduled brands present"
  );

  // Depth upsert creates missing row without wiping others
  upsertQueueBrandFromDepthResult(
    ROOT,
    {
      brandId: "recNEWDEPTHTEST01",
      brandName: "Depth Upsert Test",
      factoryStatus: FACTORY_2_STATUSES.NEEDS_REMEDIATION,
    },
    { batchTag: "BATCH_TEST", researchClass: "EXISTING_CANONICAL" }
  );
  const after = loadQueue(ROOT);
  assert(
    after.brands.some((b) => b.brandId === "recNEWDEPTHTEST01"),
    "depth upsert adds missing brand to master queue"
  );
  assert(
    after.brands.some((b) => b.brandId === AUTOGRAPH_BRAND_ID),
    "Autograph preserved through upsert"
  );

  const dash2 = buildPortfolioDashboard(ROOT);
  assert(dash2.summary.researchSpendUsdTotal === 0, "research spend remains $0");

  // Live workspace smoke: authorized registry IDs unique
  const ids = AUTHORIZED_FACTORY_2_BATCHES.flatMap((b) => b.brands.map((x) => x.brandId));
  assert(new Set(ids).size === ids.length, "authorized brand IDs are unique across batches");

  rmSync(ROOT, { recursive: true, force: true });

  // Live repo reconcile must not shrink below staged+authorized expectation
  const liveRoot = join(dirname(fileURLToPath(import.meta.url)), "..");
  if (existsSync(join(liveRoot, "data/brand-explorer/factory-2-0/queue.json"))) {
    const live = reconcileMasterQueue({ repoRoot: liveRoot });
    assert(
      live.queue.brands.length >= authorizedBrandCount(),
      `live master queue ${live.queue.brands.length} >= authorized ${authorizedBrandCount()}`
    );
    const batch5 = live.queue.brands.filter(
      (b) => b.batchId === "BATCH_5" || (b.batchIds || []).includes("BATCH_5")
    );
    assert(batch5.length === 10, "live queue includes all Batch 5 brands");
    const report = join(
      liveRoot,
      "reports/brand-explorer/factory-2-0/70-master-queue-reconcile.json"
    );
    assert(existsSync(report), "reconcile audit report written");
    const audit = JSON.parse(readFileSync(report, "utf8"));
    assert(audit.reconciliation.masterQueueCount >= authorizedBrandCount(), "audit master count");
  }

  if (failed) {
    console.error(`\n${failed} assertion(s) failed`);
    process.exit(1);
  }
  console.log("\nAll master-queue reconcile tests passed");
}

main();
