/**
 * One narrowly scoped corrected Parallel live validation within remaining $2 cap.
 * Uses job segment bounded-live-v1-idlock (does not resubmit known contaminated job).
 *
 * Usage:
 *   node scripts/gdi-parallel-identity-lock-live-v1.mjs
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  executeBoundedLiveParallelDiscovery,
  PARALLEL_IDENTITY_LOCK_JOB_SEGMENT,
} from "../lib/group-demand-intelligence/workflow/parallel-discovery-adapter-v1.js";
import {
  readParallelBudgetLedger,
  remainingParallelBudgetUsd,
} from "../lib/group-demand-intelligence/parallel/budget-ledger-v1.js";
import { quarantineWrongMarketCandidates } from "../lib/group-demand-intelligence/workflow/geographic-relevance-gate-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const HOTEL_ID = "recnPB3Q02NO8Ma1o";
const OUT = path.join(
  ROOT,
  "reports/gdi-operational-baseline/san-diego-parallel-identity-lock-live"
);

const ledgerPath = path.join(
  ROOT,
  "reports/gdi-drawer-completeness/_local-repaired-store/parallel-budget/ledger.json"
);
const jobStorePath = path.join(
  ROOT,
  "reports/gdi-drawer-completeness/_local-repaired-store/parallel-jobs"
);

// Clear polluted GDI_DATA_ROOT; pin ledger/job store to reconciled path.
delete process.env.GDI_DATA_ROOT;
process.env.GDI_PARALLEL_BUDGET_LEDGER_PATH = ledgerPath;
process.env.GDI_PARALLEL_JOB_STORE_PATH = jobStorePath;
process.env.GDI_PARALLEL_ENABLED = "1";
process.env.GDI_PARALLEL_HOTEL_ALLOWLIST =
  process.env.GDI_PARALLEL_HOTEL_ALLOWLIST || HOTEL_ID;

const hotelConfig = JSON.parse(
  fs.readFileSync(
    path.join(ROOT, `config/group-demand-intelligence/hotels/${HOTEL_ID}.json`),
    "utf8"
  )
);

const ledgerBefore = readParallelBudgetLedger({
  env: process.env,
  budgetUsd: 2,
});
const remaining = remainingParallelBudgetUsd(ledgerBefore);
const floor = 0.5;

fs.mkdirSync(OUT, { recursive: true });

if (remaining < floor) {
  const blocked = {
    ok: false,
    reason: "remaining_budget_below_safe_floor",
    remainingUsd: remaining,
    requiredFloorUsd: floor,
    note: "Finish non-paid verification only — do not create fresh $2 allowance",
  };
  fs.writeFileSync(
    path.join(OUT, "blocked-budget.json"),
    JSON.stringify(blocked, null, 2) + "\n"
  );
  console.log(JSON.stringify(blocked, null, 2));
  process.exit(0);
}

const result = await executeBoundedLiveParallelDiscovery(HOTEL_ID, {
  maxUsd: Math.min(remaining, floor),
  maxTasks: 1,
  processor: "base",
  jobSegment: PARALLEL_IDENTITY_LOCK_JOB_SEGMENT,
  hotelConfig,
  authorizePaidDiscovery: true,
  researchGaps: [
    "Dated 2026–2027 association, corporate, university, or convention-adjacent lodging occasions with travel cohort evidence in San Diego / North Harbor Drive / downtown waterfront / Convention Center catchment",
    "Prefer occasions with housing, room-block, or overflow language relevant to a 600-room bayfront full-service hotel",
  ],
  privateEventGaps: [
    "Waterfront or downtown San Diego private-event / venue-referral lodging where Wyndham San Diego Bayside is a complementary overnight partner (not Bethesda fixtures)",
  ],
  timeoutMs: Number(process.env.GDI_PARALLEL_TIMEOUT_MS || 240000),
});

const accepted = Array.isArray(result.candidates) ? result.candidates : [];
const quarantined = Array.isArray(result.quarantinedCandidates)
  ? result.quarantinedCandidates
  : [];

// Double-check gate on accepted (defense in depth)
const recheck = quarantineWrongMarketCandidates(accepted, hotelConfig, {
  hotelId: HOTEL_ID,
});

const ledgerAfter = readParallelBudgetLedger({ env: process.env, budgetUsd: 2 });

const summary = {
  ok: true,
  committedBuild: "f911ff9ae4b5d7d0c83c0e94e0400f1a74271bfd",
  hotelId: HOTEL_ID,
  hotelName: hotelConfig.displayName,
  jobSegment: PARALLEL_IDENTITY_LOCK_JOB_SEGMENT,
  providerRunId: result.providerRunId || null,
  status: result.status,
  executionKind: result.executionKind,
  paidCalls: result.paidCalls,
  estimatedUsd: result.estimatedUsd ?? null,
  actualBilledUsd: result.actualBilledUsd ?? null,
  geographicGate: result.geographicGate || null,
  acceptedInTerritory: recheck.acceptedCount,
  quarantinedWrongMarket: quarantined.length + recheck.quarantinedCount,
  candidates: recheck.accepted,
  quarantined: [...quarantined, ...recheck.quarantined],
  budget: {
    beforeRemainingUsd: remaining,
    afterRemainingUsd: remainingParallelBudgetUsd(ledgerAfter),
    estimatedSpentUsd: ledgerAfter.estimatedSpentUsd,
    budgetUsd: ledgerAfter.budgetUsd,
    note: "Includes prior research + probe reconciliation; actual billed unknown",
  },
  note: result.note,
};

fs.writeFileSync(
  path.join(OUT, "identity-lock-live-result.json"),
  JSON.stringify({ ...summary, rawStatus: result.status, plan: result.plan }, null, 2) +
    "\n"
);
console.log(JSON.stringify(summary, null, 2));
