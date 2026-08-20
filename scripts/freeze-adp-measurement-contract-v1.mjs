#!/usr/bin/env node
/**
 * Freeze ADP_MEASUREMENT_CONTRACT_V1 to a machine-readable contract file.
 *
 * Usage:
 *   node scripts/freeze-adp-measurement-contract-v1.mjs
 */

import { mkdirSync, writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import {
  buildFrozenMeasurementContractDocument,
  assertContractHashMatches,
  MEASUREMENT_CONTRACT_VERSION,
} from "../lib/ai-demand-positioning/contracts/adp-measurement-contract-v1.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const outPath = join(
  process.cwd(),
  "data/ai-demand-positioning/contracts/adp-measurement-contract-v1.json"
);

const doc = buildFrozenMeasurementContractDocument(new Date().toISOString());
const check = assertContractHashMatches(doc);
if (!check.ok) {
  console.error("Contract hash self-check failed", check);
  process.exit(1);
}

mkdirSync(dirname(outPath), { recursive: true });
writeFileSync(outPath, JSON.stringify(doc, null, 2) + "\n");

const epochPath = join(
  process.cwd(),
  "data/ai-demand-positioning/contracts/adp-official-baseline-epoch-v1.json"
);
writeFileSync(
  epochPath,
  JSON.stringify(
    {
      epoch: doc.officialBaselineEpoch,
      START_PERIOD: doc.startPeriodMarker,
      CUSTOMER_HISTORY_START: doc.customerHistoryStart,
      measurementContractVersion: MEASUREMENT_CONTRACT_VERSION,
      measurementContractHash: doc.measurementContractHash,
      freezeTimestamp: doc.freezeTimestamp,
      propertyUniverse: doc.propertyUniverse,
    },
    null,
    2
  ) + "\n"
);

console.log(
  JSON.stringify(
    {
      MEASUREMENT_CONTRACT: MEASUREMENT_CONTRACT_VERSION,
      FROZEN: "YES",
      CONTRACT_HASH: doc.measurementContractHash,
      CONTRACT_FILE: outPath,
      EPOCH_FILE: epochPath,
      FREEZE_TIMESTAMP: doc.freezeTimestamp,
    },
    null,
    2
  )
);
