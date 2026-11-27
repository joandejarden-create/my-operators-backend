#!/usr/bin/env node
/**
 * CI / scheduler entry — Hotel Explorer Intelligence Factory V1.1
 *
 *   node scripts/hotel-explorer/run-hotel-explorer-factory.mjs --hotel-id recLuxvwwxID7U2B8 --mode Audit
 *   node scripts/hotel-explorer/run-hotel-explorer-factory.mjs --hotel-name "Bethesda Marriott" --mode Full
 *   node scripts/hotel-explorer/run-hotel-explorer-factory.mjs --batch-file configs/hotel-explorer/he50-eight-hotel-batch-v1.json --mode Audit
 */
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import {
  runHotelExplorerFactory,
  runHotelExplorerFactoryBatch,
  HE_FACTORY_MODES,
} from "../../lib/hotel-intelligence/he-factory/he-intelligence-factory-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
for (const p of [path.join(ROOT, ".env"), "C:/Dev/dealality/my-operators-backend/.env"]) {
  dotenv.config({ path: p, override: false });
}

function arg(name, alias) {
  const keys = [`--${name}`];
  if (alias) keys.push(`--${alias}`);
  for (const k of keys) {
    const i = process.argv.indexOf(k);
    if (i >= 0 && process.argv[i + 1] && !process.argv[i + 1].startsWith("--")) {
      return process.argv[i + 1];
    }
  }
  return null;
}

function flag(name) {
  return process.argv.includes(`--${name}`);
}

async function main() {
  const mode = arg("mode") || "Audit";
  if (!HE_FACTORY_MODES.includes(mode)) {
    console.error(JSON.stringify({ ok: false, error: "INVALID_MODE", mode, modes: HE_FACTORY_MODES }));
    process.exit(2);
  }

  const opts = {
    mode,
    hotelId: arg("hotel-id", "hotelId"),
    hotelName: arg("hotel-name", "hotelName"),
    apply: flag("apply"),
    maxSpendUsd: Number(arg("max-spend-usd") || arg("maxSpendUsd") || 0),
    outputDir:
      arg("output-dir", "outputDir") ||
      path.join(ROOT, "reports/hotel-explorer/he-factory-v1-1"),
    resume: flag("resume"),
    skipBrowser: !flag("browser"),
    batchFile: arg("batch-file", "batchFile"),
  };

  const result = opts.batchFile
    ? await runHotelExplorerFactoryBatch(opts)
    : await runHotelExplorerFactory(opts);

  console.log(JSON.stringify(result, null, 2));
  process.exit(result.exitCode ?? (result.ok ? 0 : 1));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
