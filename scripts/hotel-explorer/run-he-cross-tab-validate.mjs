#!/usr/bin/env node
/**
 * Hotel Explorer Factory — Validate mode helper for cross-tab / submarket / org gates.
 *
 * Usage:
 *   node scripts/hotel-explorer/run-he-cross-tab-validate.mjs --hotelId=recLuxvwwxID7U2B8
 *   node scripts/hotel-explorer/run-he-cross-tab-validate.mjs --fixture
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { validateBethesdaGoldenParityFixture } from "../../lib/hotel-intelligence/he-cross-tab-parity-v1.js";
import { runCrossTabFactoryGates } from "../../lib/hotel-intelligence/he-factory/he-cross-tab-factory-gates-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const args = process.argv.slice(2);
const fixtureOnly = args.includes("--fixture");
const hotelIdArg = args.find((a) => a.startsWith("--hotelId="));
const hotelId = hotelIdArg
  ? hotelIdArg.split("=")[1]
  : "recLuxvwwxID7U2B8";

async function main() {
  if (fixtureOnly) {
    const result = validateBethesdaGoldenParityFixture();
    const out = path.join(
      ROOT,
      "reports/hotel-explorer/HE-CROSS-TAB-PARITY-BETHESDA-FIXTURE-V1.json"
    );
    fs.mkdirSync(path.dirname(out), { recursive: true });
    fs.writeFileSync(out, JSON.stringify(result, null, 2));
    console.log(JSON.stringify({ mode: "fixture", ok: result.ok, gates: result.gates }, null, 2));
    process.exit(result.ok ? 0 : 1);
  }

  const result = await runCrossTabFactoryGates({ hotelId });
  const out = path.join(
    ROOT,
    `reports/hotel-explorer/HE-CROSS-TAB-FACTORY-GATES-${hotelId}.json`
  );
  fs.mkdirSync(path.dirname(out), { recursive: true });
  fs.writeFileSync(out, JSON.stringify(result, null, 2));
  console.log(
    JSON.stringify(
      {
        mode: "validate",
        hotelId,
        ok: result.ok,
        gates: result.gates,
        cohorts: result.cohorts,
        conflictCount: result.conflicts?.length || 0,
      },
      null,
      2
    )
  );
  process.exit(result.ok ? 0 : 1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
