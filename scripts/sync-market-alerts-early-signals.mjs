#!/usr/bin/env node
/**
 * Early Signal production sync (V1.2).
 *
 *   npm run market-alerts:early-signals:sync -- --dry-run
 *   npm run market-alerts:early-signals:sync -- --apply --limit 50
 *
 * Requires MARKET_ALERTS_EARLY_SIGNALS_ENABLED=true for --apply.
 */
import fs from "fs";
import path from "path";
import "../load-env.js";
import { runEarlySignalProductionSync } from "../lib/market-alerts-early-signal-production.js";
import {
  EARLY_SIGNAL_DISABLED_FAMILIES,
  EARLY_SIGNAL_PRODUCTION_FAMILIES,
  isEarlySignalProductionEnabled,
} from "../lib/market-alerts-early-signal-config.js";
import {
  maybeRunEarlySignalProductionSync,
} from "../lib/market-alerts-early-signal-schedule.js";

function argValue(flag) {
  const i = process.argv.indexOf(flag);
  if (i === -1) return null;
  return process.argv[i + 1] || null;
}

async function main() {
  const apply = process.argv.includes("--apply");
  const dryRun = !apply || process.argv.includes("--dry-run");
  const limit = Math.min(
    Math.max(parseInt(argValue("--limit") || "50", 10) || 50, 1),
    100
  );
  const force = process.argv.includes("--force");

  if (apply && !isEarlySignalProductionEnabled() && !force) {
    console.error(
      "Refusing --apply: set MARKET_ALERTS_EARLY_SIGNALS_ENABLED=true (or pass --force for dry-run preview only)."
    );
    process.exit(1);
  }

  console.log(
    JSON.stringify(
      {
        mode: dryRun ? "dry-run" : "apply",
        productionFamilies: EARLY_SIGNAL_PRODUCTION_FAMILIES,
        disabledFamilies: EARLY_SIGNAL_DISABLED_FAMILIES,
        limit,
        enabled: isEarlySignalProductionEnabled(),
      },
      null,
      2
    )
  );

  const result = apply
    ? await maybeRunEarlySignalProductionSync({ dryRun: false, limit, force })
    : await runEarlySignalProductionSync({ dryRun: true, limit });

  const outDir = path.join(process.cwd(), "data");
  fs.mkdirSync(outDir, { recursive: true });
  const stamp = dryRun ? "dry-run" : "production-run-1";
  const outPath = path.join(outDir, `market-alerts-early-signal-${stamp}.json`);
  fs.writeFileSync(outPath, JSON.stringify(result, null, 2));

  console.log(JSON.stringify(result, null, 2));
  console.log(`\nWrote ${outPath}`);

  if (!result.ok && !result.skipped) {
    process.exit(1);
  }
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
