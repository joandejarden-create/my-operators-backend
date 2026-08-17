#!/usr/bin/env node
/**
 * Wyndham factory promotion — Dazzler / Trademark.
 *
 *   npm run brand-explorer-wyndham-factory-promotion -- --brand dazzler --stage status-promotion
 *   npm run brand-explorer-wyndham-factory-promotion -- --brand dazzler --stage status-promotion --apply …
 *   npm run brand-explorer-wyndham-factory-promotion -- --brand dazzler --stage public-release --apply …
 */
import "../load-env.js";
import {
  PROMOTION_VERSION,
  STAGES,
  STATUS_PROMOTION_FLAGS,
  PUBLIC_RELEASE_FLAGS,
  runWyndhamStatusPromotion,
  runWyndhamPublicRelease,
} from "../lib/partner-intelligence/brand-explorer-wyndham-factory-promotion.js";

function argValue(argv, name) {
  const i = argv.indexOf(name);
  return i >= 0 ? argv[i + 1] : null;
}

async function main() {
  const argv = process.argv.slice(2);
  const brand = argValue(argv, "--brand");
  const stage = argValue(argv, "--stage");
  const apply = argv.includes("--apply");

  if (!brand || !stage || !STAGES.includes(stage)) {
    console.error(
      `Usage: --brand <dazzler|trademark> --stage <${STAGES.join("|")}> [--apply + flags]`
    );
    process.exit(1);
  }

  console.log(`[${PROMOTION_VERSION}] brand=${brand} stage=${stage} mode=${apply ? "APPLY" : "dry-run"}`);

  let result;
  if (stage === "status-promotion") {
    if (apply) {
      const missing = STATUS_PROMOTION_FLAGS.filter((f) => !argv.includes(f));
      if (missing.length) {
        console.error("Missing flags:\n  " + missing.join("\n  "));
        process.exit(1);
      }
    }
    result = await runWyndhamStatusPromotion({ brandSlug: brand, apply, argv });
  } else {
    if (apply) {
      const missing = PUBLIC_RELEASE_FLAGS.filter((f) => !argv.includes(f));
      if (missing.length) {
        console.error("Missing flags:\n  " + missing.join("\n  "));
        process.exit(1);
      }
    }
    result = await runWyndhamPublicRelease({ brandSlug: brand, apply, argv });
  }

  console.log(`Wrote ${result.paths.jsonPath}`);
  console.log(`Wrote ${result.paths.mdPath}`);
  console.log(
    `applyPerformed=${result.report.applyPerformed} writePerformed=${result.report.writePerformed} refused=${result.report.refused === true}`
  );
  if (apply && result.report.applyPerformed !== true && result.report.refused !== true) {
    const reason = result.report.applyResult?.reason || result.report.applyOutcome?.reason || result.report.reason;
    if (reason && reason !== "already_active") {
      console.error(`Apply did not complete: ${reason}`);
      process.exitCode = 1;
    }
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
