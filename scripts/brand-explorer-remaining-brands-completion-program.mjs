#!/usr/bin/env node
/**
 * Remaining Brand Explorer brands completion program — two lanes, dry-run by default.
 */
import "dotenv/config";
import {
  PROGRAM_VERSION,
  parseProgramArgs,
  runRemainingBrandsCompletionProgram,
  writeProgramReports,
} from "../lib/partner-intelligence/brand-explorer-remaining-brands-completion-program.js";

async function main() {
  const opts = parseProgramArgs(process.argv.slice(2));
  console.log(`[${PROGRAM_VERSION}] remaining brands completion program`);
  console.log(`Lane: ${opts.lane}`);
  console.log(`Brands: ${opts.brands.join(", ")}`);
  console.log(`Mode: ${opts.apply ? "APPLY" : "dry-run"}`);

  const result = await runRemainingBrandsCompletionProgram({
    brands: opts.brands,
    lane: opts.lane,
    apply: opts.apply,
    argv: opts.argv,
  });
  const paths = writeProgramReports(result);
  console.log(`Wrote ${paths.jsonPath}`);
  console.log(`Wrote ${paths.mdPath}`);

  if (result.lane1) {
    console.log(
      `Lane 1: fullyReady=${result.lane1.summary?.fullyReadyCount} heldAccidental=${result.lane1.summary?.heldAccidentalUnlockCount} restoreApplied=${result.lane1.visibilityFormalization?.publicRestoreApplied}`
    );
  }
  if (result.lane2) {
    console.log(
      `Lane 2: plannedWrites=${result.lane2.summary?.plannedWriteCount} applied=${result.lane2.summary?.applied} blocked=${(result.lane2.summary?.blockedSlugs || []).join(",") || "none"}`
    );
  }
  if (result.lane2Integrity) {
    console.log(
      `Lane 2 integrity: pass=${result.lane2Integrity.summary?.passCount}/${result.lane2Integrity.summary?.brandCount}`
    );
  }
  if (result.lane2AssetPack) {
    console.log(
      `Lane 2 asset pack: ready=${result.lane2AssetPack.summary?.readyCount}/${result.lane2AssetPack.summary?.brandCount} blocked=${(result.lane2AssetPack.summary?.blockedSlugs || []).join(",") || "none"}`
    );
  }
  if (result.lane2Materialization) {
    console.log(
      `Lane 2 materialization: patches=${result.lane2Materialization.summary?.patchCount} applied=${result.lane2Materialization.summary?.applied}`
    );
  }

  const lane1Fail = (result.lane1?.steps || []).some((s) => s.ok === false);
  const lane2Blocked = (result.lane2?.summary?.blockedSlugs || []).length > 0;
  const integrityFail =
    result.lane2Integrity != null &&
    (result.lane2Integrity.summary?.failCount ?? 0) > 0;
  const assetPackBlocked =
    result.lane2AssetPack != null &&
    (result.lane2AssetPack.summary?.readyCount ?? 0) <
      (result.lane2AssetPack.summary?.brandCount ?? 0);
  if (lane1Fail || (opts.apply && lane2Blocked) || integrityFail || assetPackBlocked) {
    process.exitCode = 1;
  }
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
