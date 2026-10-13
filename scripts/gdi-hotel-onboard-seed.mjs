/**
 * Generic GDI hotel onboard seed — Fit + Research Targets.
 *
 * Usage:
 *   node scripts/gdi-hotel-onboard-seed.mjs --hotel recG66DQJKP2c0UNh
 *   node scripts/gdi-hotel-onboard-seed.mjs --hotel recG66DQJKP2c0UNh --apply
 *   node scripts/gdi-hotel-onboard-seed.mjs --hotel recG66DQJKP2c0UNh --recreate-proof
 *   node scripts/gdi-hotel-onboard-seed.mjs --hotel recG66DQJKP2c0UNh --apply --jev-shadow
 *
 * Default: dry-run. No Surfe. No weekly discovery. Jev shadow optional.
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  proposeHotelOnboardSeed,
  applyHotelOnboardSeed,
  compareOnboardSeedProposals,
  assessWeeklyReadinessFromSeed,
} from "../lib/group-demand-intelligence/research-coverage/onboard-hotel-research-graph.js";
import { runSeedJevShadowComparison, describeAdpJevShadowIntegrationPoints } from "../lib/group-demand-intelligence/research-coverage/seed-jev-shadow.js";
import {
  reportCanonicalBaseEnv,
  resolveGdiCanonicalBaseId,
  CANONICAL_INTELLIGENCE_BASE_ID,
} from "../lib/group-demand-intelligence/canonical-airtable-base.js";
import { getAdpPersistencePolicy } from "../lib/ai-demand-positioning/adp-persistence-policy.js";
import { isPilotReferenceLogicEnabled } from "../lib/group-demand-intelligence/pilot-path-policy.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const APPLY = process.argv.includes("--apply");
const RECREATE = process.argv.includes("--recreate-proof");
const JEV_SHADOW = process.argv.includes("--jev-shadow");

function argVal(flag) {
  const i = process.argv.indexOf(flag);
  return i >= 0 ? process.argv[i + 1] : null;
}

async function main() {
  const hotelId = argVal("--hotel") || "recG66DQJKP2c0UNh";
  const outDir = path.join(
    ROOT,
    "reports",
    "group-demand-intelligence",
    "replication-gate-repairs-v1"
  );
  fs.mkdirSync(outDir, { recursive: true });

  const envReport = reportCanonicalBaseEnv();
  let gdiBase = null;
  try {
    gdiBase = resolveGdiCanonicalBaseId();
  } catch (e) {
    gdiBase = { error: e.code || e.message, status: "MISSING" };
  }

  const fixedNow = "2026-09-24T12:00:00.000Z";
  const first = proposeHotelOnboardSeed(hotelId, { now: fixedNow });
  const second = proposeHotelOnboardSeed(hotelId, { now: fixedNow });
  const recreate = compareOnboardSeedProposals(first, second);
  const weekly = assessWeeklyReadinessFromSeed(first);
  const adpPolicy = getAdpPersistencePolicy();

  let applyResult = null;
  if (APPLY) {
    if (gdiBase?.baseId && gdiBase.baseId !== CANONICAL_INTELLIGENCE_BASE_ID) {
      throw new Error(`REFUSED: GDI base ${gdiBase.baseId} is not canonical`);
    }
    applyResult = await applyHotelOnboardSeed(hotelId, {
      dryRun: false,
      proposal: first,
    });
  } else {
    applyResult = await applyHotelOnboardSeed(hotelId, {
      dryRun: true,
      proposal: first,
    });
  }

  let jevShadow = null;
  if (JEV_SHADOW) {
    jevShadow = await runSeedJevShadowComparison(first, { limit: 12 });
  }

  // Recreate proof is dry-run equivalence (safest — no delete of production)
  let recreateLive = null;
  if (RECREATE) {
    const again = proposeHotelOnboardSeed(hotelId, { now: fixedNow });
    recreateLive = compareOnboardSeedProposals(first, again);
  }

  const report = {
    generatedAt: new Date().toISOString(),
    hotelId,
    dryRun: !APPLY,
    canonicalBase: {
      expected: CANONICAL_INTELLIGENCE_BASE_ID,
      envReport,
      gdiResolve: gdiBase,
    },
    adpPersistencePolicy: adpPolicy,
    pilotLogicEnabled: isPilotReferenceLogicEnabled(hotelId),
    profileCompleteness: first.input,
    dryRun: {
      fits: first.totals.fits,
      targets: first.totals.targets,
      generators: first.totals.generators,
      programs: first.totals.programs,
      rejected: first.totals.rejected,
      byType: first.totals.byType,
      byPriority: first.totals.byPriority,
    },
    applied: applyResult
      ? {
          dryRun: applyResult.dryRun,
          writeStats: applyResult.writeStats,
          performance: applyResult.performance,
          fits: first.totals.fits,
          targets: first.totals.targets,
        }
      : null,
    recreateProof: recreateLive || recreate,
    weeklyReadiness: weekly,
    jevShadow,
    adpJevPrep: describeAdpJevShadowIntegrationPoints(),
    quality: first.quality,
    security: {
      hotelScoped: first.fits.every((f) => f.hotelId === hotelId),
      pilotBleedRejected: first.quality.pilotBleedRejected,
      bethesdaIdsInSeed: first.fits.some((f) =>
        String(f.fitRationale || "").toLowerCase().includes("bethesda")
      )
        ? 1
        : 0,
    },
  };

  const outPath = path.join(outDir, `onboard-seed-${hotelId}${APPLY ? "-apply" : "-dry"}.json`);
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
  console.log(JSON.stringify({ ok: true, outPath, summary: {
    fits: first.totals.fits,
    targets: first.totals.targets,
    weekly: weekly.status,
    recreateDeterministic: (recreateLive || recreate).deterministic,
    applied: APPLY,
    pilotLogic: report.pilotLogicEnabled,
  } }, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
