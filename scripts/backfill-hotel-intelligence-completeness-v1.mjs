/**
 * Backfill HI completeness for active ADP/GDI hotels with NOT_RESEARCHED domains.
 *
 *   node scripts/backfill-hotel-intelligence-completeness-v1.mjs --dry-run
 *   node scripts/backfill-hotel-intelligence-completeness-v1.mjs --apply
 *   node scripts/backfill-hotel-intelligence-completeness-v1.mjs --apply --only=recESHsNsWUFYZrxR,recUOyzOXn2Zdp98I
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { listAdpGdiHotelUniverse } from "../lib/hotel-census/adp-gdi-canonical-identity.js";
import { onboardHotelIntelligence } from "../lib/hotel-intelligence/onboarding/onboard-hotel-intelligence.js";
import { isHotelIntelligenceComplete } from "../lib/hotel-intelligence/onboarding/hi-completeness-gate.js";
import { HI_DOMAIN_STATUS, OVERALL_HI_STATUS } from "../lib/hotel-intelligence/onboarding/domain-status-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT = path.join(
  __dirname,
  "../reports/hotel-intelligence/completeness-onboarding-v1"
);

const args = process.argv.slice(2);
const APPLY = args.includes("--apply");
const DRY = !APPLY;
const onlyArg = args.find((a) => a.startsWith("--only="));
const ONLY = onlyArg
  ? onlyArg
      .slice("--only=".length)
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean)
  : null;

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  let universe = listAdpGdiHotelUniverse().filter((h) => h.adp || h.gdi);
  if (ONLY?.length) {
    universe = universe.filter((h) =>
      ONLY.includes(h.canonicalHotelId) || ONLY.includes(h.key) || ONLY.includes(h.adpPropertyId)
    );
  }

  console.log(`[backfill] mode=${APPLY ? "apply" : "dry-run"} hotels=${universe.length}`);

  const results = [];
  const cost = { queries: 0, fetches: 0, hotels: 0, hotelsResearched: 0, domainsTouched: 0 };

  for (const h of universe) {
    const hpcId = h.canonicalHotelId || h.key;
    const before = await isHotelIntelligenceComplete(hpcId);
    if (before.complete && !args.includes("--force")) {
      console.log(`[skip-complete] ${h.displayName}`);
      results.push({
        hpcHotelId: hpcId,
        hotelName: h.displayName,
        skipped: true,
        reason: "already_complete",
        before: before.overallStatus,
        after: before.overallStatus,
      });
      continue;
    }

    console.log(`[onboard] ${h.displayName} (${before.overallStatus})`);
    cost.hotelsResearched += 1;
    const out = await onboardHotelIntelligence(hpcId, {
      mode: APPLY ? "apply" : "dry-run",
      forceRefresh: args.includes("--force"),
    });
    cost.fetches += out.research?.cost?.httpFetches || 0;
    cost.queries += out.research?.cost?.searchQueries || 0;
    cost.hotels += 1;
    cost.domainsTouched += (before.blockingDomains || []).length;

    results.push({
      hpcHotelId: hpcId,
      hotelName: out.hotelName || h.displayName,
      skipped: false,
      before: out.before,
      after: {
        overallStatus: out.after?.overallStatus,
        coverage: out.after?.coverage,
        complete: out.after?.complete,
        domains: Object.fromEntries(
          Object.entries(out.after?.domains || {}).map(([k, v]) => [k, v.domainStatus])
        ),
      },
      research: out.research,
      gdiEligible: out.gdiEligible,
      adpGdiReady: out.adpGdiReady,
    });
  }

  const afterComplete = results.filter(
    (r) => r.after?.complete || r.after?.overallStatus === OVERALL_HI_STATUS.HI_COMPLETE
  ).length;
  const stillIncomplete = results.filter(
    (r) =>
      !r.skipped &&
      r.after?.overallStatus === OVERALL_HI_STATUS.HI_INCOMPLETE
  );

  const summary = {
    mode: APPLY ? "apply" : "dry-run",
    generatedAt: new Date().toISOString(),
    hotelsConsidered: universe.length,
    hotelsResearched: cost.hotelsResearched,
    afterComplete,
    stillIncomplete: stillIncomplete.length,
    cost,
    results,
  };

  const name = APPLY ? "BACKFILL_APPLY.json" : "BACKFILL_DRY_RUN.json";
  fs.writeFileSync(path.join(OUT, name), JSON.stringify(summary, null, 2) + "\n");
  console.log(
    JSON.stringify(
      {
        mode: summary.mode,
        hotelsResearched: summary.hotelsResearched,
        afterComplete: summary.afterComplete,
        stillIncomplete: summary.stillIncomplete,
        fetches: cost.fetches,
      },
      null,
      2
    )
  );
  console.log(`[out] ${path.join(OUT, name)}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
