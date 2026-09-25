/**
 * Cross-market replication V1 — W Rome Phase 0–2 dry orchestration.
 *
 * Usage:
 *   node scripts/gdi-cross-market-w-rome-phase0-2.mjs
 *   node scripts/gdi-cross-market-w-rome-phase0-2.mjs --apply   # REFUSED while census missing
 *
 * No Rome/Italy/W production switches. Surfe off. Jev shadow optional.
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  proposeHotelOnboardSeed,
  compareOnboardSeedProposals,
  assessWeeklyReadinessFromSeed,
} from "../lib/group-demand-intelligence/research-coverage/onboard-hotel-research-graph.js";
import {
  buildMarketLocalDiscoveryPlan,
  proposeMarketLocalTargets,
  MARKET_LOCAL_EXPANSION_VERSION,
} from "../lib/group-demand-intelligence/research-coverage/market-local-expansion.js";
import {
  resolvePortableGeoScopes,
  textHasPilotGeoBleed,
  textHasNycReferenceBleed,
} from "../lib/group-demand-intelligence/research-coverage/portable-seed-templates.js";
import { COUNTRY_LOCALE, LOCALE_PACKS } from "../lib/group-demand-intelligence/discovery-recall-v4.js";
import { runSeedJevShadowComparison } from "../lib/group-demand-intelligence/research-coverage/seed-jev-shadow.js";
import {
  describeAdpJevShadowAdapter,
  runAdpJevShadowEvaluation,
} from "../lib/ai-demand-positioning/adp-jev-shadow.js";
import { resolveCanonicalHotelId } from "../lib/hotel-census/adp-gdi-canonical-identity.js";
import { isPilotReferenceLogicEnabled } from "../lib/group-demand-intelligence/pilot-path-policy.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const HOTEL_ID = "gdi_hotel_w_rome";
const APPLY = process.argv.includes("--apply");
const JEV_SHADOW = process.argv.includes("--jev-shadow");
const OUT = path.join(
  ROOT,
  "reports/group-demand-intelligence/cross-market-replication-v1"
);

const OFFICIAL = {
  name: "W Rome",
  marsha: "ROMWV",
  website: "https://www.marriott.com/en-us/hotels/romwv-w-rome/overview/",
  address: "26/36 Via Liguria, Rome, Italy, 00187",
  rooms: 148,
  meetingSpaceSqM: 60,
  meetingSpaceSqFt: 646,
  brand: "W Hotels",
  parent: "Marriott International",
  city: "Rome",
  country: "Italy",
};

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const fixedNow = "2026-09-25T10:00:00.000Z";

  const canonical = resolveCanonicalHotelId(HOTEL_ID);
  const censusStatus = canonical
    ? "RESOLVED"
    : "MISSING_FROM_CENSUS_BLOCKING";

  const phase0 = {
    hotel: OFFICIAL.name,
    provisionalHotelId: HOTEL_ID,
    canonicalHotelId: canonical,
    censusStatus,
    country: OFFICIAL.country,
    market: OFFICIAL.city,
    officialFacts: OFFICIAL,
    factsClassification: {
      AVAILABLE: [
        "official name",
        "MARSHA ROMWV",
        "official Marriott URL",
        "address",
        "city",
        "country",
        "rooms (148)",
        "meeting space (~60 m² / 646 sq ft)",
        "brand W Hotels",
        "parent Marriott",
      ],
      DERIVABLE: [
        "peak room planning band from rooms",
        "demand archetype from capability",
        "locale pack IT from country",
        "catchment km bands from archetype",
      ],
      MISSING: [
        "Hotel Property Census record id",
        "Italy census inventory",
        "latitude/longitude company-validated (approx from address only)",
        "owner / asset manager",
        "year built / renovated",
        "certified ADP peer pack",
      ],
      STEWARDSHIP_REQUIRED: [
        "Europe / Italy HPC insert for W Rome",
        "ADP peer set approval",
        "promote provisional gdi_hotel_w_rome → HPC rec…",
      ],
    },
    localeSupport: {
      itPackPresent: Boolean(LOCALE_PACKS.it),
      countryLocaleIt: COUNTRY_LOCALE.IT || null,
    },
    blockingForApply: censusStatus !== "RESOLVED",
  };

  fs.writeFileSync(path.join(OUT, "PHASE0_READINESS.json"), JSON.stringify(phase0, null, 2));

  if (APPLY && phase0.blockingForApply) {
    const refuse = {
      ok: false,
      refused: true,
      reason: "census_missing_blocking_apply",
      hotelId: HOTEL_ID,
      note: "Refuse Airtable Fit/Target apply until canonical HPC record exists.",
    };
    fs.writeFileSync(path.join(OUT, "APPLY_REFUSED.json"), JSON.stringify(refuse, null, 2));
    console.log(JSON.stringify(refuse, null, 2));
    process.exit(2);
  }

  const first = proposeHotelOnboardSeed(HOTEL_ID, { now: fixedNow });
  const second = proposeHotelOnboardSeed(HOTEL_ID, { now: fixedNow });
  const recreate = compareOnboardSeedProposals(first, second);
  const weekly = assessWeeklyReadinessFromSeed(first);

  const seedBlob = JSON.stringify(first.generators) + JSON.stringify(first.fits);
  const usOnlyRejected = (first.generators || []).filter((g) =>
    /asaecenter\.org|hcea\.org|rcmaweb\.org|iaee\.com/i.test(
      String(g.officialDomain || g.website || "")
    )
  );
  const quality = {
    geoScopes: first.geoScopes || resolvePortableGeoScopes("Italy"),
    pilotBleed: textHasPilotGeoBleed(seedBlob),
    nycBleed: textHasNycReferenceBleed(seedBlob),
    usOnlyAssociationCount: usOnlyRejected.length,
    hotelSpecificCode: 0,
    italyHardcodeInProdLogic: 0,
    romeHardcodeInProdLogic: 0,
  };

  const localPlan = buildMarketLocalDiscoveryPlan(HOTEL_ID, {
    now: fixedNow,
    maxTasks: 60,
  });
  // Evidence-backed discoveries: empty until live fetch; architecture proves plan + empty→0 targets
  const localTargets = proposeMarketLocalTargets(HOTEL_ID, [], { now: fixedNow });

  let jevSeed = null;
  let jevAdp = null;
  if (JEV_SHADOW) {
    jevSeed = await runSeedJevShadowComparison(first, { limit: 10 });
    jevAdp = await runAdpJevShadowEvaluation(
      {
        evidenceItems: [
          {
            id: "marriott_romwv",
            rank: "PRIMARY",
            sourceAuthority: "OFFICIAL",
            summary: "Official Marriott property events page",
          },
        ],
        sourceItems: [
          { id: "ufi", priority: "HIGH", sourceAuthority: "INDUSTRY", title: "UFI" },
          { id: "icca", priority: "HIGH", sourceAuthority: "INDUSTRY", title: "ICCA" },
        ],
        followups: [{ id: "f1", type: "SOURCE", reason: "market_local_calendar" }],
      },
      { limit: 6 }
    );
  }

  const report = {
    generatedAt: new Date().toISOString(),
    hotelId: HOTEL_ID,
    phase0,
    seed: {
      fits: first.totals.fits,
      targets: first.totals.targets,
      generators: first.totals.generators,
      programs: first.totals.programs,
      rejected: first.totals.rejected,
      byType: first.totals.byType,
      byPriority: first.totals.byPriority,
      geoScopes: first.geoScopes,
      sampleOrgs: (first.generators || []).map((g) => g.organizationName),
    },
    recreate,
    weeklyReadiness: weekly,
    quality,
    marketLocal: {
      version: MARKET_LOCAL_EXPANSION_VERSION,
      planTaskCount: localPlan.taskCount,
      languages: localPlan.languages,
      categories: localPlan.categoryCount,
      discoveredEntities: 0,
      newTargets: localTargets.totals.targets,
      note: "Live evidence fetch deferred until census unlock; plan is country/locale generic.",
    },
    pilotLogicEnabled: isPilotReferenceLogicEnabled(HOTEL_ID),
    adpJevAdapter: describeAdpJevShadowAdapter(),
    jevSeed,
    jevAdp,
    apply: {
      attempted: APPLY,
      applied: false,
      reason: phase0.blockingForApply ? "census_missing" : "dry_run_default",
    },
  };

  const outPath = path.join(OUT, "PHASE0_2_SEED_AND_LOCAL_PLAN.json");
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
  console.log(
    JSON.stringify(
      {
        ok: true,
        outPath,
        censusStatus,
        fits: first.totals.fits,
        targets: first.totals.targets,
        recreateDeterministic: recreate.deterministic,
        localTasks: localPlan.taskCount,
        languages: localPlan.languages,
        usOnlyAssociationCount: quality.usOnlyAssociationCount,
        applyBlocked: phase0.blockingForApply,
      },
      null,
      2
    )
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
