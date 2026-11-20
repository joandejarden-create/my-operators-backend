#!/usr/bin/env node
/**
 * Surfe Usage V1.3 — seed layout-validation events using REAL GDI hotel + opportunity IDs.
 * Classified as DEVELOPMENT (not FIXTURE_TEST, not PRODUCTION). Never invents names.
 * No paid Surfe API calls. Idempotent on clientRequestId.
 *
 * Usage:
 *   node scripts/gdi-surfe-usage-seed-layout-demos-v1-3.mjs --dry-run
 *   node scripts/gdi-surfe-usage-seed-layout-demos-v1-3.mjs --apply
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const apply = process.argv.includes("--apply");
const dryRun = !apply;

const {
  appendSurfeLiveAccounting,
  summarizeSurfeLiveAccounting,
} = await import("../lib/group-demand-intelligence/gdi-surfe-live-gate-v1.js");
const {
  enrichSurfeUsageContext,
  buildHotelContextMap,
  USAGE_CLASS,
} = await import("../lib/group-demand-intelligence/gdi-surfe-usage-context-v1.js");

function loadOpportunity(hotelId, opportunityId) {
  const file = path.join(
    ROOT,
    "data/group-demand-intelligence/hotels",
    hotelId,
    "opportunities.json",
  );
  if (!fs.existsSync(file)) {
    throw new Error(`opportunities missing for ${hotelId}`);
  }
  const raw = JSON.parse(fs.readFileSync(file, "utf8"));
  const arr = Array.isArray(raw)
    ? raw
    : raw.opportunities || raw.items || [];
  const hit = arr.find(
    (o) => String(o.id || o.opportunityId) === String(opportunityId),
  );
  if (!hit) throw new Error(`opportunity ${opportunityId} not found on ${hotelId}`);
  return hit;
}

const CASES = [
  {
    hotelId: "recLuxvwwxID7U2B8",
    opportunityId: "gdi_opp_nado_ddaa_washcon_2028",
    clientRequestId: "cr_layout_demo_v13_beth_nado",
    lookupType: "email",
    emailReturned: true,
    phoneReturned: false,
    requestingOrganizationName: "Dealality Layout Validation Org",
    requestingHotelOrOfficeId: "office_dc_regional",
    requestingOfficeCity: "Washington",
    requestingOfficeMarket: "DMV",
    requestingPortfolioId: "portfolio_layout_demo_v13",
  },
  {
    hotelId: "recCAGqkERSXgiku4",
    opportunityId: "disc_1490e4966ee8",
    clientRequestId: "cr_layout_demo_v13_sheraton_gdl",
    lookupType: "both",
    emailReturned: true,
    phoneReturned: true,
    requestingOrganizationName: "Jalisco Regional Sales Desk",
    requestingHotelOrOfficeId: "office_gdl_sales",
    requestingOfficeCity: "Guadalajara",
    requestingOfficeMarket: "Guadalajara",
    requestingPortfolioId: "portfolio_layout_demo_v13",
  },
  {
    hotelId: "recH9h3McgMNFQEla",
    opportunityId: "rb_gdi_disc_dc428c1bd4c4e412",
    clientRequestId: "cr_layout_demo_v13_radisson_tor",
    lookupType: "phone",
    emailReturned: false,
    phoneReturned: true,
    requestingOrganizationName: "Toronto Portfolio Ops",
    requestingHotelOrOfficeId: "office_toronto_ops",
    requestingOfficeCity: "Toronto",
    requestingOfficeMarket: "Greater Toronto",
    requestingPortfolioId: "portfolio_layout_demo_v13",
  },
];

const hotelMap = buildHotelContextMap();
const env = {
  ...process.env,
  GDI_SURFE_USAGE_ENVIRONMENT: "development",
};

const results = [];
for (const c of CASES) {
  const opportunity = loadOpportunity(c.hotelId, c.opportunityId);
  const hotel = hotelMap.get(c.hotelId);
  if (!hotel || !hotel.hotelName) {
    throw new Error(`canonical hotel unresolved: ${c.hotelId}`);
  }
  const ctx = enrichSurfeUsageContext({
    hotelId: c.hotelId,
    opportunityId: c.opportunityId,
    opportunity: { ...opportunity, hotelId: c.hotelId },
    body: {
      clientRequestId: c.clientRequestId,
      requestingOrganizationName: c.requestingOrganizationName,
      requestingHotelOrOfficeId: c.requestingHotelOrOfficeId,
      requestingOfficeCity: c.requestingOfficeCity,
      requestingOfficeMarket: c.requestingOfficeMarket,
      requestingPortfolioId: c.requestingPortfolioId,
      portfolioId: c.requestingPortfolioId,
    },
    hotelMap,
    env,
  });
  if (ctx.hotelName === "UNKNOWN" || ctx.opportunityTitle === "UNKNOWN") {
    throw new Error(
      `layout demo must resolve identities: hotel=${ctx.hotelName} opp=${ctx.opportunityTitle}`,
    );
  }
  // Force DEVELOPMENT classification for visual validation (not customer adoption).
  ctx.usageClass = USAGE_CLASS.DEVELOPMENT;
  ctx.dataEnvironment = "development";

  const payload = {
    ...ctx,
    _enrichedContext: ctx,
    hotelId: c.hotelId,
    opportunityId: c.opportunityId,
    opportunity,
    clientRequestId: c.clientRequestId,
    channel: c.lookupType,
    lookupType: c.lookupType,
    emailReturned: c.emailReturned,
    phoneReturned: c.phoneReturned,
    surfeAttempted: true,
    providerLatencyMs: 12,
    layoutValidationOnly: true,
    usageClass: USAGE_CLASS.DEVELOPMENT,
    dataEnvironment: "development",
    creditsCharged: c.emailReturned && c.phoneReturned ? 2 : 1,
    creditCertainty: "estimated",
    costEstimateUsd: c.emailReturned && c.phoneReturned ? 0.3 : 0.15,
    costCertainty: "estimated",
  };

  if (dryRun) {
    results.push({
      mode: "dry-run",
      hotelName: ctx.hotelName,
      hotelLocationLine: ctx.hotelLocationLine,
      opportunityTitle: ctx.opportunityTitle,
      drawerHref: ctx.opportunityDrawerHref,
      requestingOrganizationName: ctx.requestingOrganizationName,
      usageClass: ctx.usageClass,
      clientRequestId: c.clientRequestId,
    });
    continue;
  }

  const appended = appendSurfeLiveAccounting(payload, env);
  results.push({
    mode: "apply",
    deduplicated: appended.deduplicated === true,
    lookupEventId: appended.entry?.lookupEventId,
    hotelName: appended.entry?.hotelName,
    opportunityTitle: appended.entry?.opportunityTitle,
    usageClass: appended.entry?.usageClass,
    clientRequestId: c.clientRequestId,
  });
}

const summary = dryRun
  ? null
  : summarizeSurfeLiveAccounting({ hideFixtures: true }, env);

const outDir = path.join(ROOT, "reports/gdi-surfe-usage-v1-3");
fs.mkdirSync(outDir, { recursive: true });
const outFile = path.join(outDir, "layout-demo-seed.json");
fs.writeFileSync(
  outFile,
  JSON.stringify(
    {
      generatedAt: new Date().toISOString(),
      dryRun,
      note: "DEVELOPMENT layoutValidationOnly events using real GDI hotel/opportunity records. Not customer adoption. Existing FIXTURE_TEST rows remain classified separately.",
      results,
      visibleNonFixtureLookups: summary?.totals?.totalLookups ?? null,
    },
    null,
    2,
  ),
);

console.log(JSON.stringify({ dryRun, results, outFile }, null, 2));
if (dryRun) {
  console.log("Re-run with --apply to write layout demos into the Surfe ledger.");
}
