#!/usr/bin/env node
/**
 * Commercial Contact Intelligence — 100-opportunity before/after pilot (EXA_ONLY).
 * Max incremental Exa spend: $10. No Surfe.
 *
 *   node scripts/gdi-commercial-contact-intelligence-100-pilot-v1.mjs --run
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import { looksLikePersonName } from "../lib/group-demand-intelligence/secondary-named-contact-v1.js";
import {
  runCommercialContactIntelligence,
} from "../lib/group-demand-intelligence/presentation/gdi-commercial-contact-intelligence-v1.js";
import {
  COMMERCIAL_CONTACT_CLASS,
} from "../lib/group-demand-intelligence/presentation/gdi-commercial-contact-classification-v1.js";
import { inspectPublicContactSearchCredentials } from "../lib/group-demand-intelligence/presentation/gdi-public-contact-search-providers-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
dotenv.config({ path: path.join(ROOT, ".env"), override: false });
dotenv.config({
  path: "C:/Dev/dealality/my-operators-backend/.env",
  override: false,
});

const RUN = process.argv.includes("--run");
const SPEND_CEILING = Number(process.env.GDI_CCI_PILOT_BUDGET || 10);
const COHORT_SIZE = 100;
const OUT = path.join(ROOT, "reports/gdi-commercial-contact-intelligence-v1/pilot-100");
const HOTELS = path.join(ROOT, "data/group-demand-intelligence/hotels");
const PRIOR_COHORT = path.join(
  ROOT,
  "reports/gdi-public-contact-discovery-2-v1/pilot-100/COHORT.json",
);

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function buildCohort() {
  // Prefer prior PCD 2.0 cohort for continuity, else rebuild
  if (fs.existsSync(PRIOR_COHORT)) {
    const prior = JSON.parse(fs.readFileSync(PRIOR_COHORT, "utf8"));
    const rows = [];
    for (const c of prior.cohort || []) {
      const oppPath = path.join(HOTELS, c.hotelId, "opportunities.json");
      if (!fs.existsSync(oppPath)) continue;
      const doc = JSON.parse(fs.readFileSync(oppPath, "utf8"));
      const opp = (doc.opportunities || []).find((o) => o.id === c.opportunityId);
      if (!opp) continue;
      rows.push({
        hotelId: c.hotelId,
        opportunityId: c.opportunityId,
        opportunity: opp,
        baselineName: text(opp.primaryContact?.name) || c.personName,
      });
      if (rows.length >= COHORT_SIZE) break;
    }
    if (rows.length >= 50) return rows;
  }

  const hotels = fs
    .readdirSync(HOTELS)
    .filter((d) => fs.existsSync(path.join(HOTELS, d, "opportunities.json")));
  const byHotel = new Map();
  for (const hotelId of hotels) {
    const doc = JSON.parse(
      fs.readFileSync(path.join(HOTELS, hotelId, "opportunities.json"), "utf8"),
    );
    for (const o of doc.opportunities || []) {
      const name = text(o.primaryContact?.name);
      if (!looksLikePersonName(name)) continue;
      if (!byHotel.has(hotelId)) byHotel.set(hotelId, []);
      byHotel.get(hotelId).push(o);
    }
  }
  const cohort = [];
  const ids = [...byHotel.keys()];
  let i = 0;
  while (cohort.length < COHORT_SIZE && ids.some((h) => byHotel.get(h).length)) {
    const h = ids[i % ids.length];
    i += 1;
    const list = byHotel.get(h);
    if (!list.length) continue;
    const opp = list.shift();
    cohort.push({
      hotelId: h,
      opportunityId: opp.id,
      opportunity: opp,
      baselineName: text(opp.primaryContact?.name),
    });
  }
  return cohort;
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const creds = inspectPublicContactSearchCredentials();
  const cohort = buildCohort();
  fs.writeFileSync(
    path.join(OUT, "COHORT.json"),
    `${JSON.stringify(
      {
        size: cohort.length,
        providerMode: creds.chosenMode,
        rows: cohort.map((c) => ({
          hotelId: c.hotelId,
          opportunityId: c.opportunityId,
          baselineName: c.baselineName,
        })),
      },
      null,
      2,
    )}\n`,
  );

  if (!RUN) {
    console.log(JSON.stringify({ cohortOnly: true, size: cohort.length }, null, 2));
    return;
  }

  let spend = 0;
  const results = [];
  const metrics = {
    totalOpportunities: 0,
    previouslyVerifiedChannelsReused: 0,
    newPublicEmails: 0,
    newPublicPhones: 0,
    contactsWithCompleteChannels: 0,
    surfeCallsAvoided: 0,
    newlyDiscoveredVerifiedDirectBuyers: 0,
    betterRelevantEntryPoints: 0,
    improvedReferralRoutes: 0,
    incorrectBuyerClassificationsRejected: 0,
    previouslyIdentifiedContactsRetained: 0,
    noBetterPersonFound: 0,
    meaningfulCommercialUpgrades: 0,
    channelOnlyImprovements: 0,
  };

  for (const row of cohort) {
    if (spend >= SPEND_CEILING) break;
    metrics.totalOpportunities += 1;
    const beforeEmail = text(row.opportunity.primaryContact?.email);
    const beforePhone = text(row.opportunity.primaryContact?.phone);

    let result;
    try {
      result = await runCommercialContactIntelligence({
        opportunity: row.opportunity,
        authorizePaidSearch: spend < SPEND_CEILING && creds.exaConfigured,
        seedRegressionFixtures: /maritza|alejandra/i.test(row.baselineName || ""),
        maxEcosystemQueries: 2,
        maxPersonQueries: 2,
        maxPages: 3,
      });
    } catch (err) {
      results.push({
        hotelId: row.hotelId,
        opportunityId: row.opportunityId,
        error: err.message || String(err),
      });
      continue;
    }

    spend += Number(result.metrics?.searchCostUsd || 0);
    const primary = result.selection?.primary;
    const cmp = result.comparison || {};
    const afterEmail = text(primary?.email);
    const afterPhone = text(primary?.phone);

    if (beforeEmail && beforePhone) metrics.previouslyVerifiedChannelsReused += 1;
    if (afterEmail && !beforeEmail) metrics.newPublicEmails += 1;
    if (afterPhone && !beforePhone) metrics.newPublicPhones += 1;
    if (afterEmail && afterPhone) metrics.contactsWithCompleteChannels += 1;
    if (afterEmail) metrics.surfeCallsAvoided += 1;
    if (afterPhone) metrics.surfeCallsAvoided += 1;

    if (
      primary?.classification === COMMERCIAL_CONTACT_CLASS.DIRECT_HOTEL_BUYER ||
      primary?.classification ===
        COMMERCIAL_CONTACT_CLASS.VERIFIED_MEETINGS_OR_TRAVEL_BUYER
    ) {
      if (!/BUYER|HOTEL_BUYER|MEETINGS_OR_TRAVEL/i.test(
        text(row.opportunity.primaryContact?.contactClass),
      )) {
        metrics.newlyDiscoveredVerifiedDirectBuyers += 1;
      }
    }
    // Count rejected false promotions: if title looked like buyer but class isn't
    if (
      /hotel.?buyer|procurement|sourcing/i.test(
        text(row.opportunity.primaryContact?.title),
      ) &&
      primary?.classification === COMMERCIAL_CONTACT_CLASS.AUTHORITY_UNVERIFIED
    ) {
      metrics.incorrectBuyerClassificationsRejected += 1;
    }

    if (cmp.samePerson) metrics.previouslyIdentifiedContactsRetained += 1;
    if (cmp.upgradeType === "RETAINED_CORRECTLY" || cmp.upgradeType === "SAME_PERSON_CHANNEL_IMPROVED") {
      if (cmp.upgradeType === "RETAINED_CORRECTLY") metrics.noBetterPersonFound += 1;
      if (cmp.upgradeType === "SAME_PERSON_CHANNEL_IMPROVED") {
        metrics.channelOnlyImprovements += 1;
      }
    }
    if (cmp.upgradeType === "BETTER_RELEVANT_ENTRY_POINT") {
      metrics.betterRelevantEntryPoints += 1;
    }
    if (result.contactLevelChanges?.includes("REFERRAL_ROUTE_IMPROVED")) {
      metrics.improvedReferralRoutes += 1;
    }
    if (cmp.meaningfulCommercialUpgrade) metrics.meaningfulCommercialUpgrades += 1;

    results.push({
      hotelId: row.hotelId,
      opportunityId: row.opportunityId,
      baseline: result.baselinePrimary,
      enhanced: primary
        ? {
            name: primary.name,
            classification: primary.classification,
            email: primary.email,
            phone: primary.phone,
            why: primary.salesFriendlyWhy,
            channelScore: primary.channelRecoveryScore,
            commercialScore: primary.commercialRelevanceScore,
          }
        : null,
      comparison: cmp,
      contactChanges: result.contactLevelChanges,
      searchCostUsd: result.metrics?.searchCostUsd || 0,
    });
  }

  const newChannels = metrics.newPublicEmails + metrics.newPublicPhones;
  const report = {
    generatedAt: new Date().toISOString(),
    providerMode: creds.chosenMode,
    spendCeilingUsd: SPEND_CEILING,
    actualSpendUsd: Number(spend.toFixed(4)),
    surfeCalls: 0,
    metrics: {
      ...metrics,
      costPerVerifiedNewChannel:
        newChannels > 0 ? Number((spend / newChannels).toFixed(4)) : null,
      costPerMeaningfulCommercialUpgrade:
        metrics.meaningfulCommercialUpgrades > 0
          ? Number((spend / metrics.meaningfulCommercialUpgrades).toFixed(4))
          : null,
    },
    results,
  };

  fs.writeFileSync(
    path.join(OUT, "PILOT_RESULTS.json"),
    `${JSON.stringify(report, null, 2)}\n`,
  );
  fs.writeFileSync(
    path.join(OUT, "PILOT_REPORT.md"),
    `# Commercial Contact Intelligence — 100 pilot

| Metric | Value |
|--------|-------|
| Mode | ${creds.chosenMode} |
| Opportunities | ${metrics.totalOpportunities} |
| Meaningful commercial upgrades | ${metrics.meaningfulCommercialUpgrades} |
| Better entry points | ${metrics.betterRelevantEntryPoints} |
| New verified direct buyers | ${metrics.newlyDiscoveredVerifiedDirectBuyers} |
| Retained correctly / no better | ${metrics.previouslyIdentifiedContactsRetained} / ${metrics.noBetterPersonFound} |
| New emails / phones | ${metrics.newPublicEmails} / ${metrics.newPublicPhones} |
| Surfe avoided | ${metrics.surfeCallsAvoided} |
| Spend | $${report.actualSpendUsd} / $${SPEND_CEILING} |
| Cost / new channel | ${report.metrics.costPerVerifiedNewChannel ?? "n/a"} |
| Cost / commercial upgrade | ${report.metrics.costPerMeaningfulCommercialUpgrade ?? "n/a"} |

Surfe: **0**. Full DB backfill: **not started**.
`,
  );
  console.log(
    JSON.stringify(
      { ...report, results: `[${results.length} rows]` },
      null,
      2,
    ),
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
