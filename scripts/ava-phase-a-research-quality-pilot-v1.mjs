/**
 * AVA Phase A — research completeness & quality pilot (6 hotels, $0 paid).
 *
 *   AVA_STORAGE_MODE=local node scripts/ava-phase-a-research-quality-pilot-v1.mjs
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  discoverAndAssessPlatformListingsV3,
  applyPlatformDiscoveryV3ToPayload,
  LISTING_EXISTENCE,
} from "../lib/ai-visibility-audit-action-plan/platform-listing-discovery-v3.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const REPORT = path.join(
  ROOT,
  "reports/dealality-ai-visibility-audit-action-plan-pilot-v1"
);
const PAYLOAD_DIR = path.join(ROOT, "lib/ai-visibility-audit-action-plan/payloads");
const PRIOR = path.join(REPORT, "AVA_V3_QUALITY_AND_PLATFORM_RESEARCH_PILOT_RESULTS.json");

const COHORT = [
  "waterstone-boca-raton-v1.json",
  "ac-hotel-a-coruna-v1.json",
  "castillo-hotel-son-vida-v1.json",
  "sheraton-mallorca-arabella-golf-v1.json",
  "grand-fiesta-americana-chapultepec-v1.json",
  "fiesta-americana-condesa-cancun-ai-v1.json",
];

function priorSnapshot(slug) {
  if (!fs.existsSync(PRIOR)) return null;
  const doc = JSON.parse(fs.readFileSync(PRIOR, "utf8"));
  return (doc.platformResearch || []).find((h) => h.slug === slug) || null;
}

async function main() {
  const hotels = [];
  console.log("=== Phase A research quality (6 hotels, $0) ===");
  for (const file of COHORT) {
    const slug = file.replace(/-v1\.json$/i, "");
    const payloadPath = path.join(PAYLOAD_DIR, file);
    let payload = JSON.parse(fs.readFileSync(payloadPath, "utf8"));
    const before = priorSnapshot(slug);
    console.log("Research", file);
    const discovery = await discoverAndAssessPlatformListingsV3(payload, {
      skipLiveSearch: false,
    });
    payload = applyPlatformDiscoveryV3ToPayload(payload, discovery);
    fs.writeFileSync(payloadPath, `${JSON.stringify(payload, null, 2)}\n`);

    const listings = discovery.assessments
      .filter((a) => a.relevant !== false)
      .map((a) => ({
        platform: a.platform,
        category: a.category,
        url: a.url || null,
        listingExistence: a.listingExistence,
        contentAssessment: a.contentAssessment,
        customerStatus: a.customerStatus,
        discoveryStrategy: a.discoveryStrategy || null,
        notes: a.notes || null,
      }));

    const row = {
      slug,
      hotel: payload.property?.name,
      propertyId: payload.property?.propertyId,
      paidResearchUsd: 0,
      before: before
        ? {
            withUrl: before.after?.withUrl,
            verifiedConsistent: before.after?.verifiedConsistent,
            notFoundAfterResearch: before.after?.notFoundAfterResearch,
            researchIncomplete: before.after?.researchIncomplete,
            listings: (before.listings || [])
              .filter((l) => l.url)
              .map((l) => ({ platform: l.platform, url: l.url, status: l.researchStatus })),
          }
        : null,
      after: discovery.summary,
      processGate: discovery.processGate,
      customerResearchReadiness: discovery.customerResearchReadiness,
      listings,
      exactUrls: listings
        .filter((l) => l.listingExistence === LISTING_EXISTENCE.EXACT_LISTING_VERIFIED)
        .map((l) => ({ platform: l.platform, url: l.url })),
    };
    hotels.push(row);
    console.log(
      `  ${row.hotel} exact=${row.after.exactListingVerified} unresolvedHP=${row.after.highPriorityUnresolved.length} process=${row.processGate.pass} readiness=${row.customerResearchReadiness.verdict}`
    );
  }

  const out = {
    contract: "AVA_PHASE_A_RESEARCH_QUALITY_PILOT_V1",
    generatedAt: new Date().toISOString(),
    paidResearchUsd: 0,
    publicationAuthorized: false,
    deploymentAuthorized: false,
    adpMeasurementsChanged: false,
    hotels,
    cohortVerdict: {
      researchProcess:
        hotels.every((h) => h.processGate?.pass) ? "PASS" : "FAIL",
      customerResearchReadiness: hotels.every(
        (h) =>
          h.customerResearchReadiness?.verdict === "PASS" ||
          h.customerResearchReadiness?.verdict === "PASS_WITH_MATERIAL_LIMITATIONS"
      )
        ? hotels.some(
            (h) =>
              h.customerResearchReadiness?.verdict ===
              "PASS_WITH_MATERIAL_LIMITATIONS"
          )
          ? "PASS_WITH_MATERIAL_LIMITATIONS"
          : "PASS"
        : "RESEARCH_FOLLOW_UP_REQUIRED",
    },
  };

  fs.writeFileSync(
    path.join(REPORT, "AVA_PHASE_A_RESEARCH_QUALITY_PILOT_RESULTS.json"),
    `${JSON.stringify(out, null, 2)}\n`
  );
  console.log("\nWrote AVA_PHASE_A_RESEARCH_QUALITY_PILOT_RESULTS.json");
  console.log("Cohort process:", out.cohortVerdict.researchProcess);
  console.log("Cohort readiness:", out.cohortVerdict.customerResearchReadiness);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
