/**
 * Audit Hotel Intelligence completeness for all active ADP/GDI hotels.
 *
 *   node scripts/audit-hotel-intelligence-completeness-v1.mjs
 *   node scripts/audit-hotel-intelligence-completeness-v1.mjs --json
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { listAdpGdiHotelUniverse } from "../lib/hotel-census/adp-gdi-canonical-identity.js";
import { buildHotelIntelligenceProfile } from "../lib/hotel-intelligence/adp-attributes/build-hotel-intelligence-profile.js";
import { countActiveAdpAttributes } from "../lib/hotel-intelligence/onboarding/adp-attribute-counts.js";
import { evaluateHotelDomainStatuses } from "../lib/hotel-intelligence/onboarding/domain-status-store.js";
import {
  HI_DOMAIN,
  HI_DOMAIN_STATUS,
  OVERALL_HI_STATUS,
  REQUIRED_HI_DOMAINS,
} from "../lib/hotel-intelligence/onboarding/domain-status-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT = path.join(
  __dirname,
  "../reports/hotel-intelligence/completeness-onboarding-v1"
);

function gitHead() {
  try {
    return fs
      .readFileSync(path.join(__dirname, "../.git/HEAD"), "utf8")
      .trim()
      .replace(/^ref:\s+/, "");
  } catch {
    return null;
  }
}

async function auditOne(row) {
  const hpcId = row.canonicalHotelId || row.key;
  const profile = await buildHotelIntelligenceProfile(hpcId);
  if (!profile.ok) {
    return {
      hpcHotelId: hpcId,
      hotelName: row.displayName,
      adpPropertyId: row.adpPropertyId,
      overallStatus: OVERALL_HI_STATUS.HI_ERROR,
      error: profile.error,
      domains: Object.fromEntries(
        REQUIRED_HI_DOMAINS.map((d) => [d, HI_DOMAIN_STATUS.ERROR])
      ),
    };
  }
  let adpCount = 0;
  try {
    adpCount = await countActiveAdpAttributes(hpcId);
  } catch (err) {
    adpCount = 0;
  }
  const evaluated = evaluateHotelDomainStatuses(hpcId, profile, {
    adpAttributeActiveCount: adpCount,
  });
  return {
    hpcHotelId: hpcId,
    hotelName: profile.identity?.hotelName || row.displayName,
    adpPropertyId: profile.identity?.adpPropertyId || row.adpPropertyId,
    overallStatus: evaluated.overallStatus,
    coverage: evaluated.coverage,
    domains: Object.fromEntries(
      REQUIRED_HI_DOMAINS.map((d) => [
        d,
        {
          status: evaluated.domains[d].domainStatus,
          rowCount: evaluated.domains[d].rowCount,
          notes: evaluated.domains[d].notes,
          falseCompletenessFlag: evaluated.domains[d].falseCompletenessFlag || null,
        },
      ])
    ),
    evidenceSummary: profile.evidenceSummary,
    falseCompletenessFlags: evaluated.falseCompletenessFlags,
  };
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const universe = listAdpGdiHotelUniverse().filter((h) => h.adp || h.gdi);
  console.log(`[audit] active ADP/GDI hotels=${universe.length}`);

  const rows = [];
  for (const h of universe) {
    process.stdout.write(`[audit] ${(h.displayName || h.key).slice(0, 48)}\n`);
    rows.push(await auditOne(h));
  }

  const complete = rows.filter((r) => r.overallStatus === OVERALL_HI_STATUS.HI_COMPLETE);
  const incomplete = rows.filter((r) => r.overallStatus === OVERALL_HI_STATUS.HI_INCOMPLETE);
  const errored = rows.filter((r) => r.overallStatus === OVERALL_HI_STATUS.HI_ERROR);

  const notResearchedDomains = [];
  for (const r of rows) {
    for (const d of REQUIRED_HI_DOMAINS) {
      if (r.domains?.[d]?.status === HI_DOMAIN_STATUS.NOT_RESEARCHED) {
        notResearchedDomains.push({
          hotel: r.hotelName,
          hpcHotelId: r.hpcHotelId,
          domain: d,
        });
      }
    }
  }

  const summary = {
    headRef: gitHead(),
    generatedAt: new Date().toISOString(),
    activeHotels: universe.length,
    hiComplete: complete.length,
    hiIncomplete: incomplete.length,
    hiError: errored.length,
    notResearchedDomainCount: notResearchedDomains.length,
    notResearchedByDomain: Object.fromEntries(
      REQUIRED_HI_DOMAINS.map((d) => [
        d,
        notResearchedDomains.filter((x) => x.domain === d).length,
      ])
    ),
    hotels: rows,
    notResearchedDomains,
  };

  fs.writeFileSync(
    path.join(OUT, "HI_COMPLETENESS_AUDIT_BEFORE.json"),
    JSON.stringify(summary, null, 2) + "\n"
  );
  fs.writeFileSync(
    path.join(OUT, "FULL_HOTEL_MATRIX.json"),
    JSON.stringify(
      rows.map((r) => ({
        hotel: r.hotelName,
        hpcHotelId: r.hpcHotelId,
        commercial: r.domains?.[HI_DOMAIN.COMMERCIAL_PROFILE]?.status,
        eventSpaces: r.domains?.[HI_DOMAIN.EVENT_SPACES]?.status,
        demandNodes: r.domains?.[HI_DOMAIN.DEMAND_NODES]?.status,
        seasonalityNeed: r.domains?.[HI_DOMAIN.SEASONALITY_NEED_PERIODS]?.status,
        evidence: r.domains?.[HI_DOMAIN.HI_EVIDENCE]?.status,
        adpAttrs: r.domains?.[HI_DOMAIN.ADP_ATTRIBUTES]?.status,
        overall: r.overallStatus,
        coverage: r.coverage?.label,
      })),
      null,
      2
    ) + "\n"
  );

  console.log(
    JSON.stringify(
      {
        activeHotels: summary.activeHotels,
        hiComplete: summary.hiComplete,
        hiIncomplete: summary.hiIncomplete,
        notResearchedDomainCount: summary.notResearchedDomainCount,
        notResearchedByDomain: summary.notResearchedByDomain,
      },
      null,
      2
    )
  );
  console.log(`[out] ${OUT}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
