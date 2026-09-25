/**
 * Second-hotel replication V1 — Renaissance GDI preflight (read-only).
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { listTargetsForHotel } from "../lib/group-demand-intelligence/research-coverage/index.js";
import {
  getDgBase,
  isDgAirtableConfigured,
} from "../lib/group-demand-intelligence/demand-generators/airtable-client.js";
import {
  HOTEL_DEMAND_GENERATOR_FIT_TABLE_NAME,
  MAP_HOTEL_GENERATOR_FIT,
} from "../lib/group-demand-intelligence/demand-generators/airtable-field-map.js";
import {
  loadHotelDemandConfig,
  buildHotelGroupDemandProfile,
  isHotelOnboardedForGdi,
} from "../lib/group-demand-intelligence/hotel-profile.js";
import { isPilotReferenceLogicEnabled } from "../lib/group-demand-intelligence/pilot-path-policy.js";
import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { textHasPilotGeoBleed } from "../lib/group-demand-intelligence/research-coverage/portable-seed-templates.js";
import {
  reportCanonicalBaseEnv,
  resolveGdiCanonicalBaseId,
  CANONICAL_INTELLIGENCE_BASE_ID,
} from "../lib/group-demand-intelligence/canonical-airtable-base.js";

const HOTEL = "recG66DQJKP2c0UNh";
const BETHESDA = "recLuxvwwxID7U2B8";
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT = path.join(
  __dirname,
  "../reports/group-demand-intelligence/second-hotel-replication-v1"
);

async function listFits(hotelId) {
  if (!isDgAirtableConfigured()) return [];
  const base = getDgBase();
  const F = MAP_HOTEL_GENERATOR_FIT;
  const formula = `{${F.hotelId}} = "${String(hotelId).replace(/"/g, '\\"')}"`;
  const out = [];
  await base(HOTEL_DEMAND_GENERATOR_FIT_TABLE_NAME)
    .select({ filterByFormula: formula, pageSize: 100 })
    .eachPage((records, next) => {
      for (const r of records) {
        const f = r.fields || {};
        out.push({
          fitId: f[F.fitId],
          hotelId: f[F.hotelId],
          demandGeneratorId: f[F.demandGeneratorId],
          organizationName: f[F.organizationName],
          generatorPriority: f[F.generatorPriority],
          fitRationale: f[F.fitRationale],
        });
      }
      next();
    });
  return out;
}

function countBy(arr, key) {
  const m = {};
  for (const x of arr) {
    const k = x[key] || "UNKNOWN";
    m[k] = (m[k] || 0) + 1;
  }
  return m;
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const config = loadHotelDemandConfig(HOTEL);
  const profile = buildHotelGroupDemandProfile(HOTEL);
  const targets = await listTargetsForHotel(HOTEL);
  const fits = await listFits(HOTEL);
  let opps = [];
  try {
    const bag = loadOpportunitiesCanonical(HOTEL);
    opps = Array.isArray(bag) ? bag : bag?.opportunities || [];
  } catch {
    opps = [];
  }

  const bleedHits = [];
  for (const t of targets) {
    const blob = JSON.stringify(t);
    if (textHasPilotGeoBleed(blob) || blob.includes(BETHESDA)) {
      bleedHits.push({ kind: "target", id: t.targetId, name: t.canonicalName });
    }
  }
  for (const f of fits) {
    const blob = JSON.stringify(f);
    if (textHasPilotGeoBleed(blob) || blob.includes(BETHESDA)) {
      bleedHits.push({ kind: "fit", id: f.fitId, name: f.organizationName });
    }
  }
  for (const o of opps) {
    if (String(o.hotelId) !== HOTEL) {
      bleedHits.push({ kind: "opp_wrong_hotel", id: o.id || o.opportunityId });
    }
    if (textHasPilotGeoBleed(JSON.stringify(o))) {
      bleedHits.push({ kind: "opp_bleed", id: o.id || o.opportunityId });
    }
  }

  let gdiBase;
  try {
    gdiBase = resolveGdiCanonicalBaseId();
  } catch (e) {
    gdiBase = { error: e.code || e.message };
  }

  const report = {
    generatedAt: new Date().toISOString(),
    hotelId: HOTEL,
    hotelName: config?.displayName,
    onboarded: isHotelOnboardedForGdi(HOTEL),
    pilotLogicEnabled: isPilotReferenceLogicEnabled(HOTEL),
    canonicalBase: {
      expected: CANONICAL_INTELLIGENCE_BASE_ID,
      env: reportCanonicalBaseEnv(),
      gdi: gdiBase,
    },
    profile: {
      brand: config?.capabilityProfile?.softBrand,
      rooms: config?.capabilityProfile?.totalGuestrooms,
      meetingSqFt: config?.capabilityProfile?.totalMeetingSpaceSqFt,
      market: config?.demandTerritory?.label,
      serviceLevel: config?.capabilityProfile?.serviceLevel,
      website: profile?.officialWebsite || profile?.website || null,
      censusRecordId: config?.aliases?.censusRecordId,
      adpPropertyId: config?.aliases?.adpPropertyId,
    },
    fits: {
      count: fits.length,
      byPriority: countBy(fits, "generatorPriority"),
      sample: fits.slice(0, 5),
    },
    targets: {
      count: targets.length,
      byType: countBy(targets, "targetType"),
      byPriority: countBy(targets, "priority"),
      byStatus: countBy(targets, "status"),
      byCadence: countBy(targets, "researchCadence"),
      withNextResearchAt: targets.filter((t) => t.nextResearchAt).length,
      withPrimaryUrl: targets.filter((t) => t.primarySourceUrl).length,
    },
    opportunities: { count: opps.length },
    hotelScoped: {
      bleedHits: bleedHits.length,
      bleedSample: bleedHits.slice(0, 10),
      ok: bleedHits.length === 0,
    },
  };

  const outPath = path.join(OUT, "PHASE1_PREFLIGHT.json");
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
  console.log(JSON.stringify({ ok: true, outPath, summary: {
    fits: fits.length,
    targets: targets.length,
    opps: opps.length,
    pilotLogic: report.pilotLogicEnabled,
    hotelScopedOk: report.hotelScoped.ok,
    byType: report.targets.byType,
    byPriority: report.targets.byPriority,
  } }, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
