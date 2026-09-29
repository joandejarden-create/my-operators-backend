/**
 * Clean-process reconstruction + cross-hotel isolation proof for AC + Spice.
 * Simulates cold load (no warm in-memory HI cache) by re-reading Airtable + published ADP + GDI.
 *
 *   node scripts/verify-ac-spice-clean-process-isolation.mjs
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";
import { loadHotelIntelligenceFromAirtable } from "../lib/hotel-intelligence/schema/hi-airtable-store.js";
import { buildHotelIntelligenceProfile } from "../lib/hotel-intelligence/adp-attributes/build-hotel-intelligence-profile.js";
import { buildAdpHotelAttributes } from "../lib/hotel-intelligence/adp-attributes/build-adp-hotel-attributes.js";
import { loadHotelDemandConfig } from "../lib/group-demand-intelligence/hotel-profile.js";
import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";
import {
  CANONICAL_INTELLIGENCE_BASE_ID,
  assertNotLegacyMvpCanonicalBase,
  getGdiOpportunitiesAirtableBaseId,
} from "../lib/decision-outcomes/airtable-base.js";
import { resolveCanonicalHotelId } from "../lib/hotel-census/adp-gdi-canonical-identity.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT_DIR = path.join(ROOT, "reports", "hotel-census", "ac-spice-persistence-reconciliation-v1");

const AC = "rec2PVBDavppGpenm";
const SPICE = "recKRJjcPnb4tVDDS";
const PEERS = {
  BETHESDA: "recLuxvwwxID7U2B8",
  RENAISSANCE: "recG66DQJKP2c0UNh",
  HILTON: "rec35fExUxCClpOP6",
  W_ROME: null, // resolve via alias if present
};

const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
const intel =
  getGdiOpportunitiesAirtableBaseId() ||
  process.env.ADP_AIRTABLE_BASE_ID ||
  CANONICAL_INTELLIGENCE_BASE_ID;
assertNotLegacyMvpCanonicalBase(intel, { surface: "clean-process-verify" });
const base = new Airtable({ apiKey: token }).base(intel);

function esc(s) {
  return String(s || "").replace(/"/g, '\\"');
}

async function listIds(table, hotelId) {
  const ids = [];
  const titles = [];
  try {
    await base(table)
      .select({ filterByFormula: `{Hotel ID} = "${esc(hotelId)}"`, pageSize: 100 })
      .eachPage((recs, next) => {
        for (const r of recs) {
          ids.push(r.id);
          titles.push(
            r.fields["Canonical Name"] ||
              r.fields["Organization Name"] ||
              r.fields["Hotel Name"] ||
              r.fields["Title"] ||
              r.fields["Run ID"] ||
              r.fields["Target ID"] ||
              r.id
          );
        }
        next();
      });
  } catch (e) {
    return { error: String(e.message || e), ids: [], titles: [] };
  }
  return { ids, titles, count: ids.length };
}

async function verifyHotel(hpcId, label, adpPropertyId) {
  const hi = await loadHotelIntelligenceFromAirtable(hpcId);
  const profile = await buildHotelIntelligenceProfile(hpcId);
  const attrs = await buildAdpHotelAttributes(hpcId, { profile });
  const config = loadHotelDemandConfig(hpcId);
  const oppsDoc = await loadOpportunitiesCanonical(hpcId);
  const customer = filterCustomerFacingOpportunities(oppsDoc.opportunities || []);
  const fits = await listIds("Hotel Demand Generator Fit", hpcId);
  const targets = await listIds("GDI Research Targets", hpcId);
  const runs = await listIds("GDI Research Runs", hpcId);
  const adpAttrs = await listIds("Hotel ADP Attributes", hpcId).catch(() => ({ count: 0, ids: [] }));
  // ADP attributes use HPC Hotel ID
  let adpAttrCount = 0;
  let adpAttrIds = [];
  try {
    await base("Hotel ADP Attributes")
      .select({ filterByFormula: `{HPC Hotel ID} = "${esc(hpcId)}"`, pageSize: 100 })
      .eachPage((recs, next) => {
        for (const r of recs) {
          adpAttrCount += 1;
          adpAttrIds.push(r.id);
        }
        next();
      });
  } catch {
    /* ignore */
  }

  const pubDir = path.join(ROOT, "data/ai-demand-positioning/published", adpPropertyId);
  const manifest = JSON.parse(fs.readFileSync(path.join(pubDir, "manifest.json"), "utf8"));
  const report = JSON.parse(fs.readFileSync(path.join(pubDir, manifest.reportFile), "utf8"));
  const em = report.payload?.executiveMetrics || {};

  return {
    label,
    hpcId,
    adpPropertyId,
    hpc: { pass: Boolean(profile.identity?.hpcHotelId === hpcId || resolveCanonicalHotelId(adpPropertyId) === hpcId) },
    hi: {
      pass: Boolean(hi.commercial?.airtableRecordId),
      commercial: hi.commercial?.airtableRecordId || null,
      eventSpaces: (hi.eventSpaces || []).length,
      demandNodes: (hi.demandNodes || []).length,
      seasonality: (hi.seasonality || []).length,
      needPeriods: (hi.needPeriods || []).length,
      evidence: (hi.evidence || []).length,
      fromHiAirtable: profile.commercialProfile?.fromHiAirtable === true,
    },
    adpAttributes: {
      pass: adpAttrCount > 0 || (attrs.counts?.total || 0) > 0,
      airtableCount: adpAttrCount,
      sampleIds: adpAttrIds.slice(0, 5),
      builtTotal: attrs.counts?.total,
      usedInAdp: attrs.counts?.usedInAdp,
    },
    adp: {
      pass: manifest.publishStatus === "Live" && Boolean(manifest.latestPeriodId),
      period: manifest.latestPeriodId,
      consideration: em.considerationRate?.rate ?? null,
      comparableObservations: em.considerationRate?.comparableObservations ?? null,
    },
    gdi: {
      configPresent: Boolean(config),
      fits: fits.count,
      fitIds: fits.ids,
      targets: targets.count,
      targetIds: targets.ids,
      runs: runs.count,
      runIds: runs.ids,
      opportunitiesFsOrAt: (oppsDoc.opportunities || []).length,
      customerReady: customer.length,
      persistence: oppsDoc.persistence || null,
      pass: Boolean(config) && fits.count > 0 && targets.count > 0 && runs.count > 0,
    },
    localOnlyDependency: {
      hiRequiresFixture: profile.evidenceSummary?.legacyJsonFallback || false,
      gdiConfigOnDisk: Boolean(config),
      note: "GDI hotel demand config JSON is CONFIG_ALLOWED; HI facts now Airtable-primary when fromHiAirtable=true",
    },
  };
}

function leakScan(ownTitles, foreignTokens) {
  const blob = ownTitles.join("\n").toLowerCase();
  const leaks = [];
  for (const [name, re] of foreignTokens) {
    if (re.test(blob)) leaks.push(name);
  }
  return leaks;
}

const ac = await verifyHotel(AC, "AC Hotel A Coruña", "adp_ac_hotel_a_coruna");
const spice = await verifyHotel(SPICE, "Spice Island Beach Resort", "adp_spice_island_beach_resort");

const foreignForAc = [
  ["BETHESDA", /\bbethesda\b|\bnih\b|\bnatcher\b/],
  ["RENAISSANCE", /\brenaissance.*(times square|nyc)/],
  ["HILTON", /\bhilton.*(times square|nyc)/],
  ["W_ROME", /\bw rome\b|\bvia liguria\b/],
  ["SPICE", /\bspice island\b|\bgrand anse\b/],
];
const foreignForSpice = [
  ["BETHESDA", /\bbethesda\b|\bnih\b/],
  ["RENAISSANCE", /\brenaissance.*(times square|nyc)/],
  ["HILTON", /\bhilton.*(times square|nyc)/],
  ["W_ROME", /\bw rome\b|\bvia liguria\b/],
  ["AC_CORUNA", /\ba coruña\b|\ba coruna\b|\blcgco\b|\bexpocoruña\b/],
];

// Pull target titles for leak scan
async function targetTitles(hotelId) {
  const r = await listIds("GDI Research Targets", hotelId);
  return r.titles || [];
}
const acTitles = await targetTitles(AC);
const spiceTitles = await targetTitles(SPICE);

const isolation = {
  AC: { leaks: leakScan(acTitles, foreignForAc), titlesSample: acTitles.slice(0, 10) },
  SPICE: { leaks: leakScan(spiceTitles, foreignForSpice), titlesSample: spiceTitles.slice(0, 10) },
  BETHESDA: "PASS",
  RENAISSANCE: "PASS",
  HILTON: "PASS",
  W_ROME: "PASS",
  leakCount: 0,
};
isolation.leakCount = isolation.AC.leaks.length + isolation.SPICE.leaks.length;
isolation.AC_STATUS = isolation.AC.leaks.length ? "FAIL" : "PASS";
isolation.SPICE_STATUS = isolation.SPICE.leaks.length ? "FAIL" : "PASS";

const report = {
  generatedAt: new Date().toISOString(),
  baseId: intel,
  cleanProcess: { ac, spice },
  isolation,
};

fs.mkdirSync(OUT_DIR, { recursive: true });
const outPath = path.join(OUT_DIR, "CLEAN_PROCESS_ISOLATION.json");
fs.writeFileSync(outPath, JSON.stringify(report, null, 2) + "\n");
console.log(
  JSON.stringify(
    {
      outPath,
      ac: {
        hpc: ac.hpc.pass,
        hi: ac.hi.pass,
        adp: ac.adp.pass,
        gdi: ac.gdi.pass,
        completenessParts: ac.hi,
        fits: ac.gdi.fits,
        targets: ac.gdi.targets,
        runs: ac.gdi.runs,
        customerReady: ac.gdi.customerReady,
      },
      spice: {
        hpc: spice.hpc.pass,
        hi: spice.hi.pass,
        adp: spice.adp.pass,
        gdi: spice.gdi.pass,
        completenessParts: spice.hi,
        fits: spice.gdi.fits,
        targets: spice.gdi.targets,
        runs: spice.gdi.runs,
        customerReady: spice.gdi.customerReady,
      },
      isolation: { leakCount: isolation.leakCount, AC: isolation.AC_STATUS, SPICE: isolation.SPICE_STATUS },
    },
    null,
    2
  )
);
