#!/usr/bin/env node
/**
 * Local-only materialization of authentic Design Hotels pilot research
 * into canonical hotel research stores + selectable configs.
 *
 * - Does NOT overwrite non-empty opportunities.json
 * - Does NOT create STAGED/CURRENT publication packages
 * - Does NOT write Airtable
 * - Applies soft cross-segment dedupe (org+year+title family)
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { pathToFileURL } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const FORCE = process.argv.includes("--force");

async function loadCanonicalDedupe() {
  const candidates = [
    path.join(
      ROOT,
      "lib/group-demand-intelligence/research/gdi-cross-segment-opportunity-dedupe-v2.js",
    ),
    "C:/Users/joand/AppData/Local/Temp/dealality-gdi-repeatable-processing-v2-20261123/lib/group-demand-intelligence/research/gdi-cross-segment-opportunity-dedupe-v2.js",
  ];
  for (const p of candidates) {
    if (fs.existsSync(p)) {
      return import(pathToFileURL(p).href);
    }
  }
  throw new Error("CANONICAL_DEDUPE_MODULE_MISSING");
}

const HOTELS = Object.freeze([
  {
    hotelId: "rece0NdV9FCnaFlVS",
    hotelName: "Condesa DF",
  },
  {
    hotelId: "recaokE8psIbuVeuH",
    hotelName: "LaLuna Boutique Hotels & Villas",
  },
  {
    hotelId: "rec1t35e3sPerzxYu",
    hotelName: "Memmo Príncipe Real",
  },
]);

const PILOT_ROOT = path.resolve(
  process.env.GDI_DESIGN_HOTELS_PILOT_ROOT ||
    "C:/Users/joand/AppData/Local/Temp/dealality-gdi-repeatable-processing-v2-20261123/reports/gdi-design-hotels-live-research-pilot-v1",
);

function text(v) {
  return String(v == null ? "" : v)
    .replace(/\s+/g, " ")
    .trim();
}

function latestRunDir(hotelId) {
  const base = path.join(PILOT_ROOT, "hotels", hotelId);
  if (!fs.existsSync(base)) return null;
  const dirs = fs
    .readdirSync(base, { withFileTypes: true })
    .filter((d) => d.isDirectory())
    .map((d) => {
      const full = path.join(base, d.name);
      const screeningPath = path.join(full, "pre-deep-candidate-screening.json");
      let eligible = 0;
      if (fs.existsSync(screeningPath)) {
        try {
          const j = JSON.parse(fs.readFileSync(screeningPath, "utf8"));
          eligible = Array.isArray(j.eligibleCandidates)
            ? j.eligibleCandidates.length
            : 0;
        } catch {
          eligible = 0;
        }
      }
      return {
        name: d.name,
        full,
        eligible,
        mtime: fs.statSync(full).mtimeMs,
      };
    })
    // Prefer runs that actually captured eligible research inventory.
    .sort((a, b) => b.eligible - a.eligible || b.mtime - a.mtime);
  return dirs[0] ? dirs[0].full : null;
}

function toCustomerResearchOpp(raw, hotel, deepSelectedIds) {
  const id = text(raw.id || raw.opportunityId);
  const deep = deepSelectedIds.has(id);
  const named = Array.isArray(raw.namedStakeholders)
    ? raw.namedStakeholders
    : [];
  const primary = named.find((n) => text(n?.name)) || null;
  return {
    id,
    opportunityId: id,
    hotelId: hotel.hotelId,
    hotelName: hotel.hotelName,
    title: raw.title || raw.programmeName || null,
    organizationName: raw.organizationName || null,
    organization: raw.organizationName || null,
    programmeName: raw.programmeName || null,
    researchSegment: raw.researchSegment || null,
    segmentAssociations: raw.segmentAssociations || [raw.researchSegment].filter(Boolean),
    demandType: raw.demandType || null,
    hotelOpportunityThesis: raw.hotelOpportunityThesis || null,
    evidence: raw.evidence || null,
    acceptedClaims: raw.acceptedClaims || null,
    seedUrls: raw.seedUrls || [],
    investigatedUrls: raw.investigatedUrls || [],
    sourceUrls: [
      ...(Array.isArray(raw.seedUrls) ? raw.seedUrls : []),
      ...(Array.isArray(raw.investigatedUrls) ? raw.investigatedUrls : []),
    ].filter(Boolean),
    namedStakeholders: named,
    primaryContact: primary
      ? {
          name: primary.name || null,
          role: primary.role || primary.title || null,
          organization: primary.organization || raw.organizationName || null,
          email: primary.email || null,
          customerFacingNamed: Boolean(primary.name) && primary.suppressCustomerName !== true,
        }
      : null,
    marketScopeClass:
      raw.marketScopeClass ||
      raw.scope ||
      (raw.preserveAsCompetitiveIntelligence ? "Competitive" : "Hotel"),
    commercialDisposition:
      raw.commercialDispositionAtIntake || raw.intakeDisposition || "RESEARCH_CONTINUATION",
    salesReady: false,
    customerPublicationReady: false,
    publicationOccurred: false,
    deepResearched: deep,
    researchLifecycleStatus: deep ? "DEEP_RESEARCHED" : "RESEARCH_INVENTORY",
    materialization: {
      source: "gdi-design-hotels-live-research-pilot-v1",
      runDir: null,
      schema: "gdi-design-hotels-local-research-materialize-v1",
      notPublication: true,
    },
  };
}

async function materializeHotel(hotel, targetRoot = ROOT, dedupeMod) {
  const runDir = latestRunDir(hotel.hotelId);
  if (!runDir) {
    return { hotelId: hotel.hotelId, ok: false, error: "PILOT_RUN_MISSING" };
  }
  const screeningPath = path.join(runDir, "pre-deep-candidate-screening.json");
  const deepPath = path.join(runDir, "deep-research-selection-register.json");
  if (!fs.existsSync(screeningPath)) {
    return { hotelId: hotel.hotelId, ok: false, error: "SCREENING_MISSING", runDir };
  }
  const screening = JSON.parse(fs.readFileSync(screeningPath, "utf8"));
  const deep = fs.existsSync(deepPath)
    ? JSON.parse(fs.readFileSync(deepPath, "utf8"))
    : { selected: [] };
  const deepIds = new Set(
    (deep.selected || []).map((s) => text(s.id || s.opportunityId)).filter(Boolean),
  );
  const rawEligible = Array.isArray(screening.eligibleCandidates)
    ? screening.eligibleCandidates
    : [];
  const deduped = dedupeMod.dedupeCandidatesAcrossSegments(rawEligible, {
    preserveEvidence: true,
  });
  const accepted =
    deduped.canonicalCandidates ||
    deduped.canonical ||
    deduped.accepted ||
    [];
  const suppressed =
    deduped.suppressedCandidates || deduped.suppressed || [];
  const opportunities = accepted.map((c) => {
    const opp = toCustomerResearchOpp(c, hotel, deepIds);
    opp.materialization.runDir = runDir;
    opp.materialization.dedupeContract = dedupeMod.GDI_CROSS_SEGMENT_OPPORTUNITY_DEDUPE_V2;
    return opp;
  });

  const hotelDir = path.join(
    targetRoot,
    "data/group-demand-intelligence/hotels",
    hotel.hotelId,
  );
  fs.mkdirSync(hotelDir, { recursive: true });
  const oppPath = path.join(hotelDir, "opportunities.json");
  if (fs.existsSync(oppPath) && !FORCE) {
    const existing = JSON.parse(fs.readFileSync(oppPath, "utf8"));
    const existingOpps = existing.opportunities || existing;
    if (Array.isArray(existingOpps) && existingOpps.length > 0) {
      return {
        hotelId: hotel.hotelId,
        ok: true,
        skipped: true,
        reason: "EXISTING_NONEMPTY_PRESERVED",
        existingCount: existingOpps.length,
        wouldWrite: opportunities.length,
        rawEligible: rawEligible.length,
        afterDedupe: accepted.length,
        suppressed: suppressed.length,
        deepSelected: deepIds.size,
      };
    }
  }

  const hotelCount = opportunities.filter((o) => o.marketScopeClass !== "Competitive").length;
  const competitiveCount = opportunities.filter((o) => o.marketScopeClass === "Competitive").length;
  const namedCount = opportunities.filter((o) => o.primaryContact?.name).length;

  const doc = {
    schema: "gdi-hotel-opportunities-v1",
    hotelId: hotel.hotelId,
    hotelName: hotel.hotelName,
    updatedAt: new Date().toISOString(),
    source: "design-hotels-live-research-pilot-local-materialize-v1",
    notCustomerPublication: true,
    funnel: {
      rawEligible: rawEligible.length,
      afterDedupe: accepted.length,
      suppressedDuplicates: suppressed.length,
      deepSelected: deepIds.size,
      salesReady: 0,
    },
    opportunities,
  };
  fs.writeFileSync(oppPath, JSON.stringify(doc, null, 2), "utf8");

  // Ensure selectable config present
  const cfgSrc = path.join(
    path.dirname(PILOT_ROOT),
    "../config/group-demand-intelligence/hotels",
    `${hotel.hotelId}.json`,
  );
  const cfgAlt = path.resolve(
    "C:/Users/joand/AppData/Local/Temp/dealality-gdi-repeatable-processing-v2-20261123/config/group-demand-intelligence/hotels",
    `${hotel.hotelId}.json`,
  );
  const cfgDst = path.join(
    targetRoot,
    "config/group-demand-intelligence/hotels",
    `${hotel.hotelId}.json`,
  );
  const cfgFrom = fs.existsSync(cfgSrc) ? cfgSrc : cfgAlt;
  if (fs.existsSync(cfgFrom) && !fs.existsSync(cfgDst)) {
    fs.mkdirSync(path.dirname(cfgDst), { recursive: true });
    fs.copyFileSync(cfgFrom, cfgDst);
  }

  return {
    hotelId: hotel.hotelId,
    hotelName: hotel.hotelName,
    ok: true,
    skipped: false,
    runDir,
    oppPath,
    rawEligible: rawEligible.length,
    afterDedupe: accepted.length,
    suppressed: suppressed.length,
    deepSelected: deepIds.size,
    hotelCount,
    competitiveCount,
    namedContactCoverage: namedCount,
    salesReady: 0,
    suppressedSamples: suppressed.slice(0, 5),
  };
}

async function main() {
  const dedupeMod = await loadCanonicalDedupe();
  const hotels = [];
  for (const h of HOTELS) {
    hotels.push(await materializeHotel(h, ROOT, dedupeMod));
  }
  const out = {
    schema: "gdi-design-hotels-local-research-materialize-v1",
    at: new Date().toISOString(),
    force: FORCE,
    dedupeContract: dedupeMod.GDI_CROSS_SEGMENT_OPPORTUNITY_DEDUPE_V2,
    targetRoot: ROOT,
    pilotRoot: PILOT_ROOT,
    hotels,
  };
  const reportDir = path.join(ROOT, "reports/gdi-design-hotels-local-visibility-v1");
  fs.mkdirSync(reportDir, { recursive: true });
  fs.writeFileSync(
    path.join(reportDir, "MATERIALIZE_RESULT.json"),
    JSON.stringify(out, null, 2),
  );
  console.log(JSON.stringify(out, null, 2));
  if (out.hotels.some((h) => !h.ok)) process.exit(2);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
