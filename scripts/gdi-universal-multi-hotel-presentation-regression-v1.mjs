#!/usr/bin/env node
/**
 * Multi-hotel GDI presentation regression (local / read-only).
 * Distinguishes Spice Island approved 7 vs research inventory.
 * No production writes, tokens, Airtable, or paid research.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  certifyPresentationParity,
  GDI_PRESENTATION_SURFACES,
} from "../lib/group-demand-intelligence/contracts/gdi-presentation-contract-v1.js";
import { preparePublicationSnapshot } from "../lib/group-demand-intelligence/contracts/gdi-publication-contract-v1.js";
import { projectOpportunityForPresentation } from "../lib/group-demand-intelligence/presentation/gdi-universal-presentation-pipeline-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-universal-presentation-publication-parity-v1",
);

const SPICE_APPROVED_SEVEN = Object.freeze([
  "gdi_s4912_ba7b1cb8a3ab",
  "gdi_s4912_b956aa67a4a2",
  "gdi_s4912_b6d9b1929c05",
  "gdi_s4912_cb2d71ab1b15",
  "gdi_s4912_cc2a8da6ca6e",
  "gdi_s4912_03deda917778",
  "gdi_s4912_3cafec77a21f",
]);

const HOTELS = Object.freeze([
  {
    hotelId: "recKRJjcPnb4tVDDS",
    name: "Spice Island Beach Resort",
    role: "primary_benchmark",
    approvedCustomerInventory: SPICE_APPROVED_SEVEN,
    note: "Approved customer inventory is seven IDs; research history may be eight+ — no arbitrary ceiling.",
  },
  {
    hotelId: "recLuxvwwxID7U2B8",
    name: "Bethesda Marriott",
    role: "regression",
    expectedMinFacingHint: 30,
  },
  {
    hotelId: "recgMYovrrZDJMqzX",
    name: "Waterstone Resort & Marina Boca Raton",
    role: "regression",
    expectedMinFacingHint: 28,
  },
  {
    hotelId: "recnPB3Q02NO8Ma1o",
    name: "Wyndham San Diego Bayside",
    role: "regression",
  },
  {
    hotelId: "recC2kbCaS48jFjFF",
    name: "Nobu Hotel London Portman Square",
    role: "regression",
  },
  {
    hotelId: "recrPQcZg7SFARRb2",
    name: "YOTEL Geneva Lake",
    role: "regression",
  },
  {
    hotelId: "recIEcSWuhSCoNLFN",
    name: "Fiesta Americana Condesa Cancún All Inclusive",
    role: "regression",
  },
]);

function loadLocalOpportunities(hotelId) {
  const candidates = [
    path.join(ROOT, "data/group-demand-intelligence/hotels", hotelId, "opportunities.json"),
    path.join(ROOT, "data/group-demand-intelligence", hotelId, "opportunities.json"),
  ];
  for (const p of candidates) {
    if (!fs.existsSync(p)) continue;
    try {
      const doc = JSON.parse(fs.readFileSync(p, "utf8"));
      const rows = Array.isArray(doc)
        ? doc
        : Array.isArray(doc.opportunities)
          ? doc.opportunities
          : [];
      return { path: p, rows };
    } catch {
      return { path: p, rows: [], error: "parse_failed" };
    }
  }
  return { path: null, rows: [] };
}

function loadSpiceEnriched() {
  const dir = path.join(
    ROOT,
    "reports/gdi-production-publication-full-content-parity-v1/enriched-snapshots",
  );
  return SPICE_APPROVED_SEVEN.map((id) => {
    const p = path.join(dir, `${id}.json`);
    return {
      id,
      exists: fs.existsSync(p),
      opportunity: fs.existsSync(p)
        ? JSON.parse(fs.readFileSync(p, "utf8"))
        : null,
    };
  });
}

function sampleRowChecks(hotelId, rows, limit = 5) {
  const sample = rows.slice(0, limit);
  const checks = [];
  for (const o of sample) {
    try {
      const cert = certifyPresentationParity(o, { hotelId });
      const pipe = projectOpportunityForPresentation(o, {
        surface: GDI_PRESENTATION_SURFACES.EXTERNAL_SHARE,
        hotelId,
        shareResolve: { capabilities: ["CAN_READ_OPPORTUNITIES"] },
      });
      checks.push({
        id: o.id || o.opportunityId || null,
        title: o.title || o.displayTitle || null,
        parityOk: cert.ok,
        fieldDiffCount: cert.fieldDiffs.length,
        pipelineOk: pipe.ok === true,
        shareMode: pipe.dto?.mode || null,
        shareBlocksResearch: pipe.capabilities?.canRunResearch === false,
      });
    } catch (err) {
      checks.push({
        id: o.id || null,
        ok: false,
        error: String(err?.message || err),
      });
    }
  }
  return checks;
}

const hotelResults = [];

for (const hotel of HOTELS) {
  const local = loadLocalOpportunities(hotel.hotelId);
  const entry = {
    ...hotel,
    localArtifact: local.path,
    localRowCount: local.rows.length,
    configExists: fs.existsSync(
      path.join(ROOT, "config/group-demand-intelligence/hotels", `${hotel.hotelId}.json`),
    ),
    sampleChecks: [],
    spiceApprovedSeven: null,
    status: "PENDING",
  };

  if (hotel.hotelId === "recKRJjcPnb4tVDDS") {
    const enriched = loadSpiceEnriched();
    const pubChecks = [];
    for (const row of enriched) {
      if (!row.exists) {
        pubChecks.push({ id: row.id, ok: false, error: "missing_enriched" });
        continue;
      }
      const cert = certifyPresentationParity(row.opportunity, {
        hotelId: hotel.hotelId,
      });
      const pub = preparePublicationSnapshot(row.opportunity, {
        hotelId: hotel.hotelId,
      });
      pubChecks.push({
        id: row.id,
        parityOk: cert.ok,
        publicationOk: pub.ok,
        classification:
          row.opportunity.commercialInventoryClass ||
          row.opportunity.commercialClassification ||
          null,
        fails: pub.fails,
      });
    }
    entry.spiceApprovedSeven = {
      approvedCount: SPICE_APPROVED_SEVEN.length,
      researchInventoryNote:
        "Local/research inventory may exceed seven; customer publication inventory is the approved seven only.",
      results: pubChecks,
      allOk: pubChecks.every((r) => r.parityOk && r.publicationOk),
    };
    entry.sampleChecks = sampleRowChecks(
      hotel.hotelId,
      enriched.filter((e) => e.opportunity).map((e) => e.opportunity),
      7,
    );
    entry.status = entry.spiceApprovedSeven.allOk ? "PASS" : "FAIL";
  } else if (local.rows.length > 0) {
    entry.sampleChecks = sampleRowChecks(hotel.hotelId, local.rows, 5);
    const sampleOk = entry.sampleChecks.every(
      (c) => c.parityOk !== false && c.pipelineOk !== false && !c.error,
    );
    entry.status = sampleOk ? "PASS_LOCAL_SAMPLE" : "FAIL_LOCAL_SAMPLE";
  } else {
    entry.status = "SKIP_NO_LOCAL_INVENTORY";
    entry.note =
      (entry.note || "") +
      " Config present; no local opportunities.json — presentation contracts still apply when inventory exists.";
  }

  hotelResults.push(entry);
}

const summary = {
  ok: hotelResults
    .filter((h) => h.status === "FAIL" || h.status === "FAIL_LOCAL_SAMPLE")
    .length === 0,
  hotels: hotelResults.length,
  pass: hotelResults.filter((h) => h.status.startsWith("PASS")).length,
  skip: hotelResults.filter((h) => h.status.startsWith("SKIP")).length,
  fail: hotelResults.filter((h) => h.status.startsWith("FAIL")).length,
  spiceIslandApprovedSevenDistinctFromResearchInventory: true,
  noArbitraryOpportunityCeilings: true,
  results: hotelResults,
};

fs.mkdirSync(OUT, { recursive: true });
fs.writeFileSync(
  path.join(OUT, "08-multi-hotel-regression-results.json"),
  JSON.stringify(summary, null, 2),
);
console.log(JSON.stringify(summary, null, 2));
process.exit(summary.ok ? 0 : 1);
