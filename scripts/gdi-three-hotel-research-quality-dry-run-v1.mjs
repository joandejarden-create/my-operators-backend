#!/usr/bin/env node
/**
 * Three-hotel GDI research quality dry-run (Spice / Bethesda / Waterstone).
 * Read-only against local opportunity stores. No paid research, publish, or Airtable writes.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { evaluateHotelOpportunityInventory } from "../lib/group-demand-intelligence/research/gdi-opportunity-research-quality-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-full-e2e-research-foundation-v1"
);

const HOTELS = [
  {
    hotelId: "recKRJjcPnb4tVDDS",
    hotelName: "Spice Island Beach Resort",
  },
  {
    hotelId: "recLuxvwwxID7U2B8",
    hotelName: "Bethesda Marriott",
  },
  {
    hotelId: "recgMYovrrZDJMqzX",
    hotelName: "Waterstone Resort & Marina Boca Raton",
  },
];

function writeJson(p, obj) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, JSON.stringify(obj, null, 2) + "\n");
}

function mdTable(summary) {
  return [
    `| Metric | Count |`,
    `| --- | ---: |`,
    `| Total opportunities | ${summary.total} |`,
    `| Sales Ready | ${summary.SALES_READY || 0} |`,
    `| Account Development | ${summary.ACCOUNT_DEVELOPMENT || 0} |`,
    `| Needs More Research | ${summary.NEEDS_MORE_RESEARCH || 0} |`,
    `| Excluded | ${summary.EXCLUDED || 0} |`,
    `| Credible prospects (SR+AD) | ${summary.credibleProspects} |`,
    `| Verified/named contacts | ${summary.verifiedRelevantContacts} |`,
    `| Credible demand theses | ${summary.credibleDemandTheses} |`,
    `| Hotel-fit theses | ${summary.hotelFitQuality} |`,
    `| Unsupported / flagged | ${summary.unsupportedClaims} |`,
    `| Commercially sufficient research | ${summary.commerciallySufficient} |`,
    `| Duplicate titles | ${summary.duplicateOpportunities} |`,
  ].join("\n");
}

async function main() {
  const hotelReports = [];
  for (const h of HOTELS) {
    const doc = await loadOpportunitiesCanonical(h.hotelId);
    const opps = doc?.opportunities || [];
    const report = evaluateHotelOpportunityInventory(opps, h);
    hotelReports.push(report);
    writeJson(
      path.join(OUT, `hotel-${h.hotelId}-research-quality.json`),
      report
    );
  }

  const portfolio = {
    contract: "GDI_FULL_E2E_RESEARCH_FOUNDATION_READY",
    mode: "dry-run",
    paidResearch: false,
    airtableWrites: false,
    publication: false,
    generatedAt: new Date().toISOString(),
    hotels: hotelReports.map((r) => ({
      hotelId: r.hotelId,
      hotelName: r.hotelName,
      summary: r.summary,
    })),
    portfolioTotals: hotelReports.reduce(
      (acc, r) => {
        for (const [k, v] of Object.entries(r.summary)) {
          if (typeof v === "number") acc[k] = (acc[k] || 0) + v;
        }
        return acc;
      },
      {}
    ),
  };

  writeJson(path.join(OUT, "00-three-hotel-research-quality.json"), portfolio);

  const md = [
    `# GDI three-hotel research quality dry-run`,
    ``,
    `Mode: dry-run · no paid research · no publish · no Airtable writes`,
    ``,
    ...hotelReports.flatMap((r) => [
      `## ${r.hotelName} (\`${r.hotelId}\`)`,
      ``,
      mdTable(r.summary),
      ``,
      `Quality-band note: ${r.summary.qualityOverVolumeNote}`,
      ``,
    ]),
    `## Portfolio totals`,
    ``,
    mdTable(portfolio.portfolioTotals),
    ``,
  ].join("\n");
  fs.writeFileSync(path.join(OUT, "00-three-hotel-research-quality.md"), md);

  console.log(
    JSON.stringify(
      {
        ok: true,
        outDir: OUT,
        hotels: portfolio.hotels,
        portfolioTotals: portfolio.portfolioTotals,
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
