#!/usr/bin/env node
/**
 * SEC EDGAR search coverage matrix + strategy evaluation.
 *
 *   node scripts/sec-edgar-search-coverage-eval-v2.mjs
 *   node scripts/sec-edgar-search-coverage-eval-v2.mjs --build-eligible --companies diamondrock-hospitality,marriott-international,hilton-worldwide,sunstone-hotel-investors,host-hotels-resorts,ashford-hospitality-trust
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";

import {
  buildSearchCoverageMatrix,
  coverageMatrixToMarkdown,
} from "../lib/hotel-intelligence/sec-edgar/search-coverage-matrix-v2.js";
import {
  listSearchFamilies,
  SEARCH_TAXONOMY_VERSION,
  allTaxonomyPatterns,
  contractExtractorKeywordPatterns,
} from "../lib/hotel-intelligence/sec-edgar/search-taxonomy-v2.js";
import {
  evaluateAgainstFixture,
  buildCoverageAccounting,
} from "../lib/hotel-intelligence/sec-edgar/search-strategy-eval-v2.js";
import {
  buildEligibleFilingInventory,
  saveEligibleFilingInventory,
  defaultEligibleInventoryDir,
} from "../lib/hotel-intelligence/sec-edgar/eligible-filing-inventory-v2.js";
import {
  seedAdaptiveQueue,
  proposeRelatedCompany,
  applyEnqueue,
  proposeFromHpcHotel,
  saveAdaptiveQueue,
  loadAdaptiveQueue,
  defaultQueuePath,
  listQueuedCompanies,
} from "../lib/hotel-intelligence/sec-edgar/adaptive-discovery-queue-v2.js";
import {
  listHistoricalResearchCompanies,
  listPilotCompanies,
  normalizeCik,
} from "../lib/hotel-intelligence/sec-edgar/cik-registry-v2.js";
import { createSecEdgarClient } from "../lib/hotel-intelligence/sec-edgar/edgar-client-v1.js";
import { loadDiscoveryInventory, inventorySummary, defaultInventoryRoot } from "../lib/hotel-intelligence/sec-edgar/discovery-inventory-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
for (const p of [path.join(ROOT, ".env"), "C:/Dev/dealality/my-operators-backend/.env"]) {
  if (fs.existsSync(p)) {
    dotenv.config({ path: p, override: false });
    break;
  }
}

const BUILD_ELIGIBLE = process.argv.includes("--build-eligible");
const companyArg = (() => {
  const i = process.argv.indexOf("--companies");
  if (i >= 0 && process.argv[i + 1]) {
    return process.argv[i + 1].split(",").map((s) => s.trim()).filter(Boolean);
  }
  return [];
})();

const OUT = path.join(ROOT, "reports", "sec-edgar-search-coverage-v2");
fs.mkdirSync(OUT, { recursive: true });

function loadDiamondRockFixture() {
  const p = path.join(ROOT, "fixtures", "sec-edgar", "diamondrock-franchise-expiration-schedule-v2.txt");
  return fs.readFileSync(p, "utf8");
}

async function main() {
  const matrix = buildSearchCoverageMatrix();
  fs.writeFileSync(path.join(OUT, "SEARCH_COVERAGE_MATRIX.json"), JSON.stringify(matrix, null, 2));
  fs.writeFileSync(path.join(OUT, "SEARCH_COVERAGE_MATRIX.md"), coverageMatrixToMarkdown(matrix));

  const taxonomy = {
    version: SEARCH_TAXONOMY_VERSION,
    families: listSearchFamilies(),
    patternCount: allTaxonomyPatterns().length,
    legacyContractKeywords: contractExtractorKeywordPatterns().length,
  };
  fs.writeFileSync(path.join(OUT, "SEARCH_TAXONOMY.json"), JSON.stringify(taxonomy, null, 2));

  const fixtureText = loadDiamondRockFixture();
  const fixtureEval = evaluateAgainstFixture(
    {
      text: fixtureText,
      expectedHotels: [
        "Courtyard Denver Downtown",
        "Westin San Diego",
        "Hilton Garden Inn Boston Downtown",
        "Residence Inn Atlanta Midtown",
        "Embassy Suites Chicago Downtown",
        "Marriott Chicago Downtown Magnificent Mile",
        "Hampton Inn Seattle Downtown",
        "Courtyard Boston Downtown",
        "DoubleTree by Hilton Bethesda",
      ],
      expectedExpirations: [
        { hotel: "Courtyard Denver Downtown", expiration: "2028" },
        { hotel: "Westin San Diego", expiration: "2030" },
        { hotel: "Hilton Garden Inn Boston Downtown", expiration: "2029" },
        { hotel: "DoubleTree by Hilton Bethesda", expiration: "2025" },
      ],
    },
    {
      cik: "0001298946",
      companyName: "DiamondRock Hospitality Company",
      form: "10-K",
      filingDate: "2023-02-28",
      documentUrl: "https://www.sec.gov/Archives/edgar/data/fixture/drh.htm",
    },
  );
  fs.writeFileSync(path.join(OUT, "STRATEGY_EVAL_DIAMONDROCK_FIXTURE.json"), JSON.stringify(fixtureEval, null, 2));

  // Adaptive queue demo: seed pilot + V2 priority companies; propose from HPC reverse + verified owner
  let queue = loadAdaptiveQueue(defaultQueuePath(ROOT));
  const seeds = [
    ...listPilotCompanies({ includeUnverifiedCik: true }),
    ...listHistoricalResearchCompanies({ maxPriority: 2 }),
  ];
  // dedupe seeds by slug
  const bySlug = new Map();
  for (const c of seeds) bySlug.set(c.slug, c);
  queue = seedAdaptiveQueue(queue, [...bySlug.values()]);
  const hpcProps = proposeFromHpcHotel(
    {
      id: "recLuxvwwxID7U2B8",
      name: "Bethesda Marriott",
      operator: "Marriott International, Inc.",
      owner: "Host Hotels & Resorts, Inc.",
    },
    { verified: true },
  );
  for (const p of hpcProps) {
    queue = applyEnqueue(queue, proposeRelatedCompany(queue, { ...p, depth: 1 }));
  }
  // Reject unverified expansion
  queue = applyEnqueue(
    queue,
    proposeRelatedCompany(queue, {
      legalName: "Random Unverified LLC",
      verified: false,
      depth: 1,
    }),
  );
  saveAdaptiveQueue(queue, defaultQueuePath(ROOT));
  fs.writeFileSync(
    path.join(OUT, "ADAPTIVE_QUEUE_SNAPSHOT.json"),
    JSON.stringify(
      {
        queued: listQueuedCompanies(queue).length,
        sample: listQueuedCompanies(queue).slice(0, 15),
        rejectedTail: (queue.rejected || []).slice(-5),
        caps: queue.caps,
      },
      null,
      2,
    ),
  );

  let eligibleSummary = [];
  if (BUILD_ELIGIBLE) {
    const client = createSecEdgarClient();
    const slugs =
      companyArg.length > 0
        ? companyArg
        : [
            "diamondrock-hospitality",
            "marriott-international",
            "hilton-worldwide",
            "sunstone-hotel-investors",
            "host-hotels-resorts",
            "ashford-hospitality-trust",
          ];
    const companies = listHistoricalResearchCompanies({ slugs });
    for (const co of companies) {
      let cik = normalizeCik(co.cik);
      if (co.ticker) {
        const live = await client.resolveCikByTicker(co.ticker);
        if (live?.cik) cik = live.cik;
      }
      console.error(`[eligible] ${co.slug}…`);
      const inv = await buildEligibleFilingInventory(client, { ...co, cik }, { sinceYear: 1994 });
      if (inv.ok) {
        saveEligibleFilingInventory(inv, defaultEligibleInventoryDir(ROOT));
        eligibleSummary.push({
          slug: co.slug,
          cik,
          ...inv.coverage,
          exhibitGapNote: inv.exhibitGapNote,
        });
      } else {
        eligibleSummary.push({ slug: co.slug, error: inv.error });
      }
    }
    fs.writeFileSync(
      path.join(OUT, "ELIGIBLE_FILING_INVENTORY_SUMMARY.json"),
      JSON.stringify({ generatedAt: new Date().toISOString(), companies: eligibleSummary }, null, 2),
    );
  } else if (fs.existsSync(path.join(defaultEligibleInventoryDir(ROOT), "_index.json"))) {
    eligibleSummary = Object.entries(
      JSON.parse(fs.readFileSync(path.join(defaultEligibleInventoryDir(ROOT), "_index.json"), "utf8"))
        .companies || {},
    ).map(([slug, v]) => ({ slug, ...(v.coverage || {}) }));
  }

  const inventory = loadDiscoveryInventory(defaultInventoryRoot(ROOT));
  const invSum = inventorySummary(inventory);
  const eligibleTotal = eligibleSummary.reduce((n, c) => n + (c.eligibleFound || 0), 0);
  // Processed from checkpoint if present
  let processed = 0;
  const cpPath = path.join(ROOT, "data", "sec-edgar", "checkpoints", "marriott-hilton-historical-v2.json");
  if (fs.existsSync(cpPath)) {
    try {
      processed = Object.keys(JSON.parse(fs.readFileSync(cpPath, "utf8")).processedDocuments || {}).length;
    } catch {
      processed = 0;
    }
  }

  const accounting = buildCoverageAccounting({
    eligibleFilings: eligibleTotal || null,
    processedFilings: processed,
    newUniqueHotels: invSum.totalHotels,
    verifiedContractDates: inventory.hotels.filter((h) =>
      (h.contractFindings || []).some((f) =>
        String(f.verificationClass || "").startsWith("VERIFIED_"),
      ),
    ).length,
    verifiedOwnersOperators: 0,
    paidSpendUsd: 0,
    falsePositiveRate: fixtureEval.falsePositiveHotelCount
      ? fixtureEval.falsePositiveHotelCount / Math.max(1, fixtureEval.hotelsDiscovered)
      : null,
    duplicateRate: fixtureEval.duplicateHotelNames
      ? fixtureEval.duplicateHotelNames / Math.max(1, fixtureEval.hotelsDiscovered)
      : null,
  });

  const report = {
    generatedAt: new Date().toISOString(),
    coverageMatrixSummary: matrix.summary,
    taxonomy,
    fixtureEvaluation: fixtureEval,
    eligibleFilingSummary: eligibleSummary,
    durableInventory: invSum,
    coverageAccounting: accounting,
    hpcBackfillOpportunities: {
      matchedInInventory: invSum.matchedHpc,
      unmatchedRetained: invSum.notInHpc,
      note: "Unmatched hotels retained for future HPC reconciliation; no auto HPC create.",
    },
    researchSpendUsd: 0,
    remainingGaps: matrix.rows.filter((r) => r.status === "PARTIAL" || r.status === "MISSING").map((r) => ({
      area: r.area,
      status: r.status,
      gap: r.gap,
    })),
  };

  fs.writeFileSync(path.join(OUT, "COVERAGE_EVAL_REPORT.json"), JSON.stringify(report, null, 2));
  fs.writeFileSync(
    path.join(OUT, "README.md"),
    `# SEC EDGAR Search Coverage & Adaptive Discovery V2

**Generated:** ${report.generatedAt}

## Coverage matrix
${Object.entries(matrix.summary)
  .map(([k, v]) => `- **${k}:** ${v}`)
  .join("\n")}

See \`SEARCH_COVERAGE_MATRIX.md\`.

## Taxonomy
- Version: \`${taxonomy.version}\`
- Families: ${taxonomy.families.length}
- Patterns: ${taxonomy.patternCount}
- Legacy contract keywords (audit baseline): ${taxonomy.legacyContractKeywords}

## DiamondRock fixture eval
- Hotel recall: **${fixtureEval.recallHotels}**
- Expiration recall: **${fixtureEval.recallExpirations}**
- Hotels discovered: ${fixtureEval.hotelsDiscovered}
- False-positive hotel count: ${fixtureEval.falsePositiveHotelCount}

## Coverage accounting
- Eligible filings (if inventory built): ${accounting.eligibleFilings}
- Processed (checkpoint): ${accounting.processedFilings}
- Remaining: ${accounting.remainingFilings}
- Durable hotels: ${invSum.totalHotels}

## Build eligible inventories
\`\`\`bash
npm run sec-edgar-search-coverage:eval -- --build-eligible --companies diamondrock-hospitality,marriott-international,hilton-worldwide,sunstone-hotel-investors,host-hotels-resorts,ashford-hospitality-trust
\`\`\`
`,
  );

  console.log(
    JSON.stringify(
      {
        ok: true,
        outDir: OUT,
        matrix: matrix.summary,
        fixtureRecallHotels: fixtureEval.recallHotels,
        fixtureRecallExpirations: fixtureEval.recallExpirations,
        eligibleCompanies: eligibleSummary.length,
        inventoryHotels: invSum.totalHotels,
        accounting,
      },
      null,
      2,
    ),
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
