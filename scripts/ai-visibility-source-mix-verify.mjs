#!/usr/bin/env node
/**
 * Read-only: SOURCE_MIX for Marriott / CALA / OpenAI / EN.
 * No provider calls. No evidence mutation.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { createBrandAiVisibilityReadStore } from "../lib/ai-visibility/storage/index.js";
import {
  parseGeographyQuery,
  findMatchingSummaries,
} from "../lib/ai-visibility/brand-read-service.js";
import { loadObservationsFromBatchSummary } from "../lib/ai-visibility/cohort-observations.js";
import { buildSourceExecutivePanel } from "../lib/ai-visibility/citation-intelligence.js";
import { getShowcasePortfolioBrandIds } from "../lib/ai-visibility/brand-ai-showcase-companies.js";
import { resolvePortfolioOwnedDomains } from "../lib/ai-visibility/brand-website-wiring.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const outPath = path.join(
  __dirname,
  "../data/ai-visibility/runtime/source-mix-cala-openai-en.json"
);

async function main() {
  const store = createBrandAiVisibilityReadStore({});
  const geo = parseGeographyQuery({ geography: "CALA" });
  const summaries = await findMatchingSummaries(store, geo, "openai", {
    language: "en",
  });
  if (!summaries.length) {
    throw new Error("No CALA/OpenAI/EN summaries found");
  }
  const latest = summaries[0];
  const { observations } = await loadObservationsFromBatchSummary(store, latest, {
    matchedSlotKeys: latest._matchedSlotKeys,
  });

  const portfolio = getShowcasePortfolioBrandIds("marriott");
  const brandIds = portfolio?.brandIds || [];
  const portfolioOwned = resolvePortfolioOwnedDomains({ brandIds });
  const owned = portfolioOwned.ownedDomainEntries?.length
    ? portfolioOwned.ownedDomainEntries
    : portfolioOwned.ownedDomainList || [];

  const panel = buildSourceExecutivePanel(observations, { ownedDomains: owned });
  const mix = panel.SOURCE_MIX || {};
  const report = {
    PROVIDER: "openai",
    GEOGRAPHY: "CALA",
    LANGUAGE: "en",
    SOURCE_MIX: {
      SUCCESSFUL_COMPARABLE_RESPONSES: mix.SUCCESSFUL_COMPARABLE_RESPONSES,
      OWNED_ONLY_N: mix.OWNED_ONLY_N,
      OWNED_ONLY_RATE: mix.OWNED_ONLY_RATE?.display ?? null,
      MIXED_SOURCES_N: mix.MIXED_SOURCES_N,
      MIXED_SOURCES_RATE: mix.MIXED_SOURCES_RATE?.display ?? null,
      EXTERNAL_ONLY_N: mix.EXTERNAL_ONLY_N,
      EXTERNAL_ONLY_RATE: mix.EXTERNAL_ONLY_RATE?.display ?? null,
      NO_CITATIONS_N: mix.NO_CITATIONS_N,
      NO_CITATIONS_RATE: mix.NO_CITATIONS_RATE?.display ?? null,
      MUTUALLY_EXCLUSIVE: mix.MUTUALLY_EXCLUSIVE,
      SUMS_TO_DENOMINATOR: mix.SUMS_TO_DENOMINATOR,
    },
    SOURCE_MIX_INTERPRETATION: panel.SOURCE_MIX_INTERPRETATION,
    CITATION_RATE: panel.CITATION_RATE?.display ?? null,
    RESPONSES_WITH_CITATIONS: panel.RESPONSES_WITH_CITATIONS,
    OWNED_SOURCE_CITATION_RATE: panel.OWNED_SOURCE_CITATION_RATE?.display ?? null,
    EXTERNAL_SOURCE_CITATION_RATE:
      panel.EXTERNAL_SOURCE_CITATION_RATE?.display ?? null,
    TOP_OWNED_DOMAIN: panel.TOP_OWNED_DOMAIN,
    TOP_EXTERNAL_DOMAIN: panel.TOP_EXTERNAL_DOMAIN,
  };

  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2), "utf8");
  console.log(JSON.stringify(report, null, 2));
  console.error(`Wrote ${outPath}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
