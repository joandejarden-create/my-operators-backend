#!/usr/bin/env node
import "dotenv/config";
import { runLane2FounderPackets } from "../lib/partner-intelligence/brand-explorer-lane2-founder-packets.js";
import { resolveFullBuildSlug } from "../lib/partner-intelligence/brand-explorer-full-build-content.js";
import { runLane2PostDraftIntegrity } from "../lib/partner-intelligence/brand-explorer-lane2-post-draft-integrity.js";

const brands = [
  "autograph-collection",
  "handwritten-collection",
  "radisson-collection",
  "tapestry-collection-by-hilton",
  "vignette-collection",
].map((b) => resolveFullBuildSlug(b));

const integrity = await runLane2PostDraftIntegrity({ brands });
console.log(
  `[integrity] pass=${integrity.summary?.passCount}/${integrity.summary?.brandCount}`
);

const gateNotesBySlug = Object.fromEntries(
  brands.map((slug) => [
    slug,
    {
      tabFactoryPass: false,
      tabFactoryCaution: true,
      tabLevel: {
        tabFactory: "fail — residual empty/thin fields (see brand-explorer-tab-factory-audit.md)",
        imageUniqueness: "pass",
        imageRoleMatch: "pass",
        renderedCompleteness: "fail — residual thin fields under internal/external audit",
        sectionPatternParity: "unit contracts pass; live brand parity not fully green",
        sourceProvenance: "see provenance report if generated",
      },
      sourceProvenance:
        "Official property CDNs (Marriott / Accor ahstatic / Radisson media / Hilton im / IHG Scene7). Autograph Airtable ingest used wsrv.nl fetch proxy for Marriott CDN reliability; image bytes remain Marriott official photography.",
      tabFactoryPass: false,
    },
  ])
);

const packets = await runLane2FounderPackets({ brands, gateNotesBySlug });
console.log("[founder-packets] recommendations:");
for (const [slug, rec] of Object.entries(packets.recommendations)) {
  console.log(`  ${slug}: ${rec}`);
}
