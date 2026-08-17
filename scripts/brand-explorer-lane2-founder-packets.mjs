#!/usr/bin/env node
import "dotenv/config";
import { runLane2FounderPackets } from "../lib/partner-intelligence/brand-explorer-lane2-founder-packets.js";
import { resolveFullBuildSlug } from "../lib/partner-intelligence/brand-explorer-full-build-content.js";

const brandsIdx = process.argv.indexOf("--brands");
const raw =
  brandsIdx >= 0 && process.argv[brandsIdx + 1]
    ? process.argv[brandsIdx + 1].split(",").map((s) => s.trim()).filter(Boolean)
    : [
        "autograph",
        "handwritten",
        "radisson-collection",
        "tapestry",
        "vignette",
      ];

const brands = raw.map((b) => resolveFullBuildSlug(b));
const result = await runLane2FounderPackets({ brands });
console.log(
  `[lane2-founder-packets] wrote summary for ${result.brands.length} brands; restore=false release=false`
);
for (const [slug, rec] of Object.entries(result.recommendations)) {
  console.log(`  ${slug}: ${rec}`);
}
