#!/usr/bin/env node
/**
 * Batch build + verify market specs (first pass).
 */
import { execSync } from "child_process";
import { ALL_MARKET_BUILD_SPECS } from "../lib/radar-buildout/tier1-territories-manifest.js";

const root = process.cwd();
const slugs = ALL_MARKET_BUILD_SPECS.map((s) => s.slug);

for (const slug of slugs) {
  console.log("\n=== BUILD", slug, "===");
  execSync(`npm run build:${slug}-fixtures`, { cwd: root, stdio: "inherit" });
  execSync(`npm run build:${slug}-ti-fixtures`, { cwd: root, stdio: "inherit" });
}

for (const slug of slugs) {
  console.log("\n=== VERIFY", slug, "===");
  try {
    execSync(`npm run verify:${slug}-google`, { cwd: root, stdio: "inherit" });
  } catch {
    console.error("Verify failed:", slug);
  }
}

console.log("\nBatch build+verify done.");
