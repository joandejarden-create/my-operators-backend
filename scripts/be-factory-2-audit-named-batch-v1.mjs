#!/usr/bin/env node
/**
 * Audit live Presentation coverage for a named Factory 2.0 batch.
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { join } from "path";
import { fetchPresentationBlocksForBrand } from "../lib/brand-explorer/be-presentation-writeback-apply-v1.js";

const ROOT = process.cwd();
const BATCH = [
  { brandId: "rec9aZp7GHtzUEg0c", brandName: "AC Hotels by Marriott" },
  { brandId: "recahVIW4aCx0Ao84", brandName: "Moxy Hotels" },
  { brandId: "recDwzv86TWnz2gGB", brandName: "Vignette Collection" },
  { brandId: "recsggfbKlJbjeRP9", brandName: "Canopy by Hilton" },
  { brandId: "recvvmiyReHhiKdoK", brandName: "EVEN Hotels" },
];

function summarize(blocks) {
  const byPrefix = {};
  const slots = new Set();
  let loyaltyProof = 0;
  let gallery = 0;
  let feeBuckets = 0;
  for (const b of blocks || []) {
    const k = String(b.slotKey || "").trim();
    if (!k) continue;
    slots.add(k);
    const pref = k.split(".")[0];
    byPrefix[pref] = (byPrefix[pref] || 0) + 1;
    if (k === "loyalty.proof" && String(b.body || "").trim().length >= 24) loyaltyProof += 1;
    if (/^materials\.gallery\.\d+$/.test(k) && (b.imageUrl || /^https?:/i.test(String(b.body || ""))))
      gallery += 1;
    if (/^economics\.fee/.test(k)) feeBuckets += 1;
  }
  return {
    blockCount: (blocks || []).length,
    uniqueSlots: slots.size,
    byPrefix,
    loyaltyProofSubstantive: loyaltyProof,
    galleryFilled: gallery,
    economicsFeeSlots: feeBuckets,
    hasCommercialIntro: slots.has("commercial.intro"),
    hasLoyaltyProgram: slots.has("loyalty.program"),
    hasWhyValue: slots.has("overview.why_value"),
  };
}

async function main() {
  const rows = [];
  for (const b of BATCH) {
    const live = await fetchPresentationBlocksForBrand(b.brandId, b.brandName);
    rows.push({
      ...b,
      liveBlockCount: live.blocks.length,
      audit: summarize(live.blocks),
    });
    console.error(`audited ${b.brandName}: ${live.blocks.length} blocks`);
  }
  const out = {
    generatedAt: new Date().toISOString(),
    brands: rows,
  };
  const dir = join(ROOT, "reports/brand-explorer/factory-2-0");
  mkdirSync(dir, { recursive: true });
  writeFileSync(join(dir, "11-batch-five-live-audit.json"), JSON.stringify(out, null, 2) + "\n");
  console.log(JSON.stringify(out, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
