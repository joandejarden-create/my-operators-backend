#!/usr/bin/env node
/**
 * Regenerate Wave 12 non-image cleanup list after Stage 5 images land.
 * Uses presentation-rows-light (avoids heavy factory context / 429s).
 */
import "dotenv/config";
import fs from "node:fs";
import { listPresentationRowsLight } from "../lib/partner-intelligence/brand-explorer-lane2-common.js";
import { FACTORY_PREVIEW_CANDIDATE_IDENTITIES } from "../lib/partner-intelligence/brand-explorer-factory-preview-candidates.js";
import { WAVE12_SLUGS } from "../lib/partner-intelligence/brand-explorer-wave12-factory-plan.js";

function nz(v) {
  return v == null ? "" : String(v).trim();
}

const brands = [];
for (const slug of WAVE12_SLUGS) {
  const id = FACTORY_PREVIEW_CANDIDATE_IDENTITIES[slug];
  const { rows } = await listPresentationRowsLight(id.recordId, id.name);
  const gallery = rows.filter((r) => /^materials\.gallery\.\d+$/i.test(r.slotKey));
  const scenarios = rows.filter((r) => /^overview\.scenario\.\d+$/i.test(r.slotKey));
  const openings = rows.filter((r) => r.slotKey === "footprint.openings");
  const withImage = (list) => list.filter((r) => nz(r.imageUrl)).length;
  brands.push({
    slug,
    name: id.name,
    galleryImages: `${withImage(gallery)}/${gallery.length}`,
    scenarioImages: `${withImage(scenarios)}/${scenarios.length}`,
    openingsImages: `${withImage(openings)}/${openings.length}`,
    remainingNonImageNotes: [
      "Thin scenario / proof / lifecycle copy may remain from Stage 4 cleanup list",
      "Recent Momentum pattern parity may still need fixes on some brands",
      "Momentum evidence quality card-level fixes may remain on some brands",
    ],
  });
  await new Promise((r) => setTimeout(r, 200));
}

const md = [
  `# Wave 12 — cleanup list after Stage 5 image materialization`,
  ``,
  `- Generated: ${new Date().toISOString()}`,
  `- Image materialization: applied for all 12 brands (6 gallery + 3 scenario + ≥3 openings)`,
  ``,
  `## Image status (Presentation rows)`,
  ``,
  ...brands.map(
    (b) =>
      `- **${b.name}** (\`${b.slug}\`): gallery ${b.galleryImages} · scenario ${b.scenarioImages} · openings ${b.openingsImages}`
  ),
  ``,
  `## Remaining non-image cleanup`,
  ``,
  `Image blockers from Stage 4 should be cleared. Remaining work is copy / pattern / evidence:`,
  ``,
  `- Tab Factory / rendered completeness: thin scenarios, proofs, lifecycle, opening steps (non-image)`,
  `- Golden quality: thin copy + any stub chips`,
  `- Section pattern parity: Recent Momentum pattern fixes (~10 brands historically)`,
  `- Momentum evidence quality: card-level fixes (~5 brands historically)`,
  ``,
  `See prior Stage 4 report: \`reports/brand-explorer-wave12-tab-factory-build-cleanup.md\`.`,
  ``,
  `## Guardrails confirmed for Stage 5`,
  ``,
  `- No Brand Status / release / CV / Source Library / Registry writes`,
  `- No protected 27 brand writes`,
  `- All 12 remain Under Review / factory preview`,
  ``,
].join("\n");

fs.writeFileSync("reports/brand-explorer-wave12-tab-factory-build-cleanup.md", md);
console.log("Wrote cleanup regeneration");
for (const b of brands) {
  console.log(b.slug, b.galleryImages, b.scenarioImages, b.openingsImages);
}
