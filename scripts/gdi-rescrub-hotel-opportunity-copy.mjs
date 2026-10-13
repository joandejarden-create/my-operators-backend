/**
 * Re-apply qualification + rad enrichment for a hotel bag to scrub
 * hotel-specific template bleed (generic repair — no hotel-name switches).
 *
 *   node scripts/gdi-rescrub-hotel-opportunity-copy.mjs --hotel recG66DQJKP2c0UNh
 *   node scripts/gdi-rescrub-hotel-opportunity-copy.mjs --hotel recG66DQJKP2c0UNh --apply
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { applyQualificationPrecisionPass } from "../lib/group-demand-intelligence/qualification-precision-pass.js";
import { applyRadFeedbackEnrichmentPass } from "../lib/group-demand-intelligence/rad-feedback-enrichment.js";
import {
  loadOpportunitiesCanonical,
  saveOpportunitiesCanonical,
} from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { textHasPilotGeoBleed } from "../lib/group-demand-intelligence/research-coverage/portable-seed-templates.js";

const APPLY = process.argv.includes("--apply");
function argVal(flag) {
  const i = process.argv.indexOf(flag);
  return i >= 0 ? process.argv[i + 1] : null;
}

async function main() {
  const hotelId = argVal("--hotel");
  if (!hotelId) throw new Error("--hotel required");
  const bag = await loadOpportunitiesCanonical(hotelId);
  const before = Array.isArray(bag) ? bag : bag?.opportunities || [];
  let bleedBefore = 0;
  for (const o of before) {
    if (/\bbethesda marriott\b/i.test(JSON.stringify(o))) bleedBefore += 1;
  }

  const qp = applyQualificationPrecisionPass(before);
  const rad = applyRadFeedbackEnrichmentPass(qp.opportunities, hotelId);
  const after = rad.opportunities;

  let bleedAfter = 0;
  const remaining = [];
  for (const o of after) {
    const blob = JSON.stringify(o);
    if (/\bbethesda marriott\b/i.test(blob) || textHasPilotGeoBleed(o.recommendedAction || "")) {
      bleedAfter += 1;
      remaining.push({ id: o.id, action: (o.recommendedAction || "").slice(0, 160) });
    }
  }

  if (APPLY) {
    await saveOpportunitiesCanonical(hotelId, {
      hotelId,
      opportunities: after,
      updatedAt: new Date().toISOString(),
      source: "gdi_rescrub_hotel_opportunity_copy",
    });
  }

  const __dirname = path.dirname(fileURLToPath(import.meta.url));
  const outDir = path.join(
    __dirname,
    "../reports/group-demand-intelligence/second-hotel-replication-v1"
  );
  fs.mkdirSync(outDir, { recursive: true });
  const report = {
    hotelId,
    apply: APPLY,
    persistence: bag?.persistence || bag?.source || null,
    beforeCount: before.length,
    afterCount: after.length,
    bethesdaMarriottBleedBefore: bleedBefore,
    bethesdaMarriottBleedAfter: bleedAfter,
    remainingSample: remaining.slice(0, 5),
    sampleActions: after
      .filter((o) => o.opportunityType === "PRIMARY_PURSUIT")
      .slice(0, 3)
      .map((o) => ({ id: o.id, recommendedAction: (o.recommendedAction || "").slice(0, 180) })),
  };
  fs.writeFileSync(
    path.join(outDir, `RESCRUB_${hotelId}${APPLY ? "" : "_DRY"}.json`),
    JSON.stringify(report, null, 2)
  );
  console.log(JSON.stringify(report, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
