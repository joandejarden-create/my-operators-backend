/**
 * Rebuild Kimpton loyalty.* presentation slots from IHG One Rewards tier benefits
 * (official ihg.com page) + Kimpton brochure stats — then apply to Airtable.
 *
 *   node scripts/apply-kimpton-loyalty-presentation.mjs
 *   node scripts/apply-kimpton-loyalty-presentation.mjs --apply
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { spawnSync } from "child_process";
import "../load-env.js";
import { PILOT_BRANDS } from "../api/lib/partner-intelligence-explorer-field-registry.js";
import { applyIhgLoyaltyPresentationSlots } from "../lib/partner-intelligence/build-ihg-loyalty-presentation-slots.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const APPLY = process.argv.includes("--apply");
const PILOT = PILOT_BRANDS.kimptonHotels;
const REPORT_PATH = path.join(ROOT, "reports", "kimpton-brand-source-pipeline.json");
const TEMPLATE_PATH = path.join(ROOT, "fixtures", "brand-explorer-presentation-kimpton-full.json");
const OUT_PATH = path.join(ROOT, "fixtures", "brand-explorer-presentation-kimpton-loyalty.json");

function loadMergedFacts() {
  if (fs.existsSync(REPORT_PATH)) {
    const report = JSON.parse(fs.readFileSync(REPORT_PATH, "utf8"));
    if (report.mergedSample?.length) {
      return report.mergedSample.map((s) => ({
        fieldKey: s.fieldKey,
        extractedValue: s.value,
        evidenceText: s.evidence,
        pageSectionAnchor: s.source,
        dataGap: "No",
      }));
    }
  }
  return [
    { fieldKey: "be.loyalty.roomContributionPct", extractedValue: "50.8", dataGap: "No" },
    { fieldKey: "be.loyalty.enterpriseBookingPct", extractedValue: "88.5", dataGap: "No" },
    { fieldKey: "be.loyalty.memberCount", extractedValue: "100", dataGap: "No" },
  ];
}

async function main() {
  const template = JSON.parse(fs.readFileSync(TEMPLATE_PATH, "utf8"));
  const mergedFacts = loadMergedFacts();
  const rows = applyIhgLoyaltyPresentationSlots(template.rows, mergedFacts, {
    brandName: PILOT.brandName,
  });

  const loyaltyOnly = rows.filter((r) => r.slotKey && r.slotKey.startsWith("loyalty."));
  const out = {
    targetBrandBasicsName: PILOT.brandName,
    brandNameFallback: PILOT.brandName,
    instructions: "Loyalty slots only — IHG tier benefits source",
    rows: loyaltyOnly,
  };
  fs.writeFileSync(OUT_PATH, JSON.stringify(out, null, 2));
  console.log("Wrote", OUT_PATH, "| loyalty rows:", loyaltyOnly.length);

  if (!APPLY) {
    console.log("Dry run. Pass --apply to push loyalty.* slots to Airtable.");
    return;
  }

  const pres = spawnSync(
    "node",
    [
      path.join(ROOT, "scripts", "apply-brand-explorer-presentation-fixture.mjs"),
      "--brand-name",
      PILOT.brandName,
      "--fixture",
      OUT_PATH,
      "--replace-slot-prefix",
      "loyalty.",
    ],
    { stdio: "inherit", cwd: ROOT, env: process.env }
  );
  process.exit(pres.status || 0);
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
