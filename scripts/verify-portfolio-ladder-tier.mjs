/**
 * Verify portfolio ladder tier resolution for a CHI brand (API + slot).
 *   node scripts/verify-portfolio-ladder-tier.mjs "Ascend Hotel Collection"
 */
import "../load-env.js";
import Airtable from "airtable";
import { resolvePortfolioLadderTier, PORTFOLIO_LADDER_TIER_LABELS } from "../lib/brand-explorer-portfolio-ladder.mjs";

const brandName = process.argv[2] || "Ascend Hotel Collection";
const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID
);

const basics = await base("Brand Setup - Brand Basics")
  .select({ filterByFormula: `{Brand Name} = "${brandName.replace(/"/g, '\\"')}"`, maxRecords: 1 })
  .all();
if (!basics.length) {
  console.error("Brand not found:", brandName);
  process.exit(1);
}

const pres = await base("Brand Setup - Brand Explorer Presentation")
  .select({ filterByFormula: `{Brand Name} = "${brandName.replace(/"/g, '\\"')}"`, maxRecords: 500 })
  .all();

const blocks = pres
  .filter((r) => {
    const active = r.get("Active");
    return !(
      active === false ||
      String(active).toLowerCase() === "no" ||
      String(active).toLowerCase() === "false" ||
      active === 0
    );
  })
  .map((r) => ({
    slotKey: String(r.get("Slot Key") || "").trim(),
    body: String(r.get("Body") || "").trim(),
    title: String(r.get("Title") || "").trim(),
  }));

const brand = {
  name: brandName,
  hotelChainScale: basics[0].get("Hotel Chain Scale"),
  brandExplorer: { version: 1, blocks },
};

const tier = resolvePortfolioLadderTier(brand);
console.log(`${brandName}`);
console.log(`  Hotel Chain Scale: ${brand.hotelChainScale}`);
const ctx = blocks.find((b) => b.slotKey === "overview.portfolio_context");
const legacy = blocks.find((b) => b.slotKey === "overview.portfolio_ladder_tier");
console.log(
  `  overview.portfolio_context: title=${ctx?.title ?? "(missing)"} body=${ctx?.body ? `${ctx.body.slice(0, 60)}…` : "(missing)"}`
);
if (legacy) console.log(`  (legacy overview.portfolio_ladder_tier body: ${legacy.body})`);
console.log(`  Resolved tier: ${tier} → ${PORTFOLIO_LADDER_TIER_LABELS[tier]}`);
console.log(`  Expected UI active column: ${tier + 1} of 4 (0-based index ${tier})`);
