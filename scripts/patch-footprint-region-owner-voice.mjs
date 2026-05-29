import "../load-env.js";
import Airtable from "airtable";

const TABLE = "Brand Setup - Brand Explorer Presentation";

const BODY_BY_SLOT = {
  "footprint.region.am":
    "Americas\n\nThe Americas are the core strength zone for Choice-affiliated brands, with established operating support, distribution reach, and owner familiarity. This region positions the brand as a practical, growth-ready option in high-volume franchise corridors.",
  "footprint.region.cala":
    "CALA\n\nCALA positions the brand around gateway-city and resort momentum, with strong relevance for owners targeting mixed business-leisure demand. Brand presence in this region reinforces international credibility while supporting locally tailored commercial strategies.",
  "footprint.region.eu":
    "Europe\n\nEurope strengthens the brand’s international profile through design-forward, globally recognizable positioning. Regional visibility here supports premium perception and helps owners present a stronger global brand story to investors and guests.",
  "footprint.region.mea":
    "MEA\n\nMEA expands the brand’s global footprint in high-visibility markets and supports international traveler recognition. Presence in this region enhances portfolio depth and reinforces long-term global positioning for growth-oriented owners.",
  "footprint.region.apac":
    "APAC\n\nAPAC gives the brand strategic exposure in dynamic travel markets and adds global relevance with internationally minded guests. This regional positioning supports a forward-growth narrative and strengthens cross-market brand visibility.",
};

function parseArgs(argv) {
  return {
    dryRun: argv.includes("--dry-run"),
  };
}

function esc(v) {
  return String(v || "").replace(/"/g, '\\"');
}

async function main() {
  const { dryRun } = parseArgs(process.argv);
  const key = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!key || !baseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");

  const base = new Airtable({ apiKey: key }).base(baseId);
  const slotFormula = `OR(${Object.keys(BODY_BY_SLOT)
    .map((k) => `{Slot Key} = "${esc(k)}"`)
    .join(",")})`;

  const rows = await base(TABLE).select({ filterByFormula: slotFormula, maxRecords: 2000 }).all();
  console.log(`${dryRun ? "[dry-run] " : ""}Found ${rows.length} region row(s).`);

  let updated = 0;
  for (const r of rows) {
    const slotKey = String(r.get("Slot Key") || "").trim();
    const brand = String(r.get("Brand Name") || "").trim();
    const nextBody = BODY_BY_SLOT[slotKey];
    if (!nextBody) continue;
    const current = String(r.get("Body") || "");
    if (current.trim() === nextBody.trim()) continue;
    console.log(`- ${brand}: ${slotKey}`);
    if (!dryRun) {
      await base(TABLE).update(r.id, { Body: nextBody });
    }
    updated += 1;
  }

  console.log(`${dryRun ? "Would update" : "Updated"} ${updated} row(s).`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

