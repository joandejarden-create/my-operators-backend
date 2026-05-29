import "../load-env.js";
import Airtable from "airtable";

const TABLE = "Brand Setup - Brand Explorer Presentation";
const BASICS = "Brand Setup - Brand Basics";

const REQUIRED_KEYS = [
  "loyalty.kpi.members",
  "loyalty.kpi.hotels",
  "loyalty.kpi.markets",
  "loyalty.kpi.mix",
  "loyalty.hero_title",
  "loyalty.ecosystem",
  "loyalty.owner_lens",
  "loyalty.earn",
  "loyalty.redeem",
  "loyalty.implications.pnl",
  "loyalty.implications.ops",
  "loyalty.implications.systems",
];

async function listChiBrands(base) {
  const rows = await base(BASICS).select({ maxRecords: 500 }).all();
  return rows
    .filter((r) => String(r.get("Parent Company") || "").includes("Choice Hotels International"))
    .map((r) => String(r.get("Brand Name") || "").trim())
    .filter(Boolean)
    .sort((a, b) => a.localeCompare(b));
}

function bySlotKey(rows) {
  const out = new Map();
  for (const r of rows) {
    const k = String(r.get("Slot Key") || "").trim();
    if (!k) continue;
    if (!out.has(k)) out.set(k, []);
    out.get(k).push(r);
  }
  return out;
}

function okFactChecks(rowsByKey) {
  const earn = String(rowsByKey.get("loyalty.earn")?.[0]?.get("Body") || "");
  const redeem = String(rowsByKey.get("loyalty.redeem")?.[0]?.get("Body") || "");
  const eliteRows = rowsByKey.get("loyalty.elite") || [];
  const eliteTitles = eliteRows.map((r) => String(r.get("Title") || "").trim());

  const members = String(rowsByKey.get("loyalty.kpi.members")?.[0]?.get("Body") || "");
  const hotels = String(rowsByKey.get("loyalty.kpi.hotels")?.[0]?.get("Body") || "");
  const markets = String(rowsByKey.get("loyalty.kpi.markets")?.[0]?.get("Body") || "");
  const mix = String(rowsByKey.get("loyalty.kpi.mix")?.[0]?.get("Body") || "");

  return {
    earn10Points: /10 points per \$1/i.test(earn),
    earnGold5: /5 nights|10,000 Elite Qualifying Credits|10k EQCs/i.test(earn),
    redeem8000: /8,000 points/i.test(redeem),
    tiers: ["Member", "Gold", "Platinum", "Diamond", "Titanium"].every((t) =>
      eliteTitles.includes(t)
    ),
    kpiMembers: /70M|70\s*million/i.test(members),
    kpiHotels: /7,100\+|7100\+/i.test(hotels) && !/^0\+/i.test(hotels),
    kpiMarkets: markets.trim().length > 0 && markets !== "—",
    kpiMix: mix.trim().length > 0 && mix !== "—",
  };
}

async function main() {
  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID
  );
  const brands = await listChiBrands(base);
  let failed = 0;

  for (const brandName of brands) {
    const esc = brandName.replace(/"/g, '\\"');
    const rows = await base(TABLE)
      .select({
        filterByFormula: `AND({Brand Name} = "${esc}", FIND("loyalty.", {Slot Key}) = 1)`,
        maxRecords: 200,
      })
      .all();

    const map = bySlotKey(rows);
    const missing = REQUIRED_KEYS.filter((k) => !map.has(k));
    const facts = okFactChecks(map);
    const factPass = Object.values(facts).every(Boolean);

    const ok = missing.length === 0 && factPass;
    if (!ok) failed++;

    console.log(
      `${ok ? "OK  " : "FAIL"} ${brandName} | rows=${rows.length} | missingKeys=${missing.length} | facts=${JSON.stringify(
        facts
      )}`
    );
    if (missing.length) console.log(`  missing: ${missing.join(", ")}`);
  }

  if (failed) process.exit(1);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
