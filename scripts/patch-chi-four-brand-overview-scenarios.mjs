/**
 * Patch overview.scenario.* Body (and combined overview.scenarios) for four CHI stub brands.
 * Title and Image attachments are not modified.
 *
 *   node scripts/patch-chi-four-brand-overview-scenarios.mjs --dry-run
 *   node scripts/patch-chi-four-brand-overview-scenarios.mjs
 */
import "../load-env.js";
import Airtable from "airtable";
import { resolveProfileForAirtableName } from "./lib/choice-chi-brand-resolve.mjs";
import { stubScenarioBodiesForProfile } from "./lib/choice-chi-stub-scenarios.mjs";

const TABLE = "Brand Setup - Brand Explorer Presentation";
const TARGET_BRANDS = [
  "Park Plaza by Choice",
  "Radisson Collection by Choice",
  "Radisson Inn & Suites",
  "WoodSpring Suites",
];
const SCENARIO_TITLES = [
  "Conversion & repositioning",
  "CALA / gateway growth",
  "Portfolio standardization",
];

function parseArgs(argv) {
  return { dryRun: argv.includes("--dry-run") };
}

async function selectRowsForBrand(base, brandName) {
  const esc = brandName.replace(/"/g, '\\"');
  const merged = [];
  const seen = new Set();
  const pushAll = (rows) => {
    for (const r of rows) {
      if (!seen.has(r.id)) {
        seen.add(r.id);
        merged.push(r);
      }
    }
  };
  try {
    pushAll(
      await base(TABLE)
        .select({
          filterByFormula: `AND(OR({Brand Name} = "${esc}", {Brand} = "${esc}"), OR({Slot Key} = "overview.scenarios", FIND("overview.scenario.", {Slot Key}) = 1))`,
          maxRecords: 20,
        })
        .all()
    );
  } catch {
    pushAll(
      await base(TABLE)
        .select({
          filterByFormula: `{Brand} = "${esc}"`,
          maxRecords: 500,
        })
        .all()
    );
    return merged.filter((r) => {
      const sk = String(r.get("Slot Key") || "").trim();
      return sk === "overview.scenarios" || sk.startsWith("overview.scenario.");
    });
  }
  return merged;
}

async function main() {
  const { dryRun } = parseArgs(process.argv);
  const key = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!key || !baseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");
  const base = new Airtable({ apiKey: key }).base(baseId);

  let total = 0;
  for (const brandName of TARGET_BRANDS) {
    const profile = resolveProfileForAirtableName(brandName);
    const bodies = stubScenarioBodiesForProfile(profile.name);
    if (!bodies?.length) {
      console.warn(`Skip ${brandName}: no stub scenario bodies for profile "${profile.name}"`);
      continue;
    }

    const records = await selectRowsForBrand(base, brandName);
    const bySlot = new Map();
    for (const r of records) {
      bySlot.set(String(r.get("Slot Key") || "").trim(), r);
    }

    const updates = [];
    for (let i = 0; i < 3; i++) {
      const slot = `overview.scenario.${i + 1}`;
      const rec = bySlot.get(slot);
      if (!rec) {
        console.warn(`  ${brandName}: missing row ${slot}`);
        continue;
      }
      const body = bodies[i] || "";
      const before = String(rec.get("Body") || "").trim();
      if (before === body.trim()) continue;
      updates.push({
        id: rec.id,
        slot,
        title: SCENARIO_TITLES[i],
        before: before.slice(0, 72),
        fields: { Body: body },
      });
    }

    const combined = bodies.join("\n\n");
    const scenAll = bySlot.get("overview.scenarios");
    if (scenAll && String(scenAll.get("Body") || "").trim() !== combined.trim()) {
      updates.push({
        id: scenAll.id,
        slot: "overview.scenarios",
        title: "",
        before: String(scenAll.get("Body") || "").slice(0, 72),
        fields: { Body: combined },
      });
    }

    if (!updates.length) {
      console.log(`${brandName}: already up to date`);
      continue;
    }

    console.log(`\n${brandName} (${updates.length} body update(s))`);
    for (const u of updates) {
      console.log(`  ${u.slot}${u.title ? ` · ${u.title}` : ""}`);
      console.log(`    was: ${u.before}…`);
      console.log(`    now: ${String(u.fields.Body).slice(0, 72)}…`);
      if (!dryRun) await base(TABLE).update(u.id, u.fields);
      total++;
    }
  }

  console.log(`\n${dryRun ? "Would update" : "Updated"} ${total} row(s).`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
