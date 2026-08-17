/**
 * Patch Everhome Suites "Where This Brand Creates the Most Value" slots in Airtable.
 * Updates Title + Body in place (preserves Image attachments on scenario cards).
 *
 *   node scripts/patch-everhome-overview-value-scenarios.mjs --dry-run
 *   node scripts/patch-everhome-overview-value-scenarios.mjs
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import "../load-env.js";
import Airtable from "airtable";
import { sanitizeExternalCopy } from "../lib/external-owner-copy.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const TABLE = "Brand Setup - Brand Explorer Presentation";
const BRAND_NAME = "Everhome Suites";
const FIXTURE = path.join(
  ROOT,
  "fixtures/brand-explorer-presentation-everhome-value-scenarios.json"
);

const SLOT_KEYS = [
  "overview.scenarios",
  "overview.scenario.1",
  "overview.scenario.2",
  "overview.scenario.3",
  "hero.benefit_zones",
];

function parseArgs(argv) {
  return { dryRun: argv.includes("--dry-run") };
}

function sanitize(value) {
  return sanitizeExternalCopy(String(value ?? "").trim());
}

async function selectRows(base) {
  const esc = BRAND_NAME.replace(/"/g, '\\"');
  const slotOr = SLOT_KEYS.map((k) => `{Slot Key} = "${k}"`).join(", ");
  return base(TABLE)
    .select({
      filterByFormula: `AND(OR({Brand Name} = "${esc}", {Brand} = "${esc}"), OR(${slotOr}))`,
      maxRecords: 20,
    })
    .all();
}

async function main() {
  const { dryRun } = parseArgs(process.argv);
  const key = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!key || !baseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");

  const data = JSON.parse(fs.readFileSync(FIXTURE, "utf8"));
  const bySlot = new Map(
    (data.rows || []).map((r) => [String(r.slotKey || "").trim(), r])
  );

  const base = new Airtable({ apiKey: key }).base(baseId);
  const records = await selectRows(base);
  const existing = new Map(
    records.map((r) => [String(r.get("Slot Key") || "").trim(), r])
  );

  const updates = [];
  for (const slotKey of SLOT_KEYS) {
    const fixture = bySlot.get(slotKey);
    if (!fixture) continue;
    const rec = existing.get(slotKey);
    if (!rec) {
      console.warn(`Missing Airtable row: ${slotKey}`);
      continue;
    }
    const nextTitle = sanitize(fixture.title);
    const nextBody = sanitize(fixture.body);
    const fields = {};
    const curTitle = String(rec.get("Title") || "").trim();
    const curBody = String(rec.get("Body") || "").trim();
    if (curTitle !== nextTitle) fields.Title = nextTitle;
    if (curBody !== nextBody) fields.Body = nextBody;
    if (!Object.keys(fields).length) {
      console.log(`${slotKey}: already up to date`);
      continue;
    }
    updates.push({ id: rec.id, slotKey, fields, curTitle, curBody, nextTitle, nextBody });
  }

  if (!updates.length) {
    console.log("Nothing to update.");
    return;
  }

  console.log(`${BRAND_NAME}: ${updates.length} row(s) to patch`);
  for (const u of updates) {
    console.log(`\n  ${u.slotKey}`);
    if (u.fields.Title !== undefined) {
      console.log(`    title: ${u.curTitle || "(empty)"} → ${u.nextTitle || "(empty)"}`);
    }
    if (u.fields.Body !== undefined) {
      console.log(`    body was: ${u.curBody.slice(0, 80)}…`);
      console.log(`    body now: ${u.nextBody.slice(0, 80)}…`);
    }
  }

  if (dryRun) {
    console.log("\nDry run — no writes.");
    return;
  }

  for (let i = 0; i < updates.length; i += 10) {
    const chunk = updates.slice(i, i + 10).map((u) => ({ id: u.id, fields: u.fields }));
    await base(TABLE).update(chunk);
  }
  console.log(`\nUpdated ${updates.length} row(s).`);
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
