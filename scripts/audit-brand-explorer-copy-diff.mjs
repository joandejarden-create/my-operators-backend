/**
 * Compare presentation copy between two brands — flag slots still matching source.
 *
 *   node scripts/audit-brand-explorer-copy-diff.mjs --from "Radisson (Choice)" --to "Radisson Blu (Choice)"
 */
import "../load-env.js";
import Airtable from "airtable";

const TABLE = "Brand Setup - Brand Explorer Presentation";

function parseArgs() {
  let from = "";
  let to = "";
  for (let i = 2; i < process.argv.length; i++) {
    if (process.argv[i] === "--from" && process.argv[i + 1]) from = process.argv[++i];
    else if (process.argv[i] === "--to" && process.argv[i + 1]) to = process.argv[++i];
  }
  if (!from || !to) throw new Error("Require --from and --to");
  return { from, to };
}

function norm(s) {
  return String(s || "")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();
}

function bluTransform(s) {
  return norm(s)
    .replace(/radisson blu blu/g, "radisson blu")
    .replace(/radisson —/g, "radisson blu —")
    .replace(/radisson /g, "radisson blu ")
    .replace(/upscale/g, "upper-upscale")
    .replace(/a century young/g, "think in black & white blu")
    .replace(/charming simplicity/g, "enticing moments")
    .replace(/contemporary classic/g, "nordic nouveau")
    .replace(/gracious hospitality/g, "curatorial warmth");
}

async function loadBySlot(base, brandName) {
  const esc = brandName.replace(/"/g, '\\"');
  const rows = await base(TABLE)
    .select({ filterByFormula: `{Brand Name} = "${esc}"`, maxRecords: 500 })
    .all();
  const map = new Map();
  for (const r of rows) {
    const sk = String(r.get("Slot Key") || "").trim();
    if (!sk) continue;
    const body = String(r.get("Body") || "");
    const title = String(r.get("Title") || "");
    map.set(sk, { id: r.id, body, title, combined: norm(title) + "||" + norm(body) });
  }
  return map;
}

async function main() {
  const { from, to } = parseArgs();
  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);
  const fromMap = await loadBySlot(base, from);
  const toMap = await loadBySlot(base, to);

  const identical = [];
  const nearIdentical = [];
  const onlyFrom = [];
  const onlyTo = [];
  const choiceCoreHints = [];

  for (const [sk, f] of fromMap) {
    if (!toMap.has(sk)) {
      onlyFrom.push(sk);
      continue;
    }
    const t = toMap.get(sk);
    if (f.combined === t.combined) identical.push(sk);
    else if (bluTransform(f.combined) === norm(t.title) + "||" + norm(t.body)) nearIdentical.push(sk);
    const blob = (t.body + " " + t.title).toLowerCase();
    if (
      /\ba century young\b/.test(blob) ||
      /\bcharming simplicity\b/.test(blob) ||
      /\bcontemporary classic\b/.test(blob) ||
      /\bgracious hospitality\b/.test(blob) ||
      /\bkit-of-parts\b/.test(blob) ||
      (/\bcore radisson\b/.test(blob) && !/\bupper-upscale\b/.test(blob.slice(0, 80))) ||
      /\bpanama\b.*\bcore radisson\b/.test(blob) ||
      /\bradisson riviera panama\b/.test(blob) ||
      /\bparamaribo\b/.test(blob) && !/blu/.test(blob)
    ) {
      choiceCoreHints.push(sk);
    }
  }
  for (const sk of toMap.keys()) {
    if (!fromMap.has(sk)) onlyTo.push(sk);
  }

  console.log(`\n=== ${from} → ${to} ===`);
  console.log(`From slots: ${fromMap.size}  To slots: ${toMap.size}`);
  console.log(`Identical body+title: ${identical.length}`);
  console.log(`Near-identical (naive Blu transform): ${nearIdentical.length}`);
  console.log(`Still has core-Radisson / old DNA phrases: ${choiceCoreHints.length}`);
  if (identical.length) {
    console.log("\nIdentical slot keys (sample 25):");
    identical.slice(0, 25).forEach((k) => console.log("  ", k));
    if (identical.length > 25) console.log(`  … +${identical.length - 25} more`);
  }
  if (choiceCoreHints.length) {
    console.log("\nChoice/core-Radisson phrasing still on Blu (sample 20):");
    choiceCoreHints.slice(0, 20).forEach((k) => console.log("  ", k));
  }
  if (onlyTo.length) console.log(`\nOnly on ${to}: ${onlyTo.length} slots`);
  if (onlyFrom.length) console.log(`Only on ${from}: ${onlyFrom.length} slots`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
