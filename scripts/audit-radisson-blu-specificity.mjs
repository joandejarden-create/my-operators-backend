/**
 * Audit Radisson Blu (Choice) presentation for non-Blu / clone / core-Radisson leakage.
 *
 *   node scripts/audit-radisson-blu-specificity.mjs
 */
import "../load-env.js";
import Airtable from "airtable";

const TABLE = "Brand Setup - Brand Explorer Presentation";
const BLU_ID = "recWPEvxBQxVVzSq3";
const CHOICE_ID = "recywbx1YQSTCPqW1";

const CORE_RADISSON_PHRASES = [
  { re: /\ba century young\b/i, label: "Radisson DNA (not Blu)" },
  { re: /\bcharming simplicity\b/i, label: "Radisson DNA (not Blu)" },
  { re: /\bcontemporary classic\b/i, label: "Radisson DNA (not Blu)" },
  { re: /\bgracious hospitality\b/i, label: "Radisson DNA (not Blu)" },
  { re: /\bkit-of-parts\b/i, label: "core Radisson economics language" },
  { re: /\bradisson riviera panama\b/i, label: "core Radisson hotel (Panama)" },
  { re: /\bradisson puebla\b/i, label: "core Radisson hotel (Puebla)" },
  { re: /\bradisson san luis potos[ií]\b/i, label: "core Radisson hotel (SLP)" },
  { re: /\bparamaribo\b/i, label: "core Radisson hotel (Suriname)" },
  { re: /\bdoubletree by hilton\b/i, label: "Radisson (Choice) similar-brand set" },
  { re: /\bcrowne plaza\b/i, label: "Radisson (Choice) similar-brand set" },
  { re: /\bsheraton\b/i, label: "Radisson (Choice) similar-brand set" },
  { re: /\bchicago\b|\bbloomington\b|\bfargo\b|\bmall of america\b/i, label: "US Midwest Blu (not CALA focus)" },
  { re: /\bradisson blu blu\b/i, label: "typo duplicate Blu" },
];

const BLU_POSITIVE = [
  /radisson blu/i,
  /think in black/i,
  /nordic nouveau/i,
  /enticing moments/i,
  /curatorial warmth/i,
  /inspired professional/i,
  /upper-upscale/i,
  /bariloche|belo horizonte|são paulo|sao paulo|plaza el bosque|palm beach, aruba/i,
];

async function loadRows(base, basicsId, brandName) {
  const escId = basicsId.replace(/"/g, '\\"');
  let rows = [];
  try {
    rows = await base(TABLE)
      .select({ filterByFormula: `FIND("${escId}", ARRAYJOIN({Brand})) > 0`, maxRecords: 500 })
      .all();
  } catch {
    /* */
  }
  if (!rows.length && brandName) {
    const esc = brandName.replace(/"/g, '\\"');
    rows = await base(TABLE)
      .select({ filterByFormula: `{Brand Name} = "${esc}"`, maxRecords: 500 })
      .all();
  }
  return rows;
}

function norm(s) {
  return String(s || "").replace(/\s+/g, " ").trim();
}

async function main() {
  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);
  const bluRows = await loadRows(base, BLU_ID, "Radisson Blu (Choice)");
  const choiceRows = await loadRows(base, CHOICE_ID, "Radisson (Choice)");

  const choiceBySlot = new Map();
  for (const r of choiceRows) {
    const sk = String(r.get("Slot Key") || "").trim();
    if (!sk) continue;
    const title = norm(r.get("Title"));
    const body = norm(r.get("Body"));
    const key = `${sk}\0${title}`;
    if (!choiceBySlot.has(key)) choiceBySlot.set(key, body);
  }

  let identicalToChoice = 0;
  const identicalSamples = [];
  const identicalByArea = {};
  let missingBluSignal = 0;
  const missingBluSamples = [];
  const badPhrases = [];

  for (const r of bluRows) {
    const sk = String(r.get("Slot Key") || "").trim();
    const title = norm(r.get("Title"));
    const body = norm(r.get("Body"));
    const blob = `${title} ${body}`;

    const choiceKey = `${sk}\0${title}`;
    const choiceBody = choiceBySlot.get(choiceKey);
    if (choiceBody && choiceBody === body && body.length > 20) {
      identicalToChoice++;
      const area = sk.includes(".") ? sk.replace(/\.[^.]+$/, "").split(".").slice(0, 2).join(".") : sk;
      identicalByArea[area] = (identicalByArea[area] || 0) + 1;
      if (identicalSamples.length < 20) identicalSamples.push(sk + (title ? ` :: ${title}` : ""));
    }

    if (body.length > 40 && !BLU_POSITIVE.some((re) => re.test(blob))) {
      const skipSlots =
        /^economics\.(checklist|fee\.|kpi\.|legal|negotiable|rarely|opening\.step\.[1345]|risk|term_|performance_|support_|diligence)/.test(
          sk
        ) ||
        /^operations\.(flexibility|model\.)/.test(sk) ||
        sk === "standards.last_reviewed";
      if (!skipSlots) {
        missingBluSignal++;
        if (missingBluSamples.length < 15) missingBluSamples.push(sk);
      }
    }

    for (const { re, label } of CORE_RADISSON_PHRASES) {
      if (re.test(blob)) {
        badPhrases.push({ sk, title, label, snippet: body.slice(0, 100) });
      }
    }
  }

  const slotKeys = new Set(bluRows.map((r) => String(r.get("Slot Key") || "").trim()).filter(Boolean));
  const choiceSlotKeys = new Set(choiceRows.map((r) => String(r.get("Slot Key") || "").trim()).filter(Boolean));
  const onlyChoice = [...choiceSlotKeys].filter((k) => !slotKeys.has(k));
  const onlyBlu = [...slotKeys].filter((k) => !choiceSlotKeys.has(k));

  console.log("\n=== Radisson Blu (Choice) specificity audit ===\n");
  console.log(`Blu rows: ${bluRows.length} | Choice rows: ${choiceRows.length}`);
  console.log(`Blu slot keys: ${slotKeys.size} | Choice slot keys: ${choiceSlotKeys.size}`);
  console.log(`Identical body to Radisson (Choice) (same slot+title): ${identicalToChoice}`);
  if (identicalSamples.length) {
    console.log("  Samples:", identicalSamples.join("; "));
  }
  if (identicalToChoice) {
    console.log("  By area:", Object.entries(identicalByArea).sort((a, b) => b[1] - a[1]).map(([k, v]) => `${k}(${v})`).join(", "));
  }
  console.log(`Differentiated rows (approx): ${bluRows.length - identicalToChoice} of ${bluRows.length}`);
  console.log(`Rows without Blu/CALA signal (heuristic, long copy): ${missingBluSignal}`);
  if (missingBluSamples.length) {
    console.log("  Samples:", missingBluSamples.join(", "));
  }
  console.log(`Bad phrase hits: ${badPhrases.length}`);
  for (const h of badPhrases.slice(0, 15)) {
    console.log(`  [${h.label}] ${h.sk}${h.title ? ` — ${h.title}` : ""}`);
  }
  if (onlyChoice.length) console.log(`\nSlot keys only on Choice (missing on Blu): ${onlyChoice.length}`, onlyChoice.slice(0, 10).join(", "));
  if (onlyBlu.length) console.log(`Slot keys only on Blu: ${onlyBlu.length}`, onlyBlu.slice(0, 10).join(", "));

  const verdict =
    identicalToChoice > 50
      ? "NO — large share still verbatim Radisson (Choice)"
      : identicalToChoice > 15
        ? "PARTIAL — many slots still match Choice copy"
        : badPhrases.length > 5
          ? "PARTIAL — Blu page mostly updated but some wrong phrases remain"
          : "MOSTLY YES — presentation layer is Blu-differentiated; generic economics slots may still read similarly by design";

  console.log(`\nVerdict: ${verdict}\n`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
