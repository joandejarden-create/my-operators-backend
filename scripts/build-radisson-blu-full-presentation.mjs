/**
 * Build complete Blu-only presentation fixture (dedicated Blu files win; no Choice hotel leakage).
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import "../load-env.js";
import Airtable from "airtable";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(ROOT, "fixtures/brand-explorer-presentation-radisson-blu-full.json");
const BRAND = "Radisson Blu (Choice)";
const CHOICE_NAME = "Radisson (Choice)";

const CALA_HOTELS =
  "Radisson Blu Bariloche, Radisson Blu Aruba, Radisson Blu Belo Horizonte Savassi, Radisson Blu São Paulo, and Radisson Blu Plaza El Bosque Santiago";

/** Entire slot families owned by these fixtures (Choice rows ignored). */
const DEDICATED_FIXTURES = [
  "brand-explorer-presentation-economics-radisson-blu.json",
  "brand-explorer-presentation-standards-radisson-blu.json",
  "brand-explorer-presentation-radisson-blu-materials.json",
  "brand-explorer-presentation-radisson-blu-case-studies.json",
  "brand-explorer-presentation-radisson-blu-gallery.json",
  "brand-explorer-presentation-radisson-blu-footprint-openings.json",
  "brand-explorer-presentation-radisson-blu-footprint-momentum.json",
  "brand-explorer-presentation-radisson-blu-footprint-geo-growth.json",
  "brand-explorer-presentation-radisson-blu-footprint.json",
  "brand-explorer-presentation-radisson-blu-portfolio-compliance-similar.json",
  "brand-explorer-presentation-radisson-blu.example.json",
];

const OWNED_PREFIXES = [
  "economics.",
  "standards.",
  "materials.",
  "footprint.",
  "insight.similar",
  "operations.compliance.",
  "hero.",
  "overview.",
  "operations.",
  "valueOwners.",
  "loyalty.",
  "insight.",
];

function bluTransform(s) {
  if (!s) return s;
  return (
    s
      .replace(/Radisson Blu Blu/gi, "Radisson Blu")
      .replace(/Radisson —/g, "Radisson Blu —")
      .replace(/\bRadisson /g, "Radisson Blu ")
      .replace(/kit-of-parts/gi, "design-forward prototype")
      .replace(/A Century Young/gi, "Think in Black & White Blu")
      .replace(/Charming simplicity/gi, "Enticing Moments")
      .replace(/Contemporary classic/gi, "Nordic Nouveau")
      .replace(/Gracious hospitality/gi, "Curatorial Warmth")
      .replace(/upscale /gi, "upper-upscale ")
      .replace(/upscale\b/gi, "upper-upscale")
      .replace(/core Radisson/gi, "core Radisson (sibling flag)")
  );
}

function slotOverride(sk, title, body) {
  const t = title || "";
  const rules = [
    { m: () => sk === "operations.model.primary_model", b: "Franchise and management (Americas / upper-upscale prototype-dependent)" },
    { m: () => sk === "operations.model.management_option", b: "Third-party management common; owner-operated where operator has upper-upscale F&B and meetings depth" },
    { m: () => sk === "operations.model.typical_ownership", b: "Institutional sponsors and experienced owners funding Nordic Nouveau casegoods and public-space investment" },
    { m: () => sk === "operations.model.brand_involvement", b: "Active on design approval, opening milestones, and upper-upscale QA" },
    { m: () => sk === "operations.model.systems_integration", b: "Choice CRS/PMS stack and brand reporting for Choice-affiliated Blu" },
    { m: () => sk === "operations.model.pre_opening", b: "Structured pre-opening with design-forward prototype review and upper-upscale OS&E" },
    { m: () => sk === "operations.model.staffing_intensity", b: "Moderate–high—gallery-curator service and signature F&B" },
    { m: () => sk === "operations.model.fb_complexity", b: "Moderate–high—restaurant, bar, or marketplace per prototype" },
    { m: () => sk === "operations.model.training", b: "Upper-upscale service rituals and Choice University before stabilization" },
    { m: () => sk === "operations.model.reporting_discipline", b: "Monthly financial and quality reporting for upper-upscale KPIs" },
    { m: () => sk === "operations.model.qa_rhythm", b: "Recurring assessments for Nordic Nouveau and Curatorial Warmth" },
    { m: () => sk === "operations.model.technology", b: "Integrated PMS/CRS and mobile journeys for Inspired Professional guests" },
    { m: () => sk === "operations.operator_compat.tags", b: "Upper-upscale operators\nDesign-forward F&B\nMeetings & groups\nCALA Blu experience\nChoice portfolio operators" },
    { m: () => sk.startsWith("valueOwners.lifecycle."), b: (() => {
      const n = sk.split(".").pop();
      const map = {
        1: `Screen CALA gateway/resort fit—use ${CALA_HOTELS} as directional proof only.`,
        2: "Align Nordic Nouveau scope, meetings/F&B, and upper-upscale PIP with Alpha Brand Studios.",
        3: "Pre-opening: gallery-curator training and Inspired Professional campaign readiness.",
        4: "Opening: protect rate integrity and Blu service calibration in first 90–120 days.",
        5: "Ramp: loyalty and seasonal compression for urban vs resort Blu assets.",
        6: "Ongoing: upper-upscale capex and contribution-focused benchmarking.",
      };
      return map[n] || bluTransform(body);
    })() },
  ];
  for (const r of rules) {
    if (r.m()) return r.b;
  }
  return bluTransform(body);
}

function isOwnedByDedicated(sk) {
  return OWNED_PREFIXES.some((p) => sk === p.replace(/\.$/, "") || sk.startsWith(p));
}

function loadDedicatedRows() {
  const rows = [];
  for (const f of DEDICATED_FIXTURES) {
    const p = path.join(ROOT, "fixtures", f);
    if (!fs.existsSync(p)) continue;
    const data = JSON.parse(fs.readFileSync(p, "utf8"));
    for (const r of data.rows || []) {
      rows.push({
        slotKey: r.slotKey,
        title: r.title ?? "",
        body: r.body ?? "",
        sort: typeof r.sort === "number" ? r.sort : 0,
        caseSummaryOverview: r.caseSummaryOverview,
        caseSummaryOwnerObjective: r.caseSummaryOwnerObjective,
        caseSummaryBrandRelevance: r.caseSummaryBrandRelevance,
        caseSummaryInterpretation: r.caseSummaryInterpretation,
        caseSummaryTags: r.caseSummaryTags,
      });
    }
  }
  return rows;
}

function dedupeRows(rows) {
  const map = new Map();
  for (const r of rows) {
    const key = `${r.slotKey}\0${String(r.title || "").trim()}`;
    map.set(key, r);
  }
  return [...map.values()];
}

async function main() {
  const rows = dedupeRows(loadDedicatedRows());

  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);
  const esc = CHOICE_NAME.replace(/"/g, '\\"');
  const choiceRows = await base("Brand Setup - Brand Explorer Presentation")
    .select({ filterByFormula: `{Brand Name} = "${esc}"`, maxRecords: 500 })
    .all();

  const have = new Set(rows.map((r) => `${r.slotKey}\0${String(r.title || "").trim()}`));

  for (const rec of choiceRows) {
    const sk = String(rec.get("Slot Key") || "").trim();
    if (!sk || isOwnedByDedicated(sk)) continue;
    const title = String(rec.get("Title") ?? "");
    const key = `${sk}\0${title.trim()}`;
    if (have.has(key)) continue;
    have.add(key);
    rows.push({
      slotKey: sk,
      title,
      body: slotOverride(sk, title.trim(), String(rec.get("Body") || "")),
      sort: Number(rec.get("Sort Order")) || 0,
    });
  }

  const out = {
    targetBrandBasicsName: BRAND,
    brandNameFallback: BRAND,
    instructions:
      "Full Blu-specific presentation. Apply: node scripts/apply-brand-explorer-presentation-fixture.mjs --brand-record-id recWPEvxBQxVVzSq3 --fixture fixtures/brand-explorer-presentation-radisson-blu-full.json --replace",
    rows: dedupeRows(rows),
  };

  fs.writeFileSync(OUT, JSON.stringify(out, null, 2) + "\n");
  console.log(`Wrote ${out.rows.length} rows (${loadDedicatedRows().length} from dedicated Blu fixtures).`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
