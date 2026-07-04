/**
 * Compare Brand Setup - Brand Basics (Choice CHI) vs Dealality reference folder.
 * node scripts/audit-choice-basics-vs-reference.mjs
 */
import fs from "fs";
import path from "path";
import { spawnSync } from "child_process";
import { fileURLToPath } from "url";
import "../load-env.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const REF_ROOT =
  process.env.CHOICE_BRAND_REFERENCE_ROOT ||
  "G:\\My Drive\\Dealality™\\Platform Design & Build\\Brand Reference Material\\Choice Hotels International";
const EXPORT = path.join(ROOT, "fixtures", "choice-basics-audit-export.json");
const PY = path.join(__dirname, "lib", "extract-pdf-text.py");
const OUT_MD = path.join(ROOT, "docs", "choice-brand-basics-reference-audit.md");

/** Reference primary doc per Airtable brand name */
const REF_MAP = {
  "Ascend Hotel Collection": [
    "ASC_OnePager_2024_PRINT.pdf",
    "brochure--ascend.pdf",
    "Ascend Collection/Ascend Fact Sheet.pdf",
  ],
  "Cambria Hotels": ["Cambria/Cambria Pitch Deck Final.pdf"],
  "Country Inn & Suites by Radisson (Choice)": [
    "Country Inn & Suites/Country Inn & Suites Prototype Overview (4-Page).pdf",
  ],
  "Everhome Suites": ["Everhome Suites/Everhome Suites Entitlement Guide.pdf"],
  "MainStay Suites": ["Choice_ExtendedStay_MainStay_Development.pdf"],
  "Park Inn by Radisson (Choice)": [
    "Park Inn by Radisson/Canada_Choice_ParkInn_Development Presentation.pdf",
  ],
  "Radisson (Choice)": ["Radisson/Radisson One Pager 2025.pdf", "RAD_OneSheet_Final.pdf"],
  "Radisson Blu (Choice)": ["RADBLU_OnePager_New_Final.pdf", "brochure--blu.pdf"],
  "Radisson Collection": ["Radisson Collection/Brand Book - Radisson Collection - 2019 - v2.pdf"],
  "Radisson Individual (Choice)": ["RADIN_PitchDeck_PPT_New_Final.pdf"],
  "Radisson Inn & Suites": ["Radisson Inn & Suites/Radisson Inn Messaging FINAL.pdf"],
  "Sleep Inn": ["Sleep Inn/Sleep Inn - Yorkton Draft Presentation 1.pdf", "brochure--sleep-inn.pdf"],
  "WoodSpring Suites": ["Woodspring Suites/WoodSpring Entitlement Guide.pdf"],
  "Comfort Inn & Suites": ["brochure--comfort-inn.pdf"],
  "Quality Inn": ["brochure--quality-inn.pdf"],
};

const SIGNAL_PATTERNS = [
  { key: "tagline", re: /(?:tagline|selling line|essence)[:\s]+([^\n]{4,80})/gi },
  { key: "century young", re: /century young/gi },
  { key: "approachable indulgence", re: /approachable indulgence/gi },
  { key: "rested set go", re: /rested\.?\s*set\.?\s*go/gi },
  { key: "get your money", re: /get your money'?s worth/gi },
  { key: "modern comfort", re: /modern comfort/gi },
  { key: "inviting real connections", re: /inviting real connections/gi },
  { key: "explorers welcome", re: /explorers welcome/gi },
  { key: "let the destination", re: /let the destination reach you/gi },
  { key: "travel longer", re: /travel longer/gi },
  { key: "upper upscale", re: /upper[- ]upscale/gi },
  { key: "upper-midscale", re: /upper[- ]midscale/gi },
  { key: "midscale", re: /\bmidscale\b/gi },
  { key: "select-service", re: /select[- ]service/gi },
  { key: "soft brand", re: /soft brand/gi },
];

function extractPdf(relPath) {
  const full = path.join(REF_ROOT, relPath);
  if (!fs.existsSync(full)) return "";
  const r = spawnSync("python", [PY, full], {
    encoding: "utf8",
    maxBuffer: 25 * 1024 * 1024,
    env: { ...process.env, PYTHONIOENCODING: "utf-8" },
  });
  if (r.status !== 0) return "";
  return r.stdout || "";
}

function norm(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[™®©]/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

function includesPhrase(haystack, phrase) {
  return norm(haystack).includes(norm(phrase));
}

function auditBrand(brand, airtableRow, refText) {
  const issues = [];
  const at = airtableRow;
  const refN = norm(refText);

  const checks = [
    {
      field: "Brand Tagline",
      refMust: [
        { name: "century young", test: () => refN.includes("century young") },
        { name: "approachable indulgence", test: () => refN.includes("approachable indulgence") },
        { name: "rested set go", test: () => refN.includes("rested") && refN.includes("set") && refN.includes("go") },
        { name: "modern comfort", test: () => refN.includes("modern comfort") },
        { name: "get your money", test: () => refN.includes("money") && refN.includes("worth") },
        { name: "travel longer", test: () => refN.includes("travel longer") },
        { name: "let destination", test: () => refN.includes("destination reach") },
      ],
    },
  ];

  for (const c of checks) {
    const match = c.refMust.find((m) => m.test());
    if (match && at[c.field] && !includesPhrase(at[c.field], match.name.replace(/ /g, ""))) {
      const tag = at[c.field];
      if (match.name === "century young" && !includesPhrase(tag, "century")) {
        issues.push({
          severity: "high",
          field: c.field,
          msg: `Reference uses "A Century Young" but Airtable has "${tag}"`,
        });
      }
      if (match.name === "approachable indulgence" && !includesPhrase(tag, "approachable")) {
        issues.push({
          severity: "high",
          field: c.field,
          msg: `Reference tagline theme is "Approachable Indulgence" but Airtable has "${tag}"`,
        });
      }
      if (match.name === "modern comfort" && !includesPhrase(tag, "modern comfort")) {
        issues.push({
          severity: "high",
          field: c.field,
          msg: `Reference selling line is "Modern Comfort" but Airtable has "${tag}"`,
        });
      }
    }
  }

  // Chain scale hints from reference
  const scaleHints = [];
  if (refN.includes("upper upscale") || refN.includes("upper-upscale")) scaleHints.push("Upper Upscale");
  if (refN.includes("upper-midscale") || refN.includes("upper midscale")) scaleHints.push("Upper Midscale");
  if (refN.match(/\bmidscale\b/) && !refN.includes("upper-midscale")) scaleHints.push("Midscale");
  if (refN.includes("upscale") && !refN.includes("upper upscale")) scaleHints.push("Upscale");

  const atScale = at["Hotel Chain Scale"];
  if (scaleHints.length && atScale) {
    const primary = scaleHints[0];
    if (atScale !== primary && !scaleHints.includes(atScale)) {
      issues.push({
        severity: "medium",
        field: "Hotel Chain Scale",
        msg: `Reference suggests ${scaleHints.join(" / ")}; Airtable has "${atScale}"`,
      });
    }
  } else if (scaleHints.length && !atScale) {
    issues.push({
      severity: "medium",
      field: "Hotel Chain Scale",
      msg: `Empty in Airtable; reference suggests ${scaleHints.join(" / ")}`,
    });
  }

  // Pillar / essence mismatches (Radisson Inn)
  if (refN.includes("naturally grounded") && at["Brand Pillars"]) {
    const p = norm(at["Brand Pillars"]);
    if (!p.includes("naturally grounded")) {
      issues.push({
        severity: "high",
        field: "Brand Pillars",
        msg: 'Reference pillars include "Naturally Grounded, Community Centered, Heartfelt hospitality" — verify Airtable pillars match official messaging doc',
      });
    }
  }

  if (refN.includes("characterful encounters") && at["Brand Pillars"]) {
    const p = norm(at["Brand Pillars"]);
    if (!p.includes("characterful")) {
      issues.push({
        severity: "high",
        field: "Brand Pillars",
        msg: 'Reference uses "Characterful Encounters, Vivid Settings, Explorer\'s Compass" — verify Radisson Individual pillars',
      });
    }
  }

  // Short history may omit reference facts
  if (at["Brand History"] && String(at["Brand History"]).length < 150 && refText.length > 5000) {
    issues.push({
      severity: "low",
      field: "Brand History",
      msg: `Airtable history is very short (${String(at["Brand History"]).length} chars); reference has richer timeline — consider expanding within field limit`,
    });
  }

  // Generic sustainability
  if (
    at["Sustainability Positioning"] &&
    includesPhrase(at["Sustainability Positioning"], "room to be green") &&
    refN.includes("energy-efficient") &&
    !includesPhrase(at["Sustainability Positioning"], "energy")
  ) {
    const brandSpecific = /everhome|woodspring|cambria/i.test(refText.slice(0, 500));
    if (brandSpecific) {
      issues.push({
        severity: "low",
        field: "Sustainability Positioning",
        msg: "Reference mentions property-level energy-efficient design; Airtable only has corporate Room to be Green boilerplate",
      });
    }
  }

  if (!refText || refText.length < 200) {
    issues.push({
      severity: "info",
      field: "(reference)",
      msg: "Little or no extractable text from primary PDF (may be image-only)",
    });
  }

  return issues;
}

const brands = JSON.parse(fs.readFileSync(EXPORT, "utf8"));
const lines = [
  "# Choice Hotels — Brand Basics vs Reference Material Audit",
  "",
  `Generated: ${new Date().toISOString().slice(0, 10)}`,
  "",
  `Reference root: \`${REF_ROOT}\``,
  "",
  "Scope: **Brand Setup - Brand Basics** only, Parent Company = Choice Hotels International (22 brands).",
  "",
];

const noRef = [];
const withIssues = [];
const clean = [];

for (const row of brands) {
  const name = row["Brand Name"];
  const refs = REF_MAP[name];
  if (!refs) {
    noRef.push(name);
    continue;
  }
  let refText = "";
  for (const rel of refs) {
    const t = extractPdf(rel);
    if (t.length > refText.length) refText = t;
  }
  const issues = auditBrand(name, row, refText);
  if (issues.filter((i) => i.severity !== "info").length) {
    withIssues.push({ name, issues, refs });
  } else if (issues.some((i) => i.severity === "info")) {
    withIssues.push({ name, issues, refs });
  } else {
    clean.push(name);
  }
}

lines.push("## Summary", "");
lines.push(`| Metric | Count |`);
lines.push(`|--------|------:|`);
lines.push(`| Brands in Airtable (CHI) | ${brands.length} |`);
lines.push(`| With mapped reference PDFs | ${brands.length - noRef.length} |`);
lines.push(`| No reference folder/PDF mapped | ${noRef.length} |`);
lines.push(`| Flagged for review | ${withIssues.length} |`);
lines.push(`| No automated mismatches | ${clean.length} |`);
lines.push("");

lines.push("## Brands without reference material in folder", "");
lines.push("These CHI brands have **no dedicated PDF** in the Dealality reference folder (may rely on press kits / web only):", "");
for (const n of noRef) lines.push(`- ${n}`);
lines.push("");

lines.push("## Recommended updates (by priority)", "");

const high = withIssues.filter((b) => b.issues.some((i) => i.severity === "high"));
const med = withIssues.filter((b) => b.issues.some((i) => i.severity === "medium"));
const low = withIssues.filter((b) => b.issues.some((i) => i.severity === "low"));

function section(title, items) {
  lines.push(`### ${title}`, "");
  if (!items.length) {
    lines.push("_None._", "");
    return;
  }
  for (const b of items) {
    lines.push(`#### ${b.name}`, "");
    lines.push(`Reference: ${b.refs.map((r) => `\`${r}\``).join(", ")}`, "");
    for (const i of b.issues) {
      lines.push(`- **${i.severity}** — \`${i.field}\`: ${i.msg}`);
    }
    lines.push("");
  }
}

section("High — likely inaccurate vs brand team docs", high);
section("Medium — metadata / tier alignment", med);
section("Low — enrichment opportunities", low);

lines.push("## Brands with reference material and no automated flags", "");
for (const n of clean) lines.push(`- ${n}`);
lines.push("");

lines.push("## Manual review notes", "");
lines.push(
  [
    "1. **Clarion Pointe** tagline in Airtable is synthesized (\"Essentials, Elevated\"); reference folder has no Pointe PDF — validate with brand team.",
    "2. **Radisson Individual** tagline \"Explorers Welcome\" aligns with choicehotels.com; confirm vs RADIN pitch deck hero line.",
    "3. **Brand History** fields are truncated (~170 char Airtable limit) — many omit dates/stats from reference decks.",
    "4. **Target Guest Segments** are multi-select summaries; reference decks have demographic bullets that live better in Guest Psychographics.",
    "5. Compare **Radisson (Choice)** vs **Radisson** Alpha Brand Studios row if both exist — this audit is CHI parent only.",
    "6. **CHI Brands Architecture Oct 2025** and **Canada Extended Stays immersion** PDFs are portfolio-level — use for chain scale / architecture fields, not per-brand taglines.",
  ].join("\n")
);
lines.push("");

fs.mkdirSync(path.dirname(OUT_MD), { recursive: true });
fs.writeFileSync(OUT_MD, lines.join("\n"), "utf8");
console.log(`Wrote ${OUT_MD}`);
console.log(`High: ${high.length}, Medium: ${med.length}, Low: ${low.length}, No ref: ${noRef.length}`);
