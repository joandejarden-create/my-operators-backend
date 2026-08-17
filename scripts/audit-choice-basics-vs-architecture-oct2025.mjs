/**
 * Audit Brand Setup - Brand Basics (CHI) vs CHI Brands Architecture Oct 2025 PDF.
 * node scripts/audit-choice-basics-vs-architecture-oct2025.mjs
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  CHOICE_ARCHITECTURE_NOT_IN_DOC,
  resolveArchForAirtableName,
} from "../lib/choice-brand-architecture-oct2025.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const EXPORT = path.join(ROOT, "fixtures", "choice-basics-audit-export.json");
const ARCH_TXT = path.join(ROOT, "fixtures", "choice-pdf-text", "chi-brands-architecture-oct-2025.txt");
const OUT_MD = path.join(ROOT, "docs", "choice-brand-basics-architecture-oct2025-audit.md");

const NOT_IN_ARCH = CHOICE_ARCHITECTURE_NOT_IN_DOC;

function norm(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[™®©.]/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

function phraseIn(text, phrase) {
  return norm(text).includes(norm(phrase));
}

function compareTagline(at, arch) {
  const tag = at["Brand Tagline"] || "";
  const ideas = [arch.brandIdea, arch.altBrandIdea].filter(Boolean);
  if (!tag) return { ok: false, msg: "Empty tagline in Airtable" };
  if (ideas.some((i) => phraseIn(tag, i))) return { ok: true };
  return {
    ok: false,
    msg: `Architecture Brand Idea: "${arch.brandIdea}"${arch.altBrandIdea ? ` (alt: "${arch.altBrandIdea}")` : ""}; Airtable: "${tag}"`,
  };
}

function compareScale(at, arch) {
  const atScale = at["Hotel Chain Scale"] || "";
  const archTier = arch.tier || "";
  const map = {
    "premium value": ["Economy", "Value"],
    "economy extended stay": ["Economy"],
    "extended stay (midscale positioning in portfolio)": ["Midscale"],
    "extended stay": ["Midscale", "Economy"],
    upscale: ["Upscale", "Upper Upscale"],
    "upper upscale": ["Upper Upscale"],
    "upper midscale": ["Upper Midscale"],
    midscale: ["Midscale", "Upper Midscale"],
    economy: ["Economy"],
  };
  const allowed = [];
  for (const [k, v] of Object.entries(map)) {
    if (norm(archTier).includes(k) || norm(k).includes(norm(archTier))) allowed.push(...v);
  }
  if (!atScale) return { ok: false, severity: "high", msg: `Empty; architecture tier: ${archTier}` };
  if (norm(archTier) === "premium value" && !norm(atScale).includes("economy") && atScale !== "Value") {
    return {
      ok: false,
      severity: "high",
      msg: `Architecture = Premium Value (value/economy lane); Airtable = "${atScale}"`,
      note: arch.scaleNote,
    };
  }
  if (norm(archTier).includes("midscale") && !norm(archTier).includes("upper") && atScale === "Upscale") {
    return {
      ok: false,
      severity: "high",
      msg: `Architecture places brand in Midscale section; Airtable = "${atScale}"`,
      note: arch.scaleNote,
    };
  }
  if (arch.tier === "Upscale" && atScale === "Upper Upscale" && at["Brand Name"]?.includes("RED")) {
    return {
      ok: false,
      severity: "medium",
      msg: `Architecture = Upscale select-service; Airtable = "${atScale}"`,
    };
  }
  const tierNorm = norm(arch.tier);
  if (tierNorm.includes("upper upscale") && atScale !== "Upper Upscale") {
    return { ok: false, severity: "high", msg: `Architecture = Upper Upscale; Airtable = "${atScale}"` };
  }
  if (tierNorm === "upscale" && !["Upscale", "Upper Upscale"].includes(atScale)) {
    return { ok: false, severity: "medium", msg: `Architecture = Upscale; Airtable = "${atScale}"` };
  }
  if (tierNorm.includes("upper midscale") && atScale !== "Upper Midscale") {
    return { ok: false, severity: "high", msg: `Architecture = Upper Midscale; Airtable = "${atScale}"` };
  }
  if (tierNorm === "midscale" && !["Midscale", "Upper Midscale"].includes(atScale)) {
    return { ok: false, severity: "high", msg: `Architecture = Midscale; Airtable = "${atScale}"` };
  }
  return { ok: true };
}

function comparePillars(at, arch) {
  if (!arch.pillars?.length || !at["Brand Pillars"]) return { ok: null };
  const p = norm(at["Brand Pillars"]);
  const hits = arch.pillars.filter((x) => {
    const key = norm(x).split("(")[0].trim().slice(0, 12);
    return p.includes(key) || arch.pillars.some((full) => phraseIn(p, full.split("(")[0]));
  });
  const need = Math.min(2, arch.pillars.length);
  const matched = arch.pillars.filter((pillar) => {
    const stem = norm(pillar).replace(/\(.*\)/, "").trim();
    return stem.length > 4 && p.includes(stem.slice(0, Math.min(15, stem.length)));
  });
  if (matched.length >= need) return { ok: true };
  return {
    ok: false,
    msg: `Architecture pillars/themes: ${arch.pillars.join("; ")} — Airtable pillars do not reflect official framework`,
  };
}

function comparePositioning(at, arch) {
  const pos = at["Brand Positioning"] || "";
  const prop = norm(arch.strategicProposition || "");
  if (!pos || prop.length < 20) return { ok: null };
  const propWords = prop.split(" ").filter((w) => w.length > 5);
  const matchCount = propWords.filter((w) => norm(pos).includes(w)).length;
  if (matchCount >= 2) return { ok: true };
  if (norm(pos).length < 120) {
    return { ok: false, msg: "Positioning is thin stub; architecture has full strategic proposition to import" };
  }
  return {
    ok: false,
    msg: `Positioning does not echo architecture strategic proposition ("${arch.strategicProposition?.slice(0, 80)}…")`,
  };
}

const brands = JSON.parse(fs.readFileSync(EXPORT, "utf8"));
const archRaw = fs.existsSync(ARCH_TXT) ? fs.readFileSync(ARCH_TXT, "utf8") : "";

const lines = [
  "# Choice Hotels — Brand Basics vs CHI Brands Architecture (Oct 2025)",
  "",
  `**Generated:** ${new Date().toISOString().slice(0, 10)}`,
  "**Reference:** `CHI Brands Architecture _ Oct 2025.pdf` (internal brand positioning, Nov 2025)",
  "**Scope:** Brand Setup - Brand Basics · Parent Company = Choice Hotels International (22 brands)",
  "",
  "> This document is **internal positioning** (Brand Idea, strategic proposition, tier guardrails). Consumer-facing taglines in market may differ (e.g. Comfort **Rested. Set. Go.** vs architecture **Where the joy happens.**). Prefer architecture for Deal Capture **Brand Idea / pillars / tier** fields unless product explicitly tracks public campaign lines.",
  "",
  "---",
  "",
  "## Executive summary",
  "",
];

const aligned = [];
const taglineMismatch = [];
const scaleMismatch = [];
const contentGap = [];
const notInDoc = [];

for (const row of brands) {
  const name = row["Brand Name"];
  if (NOT_IN_ARCH.includes(name)) {
    notInDoc.push(name);
    continue;
  }
  const arch = resolveArchForAirtableName(name);
  if (!arch) {
    notInDoc.push(name);
    continue;
  }

  const issues = [];
  const t = compareTagline(row, arch);
  if (!t.ok) issues.push({ sev: "high", field: "Brand Tagline", ...t });

  const s = compareScale(row, arch);
  if (!s.ok) issues.push({ sev: s.severity || "high", field: "Hotel Chain Scale", ...s });

  const pil = comparePillars(row, arch);
  if (pil.ok === false) issues.push({ sev: "high", field: "Brand Pillars", ...pil });

  const pos = comparePositioning(row, arch);
  if (pos.ok === false) issues.push({ sev: "medium", field: "Brand Positioning", ...pos });

  // Service model check
  if (arch.service && row["Hotel Service Model"]) {
    const sm = norm(row["Hotel Service Model"]);
    const want = norm(arch.service);
    if (want.includes("limited") && !sm.includes("select") && !sm.includes("limited")) {
      issues.push({
        sev: "low",
        field: "Hotel Service Model",
        msg: `Architecture service: ${arch.service}; Airtable: ${row["Hotel Service Model"]}`,
      });
    }
    if (name === "Clarion" && sm.includes("full") && want.includes("full")) {
      /* ok */
    } else if (name === "Clarion" && row["Hotel Chain Scale"] === "Upscale") {
      issues.push({
        sev: "high",
        field: "Hotel Chain Scale + Positioning",
        msg: "Clarion is documented under **Midscale** brand architecture (Get Together Here), not Upscale full-service tier",
      });
    }
  }

  if (issues.length === 0) aligned.push(name);
  else {
    for (const i of issues) {
      const bucket = i.field.includes("Tagline")
        ? taglineMismatch
        : i.field.includes("Chain Scale")
          ? scaleMismatch
          : contentGap;
      bucket.push({ name, ...i });
    }
  }
}

lines.push("| Result | Count |");
lines.push("|--------|------:|");
lines.push(`| Brands covered in Oct 2025 architecture deck | ${brands.length - notInDoc.length} |`);
lines.push(`| Brands **not** in architecture deck (WIP / other sources) | ${notInDoc.length} |`);
lines.push(`| Brands with **no automated issues** | ${aligned.length} |`);
lines.push(`| Tagline / Brand Idea mismatches | ${new Set(taglineMismatch.map((x) => x.name)).size} |`);
lines.push(`| Chain scale / tier mismatches | ${new Set(scaleMismatch.map((x) => x.name)).size} |`);
lines.push(`| Positioning / pillars gaps | ${new Set(contentGap.map((x) => x.name)).size} |`);
lines.push("");

lines.push("### Highest-priority corrections vs architecture", "");
const byBrand = new Map();
for (const item of [...taglineMismatch, ...scaleMismatch, ...contentGap]) {
  if (!byBrand.has(item.name)) byBrand.set(item.name, []);
  byBrand.get(item.name).push(`${item.field}: ${item.msg}`);
}
let priorityNum = 1;
if (byBrand.size === 0 && notInDoc.length === 0) {
  lines.push("_No open gaps on automated checks for brands covered in the Oct 2025 deck._");
} else {
  for (const [brand, msgs] of byBrand) {
    lines.push(`${priorityNum}. **${brand}** — ${msgs.join("; ")}`);
    priorityNum++;
  }
  for (const n of notInDoc) {
    lines.push(
      `${priorityNum}. **${n}** — Not in Oct 2025 architecture deck; use brand one-pagers / dev materials (Radisson Individual vs Collection framework needs brand-team confirmation).`
    );
    priorityNum++;
  }
}
lines.push("");

lines.push("---", "", "## Brand-by-brand comparison", "");

function tableRow(label, archVal, atVal, status) {
  const icon = status === "ok" ? "✓" : status === "partial" ? "~" : "✗";
  return `| ${label} | ${archVal} | ${atVal || "—"} | ${icon} |`;
}

for (const row of brands) {
  const name = row["Brand Name"];
  lines.push(`### ${name}`, "");
  if (NOT_IN_ARCH.includes(name)) {
    lines.push(
      "_Not covered in CHI Brands Architecture Oct 2025 (appendix notes Radisson Individual / Cambria targets WIP). Use brand one-pagers, dev site, or prior audit._",
      ""
    );
    continue;
  }
  const arch = resolveArchForAirtableName(name);
  if (!arch) {
    lines.push("_No mapping entry._", "");
    continue;
  }

  lines.push("| Field | Architecture (Oct 2025) | Airtable today | Match |");
  lines.push("|-------|-------------------------|----------------|-------|");

  const t = compareTagline(row, arch);
  lines.push(
    tableRow("Brand Tagline / Idea", arch.brandIdea + (arch.altBrandIdea ? ` / ${arch.altBrandIdea}` : ""), row["Brand Tagline"], t.ok ? "ok" : "no")
  );
  lines.push(tableRow("Hotel Chain Scale", arch.tier, row["Hotel Chain Scale"], compareScale(row, arch).ok ? "ok" : "no"));
  lines.push(tableRow("Service (guardrails)", arch.service || "—", row["Hotel Service Model"], "partial"));
  lines.push(
    tableRow(
      "Strategic proposition (summary)",
      (arch.strategicProposition || "").slice(0, 60) + "…",
      (row["Brand Positioning"] || "").slice(0, 60) + "…",
      comparePositioning(row, arch).ok ? "ok" : comparePositioning(row, arch).ok === false ? "no" : "partial"
    )
  );

  const pil = comparePillars(row, arch);
  lines.push(
    tableRow(
      "Pillars / framework",
      (arch.pillars || []).join(", ").slice(0, 80),
      (row["Brand Pillars"] || "").slice(0, 80) + "…",
      pil.ok ? "ok" : pil.ok === false ? "no" : "partial"
    )
  );

  if (arch.scaleNote) lines.push("", `**Note:** ${arch.scaleNote}`, "");

  const issues = [];
  const checks = [
    compareTagline(row, arch),
    compareScale(row, arch),
    comparePillars(row, arch),
    comparePositioning(row, arch),
  ];
  for (const c of checks) {
    if (c.ok === false && c.msg) issues.push(c.msg);
  }
  if (issues.length) {
    lines.push("", "**Gaps:**");
    for (const m of issues) lines.push(`- ${m}`);
  } else {
    lines.push("", "_Aligned with architecture on automated checks._");
  }
  lines.push("");
}

lines.push("---", "", "## Brands not in Oct 2025 architecture PDF", "");
for (const n of notInDoc) {
  lines.push(`- **${n}**`);
}
lines.push("");
lines.push(
  "Appendix (page 39) states **Cambria and Radisson brand focuses are WIP** and upper-upscale persona views are not final. **Radisson Individual** messaging in Airtable (**Explorers Welcome**, Characterful Encounters pillars) mirrors **Radisson Collection** content in this same deck—confirm with brand team which brand owns that framework post-2025 architecture."
);
lines.push("");

lines.push("---", "", "## Aligned or acceptable variance", "");
for (const n of aligned) lines.push(`- ${n}`);
lines.push("");

lines.push("## Document notes", "");
lines.push(
  [
    "- Source PDF labeled **Internal Brand Positioning — November 2025**; confidential / not for public distribution.",
    "- **Comfort:** Public campaign **Rested. Set. Go.** may remain correct for *consumer* tagline while architecture uses **Where the joy happens.** — decide field policy.",
    "- **Cambria:** Architecture Brand Idea **Going Places** vs market **Approachable Indulgence** — both appear in CHI materials; architecture emphasizes challenger / horizon-seeker insight.",
    "- **Sleep Inn:** **Scenic Dreams** is a product pillar; architecture Brand Idea is **Dream Better Here** (Airtable uses Scenic Dreams as tagline).",
    "- Re-run: `python scripts/lib/extract-pdf-text.py \"…CHI Brands Architecture _ Oct 2025.pdf\"` then `node scripts/audit-choice-basics-vs-architecture-oct2025.mjs`",
  ].join("\n")
);
lines.push("");

fs.mkdirSync(path.dirname(OUT_MD), { recursive: true });
fs.writeFileSync(OUT_MD, lines.join("\n"), "utf8");
console.log(`Wrote ${OUT_MD}`);
console.log(`Aligned: ${aligned.length}, Not in doc: ${notInDoc.length}`);
