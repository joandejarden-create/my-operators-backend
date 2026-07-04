/**
 * Audit Brand Setup - Brand Basics (CHI) vs CHI Brands Architecture Oct 2025 PDF.
 * node scripts/audit-choice-basics-vs-architecture-oct2025.mjs
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const EXPORT = path.join(ROOT, "fixtures", "choice-basics-audit-export.json");
const ARCH_TXT = path.join(ROOT, "fixtures", "choice-pdf-text", "chi-brands-architecture-oct-2025.txt");
const OUT_MD = path.join(ROOT, "docs", "choice-brand-basics-architecture-oct2025-audit.md");

/** Authoritative fields from CHI Brands Architecture _ Oct 2025.pdf (internal positioning). */
const ARCH = {
  "Ascend Hotel Collection": {
    tier: "Upscale",
    service: "Select-Service",
    brandIdea: "Find your travel story.",
    strategicProposition: "We believe in the power of originality.",
    pillars: ["Distinct Details (Design)", "Home Away From Home (Experience)", "Heartfelt Connection (Service)"],
  },
  "Cambria Hotels": {
    tier: "Upscale",
    service: "Select-Service",
    brandIdea: "Going Places",
    strategicProposition: "A brand on the rise, for people who are too",
    pillars: ["Effortlessly Refined (Design)", "Fueling the Journey (Experience)", "Paving the Way (Service)"],
  },
  "Radisson (Choice)": {
    tier: "Upscale",
    service: "Full-Service",
    brandIdea: "Where New Feels Known",
    strategicProposition:
      "To give people the confidence to explore what's new, by offering them the safety of what's known.",
    pillars: ["Balanced Calm (Design)", "Ease of Discovery (Experience)", "Confident Authenticity (Service)"],
  },
  "Radisson Blu (Choice)": {
    tier: "Upper Upscale",
    service: "Full-Service",
    brandIdea: "Think in Blu",
    strategicProposition: "We believe in transcending the ordinary.",
    pillars: ["Nordic Nouveau (Product)", "Enticing Moments (Experience)", "Curatorial Warmth (Service)"],
  },
  "Radisson RED  (Choice)": {
    tier: "Upscale",
    service: "Select-Service",
    brandIdea: "Enjoy It!",
    strategicProposition: "We believe every moment matters.",
    pillars: ["Design With Attitude", "Share & Connect", "Fun & Flexible"],
  },
  "Radisson Collection  (Choice)": {
    tier: "Upper Upscale",
    service: "Full-Service",
    brandIdea: "Explorers Welcome.",
    strategicProposition: "We believe in fueling curiosity.",
    pillars: ["Vivid Settings", "Characterful Encounters", "Explorer's Compass"],
  },
  "Comfort Inn & Suites": {
    tier: "Upper Midscale",
    service: "Select-Service",
    brandIdea: "Where the joy happens.",
    strategicProposition: "A familiar base to unlock the joy of travel.",
    pillars: ["Rise & Shine (Product)", "Memories in the Making (Experience)", "Joy Loves Company (Service)"],
  },
  "Country Inn & Suites by Radisson (Choice)": {
    tier: "Upper Midscale",
    service: "Select-Service",
    brandIdea: "Generosity you can feel.",
    strategicProposition: "Generous hospitality with touches of home.",
    pillars: [
      "Comfortable Continuity (Product)",
      "There's One Place Like Home (Experience)",
      "Where The Heart Is (Service)",
    ],
  },
  "Sleep Inn": {
    tier: "Midscale",
    service: "Limited Service",
    brandIdea: "Dream Better Here",
    strategicProposition:
      "Deliver the lowest cost-to-build and operate midscale brand that doesn't compromise on design or guest experience.",
    pillars: ["Scenic Dreams (Product)", "Better Night's Rest (Experience)", "Happy to Help (Service)"],
  },
  Clarion: {
    tier: "Midscale",
    service: "Full-Service",
    brandIdea: "Get Together Here",
    strategicProposition:
      "Clarion delivers focused full-service hospitality designed to support meaningful gatherings.",
    pillars: ["On-site dining", "Meeting & event spaces", "Open lobby & bar", "Business support", "Fitness & wellness"],
    scaleNote: "Guardrails table lists Minimum Quality Levels as Midscale/Upper Midscale for Clarion column.",
  },
  "Clarion Pointe": {
    tier: "Midscale",
    service: "Limited Service",
    brandIdea: "stay on pointe",
    strategicProposition:
      "Clarion Pointe provides affordable elevated essentials in just the right places for a sharper, more connected stay",
    pillars: ["Focal Pointes", "Contemporary Design Touches", "Starting Pointe Breakfast", "On-Demand Connectivity"],
  },
  "Quality Inn": {
    tier: "Midscale",
    service: "Limited Service",
    brandIdea: "Get Your Money's Worth",
    strategicProposition: "Value means getting more for your money and creating memories that matter",
    pillars: ["Q Breakfast", "Q Bed", "Q Service", "Q Shower", "Q Essentials"],
  },
  "Rodeway Inn": {
    tier: "Economy",
    service: "Economy",
    brandIdea: "Good night. Great savings.",
    strategicProposition:
      "Rodeway Inn hotels give guests an affordable place to stay that they can rely on. The bare essentials. No frills, nothing fancy.",
  },
  "Econo Lodge": {
    tier: "Economy",
    service: "Economy",
    brandIdea: "Easy Stop On The Road",
    strategicProposition: "Econo Lodge hotels make it easy for guests to feel confident and capable when they travel.",
  },
  "Park Inn by Radisson (Choice)": {
    tier: "Premium Value",
    service: "Premium Value",
    brandIdea: "Have a Happy Stay",
    altBrandIdea: "Brighten up the stay",
    strategicProposition: "Delivering brighter basics with a contemporary design and elevated essentials at a competitive price.",
    scaleNote: "Grouped with Value & Economy in architecture; guardrails = Premium Value (not upper midscale).",
  },
  "Everhome Suites": {
    tier: "Extended Stay (Midscale positioning in portfolio)",
    service: "Extended Stay",
    brandIdea: "Closer to Home.®",
    strategicProposition:
      "For grey-collar work travelers and personal stay guests, Everhome Suites is more than a place to stay - it's an experience designed to keep life moving.",
  },
  "MainStay Suites": {
    tier: "Extended Stay",
    service: "Extended Stay",
    brandIdea: "Live Like Home.®",
    strategicProposition:
      "MainStay Suites is more than a place to sleep - it's a space designed to help guests stay in control of their lifestyle and maintain their routines.",
  },
  "WoodSpring Suites": {
    tier: "Economy Extended Stay",
    service: "Extended Stay",
    brandIdea: "It's Simple. Done Better.®",
    strategicProposition:
      "For blue-collar work travelers and personal stay guests, WoodSpring Suites is the straightforward, affordable extended stay hotel that delivers just what guests need",
  },
  "Suburban Studios": {
    tier: "Economy Extended Stay",
    service: "Extended Stay",
    brandIdea: "Longer Stays Made Easy",
    strategicProposition:
      "For hardworking travelers - from skilled trades to everyday guests - Suburban Studios makes longer stays easy and affordable.",
  },
};

const NOT_IN_ARCH = [
  "Radisson Individual (Choice)",
  "Radisson Inn & Suites",
  "Park Plaza (Choice)",
];

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
  const arch = ARCH[name];
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
lines.push(
  [
    "1. **Clarion** — Change **Hotel Chain Scale** from Upscale → **Midscale** (or Midscale/Upper Midscale per guardrails); align tagline to **Get Together Here**; rebuild positioning from midscale architecture section.",
    "2. **Park Inn by Radisson (Choice)** — Tier should reflect **Premium Value**, not Upper Midscale; tagline **Have a Happy Stay** / **Brighten up the stay**; replace stub copy.",
    "3. **Radisson Collection (Choice)** — Tagline **Explorers Welcome.**; pillars **Vivid Settings / Characterful Encounters / Explorer's Compass** (not generic luxury stubs).",
    "4. **Radisson (Choice)** — Replace **A Century Young** with architecture Brand Idea **Where New Feels Known**; refresh pillars to Balanced Calm / Ease of Discovery / Confident Authenticity.",
    "5. **Country Inn & Suites** — Still stub-level vs architecture (**Generosity you can feel.**, home-touch pillars, Country Hosts / cookie / breakfast RTBs).",
    "6. **Extended stay taglines** — Everhome **Closer to Home.®**, MainStay **Live Like Home.®**, WoodSpring **It's Simple. Done Better.®**, Suburban **Longer Stays Made Easy** (Airtable uses development-era lines).",
    "7. **Radisson RED (Choice)** — Tagline **Enjoy It!**; tier likely **Upscale** not Upper Upscale; replace stub pillars.",
  ].join("\n")
);
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
  const arch = ARCH[name];
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
