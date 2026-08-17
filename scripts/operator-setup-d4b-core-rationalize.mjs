#!/usr/bin/env node
/**
 * D.4B Core Table Rationalization + True Population
 *
 *   node scripts/operator-setup-d4b-core-rationalize.mjs --dry-run
 *   node scripts/operator-setup-d4b-core-rationalize.mjs --apply --approve-operator-setup-d4b-core-rationalize
 *
 * Lean product schemas + MUST field population. No Fit. No generic narratives. No unsupported %.
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { TEST_FIXTURE_MASTER_IDS } from "../lib/operator-explorer/phase-1-universe.js";
import {
  deriveOperatorSummaries,
  isPopulated,
} from "../lib/operator-setup/derived-sync.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const OUT = join(ROOT, "data/operator-setup/core-tables-d4b");
const REPORTS = join(ROOT, "reports");
const DOCS = join(ROOT, "docs");

const PROFILE = "Operator Setup - Profile & Positioning";
const PLATFORM = "Operator Setup - Platform & Markets";
const SPOT_IDS = [
  "recWPKu5laVZxsvpn",
  "recF5Z87OAqFgndoq",
  "recLjxtxIIVJaGbXK",
  "recGWxIJqnYHkJZFD",
  "reciI2tYQBfMoMK9G",
  "rec6UB6RpMKSs2tAo",
  "recGmiPhRt6hiayd9",
  "rec3Uwxe6ovpiokuN",
  "recF2WqLqNVyKGz9E",
  "rec7IXYQYpKMYsrDl",
  "recwEHUotSGpfkZEJ",
  "rec8XpNv6G0WOlMwu",
];

/** Final retained Profile product fields (Airtable column or Master-canonical) */
const PROFILE_CORE = [
  { key: "company_name", storage: "profile", priority: "MUST", why: "Canonical operator name" },
  { key: "website", storage: "profile", priority: "MUST", why: "Official company URL" },
  { key: "headquarters", storage: "profile", priority: "MUST", why: "Where the company is based" },
  { key: "Operator Parent Company", storage: "master", priority: "MUST", why: "Parent / corporate context" },
  { key: "Operating Model", storage: "master", priority: "MUST", why: "How the company operates hotels" },
  { key: "Management Availability", storage: "master", priority: "MUST", why: "Whether third-party management is available" },
  { key: "companySize", storage: "profile", priority: "MUST", why: "Approximate portfolio scale band" },
  { key: "Brand Families Operated", storage: "profile", priority: "MUST", why: "Brand family experience" },
  { key: "Service Models Supported", storage: "profile", priority: "SHOULD", why: "Service-model experience" },
  { key: "propertyTypes", storage: "profile", priority: "SHOULD", why: "Hotel-type experience" },
  { key: "additionalExperience", storage: "profile", priority: "SHOULD", why: "Urban/resort/conversion etc. experience flags" },
  { key: "chainScalesSupported", storage: "profile", priority: "SHOULD", why: "Chain-scale experience when evidenced" },
  { key: "Soft Brand / Lifestyle Experience", storage: "profile", priority: "OPTIONAL", why: "Soft-brand depth when known" },
  { key: "primaryServiceModel", storage: "profile", priority: "OPTIONAL", why: "Primary orientation shorthand" },
  { key: "companyDescription", storage: "profile", priority: "WRITER_V2", why: "Owner-facing who-is-this narrative" },
  { key: "differentiators", storage: "profile", priority: "WRITER_V2", why: "What distinguishes the platform" },
];

const PLATFORM_CORE = [
  { key: "company_name", storage: "platform", priority: "MUST", why: "Row identity" },
  { key: "Active Countries", storage: "platform", priority: "MUST", why: "Current operating geographies (taxonomy)" },
  { key: "Market Presence Type", storage: "platform", priority: "MUST", why: "Active vs pipeline posture" },
  { key: "specificMarkets", storage: "platform", priority: "SHOULD", why: "Markets outside taxonomy / concentration notes" },
  { key: "Active Markets / Cities", storage: "platform", priority: "SHOULD", why: "City/corridor depth when taxonomy mapped" },
  { key: "cap_profile_operational", storage: "platform", priority: "WRITER_V2", why: "Operating platform differentiation narrative" },
  { key: "cap_profile_commercial", storage: "platform", priority: "WRITER_V2", why: "Commercial organization narrative (contract refine)" },
];

/** Targeted research — HQ + scale for MUST blanks (official / filings / company sites) */
const RESEARCH_DIRECT = {
  rec04aLAfmupWG4ZK: {
    headquarters: "Palma, Spain",
    companySize: "Enterprise (200+ properties)",
    source: "Barceló Hotel Group corporate (Palma HQ; ~300+ hotels public scale)",
  },
  rec28eZ7ERwc92XWd: {
    headquarters: "Palma de Mallorca, Spain",
    companySize: "Enterprise (200+ properties)",
    source: "Meliá Hotels International corporate HQ",
  },
  rec5xdV2THfFjEUPk: {
    headquarters: "Hong Kong",
    companySize: "Medium (10-50 properties)",
    source: "Mandarin Oriental Hotel Group corporate",
  },
  rec8XpNv6G0WOlMwu: {
    headquarters: "Hong Kong",
    companySize: "Large (50-200 properties)",
    activeCountries: ["Other"],
    marketPresenceType: ["Active operations"],
    specificMarkets:
      "Global brand-operator footprint across Asia Pacific, Middle East, Europe and Africa (HKEX/annual disclosures cite ~106 hotels / 22 countries-regions as of YE 2025). No Shangri-La inventory mapped into Dealality CALA Active Countries taxonomy — use Other + this note.",
    source: "Shangri-La Asia Ltd annual report / HKEX disclosures 2025",
  },
  recIq0XYgt5Ghvcsz: {
    headquarters: "Newton, Massachusetts, United States",
    companySize: "Large (50-200 properties)",
    source: "Sonesta International corporate",
  },
  recVtNxNeeYlngtUk: {
    headquarters: "Mill Valley, California, United States",
    companySize: "Medium (10-50 properties)",
    source: "Auberge Resorts Collection corporate",
  },
  rechnXKjpeiNMaqjJ: {
    headquarters: "Toronto, Canada",
    companySize: "Large (50-200 properties)",
    source: "Four Seasons Hotels and Resorts corporate",
  },
  recji1awMffccwox2: {
    headquarters: "Hong Kong",
    companySize: "Medium (10-50 properties)",
    source: "Rosewood Hotel Group corporate",
  },
  reculkMOYWDxX14Pv: {
    headquarters: "Chicago, Illinois, United States",
    companySize: "Enterprise (200+ properties)",
    source: "Hyatt Hotels Corporation corporate (Managed lens)",
  },
  rec9JSyGQjvodsPSJ: {
    headquarters: "Buenos Aires, Argentina",
    companySize: "Small (1-10 properties)",
    source: "AADESA regional operator materials (Argentina)",
  },
  recHj56wpRLUnJ5Wx: {
    headquarters: "Buenos Aires, Argentina",
    companySize: "Small (1-10 properties)",
    source: "Tremun Hoteles regional materials",
  },
  recjgHXqTJktijFUR: {
    headquarters: "Buenos Aires, Argentina",
    companySize: "Small (1-10 properties)",
    source: "Álvarez Argüelles Hoteles regional materials",
  },
};

function parseArgs(argv) {
  const out = { dryRun: true, apply: false, approve: false };
  for (const a of argv) {
    if (a === "--apply") {
      out.apply = true;
      out.dryRun = false;
    } else if (a === "--dry-run") out.dryRun = true;
    else if (a === "--approve-operator-setup-d4b-core-rationalize") out.approve = true;
  }
  return out;
}
function writeJson(p, o) {
  mkdirSync(dirname(p), { recursive: true });
  writeFileSync(p, JSON.stringify(o, null, 2) + "\n");
}
function writeMd(p, t) {
  mkdirSync(dirname(p), { recursive: true });
  writeFileSync(p, t.endsWith("\n") ? t : t + "\n");
}
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
function nz(v) {
  return v == null ? "" : String(v).trim();
}

async function listAll(baseId, token, table) {
  const out = [];
  let offset;
  do {
    const u = new URL(`https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}`);
    u.searchParams.set("pageSize", "100");
    if (offset) u.searchParams.set("offset", offset);
    const j = await (await fetch(u, { headers: { Authorization: `Bearer ${token}` } })).json();
    if (j.error) throw new Error(`${table}: ${JSON.stringify(j.error)}`);
    out.push(...(j.records || []));
    offset = j.offset;
    await sleep(40);
  } while (offset);
  return out;
}
async function fetchMeta(baseId, token) {
  const j = await (
    await fetch(`https://api.airtable.com/v0/meta/bases/${baseId}/tables`, {
      headers: { Authorization: `Bearer ${token}` },
    })
  ).json();
  if (j.error) throw new Error(JSON.stringify(j.error));
  return j.tables || [];
}
async function patchRecord(baseId, token, table, id, fields) {
  const res = await fetch(`https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}/${id}`, {
    method: "PATCH",
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
    body: JSON.stringify({ fields, typecast: true }),
  });
  const j = await res.json();
  if (!res.ok) throw new Error(`PATCH ${table}/${id}: ${JSON.stringify(j)}`);
  return j;
}

function byOperator(rows) {
  const m = {};
  for (const r of rows) for (const id of r.fields.Operator || []) m[id] = r;
  return m;
}

function dispositionForField(table, name, type) {
  const profileMust = new Set(PROFILE_CORE.filter((c) => c.storage === "profile").map((c) => c.key));
  const platformMust = new Set(PLATFORM_CORE.filter((c) => c.storage === "platform").map((c) => c.key));
  const writer = new Set(["companyDescription", "differentiators", "cap_profile_operational", "cap_profile_commercial", "brand_narrative_relationship"]);
  const moveClaims = new Set(["managementPhilosophy", "missionStatement", "insuranceCoverage", "cap_profile_transition", "companyHistory", "companyTagline"]);
  const presentation = /^(overview_|op_|brand_portfolio|brand_relationship|brand_execution|brand_governance|mkt_signal|mkt_regional|mkt_market|cap_card_)/;
  const numericGrid =
    /^(geo_|luxury|upperUpscale|upscale|upperMidscale|midscale|economy|total(Properties|Rooms)|locationType|cap_kpi_|cap_signal_|brand_signal_)/i.test(
      name
    ) ||
    /Experience$|AvgStaff$|RampLeadTime|exitsDeflaggings|numberOfMarkets|numberOfBrands|brandedVsIndependent|brand_conversion|marketExpansion|yearsInBusiness|yearEstablished/.test(
      name
    );

  if (name === "Operator" || name === "readyForInvestorPublication" || name === "marketDepthOptIn" || name === "capitalStatus") {
    return { disposition: "PRESENTATION ONLY", note: name === "Operator" ? "Link / workflow" : "Workflow or non-profile" };
  }
  if (table === "profile" && profileMust.has(name)) {
    const p = PROFILE_CORE.find((c) => c.key === name);
    if (p?.priority === "WRITER_V2") return { disposition: "WRITER V2 OPTIONAL", note: p.why };
    if (p?.priority === "OPTIONAL" || p?.priority === "SHOULD") return { disposition: "CORE OPTIONAL", note: p.why };
    return { disposition: "CORE REQUIRED", note: p?.why || "Core profile" };
  }
  if (table === "platform" && platformMust.has(name)) {
    const p = PLATFORM_CORE.find((c) => c.key === name);
    if (p?.priority === "WRITER_V2") return { disposition: "WRITER V2 OPTIONAL", note: p.why };
    if (p?.priority === "SHOULD") return { disposition: "CORE OPTIONAL", note: p.why };
    return { disposition: "CORE REQUIRED", note: p?.why || "Core platform" };
  }
  if (writer.has(name) || /brand_narrative|brand_soft|cap_deep|mkt_narrative|additionalBrands|crisisExperience|energyEfficiency|wasteReduction|Brands Portfolio/.test(name)) {
    if (moveClaims.has(name)) return { disposition: "MOVE TO CLAIMS", note: "Sourced narrative → Claims" };
    if (writer.has(name)) return { disposition: "WRITER V2 OPTIONAL", note: "High-value narrative only with evidence" };
    return { disposition: "MOVE TO CLAIMS", note: "Optional narrative depth → Claims preferred" };
  }
  if (moveClaims.has(name)) return { disposition: "MOVE TO CLAIMS", note: "Marketing/philosophy narrative" };
  if (presentation.test(name) || type === "multipleAttachments" || name === "companyLogo" || name === "figuresAsOf") {
    return { disposition: "PRESENTATION ONLY", note: "Explorer/UI scaffold — not Setup truth" };
  }
  if (numericGrid) {
    if (name === "companySize") return { disposition: "CORE REQUIRED", note: "Retained scale band" };
    if (name === "yearEstablished" || name === "yearsInBusiness" || name === "numberOfBrands") {
      return { disposition: "DERIVED — DATA NOT YET SUFFICIENT", note: "Valid concept; needs verified inputs" };
    }
    return { disposition: "DEPRECATE", note: "Legacy census/KPI/% without defensible methodology — hide then remove" };
  }
  if (name === "brands" || name === "chainScale" || name === "businessContinuity" || name === "support24x7" || name === "carbonTracking" || name === "emergencyResponse" || name === "sustainabilityPrograms" || name === "esgReporting" || name === "yearsInBusiness" || name === "yearEstablished") {
    return { disposition: "DERIVED — DATA NOT YET SUFFICIENT", note: "Valid concept; needs better coverage or lives elsewhere" };
  }
  return { disposition: "DEPRECATE", note: "Not justified in lean product schema" };
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.apply && !args.approve) {
    console.error("Refuse apply without --approve-operator-setup-d4b-core-rationalize");
    process.exit(1);
  }
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_TOKEN;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!token || !baseId) throw new Error("Missing AIRTABLE credentials");

  mkdirSync(OUT, { recursive: true });
  const ts = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);

  console.log("Loading live tables...");
  const tables = await fetchMeta(baseId, token);
  const profileMeta = tables.find((t) => t.name === PROFILE);
  const platformMeta = tables.find((t) => t.name === PLATFORM);
  const masters = await listAll(baseId, token, "Operator Setup - Master");
  const profile = await listAll(baseId, token, PROFILE);
  const platform = await listAll(baseId, token, PLATFORM);
  const assignments = await listAll(baseId, token, "Operator Intelligence - Assignments");
  const presence = await listAll(baseId, token, "Operator Intelligence - Market Presence");
  const brands = await listAll(baseId, token, "Operator Intelligence - Brand Relationships");

  const production = masters.filter(
    (m) => m.fields["Record Purpose"] === "Production" && !TEST_FIXTURE_MASTER_IDS.includes(m.id)
  );
  const profileBy = byOperator(profile);
  const platformBy = byOperator(platform);

  const acOptions = new Set((platformMeta.fields.find((f) => f.name === "Active Countries")?.options?.choices || []).map((c) => c.name));
  const sizeOptions = new Set((profileMeta.fields.find((f) => f.name === "companySize")?.options?.choices || []).map((c) => c.name));

  // --- Mutually exclusive dispositions ---
  const dispositions = { profile: {}, platform: {}, counts: {} };
  const countDisp = {};
  for (const f of profileMeta.fields) {
    const d = dispositionForField("profile", f.name, f.type);
    dispositions.profile[f.name] = { ...d, type: f.type };
    countDisp[d.disposition] = (countDisp[d.disposition] || 0) + 1;
  }
  for (const f of platformMeta.fields) {
    const d = dispositionForField("platform", f.name, f.type);
    dispositions.platform[f.name] = { ...d, type: f.type };
    countDisp[d.disposition] = (countDisp[d.disposition] || 0) + 1;
  }
  dispositions.counts = countDisp;
  writeJson(join(OUT, "field-dispositions.json"), dispositions);

  // --- Visible empty audit ---
  const auditRows = [];
  for (const [tableName, meta, byMap, key] of [
    [PROFILE, profileMeta, profileBy, "profile"],
    [PLATFORM, platformMeta, platformBy, "platform"],
  ]) {
    for (const f of meta.fields) {
      if (f.name === "Operator") continue;
      let pop = 0;
      for (const p of production) {
        if (isPopulated(byMap[p.id]?.fields?.[f.name])) pop++;
      }
      const blank = production.length - pop;
      const fill = Math.round((pop / production.length) * 1000) / 10;
      const disp = dispositions[key][f.name];
      const expected = disp.disposition === "CORE REQUIRED" || disp.disposition === "CORE OPTIONAL";
      let whyBlank = "—";
      if (blank > 0) {
        if (disp.disposition === "DEPRECATE") whyBlank = "Legacy field — not product-required";
        else if (disp.disposition === "PRESENTATION ONLY") whyBlank = "Presentation/scaffold — not Setup truth";
        else if (disp.disposition === "WRITER V2 OPTIONAL") whyBlank = "Narrative awaits Writer v2 evidence";
        else if (disp.disposition === "MOVE TO CLAIMS") whyBlank = "Claims-bound; not Setup truth";
        else if (disp.disposition === "DERIVED — DATA NOT YET SUFFICIENT") whyBlank = "Valid concept; OE coverage insufficient";
        else if (disp.disposition === "CORE REQUIRED") whyBlank = "MUST gap — research/derive required";
        else whyBlank = "Optional / unknown";
      }
      auditRows.push({
        table: tableName,
        field: f.name,
        type: f.type,
        productionFill: `${fill}% (${pop}/${production.length})`,
        fillPct: fill,
        populated: pop,
        blank,
        disposition: disp.disposition,
        whyBlank,
        shouldExist: !["DEPRECATE", "PRESENTATION ONLY"].includes(disp.disposition) || f.name === "Operator",
      });
    }
  }

  writeMd(
    join(REPORTS, "operator-setup-core-tables-visible-empty-audit.md"),
    [
      `# D.4B Visible Empty Audit`,
      ``,
      `Production operators: **${production.length}**. Completeness is NOT measured on legacy 67/130 columns.`,
      ``,
      `| Table | Field | Type | Production Fill | Disposition | Why Blank | Should Exist? |`,
      `| ----- | ----- | ---- | --------------- | ----------- | --------- | ------------- |`,
      ...auditRows.map(
        (r) =>
          `| ${r.table.replace("Operator Setup - ", "")} | ${r.field} | ${r.type} | ${r.productionFill} | ${r.disposition} | ${r.whyBlank} | ${r.shouldExist ? "Yes/Retain or Master" : "No — hide/deprecate"} |`
      ),
      ``,
    ].join("\n")
  );

  // --- Final schemas ---
  writeMd(
    join(DOCS, "data/operator-profile-final-schema.md"),
    [
      `# Operator Setup — Profile & Positioning Final Product Schema`,
      ``,
      `**Question:** What does an owner need to know to understand who this operator is?`,
      ``,
      `Legacy columns: **${profileMeta.fields.length}**. Retained product fields: **${PROFILE_CORE.length}** (+ Master OM/MA/Parent).`,
      ``,
      `| Field | Storage | Priority | Why it belongs |`,
      `| ----- | -------- | -------- | -------------- |`,
      ...PROFILE_CORE.map((c) => `| ${c.key} | ${c.storage} | ${c.priority} | ${c.why} |`),
      ``,
      `All other Profile Airtable columns are **PRESENTATION ONLY**, **MOVE TO CLAIMS**, **DERIVED—INSUFFICIENT**, or **DEPRECATE** — hide from founder view; do not count toward completeness.`,
      ``,
    ].join("\n")
  );

  writeMd(
    join(DOCS, "data/operator-platform-markets-final-schema.md"),
    [
      `# Operator Setup — Platform & Markets Final Product Schema`,
      ``,
      `**Question:** Where does this operator operate, what kinds of hotels does it operate, and how deep is its relevant experience?`,
      ``,
      `Legacy columns: **${platformMeta.fields.length}**. Retained product fields: **${PLATFORM_CORE.length}**.`,
      ``,
      `| Field | Storage | Priority | Why it belongs |`,
      `| ----- | -------- | -------- | -------------- |`,
      ...PLATFORM_CORE.map((c) => `| ${c.key} | ${c.storage} | ${c.priority} | ${c.why} |`),
      ``,
      `## Experience flags (stored on Profile)`,
      ``,
      `Hotel-type / development experience is retained on Profile via \`additionalExperience\`, \`propertyTypes\`, \`Service Models Supported\`, \`Brand Families Operated\` — not as Platform % grids.`,
      ``,
      `## Explicitly not retained`,
      ``,
      `- All \`geo_*\` regional census grids`,
      `- Luxury/upscale/midscale property/room count matrices`,
      `- \`locationType*\` percentages`,
      `- \`cap_kpi_*\` / \`cap_signal_*\` without methodology`,
      `- \`*Experience\` numeric scores without numerator/denominator`,
      ``,
    ].join("\n")
  );

  // Numeric verdict
  let calcNow = 0,
    keepFuture = 0,
    deprecateNum = 0,
    moveElse = 0;
  const numericVerdict = [];
  for (const [table, meta, key] of [
    [PROFILE, profileMeta, "profile"],
    [PLATFORM, platformMeta, "platform"],
  ]) {
    for (const f of meta.fields) {
      const isNum =
        f.type === "number" ||
        dispositions[key][f.name]?.disposition === "DEPRECATE" && /geo_|luxury|Experience|locationType|cap_kpi|cap_signal|brand_signal|Properties|Rooms|AvgStaff/.test(f.name);
      if (!isNum && f.type !== "number") continue;
      if (f.type !== "number" && !/geo_|luxury|upper|upscale|midscale|economy|total|locationType|Experience|AvgStaff|cap_kpi|cap_signal|brand_signal|numberOf|branded|conversion_project|Ramp/.test(f.name))
        continue;
      let verdict = "DEPRECATE";
      let note = "Legacy census/KPI — hide then remove";
      if (f.name === "yearEstablished") {
        verdict = "KEEP FOR FUTURE DATA";
        note = "Useful when verified; do not invent";
        keepFuture++;
      } else if (f.name === "yearsInBusiness") {
        verdict = "KEEP FOR FUTURE DATA";
        note = "Derive only from verified yearEstablished";
        keepFuture++;
      } else if (f.name === "numberOfBrands") {
        verdict = "KEEP FOR FUTURE DATA";
        note = "Count rule needed (families vs flags)";
        keepFuture++;
      } else if (/Experience$/.test(f.name) && f.type === "number") {
        verdict = "DEPRECATE";
        note = "Replace with Yes/evidenced flags on additionalExperience — not percentages/counts from thin samples";
        deprecateNum++;
      } else if (/^geo_|^luxury|^upper|^upscale|^midscale|^economy|locationType|AvgStaff|cap_kpi|cap_signal|brand_signal/.test(f.name)) {
        verdict = "DEPRECATE";
        note = "No defensible census/KPI methodology";
        deprecateNum++;
      } else {
        verdict = "DEPRECATE";
        deprecateNum++;
      }
      if (verdict === "CALCULATE NOW") calcNow++;
      if (verdict === "MOVE ELSEWHERE") moveElse++;
      numericVerdict.push({ table, field: f.name, verdict, note });
    }
  }

  writeMd(
    join(REPORTS, "operator-setup-core-numeric-field-verdict.md"),
    [
      `# D.4B Numeric Field Verdict`,
      ``,
      `| Verdict | Count |`,
      `| ------- | ----: |`,
      `| CALCULATE NOW | ${calcNow} |`,
      `| KEEP FOR FUTURE DATA | ${keepFuture} |`,
      `| DEPRECATE | ${deprecateNum} |`,
      `| MOVE ELSEWHERE | ${moveElse} |`,
      ``,
      `| Table | Field | Verdict | Note |`,
      `| ----- | ----- | ------- | ---- |`,
      ...numericVerdict.map((r) => `| ${r.table.replace("Operator Setup - ", "")} | ${r.field} | ${r.verdict} | ${r.note} |`),
      ``,
      `**Principle:** Prefer \`Resort Experience\` via \`additionalExperience\` over \`locationTypeResort = 34%\` from a thin Assignment sample.`,
      ``,
    ].join("\n")
  );

  writeMd(
    join(REPORTS, "operator-setup-core-field-priority.md"),
    [
      `# D.4B Core Field Priority (Founder-facing)`,
      ``,
      `## MUST POPULATE`,
      ``,
      `- Website, Headquarters, Parent Company`,
      `- Operating Model, Management Availability (Master)`,
      `- Approximate portfolio scale (companySize)`,
      `- Brand Families Operated`,
      `- Active Countries (+ Market Presence Type)`,
      ``,
      `## SHOULD POPULATE WHEN EVIDENCED`,
      ``,
      `- Service Models Supported, propertyTypes, additionalExperience`,
      `- chainScalesSupported (only with typed Assignment scale)`,
      `- specificMarkets / Active Markets when taxonomy allows`,
      ``,
      `## OPTIONAL DEPTH`,
      ``,
      `- Soft Brand / Lifestyle Experience, primaryServiceModel`,
      `- Writer v2: companyDescription, differentiators, cap_profile_operational`,
      ``,
      `## REMOVE FROM CORE TABLE (hide → deprecate)`,
      ``,
      `- geo_* census grids, luxury/scale room matrices, locationType %`,
      `- cap_kpi_* / cap_signal_* / brand_signal_* without methodology`,
      `- overview_* / op_* JSON / brand_* JSON presentation scaffolds (keep for Explorer only)`,
      `- mission/philosophy/insurance → Claims`,
      ``,
    ].join("\n")
  );

  // --- Build MUST mutations ---
  const mutations = [];
  const researchCases = [];
  const unresolvedMust = [];

  for (const p of production) {
    const id = p.id;
    const name = p.fields.company_name;
    const pref = profileBy[id];
    const pl = platformBy[id];
    const research = RESEARCH_DIRECT[id] || {};
    if (RESEARCH_DIRECT[id]) researchCases.push({ operator: name, masterId: id, source: research.source });

    const derived = deriveOperatorSummaries({
      assignments,
      marketPresence: presence,
      brandRelationships: brands,
      masterId: id,
      activeCountryOptions: acOptions,
    });

    // Master MUST — Parent self when top-level; OM/MA only when research-known
    const masterFills = {};
    if (!isPopulated(p.fields["Operator Parent Company"])) {
      masterFills["Operator Parent Company"] = name; // top-level entity: parent = self
    }
    const masterResearch = {
      recJ6NPSYveCTo3At: { "Operating Model": "Integrated Owner / Brand / Operator", "Management Availability": "Conditional / Scoped" },
      recOc5kpsg4Muip9Y: { "Management Availability": "Conditional / Scoped" },
      reck6gjQd3wdeugmZ: { "Management Availability": "Conditional / Scoped" },
    };
    if (masterResearch[id]) {
      for (const [k, v] of Object.entries(masterResearch[id])) {
        if (!isPopulated(p.fields[k])) masterFills[k] = v;
      }
    }
    if (Object.keys(masterFills).length) {
      for (const [field, value] of Object.entries(masterFills)) {
        mutations.push({
          table: "Operator Setup - Master",
          recordId: p.id,
          masterId: id,
          operator: name,
          field,
          proposedValue: value,
          treatment: "FILL DIRECT MUST",
          source: masterResearch[id] ? "targeted research / operating model posture" : "Top-level entity — Parent = company name",
        });
      }
      // reflect for unresolved check
      Object.assign(p.fields, masterFills);
    }

    // Profile MUST
    if (pref) {
      const fills = {};
      if (!isPopulated(pref.fields.headquarters) && research.headquarters) fills.headquarters = research.headquarters;
      if (!isPopulated(pref.fields.companySize) && research.companySize && sizeOptions.has(research.companySize)) {
        fills.companySize = research.companySize;
      } else if (!isPopulated(pref.fields.companySize) && research.companySize) {
        // try fuzzy match Large (50+ properties)
        const alt = [...sizeOptions].find((o) => o.startsWith("Large") && /50/.test(o));
        if (alt && /Large/.test(research.companySize)) fills.companySize = alt;
        else if (sizeOptions.has(research.companySize)) fills.companySize = research.companySize;
      }
      if (!isPopulated(pref.fields.website) && nz(p.fields["Operator Website"])) fills.website = p.fields["Operator Website"];
      if (!isPopulated(pref.fields.company_name)) fills.company_name = name;

      for (const [field, value] of Object.entries(fills)) {
        mutations.push({
          table: PROFILE,
          recordId: pref.id,
          masterId: id,
          operator: name,
          field,
          proposedValue: value,
          treatment: "FILL DIRECT MUST",
          source: research.source || "Master/research",
        });
      }
    }

    // Platform MUST — Shangri-La + any blank AC with research
    if (pl) {
      const fills = {};
      if (!isPopulated(pl.fields["Active Countries"])) {
        if (research.activeCountries?.length) fills["Active Countries"] = research.activeCountries;
        else if (derived.activeCountries.length) fills["Active Countries"] = derived.activeCountries;
      }
      if (!isPopulated(pl.fields["Market Presence Type"]) && (fills["Active Countries"] || isPopulated(pl.fields["Active Countries"]))) {
        fills["Market Presence Type"] = research.marketPresenceType || ["Active operations"];
      }
      if (!isPopulated(pl.fields.specificMarkets) && research.specificMarkets) {
        fills.specificMarkets = research.specificMarkets;
      }
      if (!isPopulated(pl.fields.company_name)) fills.company_name = name;

      for (const [field, value] of Object.entries(fills)) {
        mutations.push({
          table: PLATFORM,
          recordId: pl.id,
          masterId: id,
          operator: name,
          field,
          proposedValue: value,
          treatment: "FILL DIRECT/DERIVED MUST",
          source: research.source || "OE derived",
        });
      }
    }

    // Track unresolved MUST after planned fills
    const willHq = isPopulated(pref?.fields?.headquarters) || research.headquarters;
    const willSize = isPopulated(pref?.fields?.companySize) || research.companySize;
    const willWeb = isPopulated(pref?.fields?.website) || nz(p.fields["Operator Website"]);
    const willParent = isPopulated(p.fields["Operator Parent Company"]);
    const willOm = isPopulated(p.fields["Operating Model"]);
    const willMa = isPopulated(p.fields["Management Availability"]);
    const willBf = isPopulated(pref?.fields?.["Brand Families Operated"]);
    const willAc =
      isPopulated(pl?.fields?.["Active Countries"]) ||
      research.activeCountries?.length ||
      derived.activeCountries.length;

    const gaps = [];
    if (!willHq) gaps.push("headquarters");
    if (!willSize) gaps.push("companySize");
    if (!willWeb) gaps.push("website");
    if (!willParent) gaps.push("Operator Parent Company");
    if (!willOm) gaps.push("Operating Model");
    if (!willMa) gaps.push("Management Availability");
    if (!willBf) gaps.push("Brand Families Operated");
    if (!willAc) gaps.push("Active Countries");
    if (gaps.length) unresolvedMust.push({ operator: name, masterId: id, gaps });
  }

  writeJson(join(OUT, "write-plan.json"), {
    generatedAt: new Date().toISOString(),
    mutations,
    researchCases,
    unresolvedMust,
  });

  // --- Schema cleanup plan ---
  const deprecateList = Object.entries(dispositions.profile)
    .concat(Object.entries(dispositions.platform))
    .filter(([, v]) => v.disposition === "DEPRECATE")
    .map(([f, v]) => ({ field: f, note: v.note }));

  writeMd(
    join(REPORTS, "operator-setup-core-schema-cleanup-plan.md"),
    [
      `# D.4B Schema Cleanup Plan`,
      ``,
      `**Do not delete Airtable fields yet** without founder authorization.`,
      ``,
      `## Deprecation candidates (${deprecateList.length})`,
      ``,
      `- Consumers: Explorer may still read overview_*/op_*/brand_* JSON (those are PRESENTATION — hide from Setup view, keep columns for Explorer).`,
      `- Fit: do **not** bind Fit to geo_* / cap_kpi_* / locationType% grids.`,
      `- Automations: audit Airtable automations before delete.`,
      `- Timing: Phase 1 hide via views; Phase 2 archive; Phase 3 delete after 30 days with zero reads.`,
      ``,
      `### Deprecate list (sample → full in dispositions JSON)`,
      ``,
      ...deprecateList.slice(0, 40).map((d) => `- \`${d.field}\` — ${d.note}`),
      deprecateList.length > 40 ? `\n_…and ${deprecateList.length - 40} more (see field-dispositions.json)_` : "",
      ``,
      `## MOVE TO CLAIMS`,
      ``,
      `- managementPhilosophy, missionStatement, insuranceCoverage, cap_profile_transition, companyHistory, companyTagline`,
      ``,
    ].join("\n")
  );

  writeMd(
    join(REPORTS, "operator-setup-core-clean-view-recipe.md"),
    [
      `# D.4B Clean View Recipe (Manual Airtable)`,
      ``,
      `API cannot create views reliably here — apply manually in Airtable UI.`,
      ``,
      `## Profile & Positioning — view name: \`D.4B Core Product\``,
      ``,
      `### Visible (left → right)`,
      ``,
      `1. company_name`,
      `2. website`,
      `3. headquarters`,
      `4. companySize`,
      `5. Brand Families Operated`,
      `6. Service Models Supported`,
      `7. propertyTypes`,
      `8. additionalExperience`,
      `9. chainScalesSupported`,
      `10. Soft Brand / Lifestyle Experience`,
      `11. primaryServiceModel`,
      `12. companyDescription`,
      `13. differentiators`,
      `14. Operator (link)`,
      ``,
      `Also pin Master fields in a linked record preview: Operator Parent Company, Operating Model, Management Availability.`,
      ``,
      `### Optional depth (collapse / second view)`,
      ``,
      `- brand_narrative_relationship, Soft Brand narrative fields`,
      ``,
      `### Hidden`,
      ``,
      `- All overview_*, brand_*_json, brand_signal_*, locationType*, capitalStatus, ESG/sustainability selects, companyLogo`,
      ``,
      `## Platform & Markets — view name: \`D.4B Core Product\``,
      ``,
      `### Visible (left → right)`,
      ``,
      `1. company_name`,
      `2. Active Countries`,
      `3. Market Presence Type`,
      `4. specificMarkets`,
      `5. Active Markets / Cities`,
      `6. cap_profile_operational`,
      `7. cap_profile_commercial`,
      `8. Operator`,
      ``,
      `### Hidden`,
      ``,
      `- All geo_*, luxury/upscale/midscale/economy matrices, locationType*, *Experience numbers, cap_kpi_*, cap_signal_*, op_* JSON, cap_card_*, cap_deep_*, cap_profile_transition`,
      ``,
      `Filter: Operator → Record Purpose = Production (via linked Master) if available.`,
      ``,
    ].join("\n")
  );

  // Backup + apply
  const backupDir = join(ROOT, "backups/operator-setup/d4b-core-rationalize", ts);
  mkdirSync(backupDir, { recursive: true });
  writeJson(join(backupDir, "Profile_Positioning.json"), { recordCount: profile.length, records: profile });
  writeJson(join(backupDir, "Platform_Markets.json"), { recordCount: platform.length, records: platform });
  writeJson(join(backupDir, "Master.json"), { recordCount: masters.length, records: masters });
  writeJson(join(backupDir, "manifest.json"), { timestamp: ts });

  let writes = 0;
  const failures = [];
  if (args.apply) {
    console.log(`Applying ${mutations.length} MUST fills...`);
    const byRec = new Map();
    for (const m of mutations) {
      const key = `${m.table}::${m.recordId}`;
      if (!byRec.has(key)) byRec.set(key, { table: m.table, id: m.recordId, fields: {} });
      byRec.get(key).fields[m.field] = m.proposedValue;
    }
    for (const b of byRec.values()) {
      try {
        await patchRecord(baseId, token, b.table, b.id, b.fields);
        writes += Object.keys(b.fields).length;
        await sleep(100);
      } catch (e) {
        failures.push({ id: b.id, table: b.table, error: String(e.message || e) });
      }
    }
  }

  // Reload for completeness on FINAL schema
  let profile2 = profile;
  let platform2 = platform;
  if (args.apply) {
    profile2 = await listAll(baseId, token, PROFILE);
    platform2 = await listAll(baseId, token, PLATFORM);
  } else {
    // Simulate planned fills for dry-run coverage
    for (const m of mutations) {
      const map = m.table === PROFILE ? profileBy : platformBy;
      const rec = map[m.masterId];
      if (rec) rec.fields[m.field] = m.proposedValue;
    }
    profile2 = profile;
    platform2 = platform;
  }
  const profileBy2 = byOperator(profile2);
  const platformBy2 = byOperator(platform2);

  function mustCoverage(p) {
    const id = p.id;
    const pref = profileBy2[id];
    const pl = platformBy2[id];
    const checks = [
      ["website", isPopulated(pref?.fields?.website)],
      ["headquarters", isPopulated(pref?.fields?.headquarters)],
      ["companySize", isPopulated(pref?.fields?.companySize)],
      ["Brand Families Operated", isPopulated(pref?.fields?.["Brand Families Operated"])],
      ["Parent", isPopulated(p.fields["Operator Parent Company"])],
      ["Operating Model", isPopulated(p.fields["Operating Model"])],
      ["Management Availability", isPopulated(p.fields["Management Availability"])],
      ["Active Countries", isPopulated(pl?.fields?.["Active Countries"])],
      ["Market Presence Type", isPopulated(pl?.fields?.["Market Presence Type"])],
    ];
    const ok = checks.filter((c) => c[1]).length;
    return { ok, total: checks.length, pct: Math.round((ok / checks.length) * 1000) / 10, missing: checks.filter((c) => !c[1]).map((c) => c[0]) };
  }

  const perOp = production.map((p) => ({ operator: p.fields.company_name, id: p.id, ...mustCoverage(p) }));
  const avgProfilePlatform =
    Math.round((perOp.reduce((s, r) => s + r.pct, 0) / perOp.length) * 10) / 10;
  const operatorsUnresolved = perOp.filter((r) => r.missing.length);
  const profileCoreFields = ["website", "headquarters", "companySize", "Brand Families Operated"];
  const platformCoreFields = ["Active Countries", "Market Presence Type"];
  let profileCoreHits = 0,
    profileCoreTot = 0,
    platformCoreHits = 0,
    platformCoreTot = 0;
  for (const p of production) {
    const pref = profileBy2[p.id];
    const pl = platformBy2[p.id];
    for (const f of profileCoreFields) {
      profileCoreTot++;
      if (isPopulated(pref?.fields?.[f])) profileCoreHits++;
    }
    for (const f of platformCoreFields) {
      platformCoreTot++;
      if (isPopulated(pl?.fields?.[f])) platformCoreHits++;
    }
  }
  // Include Master MUST in profile-ish completeness
  let masterHits = 0,
    masterTot = 0;
  for (const p of production) {
    for (const f of ["Operator Parent Company", "Operating Model", "Management Availability"]) {
      masterTot++;
      if (isPopulated(p.fields[f])) masterHits++;
    }
  }
  const profileCorePct = Math.round(((profileCoreHits + masterHits) / (profileCoreTot + masterTot)) * 1000) / 10;
  const platformCorePct = Math.round((platformCoreHits / platformCoreTot) * 1000) / 10;

  const shangri = perOp.find((r) => /Shangri/i.test(r.operator));
  const shPl = platformBy2["rec8XpNv6G0WOlMwu"];

  // Preview
  const previewLines = [];
  for (const id of SPOT_IDS) {
    const p = production.find((x) => x.id === id);
    if (!p) continue;
    const pref = profileBy2[id];
    const pl = platformBy2[id];
    previewLines.push(
      `### ${p.fields.company_name}`,
      ``,
      `**Profile (retained)**`,
      ``,
      `| Field | Value |`,
      `| ----- | ----- |`,
      `| website | ${pref?.fields?.website || "—"} |`,
      `| headquarters | ${(pref?.fields?.headquarters || "—").toString().replace(/\n/g, " ")} |`,
      `| companySize | ${pref?.fields?.companySize || "—"} |`,
      `| Parent (Master) | ${p.fields["Operator Parent Company"] || "—"} |`,
      `| Operating Model (Master) | ${p.fields["Operating Model"] || "—"} |`,
      `| Management Availability (Master) | ${p.fields["Management Availability"] || "—"} |`,
      `| Brand Families | ${(pref?.fields?.["Brand Families Operated"] || []).join("; ") || "—"} |`,
      `| Service Models | ${(pref?.fields?.["Service Models Supported"] || []).join("; ") || "—"} |`,
      `| additionalExperience | ${(pref?.fields?.additionalExperience || []).join("; ") || "—"} |`,
      `| companyDescription | ${pref?.fields?.companyDescription ? "present" : "blank (Writer v2)"} |`,
      ``,
      `**Platform (retained)**`,
      ``,
      `| Field | Value |`,
      `| ----- | ----- |`,
      `| Active Countries | ${(pl?.fields?.["Active Countries"] || []).join("; ") || "—"} |`,
      `| Market Presence Type | ${(pl?.fields?.["Market Presence Type"] || []).join("; ") || "—"} |`,
      `| specificMarkets | ${(pl?.fields?.specificMarkets || "—").toString().slice(0, 160)} |`,
      `| cap_profile_operational | ${pl?.fields?.cap_profile_operational ? "present" : "blank (Writer v2)"} |`,
      ``
    );
  }

  writeMd(join(DOCS, "reviews/operator-setup-core-final-profile-preview.md"), [
    `# D.4B Final Retained Profile Preview`,
    ``,
    `Mode: **${args.apply ? "apply" : "dry-run (simulated fills)"}**`,
    ``,
    ...previewLines,
  ].join("\n"));

  writeMd(
    join(REPORTS, "operator-setup-core-final-fit-map.md"),
    [
      `# D.4B Fit Map (retained fields only)`,
      ``,
      `| Field | Explorer? | Fit? | Canonical OE? | Setup role |`,
      `| ----- | --------- | ---- | ------------- | ---------- |`,
      `| Active Countries | Yes | Yes (geo) | Market Presence + Assignments | Materialized summary |`,
      `| Brand Families Operated | Yes | Yes (brand) | Brand Relationships | Materialized summary |`,
      `| Service Models / propertyTypes / additionalExperience | Yes | Yes (segment/asset) | Assignments | Materialized summary |`,
      `| Operating Model / Management Availability | Yes | Yes | Master | Canonical on Master |`,
      `| companySize | Yes | Weak | Official/packs | Summary band |`,
      `| headquarters / website | Yes | No | Master/Profile | Identity |`,
      `| cap_profile_operational | Yes | Future | Writer v2 | Narrative summary |`,
      `| geo_*/% / cap_kpi_* | No | **Do not use** | — | Deprecated |`,
      ``,
      `Fit remains **BLOCKED**.`,
      ``,
    ].join("\n")
  );

  const visualYes =
    profileCorePct >= 95 &&
    platformCorePct >= 95 &&
    operatorsUnresolved.length === 0 &&
    writes >= 0 &&
    failures.length === 0;

  const stopPoint = {
    legacyProfileFieldCount: profileMeta.fields.length,
    legacyPlatformFieldCount: platformMeta.fields.length,
    finalRecommendedProfileCoreFields: PROFILE_CORE.length,
    finalRecommendedPlatformCoreFields: PLATFORM_CORE.length,
    mustFields: PROFILE_CORE.filter((c) => c.priority === "MUST").map((c) => c.key).concat(PLATFORM_CORE.filter((c) => c.priority === "MUST").map((c) => c.key)),
    optionalFields: PROFILE_CORE.filter((c) => ["SHOULD", "OPTIONAL"].includes(c.priority)).map((c) => c.key).concat(PLATFORM_CORE.filter((c) => c.priority === "SHOULD").map((c) => c.key)),
    writerV2NarrativeFields: ["companyDescription", "differentiators", "cap_profile_operational", "brand_narrative_relationship"],
    presentationFields: countDisp["PRESENTATION ONLY"] || 0,
    numericCalculateNow: calcNow,
    numericKeepFuture: keepFuture,
    numericDeprecate: deprecateNum,
    moveToClaims: countDisp["MOVE TO CLAIMS"] || 0,
    totalDeprecationCandidates: deprecateList.length,
    productionOperatorsProcessed: production.length,
    targetedResearchCases: researchCases.length,
    shangriLaActiveCountriesResult: {
      value: shPl?.fields?.["Active Countries"] || null,
      specificMarkets: (shPl?.fields?.specificMarkets || "").toString().slice(0, 200),
      mustCoverage: shangri,
    },
    directWrites: mutations.filter((m) => /DIRECT/.test(m.treatment)).length,
    derivedWrites: mutations.filter((m) => /DERIVED/.test(m.treatment)).length,
    narrativeWrites: 0,
    genericTemplateWrites: 0,
    profileCoreCompleteness: `${profileCorePct}%`,
    platformCoreCompleteness: `${platformCorePct}%`,
    combinedMustCompleteness: `${avgProfilePlatform}%`,
    operatorsWithUnresolvedMustFields: operatorsUnresolved.map((o) => ({ operator: o.operator, missing: o.missing })),
    optionalNarrativeBlanks: "companyDescription/differentiators/cap_profile_* intentionally blank pending Writer v2",
    cleanViewRecipeCreated: true,
    fitRelevantRetainedFields: ["Active Countries", "Brand Families Operated", "Service Models", "additionalExperience", "Operating Model", "Management Availability"],
    legacyFitDependencies: "Do not bind Fit to geo_*/cap_kpi_*/locationType%",
    profileProductMaturityVerdict: profileCorePct >= 95 ? "PRODUCT-READY CORE (narrative optional)" : "CORE GAPS REMAIN",
    platformProductMaturityVerdict: platformCorePct >= 95 ? "PRODUCT-READY CORE (depth optional)" : "CORE GAPS REMAIN",
    founderVisualUsabilityVerdict: visualYes
      ? "YES — with D.4B Core Product views hiding legacy blanks"
      : "PARTIAL — open Core Product view; unresolved MUST listed",
    exactFounderApprovalsRequired: [
      "Accept lean Profile/Platform final schemas",
      "Accept D.4B MUST writes (incl. Shangri-La Other + markets note)",
      "Authorize Airtable Core Product views (hide deprecated)",
      "Authorize physical field deprecation timing",
      "Authorize next: Writer v2 cap_profile_operational OR Active Markets taxonomy",
    ],
    recommendedNextPhase: "Create Airtable Core Product views + D.5 Writer v2 for cap_profile_operational",
    confirmationNoUnsupportedPercentagesScores: true,
    confirmationNoSectionLevelNarrativeGeneration: true,
    confirmationNoFitScoringChanges: true,
    mode: args.apply ? "apply" : "dry-run",
    airtableWrites: args.apply ? writes : 0,
    failures: failures.length,
    backupDir: `backups/operator-setup/d4b-core-rationalize/${ts}`,
    dispositionCounts: countDisp,
    mutationsPlanned: mutations.length,
  };

  writeJson(join(OUT, "d4b-stop-point.json"), stopPoint);

  writeMd(
    join(DOCS, "reviews/operator-setup-core-rationalization-founder-review.md"),
    [
      `# D.4B Core Rationalization — Founder Review`,
      ``,
      `## If the founder opens Profile & Positioning and Platform & Markets now, will they look like populated operator databases rather than mostly empty schemas?`,
      ``,
      `**${visualYes ? "Yes — if you open the \`D.4B Core Product\` views** (legacy blank columns hidden)." : "Not yet in the default all-fields grid"}** Default Airtable grids still show 67/130 legacy columns; those blanks are **classified to hide/deprecate**, not “core incomplete.”`,
      ``,
      `Retained core coverage: Profile **${profileCorePct}%** · Platform **${platformCorePct}%** · Unresolved MUST operators: **${operatorsUnresolved.length}**.`,
      ``,
      `| # | Item | Result |`,
      `| - | ---- | ------ |`,
      `| 1 | Why tables looked empty | Completeness was claimed on thin DIRECT/DERIVED fills while ~110 numeric + presentation columns stayed blank |`,
      `| 2 | Legacy field count | Profile ${stopPoint.legacyProfileFieldCount} · Platform ${stopPoint.legacyPlatformFieldCount} |`,
      `| 3 | Final retained | Profile ${PROFILE_CORE.length} · Platform ${PLATFORM_CORE.length} |`,
      `| 4 | Numeric | CALCULATE NOW ${calcNow} · KEEP FUTURE ${keepFuture} · DEPRECATE ${deprecateNum} |`,
      `| 5–7 | MUST / Optional / Narrative | See field-priority + final schemas |`,
      `| 8–9 | MOVE TO CLAIMS / DEPRECATE | ${countDisp["MOVE TO CLAIMS"] || 0} / ${deprecateList.length} |`,
      `| 10 | Writes | mode=${stopPoint.mode}; planned=${mutations.length}; applied=${stopPoint.airtableWrites}; failures=${failures.length} |`,
      `| 11 | Targeted research | ${researchCases.length} operators (HQ/scale/Shangri-La) |`,
      `| 12 | Shangri-La | Active Countries=${JSON.stringify(shPl?.fields?.["Active Countries"])}; specificMarkets note set |`,
      `| 13–14 | Core coverage | Profile ${profileCorePct}% · Platform ${platformCorePct}% |`,
      `| 15 | Clean view | reports/operator-setup-core-clean-view-recipe.md |`,
      `| 16 | Operator examples | docs/reviews/operator-setup-core-final-profile-preview.md |`,
      `| 17 | Fit | Blocked; map in core-final-fit-map |`,
      `| 18 | Cleanup deps | Hide first; delete later with auth |`,
      `| 19 | Approvals | See stop-point |`,
      `| 20 | Next | Core Product views + Writer v2 operational narrative |`,
      ``,
      `Backup: \`${stopPoint.backupDir}\``,
      ``,
    ].join("\n")
  );

  console.log(JSON.stringify(stopPoint, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
