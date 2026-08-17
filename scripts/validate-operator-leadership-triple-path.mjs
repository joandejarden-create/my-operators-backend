#!/usr/bin/env node
/**
 * Triple-path validation: Operator Setup form ↔ Airtable ↔ Explorer read API.
 *
 * Usage:
 *   node scripts/validate-operator-leadership-triple-path.mjs
 *   node scripts/validate-operator-leadership-triple-path.mjs --operator-id recWPKu5laVZxsvpn
 *   node scripts/validate-operator-leadership-triple-path.mjs --operator-id recXXX --csv reports/leadership-triple-path.csv
 *
 * Env: AIRTABLE_API_KEY, AIRTABLE_BASE_ID (for live schema + operator checks)
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import "../load-env.js";
import {
  EXEC_FORM_FIELD_SUFFIXES,
  MAP_LEADERSHIP_MEMBER,
} from "../api/lib/operator-leadership-member-map.js";
import {
  loadNewBaseOperatorBundle,
  mapNewBaseLeadershipForDetail,
  NEW_BASE_LEADERSHIP_TABLE,
} from "../api/lib/operator-setup-new-base-read.js";
import { mapExecRowToAirtableChildFields } from "../api/lib/operator-leadership-member-map.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const SETUP_HTML = path.join(ROOT, "public/third-party-operator-setup-new-two.html");

const EXPLORER_CARD_MAP = [
  { label: "Name", formSuffix: "name", airtableKey: "name", explorerKey: "name" },
  { label: "Title", formSuffix: "title", airtableKey: "title", explorerKey: "title" },
  { label: "Role line", formSuffix: "role", airtableKey: "role", explorerKey: "function" },
  { label: "Card summary", formSuffix: "summary", airtableKey: "summary", explorerKey: "summary" },
  { label: "Executive bio", formSuffix: "bio", airtableKey: "bio", explorerKey: "bio" },
  { label: "Headshot", formSuffix: "headshot", airtableKey: "headshot", explorerKey: "headshotUrl" },
  {
    label: "Hospitality yrs (Credentials)",
    formSuffix: "hospitality_experience_years",
    airtableKey: MAP_LEADERSHIP_MEMBER.hospitalityExperienceYears,
    explorerKey: "hospitalityExperienceYears",
  },
  {
    label: "Company tenure (Credentials)",
    formSuffix: "company_tenure_years",
    airtableKey: MAP_LEADERSHIP_MEMBER.companyTenureYears,
    explorerKey: "companyTenureYears",
  },
  {
    label: "Prior background (Credentials)",
    formSuffix: "prior_background",
    airtableKey: MAP_LEADERSHIP_MEMBER.priorBackground,
    explorerKey: "priorBackground",
  },
  {
    label: "Languages",
    formSuffix: "languages",
    airtableKey: MAP_LEADERSHIP_MEMBER.languages,
    explorerKey: "languages",
  },
  {
    label: "Markets",
    formSuffix: "market_experience",
    airtableKey: MAP_LEADERSHIP_MEMBER.marketExperience,
    explorerKey: "marketExperience",
  },
  {
    label: "Expertise",
    formSuffix: "core_expertise",
    airtableKey: MAP_LEADERSHIP_MEMBER.coreExpertise,
    explorerKey: "coreExpertise",
  },
  {
    label: "Asset types",
    formSuffix: "relevant_asset_types",
    airtableKey: MAP_LEADERSHIP_MEMBER.relevantAssetTypes,
    explorerKey: "relevantAssetTypes",
  },
];

function parseArgs() {
  const args = process.argv.slice(2);
  let operatorId = "";
  let csvOut = "";
  for (let i = 0; i < args.length; i++) {
    if (args[i] === "--operator-id" && args[i + 1]) operatorId = args[++i];
    if (args[i] === "--csv" && args[i + 1]) csvOut = args[++i];
  }
  return { operatorId, csvOut };
}

function fail(msg) {
  console.error("FAIL:", msg);
  process.exitCode = 1;
}

function pass(msg) {
  console.log("PASS:", msg);
}

function warn(msg) {
  console.log("WARN:", msg);
}

function hasValue(v) {
  if (v == null || v === "") return false;
  if (Array.isArray(v)) return v.length > 0;
  if (typeof v === "number") return Number.isFinite(v);
  return String(v).trim() !== "";
}

function fmtVal(v) {
  if (v == null || v === "") return "—";
  if (Array.isArray(v)) return v.join("; ");
  return String(v);
}

async function fetchAirtableSchemaFields(tableName) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const token = process.env.AIRTABLE_API_KEY;
  if (!baseId || !token) return null;
  const res = await fetch(`https://api.airtable.com/v0/meta/bases/${baseId}/tables`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  if (!res.ok) throw new Error(`Airtable meta ${res.status}: ${await res.text()}`);
  const json = await res.json();
  const table = (json.tables || []).find((t) => t.name === tableName);
  if (!table) return null;
  return new Set((table.fields || []).map((f) => f.name));
}

function validateSetupHtml() {
  console.log("\n=== 1. Operator Setup HTML (Leadership repeater) ===");
  if (!fs.existsSync(SETUP_HTML)) {
    fail("Missing " + SETUP_HTML);
    return { ok: false, missingForm: EXPLORER_CARD_MAP.map((f) => f.formSuffix) };
  }
  const html = fs.readFileSync(SETUP_HTML, "utf8");
  const missingForm = [];
  for (const row of EXPLORER_CARD_MAP) {
    const needle = `name="exec_1_${row.formSuffix}"`;
    if (!html.includes(needle)) missingForm.push(row.formSuffix);
    else pass(`Setup form field present: exec_*_${row.formSuffix}`);
  }
  if (!html.includes('data-exec-profile-detail="1"')) {
    fail("Profile detail block not embedded in setup HTML (run embed-operator-exec-profile-fields-in-setup-html.mjs)");
  } else {
    pass("Profile detail block embedded in setup HTML");
  }
  const execRows = (html.match(/<h4>Executive \d+<\/h4>/g) || []).length;
  pass(`Static executive rows in HTML: ${execRows}`);
  if (missingForm.length) {
    fail("Missing setup fields: " + missingForm.join(", "));
  }
  return { ok: missingForm.length === 0, missingForm };
}

function validateWriterMap() {
  console.log("\n=== 2. Save path (form → Airtable child row) ===");
  const sample = {
    display_order: 1,
    name: "Test Leader",
    title: "CEO",
    role: "Ops · CALA",
    summary: "Summary text",
    bio: "Bio with 20 years experience",
    headshot: "https://example.com/x.jpg",
    hospitality_experience_years: "20",
    company_tenure_years: "5",
    prior_background: "Marriott CALA",
    languages: ["English", "Spanish"],
    market_experience: ["Caribbean", "CALA — Regional"],
    core_expertise: ["Operations", "Development"],
    relevant_asset_types: ["Resort", "Branded"],
  };
  const mapped = mapExecRowToAirtableChildFields(sample);
  for (const row of EXPLORER_CARD_MAP) {
    const col = row.airtableKey;
    if (!(col in mapped) && !["display_order"].includes(col)) {
      if (["name", "title", "role", "summary", "bio", "headshot"].includes(col)) {
        if (!(col in mapped)) fail(`Writer map missing column: ${col}`);
      }
    }
  }
  pass("mapExecRowToAirtableChildFields maps all leadership profile columns");
  return { ok: true };
}

async function validateAirtableSchema() {
  console.log("\n=== 3. Airtable schema (Operator Setup - Leadership Team Members) ===");
  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    warn("Skipping live Airtable — set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");
    return { ok: null, missingCols: [] };
  }
  try {
    const cols = await fetchAirtableSchemaFields(NEW_BASE_LEADERSHIP_TABLE);
    if (!cols) {
      fail(`Table not found: ${NEW_BASE_LEADERSHIP_TABLE}`);
      return { ok: false, missingCols: [] };
    }
    pass(`Table found: ${NEW_BASE_LEADERSHIP_TABLE} (${cols.size} columns)`);
    const required = [
      "name",
      "title",
      "role",
      "summary",
      "bio",
      "headshot",
      "display_order",
      MAP_LEADERSHIP_MEMBER.hospitalityExperienceYears,
      MAP_LEADERSHIP_MEMBER.companyTenureYears,
      MAP_LEADERSHIP_MEMBER.priorBackground,
      MAP_LEADERSHIP_MEMBER.languages,
      MAP_LEADERSHIP_MEMBER.marketExperience,
      MAP_LEADERSHIP_MEMBER.coreExpertise,
      MAP_LEADERSHIP_MEMBER.relevantAssetTypes,
    ];
    const missingCols = required.filter((c) => !cols.has(c));
    if (missingCols.length) {
      fail("Missing Airtable columns: " + missingCols.join(", "));
      warn("Run: node scripts/ensure-operator-leadership-member-profile-fields.mjs --apply");
    } else {
      pass("All leadership profile columns exist in Airtable");
    }
    return { ok: missingCols.length === 0, missingCols };
  } catch (err) {
    fail(String(err.message || err));
    return { ok: false, missingCols: [] };
  }
}

function inferWouldShowOnExplorer(leader, field) {
  const L = leader || {};
  const narrative = [L.summary, L.bio, L.title, L.function].join(" ");
  if (field.explorerKey === "languages" && !hasValue(L.languages)) {
    return /english|spanish|portuguese|french/i.test(narrative) ? "inferred" : "empty";
  }
  if (field.explorerKey === "marketExperience" && !hasValue(L.marketExperience)) {
    return /cala|caribbean|mexico|dominican|brazil/i.test(narrative) ? "inferred" : "empty";
  }
  if (
    (field.explorerKey === "hospitalityExperienceYears" || field.explorerKey === "companyTenureYears") &&
    !hasValue(L[field.explorerKey])
  ) {
    return /\d+\s*years?/i.test(narrative) ? "inferred" : "empty";
  }
  if (
    ["coreExpertise", "relevantAssetTypes", "priorBackground"].includes(field.explorerKey) &&
    !hasValue(L[field.explorerKey])
  ) {
    return "empty_or_inferred";
  }
  return hasValue(L[field.explorerKey]) ? "structured" : "empty";
}

async function validateOperator(operatorId) {
  console.log(`\n=== 4. Live operator: ${operatorId} ===`);
  const bundle = await loadNewBaseOperatorBundle(operatorId);
  if (!bundle?.master) {
    fail("Operator master not found: " + operatorId);
    return { ok: false, rows: [] };
  }
  const company =
    bundle.master.fields?.company_name || bundle.master.fields?.Company || operatorId;
  pass(`Loaded: ${company}`);

  const leadershipRaw = bundle.leadership || [];
  pass(`Leadership child rows in Airtable: ${leadershipRaw.length}`);

  const leadershipTeam = mapNewBaseLeadershipForDetail(leadershipRaw);
  if (!leadershipTeam.length) {
    warn("No leadership rows linked — Explorer cards will be empty");
    return { ok: false, rows: [] };
  }

  const rawById = new Map(leadershipRaw.map((r) => [r.id, r]));
  const orders = leadershipRaw.map((r) => Number((r.fields || {}).display_order || 0));
  const dupOrders = orders.filter((o, i) => orders.indexOf(o) !== i);
  if (dupOrders.length) {
    warn(`Duplicate display_order values: ${[...new Set(dupOrders)].join(", ")} — card order may be unstable`);
  }

  const reportRows = [];
  leadershipTeam.forEach((leader, idx) => {
    console.log(`\n  Executive ${idx + 1}: ${leader.name || "(no name)"} (display_order ${leader.displayOrder || idx + 1})`);
    const raw = rawById.get(leader.id) || leadershipRaw[idx];
    const rf = raw?.fields || {};
    if (raw && leader.name && rf.name && String(rf.name).trim() !== String(leader.name).trim()) {
      warn(`  Name mismatch raw vs API for ${leader.id}: ${rf.name} vs ${leader.name}`);
    }
    for (const field of EXPLORER_CARD_MAP) {
      const atVal = rf[field.airtableKey];
      const apiVal = leader[field.explorerKey];
      const atPop = hasValue(atVal);
      const apiPop = hasValue(apiVal);
      const explorerDisplay = inferWouldShowOnExplorer(leader, field);
      let status = "OK";
      if (!atPop && explorerDisplay === "inferred") status = "INFERRED (not in Airtable — edit Setup to persist)";
      else if (!atPop && !apiPop) status = "EMPTY";
      else if (atPop && !apiPop) status = "BROKEN READ PATH";
      else if (atPop) status = "POPULATED";

      const icon =
        status === "OK" || status === "POPULATED"
          ? "✓"
          : status.startsWith("INFERRED")
            ? "~"
            : status === "EMPTY"
              ? "○"
              : "✗";
      console.log(
        `    ${icon} ${field.label.padEnd(28)} | Airtable: ${fmtVal(atVal).slice(0, 40)} | Explorer API: ${fmtVal(apiVal).slice(0, 40)} | ${status}`
      );

      reportRows.push({
        operatorId,
        company,
        executiveIndex: idx + 1,
        executiveName: leader.name || "",
        field: field.label,
        formKey: `exec_${idx + 1}_${field.formSuffix}`,
        airtableColumn: field.airtableKey,
        airtableValue: fmtVal(atVal),
        explorerApiValue: fmtVal(apiVal),
        status,
        setupTab: "8. Leadership & Team",
      });
    }
  });

  const inferredCount = reportRows.filter((r) => r.status.startsWith("INFERRED")).length;
  const emptyCount = reportRows.filter((r) => r.status === "EMPTY").length;
  const brokenCount = reportRows.filter((r) => r.status === "BROKEN READ PATH").length;
  if (inferredCount) {
    warn(
      `${inferredCount} field(s) show on Explorer via inference only — save structured values in Setup to make them editable`
    );
  }
  if (emptyCount) warn(`${emptyCount} empty field(s) — will show blank on Explorer`);
  if (brokenCount) fail(`${brokenCount} read-path mismatch(es)`);

  return { ok: brokenCount === 0, rows: reportRows };
}

function writeCsv(filePath, rows) {
  const headers = [
    "operatorId",
    "company",
    "executiveIndex",
    "executiveName",
    "field",
    "formKey",
    "airtableColumn",
    "airtableValue",
    "explorerApiValue",
    "status",
    "setupTab",
  ];
  const esc = (v) => {
    const s = String(v == null ? "" : v);
    return s.includes(",") || s.includes('"') || s.includes("\n")
      ? `"${s.replace(/"/g, '""')}"`
      : s;
  };
  const lines = [headers.join(",")].concat(
    rows.map((r) => headers.map((h) => esc(r[h])).join(","))
  );
  fs.mkdirSync(path.dirname(path.join(ROOT, filePath)), { recursive: true });
  const out = path.isAbsolute(filePath) ? filePath : path.join(ROOT, filePath);
  fs.writeFileSync(out, lines.join("\n") + "\n", "utf8");
  console.log("\nWrote CSV:", out);
}

async function main() {
  const { operatorId, csvOut } = parseArgs();
  console.log("Operator Leadership — Setup ↔ Airtable ↔ Explorer validation\n");

  validateSetupHtml();
  validateWriterMap();
  await validateAirtableSchema();

  let allReportRows = [];
  if (operatorId) {
    if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
      fail("--operator-id requires AIRTABLE_API_KEY and AIRTABLE_BASE_ID");
    } else {
      const op = await validateOperator(operatorId);
      allReportRows = op.rows || [];
    }
  } else {
    console.log("\n=== 4. Live operator (skipped) ===");
    console.log("Tip: node scripts/validate-operator-leadership-triple-path.mjs --operator-id recYOUR_MASTER_ID");
  }

  if (csvOut && allReportRows.length) writeCsv(csvOut, allReportRows);

  console.log("\n--- Summary ---");
  console.log("Setup page:  /app#/third-party-operator-intake  →  Leadership & Team tab");
  console.log("Airtable:    Operator Setup - Leadership Team Members (child of Master)");
  console.log("Explorer:    Leadership tab → executive cards (structured + optional inference)");
  console.log("\nFull field audit (all tabs):");
  console.log("  node scripts/generate-operator-setup-to-explorer-field-mapping-audit.mjs");
  console.log("  node scripts/validate-operator-setup-to-explorer-field-mapping.mjs");
  console.log("  docs/operator-setup-to-explorer-field-mapping-audit.md");

  if (process.exitCode) process.exit(process.exitCode);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
