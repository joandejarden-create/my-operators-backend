/**
 * Export Arbor Lodging (CALA) Operator Setup field inventory with suggested copy.
 * Output: scripts/arbor-cala-form-inventory.json + .csv
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import "../load-env.js";
import {
  loadNewBaseOperatorBundle,
  buildPrefillObjectFromNewBaseRows,
  mapNewBaseCaseStudiesForDetail,
  mapNewBaseDiligenceForDetail,
} from "../api/lib/operator-setup-new-base-read.js";
import {
  fetchThirdPartyOperatorPrefillContext,
  buildBrandProfilesFromPrefill,
  resolvePrefillBrandsToNames,
} from "../api/lib/build-third-party-operator-prefill.js";
import { normalizeOperatorSetupSelectPrefill } from "../api/lib/third-party-operator-select-prefill-normalize.js";
import { getSuggestionForRow } from "./arbor-cala-field-suggestions.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const HTML_PATH = path.join(__dirname, "..", "public", "third-party-operator-setup-new-two.html");
const MASTER_ID = process.argv[2] || "rectkHHTWMc6p4i63";

const TAB_LABELS = {
  0: "Company Profile",
  1: "Positioning & Snapshot",
  2: "Operating Platform",
  3: "Brand & Relationships",
  4: "Markets & Footprint",
  5: "Owner Value & Engagement",
  6: "Infrastructure & Data",
  7: "Risk & Compliance",
  8: "Leadership & Team",
  9: "Best Fit & Preferences",
  10: "Deal Terms",
  11: "Proof & Track Record",
  12: "Diligence",
};

function stripTags(s) {
  return String(s || "")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .replace(/&amp;/g, "&")
    .trim();
}

function fmtVal(v) {
  if (v == null) return "";
  if (Array.isArray(v)) return v.map((x) => (typeof x === "object" ? JSON.stringify(x) : String(x))).join(", ");
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
}

function rawValueForPrefill(prefill, name) {
  const v = prefill[name];
  if (v == null) return "";
  if (Array.isArray(v)) return v.map((x) => (typeof x === "string" ? x : String(x))).join(", ");
  if (typeof v === "boolean") return v ? "true" : "false";
  return String(v);
}

function csvEscape(s) {
  const t = String(s).replace(/\r\n/g, "\n").replace(/\r/g, "\n");
  if (/[",\n]/.test(t)) return '"' + t.replace(/"/g, '""') + '"';
  return t;
}

function preview(s, n = 220) {
  const t = fmtVal(s).replace(/\s+/g, " ").trim();
  if (!t) return "(empty)";
  return t.length > n ? t.slice(0, n) + "…" : t;
}

function parseFormFields(html) {
  const chunks = html.split(/<div class="form-section[^"]*" data-section="/);
  const rows = [];
  for (let i = 1; i < chunks.length; i++) {
    const chunk = chunks[i];
    const m = /^(\d+)/.exec(chunk);
    if (!m) continue;
    const sec = parseInt(m[1], 10);
    const tab = TAB_LABELS[sec] || `Section ${sec}`;
    const labelFor = new Map();
    const labelRe = /<label[^>]*\bfor="([^"]+)"[^>]*>([\s\S]*?)<\/label>/gi;
    let lm;
    while ((lm = labelRe.exec(chunk))) {
      labelFor.set(lm[1], stripTags(lm[2]));
    }
    const fieldRe = /<(input|textarea|select)\s+([^>]*?)>/gi;
    let fm;
    while ((fm = fieldRe.exec(chunk))) {
      const tag = fm[1].toLowerCase();
      const attrs = fm[2];
      const nameM = /\bname="([^"]+)"/.exec(attrs);
      if (!nameM) continue;
      const name = nameM[1];
      const typeM = /\btype="([^"]+)"/.exec(attrs);
      const type = typeM ? typeM[1].toLowerCase() : tag === "textarea" ? "textarea" : tag === "select" ? "select" : "text";
      if (type === "file" || type === "button" || type === "submit" || type === "reset") continue;
      if (type === "hidden" && /^bf_q_|^lead_kpi_|^displayLeadership/.test(name)) continue;
      const idM = /\bid="([^"]+)"/.exec(attrs);
      const id = idM ? idM[1] : "";
      let label = id ? labelFor.get(id) || "" : "";
      if (!label) label = name;
      rows.push({ tab, sec, name, label, type });
    }
  }
  return rows;
}

async function main() {
  const html = fs.readFileSync(HTML_PATH, "utf8");
  const formRows = parseFormFields(html);
  const bundle = await loadNewBaseOperatorBundle(MASTER_ID);
  if (!bundle || !bundle.master) {
    console.error("No bundle for", MASTER_ID);
    process.exit(1);
  }
  const { master, profile, platform, commercial, governance } = bundle;
  const ctx = await fetchThirdPartyOperatorPrefillContext();
  const brandNameById = new Map();
  for (const brec of ctx.brandBasicsRecords || []) {
    const bf = brec.fields || {};
    const nm = String(bf["Brand Name"] || "").trim();
    if (brec.id && nm) brandNameById.set(brec.id, nm);
  }
  const prefill = buildPrefillObjectFromNewBaseRows(master, profile, platform, commercial, governance);
  resolvePrefillBrandsToNames(prefill, brandNameById);
  prefill.caseStudiesDetail = mapNewBaseCaseStudiesForDetail(bundle.cases || []);
  prefill.ownerDiligenceQa = mapNewBaseDiligenceForDetail(bundle.diligence || []);
  normalizeOperatorSetupSelectPrefill(prefill);
  buildBrandProfilesFromPrefill(prefill, ctx.brandBasicsRecords || []);

  const out = [];
  const seen = new Set();
  for (const fr of formRows) {
    const key = fr.sec + "|" + fr.name + "|" + fr.type;
    if (seen.has(key)) continue;
    seen.add(key);
    const rawStr = rawValueForPrefill(prefill, fr.name);
    const isEmpty = rawStr.trim() === "";
    const { verdict, suggestedCopyPaste } = getSuggestionForRow({
      fieldName: fr.name,
      tab: fr.tab,
      isEmpty,
      rawValue: rawStr,
    });
    out.push({
      tab: fr.tab,
      section: fr.sec,
      fieldName: fr.name,
      label: fr.label.slice(0, 200),
      current: preview(rawStr, 400),
      rawValue: rawStr,
      isEmpty,
      verdict,
      suggestedCopyPaste,
    });
  }

  const outPath = path.join(__dirname, "arbor-cala-form-inventory.json");
  fs.writeFileSync(outPath, JSON.stringify(out, null, 2), "utf8");
  console.log("Wrote", outPath, "rows:", out.length);

  const csvPath = path.join(__dirname, "arbor-cala-form-inventory.csv");
  const csvHeader = ["tab", "fieldName", "label", "verdict", "currentAnswer", "suggestedCopyPaste"].join(",");
  const csvLines = out.map((r) =>
    [r.tab, r.fieldName, r.label, r.verdict, r.current, r.suggestedCopyPaste].map(csvEscape).join(",")
  );
  fs.writeFileSync(csvPath, [csvHeader, ...csvLines].join("\n"), "utf8");
  console.log("Wrote", csvPath);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
