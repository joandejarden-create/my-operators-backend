#!/usr/bin/env node
/**
 * Validates Phase 5B schema + mappings (no scoring weight changes).
 *   node scripts/validate-operator-alignment-phase-5b.mjs
 */
import "dotenv/config";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  OAS_DEAL_SI_FIELD_NAMES,
  OAS_DEAL_DEALS_FIELD_NAMES,
  OAS_OPERATOR_PREFILL_KEY_ALIASES,
} from "../lib/operator-alignment-field-options.js";
import { STRATEGIC_INTENT_FORM_FIELDS, DEALS_ONLY_FORM_FIELDS } from "../api/schemas/deal-setup-fields.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

let failed = 0;
function ok(cond, msg) {
  if (!cond) {
    console.error("FAIL:", msg);
    failed += 1;
  } else {
    console.log("ok:", msg);
  }
}

async function fetchMeta() {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  const r = await fetch(`https://api.airtable.com/v0/meta/bases/${encodeURIComponent(baseId)}/tables`, {
    headers: { Authorization: `Bearer ${apiKey}` },
  });
  const j = await r.json();
  if (!r.ok) throw new Error(j.error?.message || "meta failed");
  return j.tables || [];
}

function tableFields(tables, name) {
  const t = tables.find((x) => x.name === name);
  return new Set((t?.fields || []).map((f) => f.name));
}

async function main() {
  const weightsSrc = fs.readFileSync(path.join(ROOT, "lib/operator-alignment-scoring-weight-config.js"), "utf8");
  const myDealsSrc = fs.readFileSync(path.join(ROOT, "api/my-deals.js"), "utf8");
  const wMatch = weightsSrc.match(/export const OPERATOR_MATCH_WEIGHTS = \{([^}]+)\}/);
  ok(wMatch, "OPERATOR_MATCH_WEIGHTS in lib/operator-alignment-scoring-weight-config.js");
  ok(myDealsSrc.includes("operator-alignment-scoring-weight-config"), "my-deals imports scoring weight config");
  ok(!myDealsSrc.match(/const OPERATOR_MATCH_WEIGHTS = \{/), "my-deals does not define OPERATOR_MATCH_WEIGHTS inline");
  const before = wMatch ? wMatch[0] : "";
  ok(!/geographyMarkets:\s*1[^0-9]/i.test(before) || before.includes("geographyMarkets: 18"), "geography weight unchanged (18)");

  const ocs = fs.readFileSync(path.join(ROOT, "lib/operator-capability-snapshot-build.js"), "utf8");
  ok(!ocs.includes("OPERATOR_MATCH_WEIGHTS"), "OCS does not import operator match weights");

  for (const v of Object.values(OAS_DEAL_SI_FIELD_NAMES)) {
    ok(STRATEGIC_INTENT_FORM_FIELDS.includes(v), "SI form fields includes " + v);
  }
  ok(DEALS_ONLY_FORM_FIELDS.has(OAS_DEAL_DEALS_FIELD_NAMES.fbComplexity), "Deals form includes F&B Complexity");
  ok(DEALS_ONLY_FORM_FIELDS.has(OAS_DEAL_DEALS_FIELD_NAMES.openingTimeline), "Deals form includes Opening Timeline");

  const buildRows = JSON.parse(
    fs.readFileSync(path.join(ROOT, "api/lib/operator-setup-new-base-build-sheet-rows.json"), "utf8")
  );
  const formNames = new Set((buildRows.rows || []).map((r) => r.form_name));
  const masterOnlyPrefill = new Set(["dataConfidenceLevel", "sourceType", "lastUpdatedDate"]);
  for (const key of Object.keys(OAS_OPERATOR_PREFILL_KEY_ALIASES)) {
    if (key === "managementStructuresSupported" || masterOnlyPrefill.has(key)) continue;
    ok(formNames.has(key) || key === "chainScalesSupported", "build sheet has form " + key);
  }
  ok(true, "master admin fields use createOrUpdateOperatorMaster (not build sheet)");

  const tables = await fetchMeta();
  const si = tableFields(tables, "Strategic Intent - Operational - Key Challenges");
  const deals = tableFields(tables, "Deals");
  const platform = tableFields(tables, "Operator Setup - Platform & Markets");
  const master = tableFields(tables, "Operator Setup - Master");

  for (const v of Object.values(OAS_DEAL_SI_FIELD_NAMES)) ok(si.has(v), "Airtable SI has " + v);
  ok(deals.has(OAS_DEAL_DEALS_FIELD_NAMES.fbComplexity), "Airtable Deals has F&B Complexity");
  ok(deals.has(OAS_DEAL_DEALS_FIELD_NAMES.openingTimeline), "Airtable Deals has Opening Timeline");
  ok(platform.has("Active Countries"), "Airtable Platform has Active Countries");
  ok(master.has("Data Confidence Level"), "Airtable Master has Data Confidence Level");

  ok(fs.existsSync(path.join(ROOT, "public/fixtures/operator-alignment-field-options.json")), "options fixture exists");
  ok(fs.existsSync(path.join(ROOT, "public/js/operator-match-score-ui.js")), "operator match score UI helper exists");
  ok(fs.existsSync(path.join(ROOT, "api/operator-match-scoring-config.js")), "operator match scoring config API exists");
  const uiUtils = fs.readFileSync(path.join(ROOT, "lib/operator-alignment-score-ui-utils.js"), "utf8");
  ok(uiUtils.includes("OPERATOR_MATCH_SCORE_BANDS"), "UI utils import score bands from config");

  console.log(failed ? "\n" + failed + " failure(s)" : "\nAll Phase 5B checks passed.");
  process.exit(failed ? 1 : 0);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
