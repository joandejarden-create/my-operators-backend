/**
 * Seed Arbor Lodging pilot source candidates into Partner Intelligence - Source Library.
 *
 *   node scripts/seed-arbor-pilot-sources.mjs
 *   node scripts/seed-arbor-pilot-sources.mjs --apply
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  MAP_PARTNER_SOURCE,
  PARTNER_INTELLIGENCE_TABLES,
} from "../api/lib/partner-intelligence-field-map.js";
import {
  PILOT_OPERATORS,
  PILOT_OPERATOR_SOURCE_CANDIDATES,
} from "../api/lib/partner-intelligence-explorer-field-registry.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const APPLY = process.argv.includes("--apply");

const OPERATOR_ID = PILOT_OPERATORS.arborLodging.recordId;
const TABLE =
  process.env.PARTNER_INTELLIGENCE_SOURCE_TABLE_ID ||
  PARTNER_INTELLIGENCE_TABLES.sourceLibrary;

async function listExistingByOperator(baseId, apiKey, operatorLinkName) {
  const formula = encodeURIComponent(
    `AND({${MAP_PARTNER_SOURCE.profileType}} = 'Operator', {${MAP_PARTNER_SOURCE.operator}} = '${operatorLinkName.replace(/'/g, "\\'")}')`
  );
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(TABLE)}?filterByFormula=${formula}&pageSize=100`;
  const res = await fetch(url, {
    headers: { Authorization: `Bearer ${apiKey}` },
  });
  const json = await res.json();
  if (!res.ok) throw new Error(json.error?.message || "List failed");
  return json.records || [];
}

function buildFields(candidate) {
  return {
    [MAP_PARTNER_SOURCE.profileType]: "Operator",
    [MAP_PARTNER_SOURCE.operator]: [OPERATOR_ID],
    [MAP_PARTNER_SOURCE.sourceTitle]: candidate.sourceTitle,
    [MAP_PARTNER_SOURCE.sourceUrl]: candidate.sourceUrl,
    [MAP_PARTNER_SOURCE.sourceType]: candidate.sourceType,
    [MAP_PARTNER_SOURCE.sourceOrigin]: candidate.sourceOrigin,
    [MAP_PARTNER_SOURCE.sourceQuality]: candidate.suggestedQuality || "Medium",
    [MAP_PARTNER_SOURCE.status]: "Found",
    [MAP_PARTNER_SOURCE.verifiedSource]: "No",
    [MAP_PARTNER_SOURCE.visibility]: "Public",
    [MAP_PARTNER_SOURCE.approvedForExtraction]: "No",
    [MAP_PARTNER_SOURCE.approvedForExplorerUse]: "No",
    [MAP_PARTNER_SOURCE.region]: PILOT_OPERATORS.arborLodging.region,
    [MAP_PARTNER_SOURCE.captureDate]: new Date().toISOString().slice(0, 10),
    [MAP_PARTNER_SOURCE.notes]: "Seeded by seed-arbor-pilot-sources.mjs — review before extraction.",
  };
}

async function main() {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("Set AIRTABLE_BASE_ID and AIRTABLE_API_KEY");

  const candidates = PILOT_OPERATOR_SOURCE_CANDIDATES.arborLodging || [];
  const operatorLinkName = PILOT_OPERATORS.arborLodging.companyName;
  const existing = await listExistingByOperator(baseId, apiKey, operatorLinkName);
  const existingTitles = new Set(
    existing.map((r) => String(r.fields?.[MAP_PARTNER_SOURCE.sourceTitle] || "").trim())
  );

  const report = {
    generatedAt: new Date().toISOString(),
    mode: APPLY ? "apply" : "dry-run",
    operatorId: OPERATOR_ID,
    table: TABLE,
    created: [],
    skipped: [],
  };

  for (const candidate of candidates) {
    if (existingTitles.has(candidate.sourceTitle)) {
      report.skipped.push({ sourceTitle: candidate.sourceTitle, reason: "already_exists" });
      console.log("SKIP (exists)", candidate.sourceTitle);
      continue;
    }

    const fields = buildFields(candidate);
    if (!APPLY) {
      report.created.push({ sourceTitle: candidate.sourceTitle, fields });
      console.log("WOULD CREATE", candidate.sourceTitle);
      continue;
    }

    const res = await fetch(`https://api.airtable.com/v0/${baseId}/${encodeURIComponent(TABLE)}`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ fields, typecast: true }),
    });
    const json = await res.json();
    if (!res.ok) {
      console.error("CREATE FAILED", candidate.sourceTitle, json);
      report.skipped.push({ sourceTitle: candidate.sourceTitle, reason: json.error?.message || "failed" });
      continue;
    }
    report.created.push({ id: json.id, sourceTitle: candidate.sourceTitle });
    console.log("CREATED", json.id, candidate.sourceTitle);
  }

  const outPath = path.join(ROOT, "reports", "seed-arbor-pilot-sources.json");
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
  console.log("Wrote", outPath);
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
