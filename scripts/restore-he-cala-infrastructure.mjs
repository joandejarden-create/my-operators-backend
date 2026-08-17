/**
 * Apply HE CALA Infrastructure & Data content to:
 *   - Operator Setup - Governance, Delivery & Diligence (scalar + JSON long-text fields)
 *   - Operator Setup - Infrastructure Platform (child table rows)
 *
 *   node scripts/restore-he-cala-infrastructure.mjs [masterRecordId] [path-to-fixture.json]
 *
 * Default master: recWPKu5laVZxsvpn
 * Default fixture: fixtures/operator-infrastructure-explorer-he-cala.json
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { randomUUID } from "crypto";
import "../load-env.js";
import {
  NEW_BASE_GOVERNANCE_TABLE,
  fetchRecordsLinkedToMaster,
  airtableFetchJson,
} from "../api/lib/operator-setup-new-base-read.js";
import { replaceOperatorInfrastructurePlatformRows } from "../api/lib/operator-setup-new-base-writer.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const DEFAULT_MASTER = "recWPKu5laVZxsvpn";
const DEFAULT_FIXTURE = path.join(ROOT, "fixtures", "operator-infrastructure-explorer-he-cala.json");

const JSON_GOVERNANCE_KEYS = [
  "infra_technology_stack_json",
  "infra_services_offered_json",
  "infra_data_domains_json",
  "infra_data_governance_json",
  "infra_analytics_support_json",
];

function parseArgs(argv) {
  const pos = argv.slice(2).filter((a) => !a.startsWith("--"));
  return {
    masterId: (pos[0] || "").trim() || DEFAULT_MASTER,
    fixturePath: (pos[1] || "").trim() || DEFAULT_FIXTURE,
  };
}

function enc(s) {
  return encodeURIComponent(s);
}

function buildGovernancePatch(raw) {
  const patch = {};
  for (const [key, value] of Object.entries(raw || {})) {
    if (value == null || value === "") continue;
    if (JSON_GOVERNANCE_KEYS.includes(key)) {
      patch[key] = Array.isArray(value) ? JSON.stringify(value) : String(value);
      continue;
    }
    patch[key] = value;
  }
  return patch;
}

async function patchGovernanceRecord(recordId, fields) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const url = `https://api.airtable.com/v0/${baseId}/${enc(NEW_BASE_GOVERNANCE_TABLE)}/${enc(recordId)}`;
  const { ok, status, json } = await airtableFetchJson(url, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ fields, typecast: true }),
  });
  if (!ok) {
    const msg = json?.error?.message || JSON.stringify(json);
    throw new Error(`Governance PATCH failed (${status}): ${msg}`);
  }
  return json;
}

function countPlatform(ip) {
  return {
    decisionSignals: (ip.decisionSignals || []).length,
    portfolioMetrics: (ip.portfolioMetrics || []).length,
    technologyStack: (ip.technologyStack || []).length,
    infrastructureServices: (ip.infrastructureServices || []).length,
    dataDomains: (ip.dataDomains || []).length,
    dataGovernance: (ip.dataGovernance || []).length,
    analyticsCapabilities: (ip.analyticsCapabilities || []).length,
    technologyMaturity: ip.technologyMaturity?.level ? 1 : 0,
  };
}

async function main() {
  const { masterId, fixturePath } = parseArgs(process.argv);
  if (!fs.existsSync(fixturePath)) {
    throw new Error(`Missing fixture: ${fixturePath}`);
  }
  const fixture = JSON.parse(fs.readFileSync(fixturePath, "utf8"));
  const governanceRaw = fixture.governanceFields || {};
  const platform = fixture.infrastructurePlatform;
  if (!platform || typeof platform !== "object") {
    throw new Error("Fixture missing infrastructurePlatform object");
  }

  const govRows = await fetchRecordsLinkedToMaster(NEW_BASE_GOVERNANCE_TABLE, masterId);
  if (!govRows.length) {
    throw new Error(`No Governance row linked to Master ${masterId}`);
  }
  const govId = govRows[0].id;
  const govPatch = buildGovernancePatch(governanceRaw);
  if (!Object.keys(govPatch).length) {
    throw new Error("Fixture has no governanceFields to patch");
  }

  await patchGovernanceRecord(govId, govPatch);
  const infraRes = await replaceOperatorInfrastructurePlatformRows(
    masterId,
    { infrastructurePlatform: platform },
    randomUUID()
  );

  console.log(
    JSON.stringify(
      {
        masterId,
        fixturePath,
        operatorName: fixture._meta?.operatorName || null,
        governanceRecordId: govId,
        governanceFieldCount: Object.keys(govPatch).length,
        platformRowCounts: countPlatform(platform),
        airtable: infraRes,
      },
      null,
      2
    )
  );
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
