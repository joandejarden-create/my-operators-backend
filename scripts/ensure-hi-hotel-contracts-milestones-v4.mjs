#!/usr/bin/env node
/**
 * V4 HI Hotel Contracts + HI Contract Milestones schema ensure.
 *
 *   node scripts/ensure-hi-hotel-contracts-milestones-v4.mjs --dry-run
 *   node scripts/ensure-hi-hotel-contracts-milestones-v4.mjs --apply   # REQUIRES HIF_SCHEMA_APPLY=1
 *
 * Fail-closed: design approval alone is insufficient for --apply.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
for (const p of ["C:/Dev/dealality/my-operators-backend/.env", path.join(ROOT, ".env")]) {
  if (fs.existsSync(p)) dotenv.config({ path: p, override: false });
}

const BASE = "appCCUsuGsE1ifoLk";
const APPLY = process.argv.includes("--apply");
const DRY = !APPLY || process.argv.includes("--dry-run");
const OUT = path.join(ROOT, "reports/sec-edgar-contract-timing-v4/SCHEMA_APPLY_PACKET.json");

const INVENTORY = "tblx77Nmh56h4xa9z";
const FINDINGS = "tblGjTWNm8G4UOF1e";
const HPC = "tbl9aY5ijiuIzzWam";
const ORGS = "tbliXEfUOWk32AN9y";

function token() {
  return (process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || "").trim();
}

async function meta(pathname) {
  const res = await fetch(`https://api.airtable.com/v0/meta/bases/${BASE}${pathname}`, {
    headers: { Authorization: `Bearer ${token()}` },
  });
  return { ok: res.ok, status: res.status, json: await res.json().catch(() => ({})) };
}

const CONTRACT_FIELDS = [
  { name: "Contract ID", type: "singleLineText" },
  { name: "Discovery Property", type: "multipleRecordLinks", options: { linkedTableId: INVENTORY } },
  { name: "HPC Hotel", type: "multipleRecordLinks", options: { linkedTableId: HPC } },
  {
    name: "Agreement Type",
    type: "singleSelect",
    options: {
      choices: ["FRANCHISE", "MANAGEMENT", "LEASE", "LICENSE", "OTHER"].map((name) => ({ name })),
    },
  },
  {
    name: "Agreement Role",
    type: "singleSelect",
    options: {
      choices: ["ORIGINAL", "AMENDMENT", "RENEWAL_EXTENSION", "TERMINATION_INSTRUMENT"].map((name) => ({
        name,
      })),
    },
  },
  { name: "Counterparty Org", type: "multipleRecordLinks", options: { linkedTableId: ORGS } },
  { name: "Counterparty Name Raw", type: "singleLineText" },
  { name: "Brand Family", type: "singleLineText" },
  { name: "Brand Label", type: "singleLineText" },
  { name: "Governing Law / Jurisdiction", type: "singleLineText" },
  { name: "Hotel Country", type: "singleLineText" },
  { name: "Hotel State / Region", type: "singleLineText" },
  { name: "Hotel City", type: "singleLineText" },
  {
    name: "Geographic Processing Class",
    type: "singleSelect",
    options: {
      choices: [
        "PRIORITY_HPC_ENRICHMENT",
        "GLOBAL_RESEARCH_STANDARD",
        "EXISTING_HPC_OPPORTUNISTIC",
      ].map((name) => ({ name })),
    },
  },
  {
    name: "Contract Status",
    type: "singleSelect",
    options: {
      choices: [
        "CURRENTLY_CORROBORATED",
        "HISTORICALLY_VERIFIED",
        "FUTURE_DATE_REQUIRES_REVALIDATION",
        "EXPIRED_STATUS_UNCONFIRMED",
        "SUPERSEDED",
        "TERMINATED_VERIFIED",
        "CONFLICTED",
      ].map((name) => ({ name })),
    },
  },
  { name: "Commencement Date", type: "date", options: { dateFormat: { name: "iso" } } },
  { name: "Scheduled Expiration Date", type: "date", options: { dateFormat: { name: "iso" } } },
  { name: "Term Years", type: "number", options: { precision: 0 } },
  { name: "Renewal Options Summary", type: "multilineText" },
  { name: "Termination Rights Summary", type: "multilineText" },
  { name: "Primary Source URL", type: "url" },
  { name: "Primary SEC Accession", type: "singleLineText" },
  {
    name: "Evidence Confidence",
    type: "singleSelect",
    options: { choices: ["HIGH", "MEDIUM", "LOW", "UNVERIFIED"].map((name) => ({ name })) },
  },
  {
    name: "Publication State",
    type: "singleSelect",
    options: {
      choices: ["CUSTOMER_BLOCKED", "INTERNAL_ONLY", "DO_NOT_PUBLISH"].map((name) => ({ name })),
    },
  },
  { name: "Schema Version", type: "singleLineText" },
  { name: "Notes / Quarantine", type: "multilineText" },
];

const MILESTONE_FIELDS = [
  { name: "Milestone ID", type: "singleLineText" },
  // Contract link added after Contracts table exists
  { name: "Discovery Property", type: "multipleRecordLinks", options: { linkedTableId: INVENTORY } },
  { name: "HPC Hotel", type: "multipleRecordLinks", options: { linkedTableId: HPC } },
  {
    name: "Milestone Type",
    type: "singleSelect",
    options: {
      choices: [
        "FRANCHISE_EXPIRATION",
        "MANAGEMENT_EXPIRATION",
        "COMMENCEMENT",
        "RENEWAL",
        "EXTENSION",
        "AMENDMENT_EFFECTIVE",
        "TERMINATION",
        "REFLAG",
        "OPTION_EXERCISE",
        "OTHER",
      ].map((name) => ({ name })),
    },
  },
  { name: "Milestone Date", type: "date", options: { dateFormat: { name: "iso" } } },
  {
    name: "Date Meaning Precision",
    type: "singleSelect",
    options: {
      choices: ["EXACT_DAY", "MONTH_YEAR", "YEAR_ONLY", "NARRATIVE"].map((name) => ({ name })),
    },
  },
  { name: "Reporting Date", type: "date", options: { dateFormat: { name: "iso" } } },
  {
    name: "Current Status Class",
    type: "singleSelect",
    options: {
      choices: [
        "CURRENTLY_CORROBORATED",
        "HISTORICALLY_VERIFIED",
        "FUTURE_DATE_REQUIRES_REVALIDATION",
        "EXPIRED_STATUS_UNCONFIRMED",
        "SUPERSEDED",
        "TERMINATED_VERIFIED",
        "CONFLICTED",
      ].map((name) => ({ name })),
    },
  },
  { name: "Status Corroborated", type: "checkbox", options: { color: "greenBright", icon: "check" } },
  { name: "Commercial Relevance Score", type: "number", options: { precision: 0 } },
  { name: "Source URL", type: "url" },
  { name: "SEC Accession", type: "singleLineText" },
  { name: "Evidence Snippet", type: "multilineText" },
  { name: "Related Finding", type: "multipleRecordLinks", options: { linkedTableId: FINDINGS } },
  {
    name: "Publication State",
    type: "singleSelect",
    options: {
      choices: ["CUSTOMER_BLOCKED", "INTERNAL_ONLY", "DO_NOT_PUBLISH"].map((name) => ({ name })),
    },
  },
  { name: "Schema Version", type: "singleLineText" },
  { name: "Notes", type: "multilineText" },
];

const ADDITIVE_INVENTORY = [
  {
    name: "Geographic Processing Class",
    type: "singleSelect",
    options: {
      choices: [
        "PRIORITY_HPC_ENRICHMENT",
        "GLOBAL_RESEARCH_STANDARD",
        "EXISTING_HPC_OPPORTUNISTIC",
      ].map((name) => ({ name })),
    },
  },
  {
    name: "Continent / Macro Region",
    type: "singleSelect",
    options: {
      choices: [
        "NORTH_AMERICA",
        "CENTRAL_AMERICA",
        "SOUTH_AMERICA",
        "CARIBBEAN",
        "EUROPE",
        "MIDDLE_EAST",
        "AFRICA",
        "ASIA",
        "OCEANIA",
        "OTHER_UNKNOWN",
      ].map((name) => ({ name })),
    },
  },
];

async function main() {
  if (!token()) {
    console.log(JSON.stringify({ ok: false, error: "NO_AIRTABLE_TOKEN" }, null, 2));
    process.exitCode = 1;
    return;
  }
  if (APPLY && String(process.env.HIF_SCHEMA_APPLY || "") !== "1") {
    console.log(
      JSON.stringify(
        {
          ok: false,
          error: "HIF_SCHEMA_APPLY_ENV_REQUIRED",
          note: "Design approval alone is insufficient. Set HIF_SCHEMA_APPLY=1 only after explicit founder Meta API authorization.",
        },
        null,
        2,
      ),
    );
    process.exitCode = 2;
    return;
  }

  const tablesRes = await meta("/tables");
  if (!tablesRes.ok) {
    console.log(JSON.stringify({ ok: false, error: "META_READ_FAILED", status: tablesRes.status }, null, 2));
    process.exitCode = 1;
    return;
  }
  const tables = tablesRes.json.tables || [];
  const byName = Object.fromEntries(tables.map((t) => [t.name, t]));
  const contractsExists = Boolean(byName["HI Hotel Contracts"]);
  const milestonesExists = Boolean(byName["HI Contract Milestones"]);
  const inv = byName["HI Hotel Discovery Inventory"];
  const findings = byName["HI Discovery Findings"];
  const invFields = new Set((inv?.fields || []).map((f) => f.name));
  const findFields = new Set((findings?.fields || []).map((f) => f.name));

  const packet = {
    inspectedAt: new Date().toISOString(),
    baseId: BASE,
    mode: APPLY && !DRY ? "APPLY" : "DRY_RUN_PACKET",
    schemaApplyAuthorized: String(process.env.HIF_SCHEMA_APPLY || "") === "1",
    live: {
      contractsExists,
      milestonesExists,
      inventoryId: inv?.id || INVENTORY,
      findingsId: findings?.id || FINDINGS,
      inventoryHasGeoClass: invFields.has("Geographic Processing Class"),
      inventoryHasContinent: invFields.has("Continent / Macro Region"),
      findingsHasLinkedContract: findFields.has("Linked Contract"),
      findingsHasLinkedMilestone: findFields.has("Linked Milestone"),
    },
    proposed: {
      createTables: [
        !contractsExists
          ? { name: "HI Hotel Contracts", fieldCount: CONTRACT_FIELDS.length }
          : null,
        !milestonesExists
          ? { name: "HI Contract Milestones", fieldCount: MILESTONE_FIELDS.length + 1 }
          : null,
      ].filter(Boolean),
      addInventoryFields: ADDITIVE_INVENTORY.filter((f) => !invFields.has(f.name)).map((f) => f.name),
      addFindingsFields: ["Linked Contract", "Linked Milestone"].filter((n) => !findFields.has(n)),
      selfLinksOnContracts: ["Supersedes Contract", "Superseded By Contract"],
      contractLinkOnMilestones: "Contract",
      publicationStateDefault: "CUSTOMER_BLOCKED",
    },
    stop: {
      schemaApply: "REQUIRES_EXPLICIT_FOUNDER_META_API_AUTHORIZATION",
      hpcCreate: "REQUIRES_PER_ROW_FOUNDER_MANIFEST_APPROVAL",
    },
    fieldBlueprints: {
      contracts: CONTRACT_FIELDS.map((f) => f.name),
      milestones: MILESTONE_FIELDS.map((f) => f.name),
    },
  };

  fs.mkdirSync(path.dirname(OUT), { recursive: true });
  fs.writeFileSync(OUT, JSON.stringify(packet, null, 2));
  console.log(JSON.stringify({ ok: true, out: OUT, packet }, null, 2));

  if (APPLY && !DRY) {
    console.log(JSON.stringify({ ok: false, error: "APPLY_NOT_EXECUTED_IN_THIS_RUN_STOP_FOR_FOUNDER" }, null, 2));
    process.exitCode = 2;
  }
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
