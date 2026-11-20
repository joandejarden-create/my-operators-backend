#!/usr/bin/env node
/**
 * V4 HI Hotel Contracts + HI Contract Milestones — corrected schema packet.
 *
 *   node scripts/ensure-hi-hotel-contracts-milestones-v4.mjs --dry-run
 *   node scripts/ensure-hi-hotel-contracts-milestones-v4.mjs --apply  # REQUIRES HIF_SCHEMA_APPLY=1
 *
 * STOP: design approval alone is insufficient for --apply.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import {
  CANONICAL_BASE_ID,
  TABLE_IDS,
  PUBLICATION_STATE_DEFAULT,
  buildContractCreateFields,
  buildContractSelfLinkFields,
  buildMilestoneCreateFields,
  buildMilestoneSelfLinkFields,
  buildAdditiveInventoryFields,
  buildFindingsLinkFields,
  buildApplySequence,
  expectedLinkGraph,
  validateBlueprintConsistency,
} from "../lib/hotel-intelligence/sec-edgar/hi-hotel-contracts-milestones-schema-v4.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
for (const p of ["C:/Dev/dealality/my-operators-backend/.env", path.join(ROOT, ".env")]) {
  if (fs.existsSync(p)) dotenv.config({ path: p, override: false });
}

const APPLY = process.argv.includes("--apply");
const DRY = !APPLY || process.argv.includes("--dry-run");
const OUT = path.join(
  ROOT,
  "reports/sec-edgar-contract-timing-v4/CORRECTED_SCHEMA_APPLY_PACKET.json",
);
const OUT_LEGACY = path.join(ROOT, "reports/sec-edgar-contract-timing-v4/SCHEMA_APPLY_PACKET.json");

function token() {
  return (process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || "").trim();
}

async function meta(pathname) {
  const res = await fetch(`https://api.airtable.com/v0/meta/bases/${CANONICAL_BASE_ID}${pathname}`, {
    headers: { Authorization: `Bearer ${token()}` },
  });
  return { ok: res.ok, status: res.status, json: await res.json().catch(() => ({})) };
}

function fieldLinkTarget(field) {
  return field?.options?.linkedTableId || null;
}

function simulateIdempotentReapply(live, proposed) {
  const already = [];
  const pending = [];
  if (live.contractsExists) already.push("HI Hotel Contracts");
  else pending.push("CREATE HI Hotel Contracts");
  if (live.milestonesExists) already.push("HI Contract Milestones");
  else pending.push("CREATE HI Contract Milestones");
  for (const f of proposed.addInventoryFields) {
    if (live.inventoryFields.has(f)) already.push(`Inventory.${f}`);
    else pending.push(`ADD Inventory.${f}`);
  }
  for (const f of proposed.addFindingsFields) {
    if (live.findingsFields.has(f)) already.push(`Findings.${f}`);
    else pending.push(`ADD Findings.${f}`);
  }
  return {
    wouldNoOpIfReapplied: pending.length === 0,
    alreadyPresent: already,
    stillPending: pending,
  };
}

async function main() {
  const blueprint = validateBlueprintConsistency();
  if (!blueprint.ok) {
    console.log(JSON.stringify({ ok: false, error: "BLUEPRINT_INCONSISTENT", blueprint }, null, 2));
    process.exitCode = 1;
    return;
  }

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
          note: "Corrected packet requires explicit founder Meta API authorization (HIF_SCHEMA_APPLY=1).",
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
    console.log(
      JSON.stringify({ ok: false, error: "META_READ_FAILED", status: tablesRes.status }, null, 2),
    );
    process.exitCode = 1;
    return;
  }

  const tables = tablesRes.json.tables || [];
  const byName = Object.fromEntries(tables.map((t) => [t.name, t]));
  const byId = Object.fromEntries(tables.map((t) => [t.id, t]));
  const contracts = byName["HI Hotel Contracts"];
  const milestones = byName["HI Contract Milestones"];
  const inv = byName["HI Hotel Discovery Inventory"] || byId[TABLE_IDS.inventory];
  const findings = byName["HI Discovery Findings"] || byId[TABLE_IDS.findings];
  const hpc = byName["Hotel Property Census"] || byId[TABLE_IDS.hpc];
  const orgs = byName["HI Organizations"] || byId[TABLE_IDS.organizations];

  const invFields = new Set((inv?.fields || []).map((f) => f.name));
  const findFields = new Set((findings?.fields || []).map((f) => f.name));
  const inventoryId = inv?.id || TABLE_IDS.inventory;
  const findingsId = findings?.id || TABLE_IDS.findings;
  const hpcId = hpc?.id || TABLE_IDS.hpc;
  const orgsId = orgs?.id || TABLE_IDS.organizations;

  const contractCreate = buildContractCreateFields({ inventoryId, hpcId, orgsId });
  const sequence = buildApplySequence({ inventoryId, hpcId, orgsId, findingsId });
  const linkGraph = expectedLinkGraph();

  // Live link-target verification when tables already exist
  const liveLinkChecks = [];
  if (contracts) {
    for (const edge of linkGraph.filter((e) => e.from === "HI Hotel Contracts")) {
      const fld = (contracts.fields || []).find((f) => f.name === edge.field);
      const targetId = fieldLinkTarget(fld);
      const targetName = targetId ? byId[targetId]?.name || targetId : null;
      liveLinkChecks.push({
        edge: `${edge.from}.${edge.field} → ${edge.to}`,
        present: Boolean(fld),
        type: fld?.type || null,
        linkedTableId: targetId,
        linkedTableName: targetName,
        ok: Boolean(fld) && fld.type === "multipleRecordLinks" && targetName === edge.to,
      });
    }
  }
  if (milestones) {
    for (const edge of linkGraph.filter((e) => e.from === "HI Contract Milestones")) {
      const fld = (milestones.fields || []).find((f) => f.name === edge.field);
      const targetId = fieldLinkTarget(fld);
      const targetName = targetId ? byId[targetId]?.name || targetId : null;
      liveLinkChecks.push({
        edge: `${edge.from}.${edge.field} → ${edge.to}`,
        present: Boolean(fld),
        type: fld?.type || null,
        linkedTableId: targetId,
        linkedTableName: targetName,
        ok: Boolean(fld) && fld.type === "multipleRecordLinks" && targetName === edge.to,
      });
    }
  }

  const addInventory = buildAdditiveInventoryFields()
    .filter((f) => !invFields.has(f.name))
    .map((f) => f.name);
  const addFindings = ["Linked Contract", "Linked Milestone"].filter((n) => !findFields.has(n));

  const live = {
    contractsExists: Boolean(contracts),
    milestonesExists: Boolean(milestones),
    contractsTableId: contracts?.id || null,
    milestonesTableId: milestones?.id || null,
    inventoryId,
    findingsId,
    hpcId,
    orgsId,
    inventoryHasGeoClass: invFields.has("Geographic Processing Class"),
    inventoryHasContinent: invFields.has("Continent / Macro Region"),
    findingsHasLinkedContract: findFields.has("Linked Contract"),
    findingsHasLinkedMilestone: findFields.has("Linked Milestone"),
    inventoryFields: invFields,
    findingsFields: findFields,
  };

  const proposed = {
    createTables: [
      !contracts
        ? {
            name: "HI Hotel Contracts",
            fieldCountOnCreate: contractCreate.length,
            fieldCountComplete: sequence.totals.contractFieldsComplete,
            fieldsOnCreate: contractCreate.map((f) => ({
              name: f.name,
              type: f.type,
              linkedTableId: f.options?.linkedTableId || null,
            })),
            selfLinksAfterCreate: buildContractSelfLinkFields("PENDING_CONTRACTS_TABLE_ID").map(
              (f) => f.name,
            ),
          }
        : null,
      !milestones
        ? {
            name: "HI Contract Milestones",
            fieldCountOnCreate: sequence.totals.milestoneFieldsOnCreate,
            fieldCountComplete: sequence.totals.milestoneFieldsComplete,
            contractField: {
              name: "Contract",
              type: "multipleRecordLinks",
              linkedTable: "HI Hotel Contracts",
              required: true,
            },
            selfLinksAfterCreate: buildMilestoneSelfLinkFields("PENDING_MILESTONES_TABLE_ID").map(
              (f) => f.name,
            ),
            fieldsOnCreateNames: buildMilestoneCreateFields({
              contractsTableId: "PENDING_CONTRACTS_TABLE_ID",
              inventoryId,
              hpcId,
              findingsId,
            }).map((f) => f.name),
          }
        : null,
    ].filter(Boolean),
    addInventoryFields: addInventory,
    addFindingsFields: addFindings,
    selfLinksOnContracts: ["Supersedes Contract", "Superseded By Contract"],
    selfLinksOnMilestones: ["Superseded By Milestone"],
    contractLinkOnMilestones: {
      name: "Contract",
      type: "multipleRecordLinks",
      to: "HI Hotel Contracts",
      required: true,
    },
    publicationStateDefault: PUBLICATION_STATE_DEFAULT,
    publicationEnforcement: sequence.publicationState.enforcement,
  };

  const idempotency = simulateIdempotentReapply(live, proposed);

  const packet = {
    module: "HI_HOTEL_CONTRACTS_MILESTONES_SCHEMA_V4_CORRECTED",
    inspectedAt: new Date().toISOString(),
    baseId: CANONICAL_BASE_ID,
    mode: APPLY && !DRY ? "APPLY" : "DRY_RUN_CORRECTED_PACKET",
    schemaApplyAuthorized: String(process.env.HIF_SCHEMA_APPLY || "") === "1",
    priorPacketIssuesFixed: [
      "Supersedes Contract / Superseded By Contract now in complete blueprint (step 2)",
      "Superseded By Milestone now in complete blueprint (step 4)",
      "Milestone Contract explicitly typed as multipleRecordLinks → HI Hotel Contracts",
      "Field counts reconciled: contracts complete 28; milestones complete 19",
      "Findings Linked Contract / Linked Milestone remain mandatory additive links",
      "Publication State CUSTOMER_BLOCKED application-enforced for both tables",
    ],
    blueprintValidation: blueprint,
    linkGraph,
    liveLinkChecks,
    live: {
      contractsExists: live.contractsExists,
      milestonesExists: live.milestonesExists,
      contractsTableId: live.contractsTableId,
      milestonesTableId: live.milestonesTableId,
      inventoryId: live.inventoryId,
      findingsId: live.findingsId,
      hpcId: live.hpcId,
      orgsId: live.orgsId,
      inventoryHasGeoClass: live.inventoryHasGeoClass,
      inventoryHasContinent: live.inventoryHasContinent,
      findingsHasLinkedContract: live.findingsHasLinkedContract,
      findingsHasLinkedMilestone: live.findingsHasLinkedMilestone,
      verifiedExistingTables: {
        "Hotel Property Census": Boolean(hpc),
        "HI Organizations": Boolean(orgs),
        "HI Hotel Discovery Inventory": Boolean(inv),
        "HI Discovery Findings": Boolean(findings),
      },
    },
    proposed,
    applySequence: sequence,
    exactMetaPayloads: {
      createContractsTable: {
        name: "HI Hotel Contracts",
        description: "Stable global hotel agreement identity (ctr_…); research-only until promotion",
        fields: contractCreate,
      },
      addContractSelfLinks: {
        afterTableIdKnown: true,
        fields: buildContractSelfLinkFields("PENDING_CONTRACTS_TABLE_ID"),
      },
      createMilestonesTable: {
        name: "HI Contract Milestones",
        description: "Dated contractual events (ms_…); Publication State required",
        fields: buildMilestoneCreateFields({
          contractsTableId: "PENDING_CONTRACTS_TABLE_ID",
          inventoryId,
          hpcId,
          findingsId,
        }),
        note: "Replace PENDING_CONTRACTS_TABLE_ID with live Contracts table id before apply",
      },
      addMilestoneSelfLinks: {
        fields: buildMilestoneSelfLinkFields("PENDING_MILESTONES_TABLE_ID"),
      },
      addInventoryFields: buildAdditiveInventoryFields().filter((f) => !invFields.has(f.name)),
      addFindingsLinkFields: {
        requires: ["contractsTableId", "milestonesTableId"],
        fieldsTemplate: buildFindingsLinkFields({
          contractsTableId: "PENDING_CONTRACTS_TABLE_ID",
          milestonesTableId: "PENDING_MILESTONES_TABLE_ID",
        }),
      },
    },
    idempotencySimulation: idempotency,
    stop: {
      schemaApply: "REQUIRES_EXPLICIT_FOUNDER_META_API_AUTHORIZATION_OF_CORRECTED_PACKET",
      hpcCreate: "REQUIRES_PER_ROW_FOUNDER_MANIFEST_APPROVAL",
      fullMigration: "BLOCKED_UNTIL_RESEARCH_MODE_PILOT_PASSES",
    },
    fieldBlueprints: {
      contractsComplete: blueprint.contractFieldNames,
      milestonesComplete: blueprint.milestoneFieldNames,
      contractsCount: blueprint.counts.contractsComplete,
      milestonesCount: blueprint.counts.milestonesComplete,
    },
  };

  // Strip Sets for JSON
  delete packet.live?.inventoryFields;
  delete packet.live?.findingsFields;

  fs.mkdirSync(path.dirname(OUT), { recursive: true });
  fs.writeFileSync(OUT, JSON.stringify(packet, null, 2));
  fs.writeFileSync(OUT_LEGACY, JSON.stringify(packet, null, 2));
  console.log(
    JSON.stringify(
      {
        ok: true,
        out: OUT,
        also: OUT_LEGACY,
        fieldCounts: packet.fieldBlueprints,
        linkGraphEdges: linkGraph.length,
        liveTablesExist: {
          contracts: live.contractsExists,
          milestones: live.milestonesExists,
        },
        schemaApplyAuthorized: packet.schemaApplyAuthorized,
        stop: packet.stop.schemaApply,
      },
      null,
      2,
    ),
  );

  if (APPLY && !DRY) {
    console.log(
      JSON.stringify(
        {
          ok: false,
          error: "APPLY_NOT_EXECUTED_STOP_FOR_FOUNDER_CORRECTED_PACKET_REVIEW",
        },
        null,
        2,
      ),
    );
    process.exitCode = 2;
  }
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
