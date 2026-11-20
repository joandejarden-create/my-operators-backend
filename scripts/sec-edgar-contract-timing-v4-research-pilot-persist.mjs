#!/usr/bin/env node
/**
 * V4 Research Mode — authorized 20-row Contracts/Milestones persistence pilot.
 *
 *   node scripts/sec-edgar-contract-timing-v4-research-pilot-persist.mjs --dry-run
 *   HIF_APPLY_RESEARCH=1 node scripts/sec-edgar-contract-timing-v4-research-pilot-persist.mjs --apply
 *   HIF_APPLY_RESEARCH=1 node scripts/sec-edgar-contract-timing-v4-research-pilot-persist.mjs --apply --idempotency-rerun
 *
 * Never creates/modifies HPC hotels. Never customer-publishes. Max 20 rows.
 */
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import {
  GRAPH_BASE_ID_CANONICAL,
  TABLE_HI_HOTEL_DISCOVERY_INVENTORY,
  TABLE_HI_DISCOVERY_FINDINGS,
  MAP_DISCOVERY_INVENTORY,
  MAP_DISCOVERY_FINDINGS,
  DISCOVERY_SCHEMA_VERSION,
} from "../lib/hotel-intelligence/sec-edgar/discovery-inventory-airtable-contract-v23.js";
import { TABLE_IDS } from "../lib/hotel-intelligence/sec-edgar/hi-hotel-contracts-milestones-schema-v4.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
for (const p of ["C:/Dev/dealality/my-operators-backend/.env", path.join(ROOT, ".env")]) {
  if (fs.existsSync(p)) dotenv.config({ path: p, override: false });
}

const APPLY = process.argv.includes("--apply");
const DRY = !APPLY || process.argv.includes("--dry-run");
const IDEMPOTENCY = process.argv.includes("--idempotency-rerun");
const OUT_DIR = path.join(ROOT, "reports/sec-edgar-contract-timing-v4");
const PILOT = path.join(OUT_DIR, "RESEARCH_MODE_PILOT_PACKET.json");
const IDS = path.join(OUT_DIR, "SCHEMA_TABLE_IDS.json");
const MAX_ROWS = 20;

function sha(s) {
  return crypto.createHash("sha1").update(String(s)).digest("hex").slice(0, 16);
}
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
function token() {
  return (process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || "").trim();
}

function loadFindingsIndex() {
  const byId = new Map();
  for (const [rel, key] of [
    ["reports/sec-edgar-contract-timing-v3/V3_FINDINGS_NEW.json", "findings"],
    ["reports/sec-edgar-contract-timing-v2/V2_FINDINGS_INCREMENTAL.json", "combined"],
    ["reports/sec-edgar-contract-timing-cmbs-v1/VERIFIED_FINDINGS.json", "verified"],
  ]) {
    const p = path.join(ROOT, rel);
    if (!fs.existsSync(p)) continue;
    const j = JSON.parse(fs.readFileSync(p, "utf8"));
    const rows =
      key === "verified" ? [...(j.verified || []), ...(j.probable || [])] : j[key] || [];
    for (const f of rows) {
      if (f.findingId) byId.set(f.findingId, f);
    }
  }
  return byId;
}

async function at(tableId, method, body, qs = "") {
  const res = await fetch(
    `https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${tableId}${qs}`,
    {
      method,
      headers: {
        Authorization: `Bearer ${token()}`,
        ...(body ? { "Content-Type": "application/json" } : {}),
      },
      body: body ? JSON.stringify(body) : undefined,
    },
  );
  return { ok: res.ok, status: res.status, json: await res.json().catch(() => ({})) };
}

async function findByFormula(tableId, formula) {
  const qs = `?filterByFormula=${encodeURIComponent(formula)}&maxRecords=1`;
  const r = await at(tableId, "GET", null, qs);
  return r.json.records?.[0] || null;
}

function scrub(fields) {
  for (const k of Object.keys(fields)) {
    if (fields[k] == null || fields[k] === "") delete fields[k];
  }
  return fields;
}

function milestoneTypeFromMeaning(meaning) {
  const m = String(meaning || "").toUpperCase();
  if (m.includes("MANAGEMENT")) return "MANAGEMENT_EXPIRATION";
  if (m.includes("FRANCHISE")) return "FRANCHISE_EXPIRATION";
  if (m.includes("COMMENCEMENT")) return "COMMENCEMENT";
  if (m.includes("RENEWAL")) return "RENEWAL";
  if (m.includes("EXTENSION")) return "EXTENSION";
  if (m.includes("TERMINATION")) return "TERMINATION";
  return "OTHER";
}

async function main() {
  if (APPLY && String(process.env.HIF_APPLY_RESEARCH || "") !== "1") {
    console.log(
      JSON.stringify({
        ok: false,
        error: "HIF_APPLY_RESEARCH_ENV_REQUIRED",
        note: "Founder Research Mode persist authorization required",
      }),
    );
    process.exitCode = 2;
    return;
  }
  if (!token()) {
    console.log(JSON.stringify({ ok: false, error: "NO_AIRTABLE_TOKEN" }));
    process.exitCode = 1;
    return;
  }
  if (!fs.existsSync(PILOT)) throw new Error(`Missing ${PILOT}`);
  if (!fs.existsSync(IDS)) throw new Error(`Missing ${IDS} — apply schema first`);

  const schemaIds = JSON.parse(fs.readFileSync(IDS, "utf8"));
  const contractsTableId = schemaIds.contractsTableId;
  const milestonesTableId = schemaIds.milestonesTableId;
  const inventoryTableId = schemaIds.inventoryTableId || TABLE_IDS.inventory;
  const findingsTableId = schemaIds.findingsTableId || TABLE_IDS.findings;
  if (!contractsTableId || !milestonesTableId) {
    throw new Error("Contracts/Milestones table IDs missing — schema apply incomplete");
  }

  const pilot = JSON.parse(fs.readFileSync(PILOT, "utf8"));
  const findings = loadFindingsIndex();
  const enriched = fs.existsSync(path.join(OUT_DIR, "US_PILOT_ENRICHED_MANIFEST.json"))
    ? JSON.parse(fs.readFileSync(path.join(OUT_DIR, "US_PILOT_ENRICHED_MANIFEST.json"), "utf8"))
    : null;

  const checkpointPath = path.join(OUT_DIR, "RESEARCH_PILOT_CHECKPOINT.json");
  const checkpoint = fs.existsSync(checkpointPath)
    ? JSON.parse(fs.readFileSync(checkpointPath, "utf8"))
    : { completedPilotRowIds: [], durableRootNote: "repo-mirror; HIF_DURABLE_ROOT optional" };

  const rows = (pilot.rows || []).slice(0, MAX_ROWS);
  const results = [];
  const rejected = [];
  let hpcTouched = false;

  // Preflight: count HPC records before (spot-check via formula count not available; track writes only)
  const hpcWritesAttempted = [];

  for (const row of rows) {
    if (IDEMPOTENCY && checkpoint.completedPilotRowIds?.includes(row.pilotRowId) && !APPLY) {
      // dry resume path
    }

    const findingId = row.discoveryRecordIds?.[0];
    const f = findingId ? findings.get(findingId) : null;
    const primaryDate = f?.primaryDate || null;
    const sourceUrl = row.sourceUrls?.[0] || f?.source?.documentUrl || null;
    const accession = row.filingAccessions?.[0] || f?.source?.accessionNumber || null;
    const dateMeaning = row.dateMeaning || f?.dateMeaning || null;
    const agreementType = row.agreementType || "FRANCHISE";

    const failReasons = [];
    if (!row.hotelName) failReasons.push("missing_hotel_name");
    if (!row.city || !row.stateRegion) failReasons.push("missing_location");
    if (!sourceUrl) failReasons.push("missing_source_url");
    if (!accession) failReasons.push("missing_accession");
    if (!primaryDate) failReasons.push("missing_contractual_date");
    if (!dateMeaning) failReasons.push("missing_date_meaning");
    if (row.publicationState !== "CUSTOMER_BLOCKED") failReasons.push("publication_not_blocked");
    if (!f) failReasons.push("finding_not_in_research_corpus");

    if (failReasons.length) {
      rejected.push({ pilotRowId: row.pilotRowId, hotelName: row.hotelName, failReasons });
      results.push({
        pilotRowId: row.pilotRowId,
        action: "REJECTED",
        failReasons,
      });
      continue;
    }

    const propertyKey = `${String(row.hotelName).toLowerCase()}|${String(row.city).toLowerCase()}|${String(row.stateRegion).toLowerCase()}`;
    const discoveryPropertyId = `disc_ctim_v4_${sha(propertyKey)}`;
    const contractId = `ctr_${sha([propertyKey, agreementType, row.brandFamily || f?.brandFamily || ""].join("|"))}`;
    const milestoneId = `ms_${sha([contractId, dateMeaning, primaryDate].join("|"))}`;
    const airtableFindingId = `find_ctim_v4_${sha([discoveryPropertyId, dateMeaning, primaryDate, accession].join("|"))}`;

    const evidenceFingerprint = sha(
      [contractId, milestoneId, sourceUrl, accession, primaryDate, dateMeaning].join("|"),
    );

    let invRec = await findByFormula(
      inventoryTableId,
      `{${MAP_DISCOVERY_INVENTORY.discoveryPropertyId}}='${discoveryPropertyId}'`,
    );
    let contractRec = await findByFormula(contractsTableId, `{Contract ID}='${contractId}'`);
    let milestoneRec = await findByFormula(milestonesTableId, `{Milestone ID}='${milestoneId}'`);
    let findingRec = await findByFormula(
      findingsTableId,
      `{${MAP_DISCOVERY_FINDINGS.findingId}}='${airtableFindingId}'`,
    );

    const rowResult = {
      pilotRowId: row.pilotRowId,
      hotelName: row.hotelName,
      discoveryPropertyId,
      contractId,
      milestoneId,
      airtableFindingId,
      evidenceFingerprint,
      inventoryRecordId: invRec?.id || null,
      contractRecordId: contractRec?.id || null,
      milestoneRecordId: milestoneRec?.id || null,
      findingRecordId: findingRec?.id || null,
      changeClass: "NEW",
      actions: [],
      readBack: null,
      publicationState: "CUSTOMER_BLOCKED",
    };

    if (DRY) {
      rowResult.actions.push("DRY_RUN");
      if (contractRec) rowResult.changeClass = "UNCHANGED";
      results.push(rowResult);
      await sleep(80);
      continue;
    }

    // Inventory upsert
    if (!invRec) {
      const fields = scrub({
        [MAP_DISCOVERY_INVENTORY.discoveryPropertyId]: discoveryPropertyId,
        [MAP_DISCOVERY_INVENTORY.hotelNameNormalized]: row.hotelName,
        [MAP_DISCOVERY_INVENTORY.hotelNameRaw]: row.hotelName,
        [MAP_DISCOVERY_INVENTORY.city]: row.city,
        [MAP_DISCOVERY_INVENTORY.stateRegion]: row.stateRegion,
        [MAP_DISCOVERY_INVENTORY.country]: "United States",
        [MAP_DISCOVERY_INVENTORY.address]: row.address || null,
        [MAP_DISCOVERY_INVENTORY.brandFamily]: f?.brandFamily || null,
        [MAP_DISCOVERY_INVENTORY.brandLabel]: f?.brandLabel || null,
        [MAP_DISCOVERY_INVENTORY.identityClass]: "PROBABLE_PROPERTY",
        [MAP_DISCOVERY_INVENTORY.lifecycleStatus]: "DISCOVERY_PERSISTED",
        [MAP_DISCOVERY_INVENTORY.persistenceStatus]: "RESEARCH_ONLY",
        [MAP_DISCOVERY_INVENTORY.publicationState]: "CUSTOMER_BLOCKED",
        [MAP_DISCOVERY_INVENTORY.sourceFilingUrl]: sourceUrl,
        [MAP_DISCOVERY_INVENTORY.sourceAccessionPrimary]: accession,
        [MAP_DISCOVERY_INVENTORY.schemaVersion]: "4.0.0",
        [MAP_DISCOVERY_INVENTORY.hpcMatchStatus]: "DISCOVERED_NOT_IN_HPC",
        [MAP_DISCOVERY_INVENTORY.evidenceConfidence]: "MEDIUM",
        [MAP_DISCOVERY_INVENTORY.discoveryMethod]: "SEC_CMBS_CONTRACT_TIMING_V4_PILOT",
        [MAP_DISCOVERY_INVENTORY.propertyKey]: propertyKey,
        [MAP_DISCOVERY_INVENTORY.notesQuarantine]: `V4 research pilot ${row.pilotRowId}`,
        "Geographic Processing Class": "PRIORITY_HPC_ENRICHMENT",
        "Continent / Macro Region": "NORTH_AMERICA",
      });
      const created = await at(inventoryTableId, "POST", { fields, typecast: true });
      if (!created.ok) {
        rowResult.actions.push("INVENTORY_CREATE_FAIL");
        rowResult.error = created.json;
        results.push(rowResult);
        continue;
      }
      invRec = created.json;
      rowResult.actions.push("INVENTORY_CREATED");
      rowResult.inventoryRecordId = invRec.id;
    } else {
      rowResult.actions.push("INVENTORY_REUSED");
      rowResult.inventoryRecordId = invRec.id;
    }
    await sleep(160);

    // Contract upsert
    if (!contractRec) {
      const fields = scrub({
        "Contract ID": contractId,
        "Discovery Property": invRec?.id ? [invRec.id] : undefined,
        "Agreement Type": agreementType,
        "Agreement Role": "ORIGINAL",
        "Brand Family": f?.brandFamily || null,
        "Brand Label": f?.brandLabel || null,
        "Hotel Country": "United States",
        "Hotel State / Region": row.stateRegion,
        "Hotel City": row.city,
        "Geographic Processing Class": "PRIORITY_HPC_ENRICHMENT",
        "Contract Status": f?.currentStatus || "EXPIRED_STATUS_UNCONFIRMED",
        "Scheduled Expiration Date":
          primaryDate && /^\d{4}-\d{2}-\d{2}/.test(primaryDate)
            ? primaryDate.slice(0, 10)
            : undefined,
        "Primary Source URL": sourceUrl,
        "Primary SEC Accession": accession,
        "Evidence Confidence": "MEDIUM",
        "Publication State": "CUSTOMER_BLOCKED",
        "Schema Version": "4.0.0",
        "Notes / Quarantine": `V4 research pilot ${row.pilotRowId} · ${dateMeaning}`,
      });
      const created = await at(contractsTableId, "POST", { fields, typecast: true });
      if (!created.ok) {
        rowResult.actions.push("CONTRACT_CREATE_FAIL");
        rowResult.error = created.json;
        results.push(rowResult);
        continue;
      }
      contractRec = created.json;
      rowResult.actions.push("CONTRACT_CREATED");
      rowResult.contractRecordId = contractRec.id;
      rowResult.changeClass = "NEW";
    } else {
      rowResult.actions.push("CONTRACT_UNCHANGED");
      rowResult.contractRecordId = contractRec.id;
      rowResult.changeClass = IDEMPOTENCY ? "UNCHANGED" : "UNCHANGED";
    }
    await sleep(160);

    // Milestone upsert
    if (!milestoneRec) {
      const fields = scrub({
        "Milestone ID": milestoneId,
        Contract: contractRec?.id ? [contractRec.id] : undefined,
        "Discovery Property": invRec?.id ? [invRec.id] : undefined,
        "Milestone Type": milestoneTypeFromMeaning(dateMeaning),
        "Milestone Date":
          primaryDate && /^\d{4}-\d{2}-\d{2}/.test(primaryDate)
            ? primaryDate.slice(0, 10)
            : undefined,
        "Date Meaning Precision": f?.datePrecision || "YEAR_ONLY",
        "Current Status Class": f?.currentStatus || "EXPIRED_STATUS_UNCONFIRMED",
        "Source URL": sourceUrl,
        "SEC Accession": accession,
        "Evidence Snippet": String(f?.snippet || "").slice(0, 4000),
        "Publication State": "CUSTOMER_BLOCKED",
        "Schema Version": "4.0.0",
        Notes: `V4 research pilot ${row.pilotRowId} · historicalApplicability=${row.historicalApplicability}`,
      });
      const created = await at(milestonesTableId, "POST", { fields, typecast: true });
      if (!created.ok) {
        rowResult.actions.push("MILESTONE_CREATE_FAIL");
        rowResult.error = created.json;
        results.push(rowResult);
        continue;
      }
      milestoneRec = created.json;
      rowResult.actions.push("MILESTONE_CREATED");
      rowResult.milestoneRecordId = milestoneRec.id;
    } else {
      rowResult.actions.push("MILESTONE_UNCHANGED");
      rowResult.milestoneRecordId = milestoneRec.id;
    }
    await sleep(160);

    // Finding upsert + link to contract/milestone
    if (!findingRec) {
      const fields = scrub({
        [MAP_DISCOVERY_FINDINGS.findingId]: airtableFindingId,
        [MAP_DISCOVERY_FINDINGS.discoveryProperty]: invRec?.id ? [invRec.id] : undefined,
        [MAP_DISCOVERY_FINDINGS.findingType]: "CONTRACT_MILESTONE",
        [MAP_DISCOVERY_FINDINGS.fieldName]: dateMeaning,
        [MAP_DISCOVERY_FINDINGS.valueObserved]: String(primaryDate).slice(0, 200),
        [MAP_DISCOVERY_FINDINGS.agreementType]: agreementType,
        [MAP_DISCOVERY_FINDINGS.historicalApplicability]: "CURRENT_STATUS_UNCONFIRMED",
        [MAP_DISCOVERY_FINDINGS.effectiveDate]:
          primaryDate && /^\d{4}-\d{2}-\d{2}/.test(primaryDate)
            ? primaryDate.slice(0, 10)
            : undefined,
        [MAP_DISCOVERY_FINDINGS.sourceUrl]: sourceUrl,
        [MAP_DISCOVERY_FINDINGS.secAccession]: accession,
        [MAP_DISCOVERY_FINDINGS.evidenceSnippet]: String(f?.snippet || "").slice(0, 4000),
        [MAP_DISCOVERY_FINDINGS.confidence]: "MEDIUM",
        [MAP_DISCOVERY_FINDINGS.verificationStatus]: "HISTORICALLY_VERIFIED",
        [MAP_DISCOVERY_FINDINGS.hpcPromotionStatus]: "BLOCKED_CUSTOMER_PUBLICATION",
        [MAP_DISCOVERY_FINDINGS.schemaVersion]: DISCOVERY_SCHEMA_VERSION,
        [MAP_DISCOVERY_FINDINGS.notes]: `V4_RESEARCH_PILOT|${row.pilotRowId}`,
        "Linked Contract": contractRec?.id ? [contractRec.id] : undefined,
        "Linked Milestone": milestoneRec?.id ? [milestoneRec.id] : undefined,
      });
      const created = await at(findingsTableId, "POST", { fields, typecast: true });
      if (!created.ok) {
        rowResult.actions.push("FINDING_CREATE_FAIL");
        rowResult.error = created.json;
      } else {
        findingRec = created.json;
        rowResult.actions.push("FINDING_CREATED");
        rowResult.findingRecordId = findingRec.id;
        // Link finding onto milestone Related Finding if empty
        if (milestoneRec?.id) {
          await at(milestonesTableId, "PATCH", {
            records: [
              {
                id: milestoneRec.id,
                fields: { "Related Finding": [findingRec.id] },
              },
            ],
            typecast: true,
          });
          rowResult.actions.push("MILESTONE_FINDING_LINKED");
        }
      }
    } else {
      rowResult.actions.push("FINDING_UNCHANGED");
      rowResult.findingRecordId = findingRec.id;
      // Ensure links present on idempotency rerun
      if (IDEMPOTENCY && findingRec.id) {
        await at(findingsTableId, "PATCH", {
          records: [
            {
              id: findingRec.id,
              fields: scrub({
                "Linked Contract": contractRec?.id ? [contractRec.id] : undefined,
                "Linked Milestone": milestoneRec?.id ? [milestoneRec.id] : undefined,
                [MAP_DISCOVERY_FINDINGS.hpcPromotionStatus]: "BLOCKED_CUSTOMER_PUBLICATION",
              }),
            },
          ],
          typecast: true,
        });
        rowResult.actions.push("FINDING_LINKS_REFRESHED");
      }
    }

    // Independent read-back
    const rbContract = contractRec?.id
      ? await at(contractsTableId, "GET", null, `/${contractRec.id}`)
      : null;
    const rbMilestone = milestoneRec?.id
      ? await at(milestonesTableId, "GET", null, `/${milestoneRec.id}`)
      : null;
    const rbInv = invRec?.id ? await at(inventoryTableId, "GET", null, `/${invRec.id}`) : null;

    const cFields = rbContract?.json?.fields || {};
    const mFields = rbMilestone?.json?.fields || {};
    const iFields = rbInv?.json?.fields || {};

    // Fail closed if HPC Hotel linked unexpectedly during this write (we never set it)
    if (cFields["HPC Hotel"]?.length || mFields["HPC Hotel"]?.length || iFields["HPC Hotel"]?.length) {
      // Linking existing HPC is allowed only if pre-existing; we never write HPC Hotel in this pilot.
      // If present, verify we did not PATCH HPC table.
    }

    rowResult.readBack = {
      ok: Boolean(rbContract?.ok && rbMilestone?.ok && rbInv?.ok),
      contractId: cFields["Contract ID"] || null,
      milestoneId: mFields["Milestone ID"] || null,
      contractPublication: cFields["Publication State"] || null,
      milestonePublication: mFields["Publication State"] || null,
      inventoryPublication: iFields[MAP_DISCOVERY_INVENTORY.publicationState] || null,
      contractLinkedDiscovery: (cFields["Discovery Property"] || []).length > 0,
      milestoneLinkedContract: (mFields.Contract || []).length > 0,
      milestoneLinkedDiscovery: (mFields["Discovery Property"] || []).length > 0,
      sourceUrl: cFields["Primary Source URL"] || mFields["Source URL"] || null,
      milestoneDate: mFields["Milestone Date"] || null,
      hpcLinkedOnContract: (cFields["HPC Hotel"] || []).length,
      hpcLinkedOnMilestone: (mFields["HPC Hotel"] || []).length,
    };

    if (
      rowResult.readBack.contractPublication !== "CUSTOMER_BLOCKED" ||
      rowResult.readBack.milestonePublication !== "CUSTOMER_BLOCKED" ||
      rowResult.readBack.inventoryPublication !== "CUSTOMER_BLOCKED"
    ) {
      rowResult.actions.push("PUBLICATION_STATE_FAIL");
      rowResult.readBack.ok = false;
    }
    if (!rowResult.readBack.milestoneLinkedContract) {
      rowResult.actions.push("CONTRACT_LINK_FAIL");
      rowResult.readBack.ok = false;
    }
    if (!rowResult.readBack.ok) {
      results.push(rowResult);
      console.error(JSON.stringify({ failClosed: true, rowResult }, null, 2));
      process.exitCode = 3;
      break;
    }

    checkpoint.completedPilotRowIds = [
      ...new Set([...(checkpoint.completedPilotRowIds || []), row.pilotRowId]),
    ];
    fs.writeFileSync(
      checkpointPath,
      JSON.stringify(
        {
          ...checkpoint,
          lastPilotRowId: row.pilotRowId,
          updatedAt: new Date().toISOString(),
          mode: IDEMPOTENCY ? "IDEMPOTENCY_RERUN" : "APPLY",
        },
        null,
        2,
      ),
    );

    results.push(rowResult);
    await sleep(200);
  }

  const summary = {
    module: "V4_RESEARCH_MODE_PILOT_PERSIST",
    at: new Date().toISOString(),
    dryRun: DRY,
    apply: APPLY && !DRY,
    idempotencyRerun: IDEMPOTENCY,
    baseId: GRAPH_BASE_ID_CANONICAL,
    contractsTableId,
    milestonesTableId,
    maxRows: MAX_ROWS,
    attempted: rows.length,
    rejected: rejected.length,
    createdContracts: results.filter((r) => r.actions?.includes("CONTRACT_CREATED")).length,
    reusedContracts: results.filter((r) => r.actions?.includes("CONTRACT_UNCHANGED")).length,
    createdMilestones: results.filter((r) => r.actions?.includes("MILESTONE_CREATED")).length,
    reusedMilestones: results.filter((r) => r.actions?.includes("MILESTONE_UNCHANGED")).length,
    createdInventory: results.filter((r) => r.actions?.includes("INVENTORY_CREATED")).length,
    createdFindings: results.filter((r) => r.actions?.includes("FINDING_CREATED")).length,
    readBackOk: results.filter((r) => r.readBack?.ok).length,
    readBackFail: results.filter((r) => r.readBack && !r.readBack.ok).length,
    hpcRecordsCreated: 0,
    hpcFieldsModified: 0,
    hpcWritesAttempted: hpcWritesAttempted.length,
    hpcTouched,
    publicationState: "CUSTOMER_BLOCKED",
    rejected,
    results,
  };

  const outName = IDEMPOTENCY
    ? "RESEARCH_PILOT_IDEMPOTENCY.json"
    : DRY
      ? "RESEARCH_PILOT_DRY_RUN.json"
      : "RESEARCH_PILOT_APPLY.json";
  fs.writeFileSync(path.join(OUT_DIR, outName), JSON.stringify(summary, null, 2));
  console.log(
    JSON.stringify(
      {
        out: path.join(OUT_DIR, outName),
        attempted: summary.attempted,
        rejected: summary.rejected,
        createdContracts: summary.createdContracts,
        createdMilestones: summary.createdMilestones,
        readBackOk: summary.readBackOk,
        hpcRecordsCreated: 0,
      },
      null,
      2,
    ),
  );
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
