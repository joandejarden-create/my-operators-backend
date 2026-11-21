#!/usr/bin/env node
/**
 * Track B global-wave Airtable persistence.
 * Default dry-run. --apply creates missing rows only (existing IDs are no-ops).
 * CUSTOMER_BLOCKED. No HPC creates. No schema changes. Track A IDs skipped.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import {
  GRAPH_BASE_ID_CANONICAL,
  TABLE_HI_HOTEL_DISCOVERY_INVENTORY,
  TABLE_HI_DISCOVERY_FINDINGS,
  MAP_DISCOVERY_INVENTORY,
  MAP_DISCOVERY_FINDINGS,
  HPC_TABLE_ID,
} from "../lib/hotel-intelligence/sec-edgar/discovery-inventory-airtable-contract-v23.js";
import {
  FOUNDATION_DESTINATIONS,
  buildDiscoveryInventoryFields,
  buildCommercialFindingFields,
} from "../lib/hotel-intelligence/sec-edgar/hif-foundation-persistence-v1.js";
import { isTrackAReservedId } from "../lib/hotel-intelligence/sec-edgar/hif-track-b-historical-foundation-v1.js";
import { contractPersistAllowed } from "../lib/hotel-intelligence/sec-edgar/hif-track-b-global-historical-waves-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
for (const p of [path.join(ROOT, ".env"), "C:/Dev/dealality/my-operators-backend/.env"]) {
  if (fs.existsSync(p)) {
    dotenv.config({ path: p, override: false });
    break;
  }
}

const APPLY = process.argv.includes("--apply");
const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
if (!token) {
  console.error("NO_AIRTABLE_TOKEN");
  process.exit(2);
}

const OUT = path.join(ROOT, "reports/sec-edgar-hif-track-b-global-historical-waves");
const batch = JSON.parse(fs.readFileSync(path.join(OUT, "WAVE_DOSSIERS.json"), "utf8"));
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function airtable(url, options = {}) {
  const res = await fetch(url, {
    ...options,
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
      ...(options.headers || {}),
    },
  });
  const json = await res.json().catch(() => ({}));
  return { ok: res.ok, status: res.status, json };
}

async function findByFormula(tableId, formula) {
  const qs = new URLSearchParams({ filterByFormula: formula, maxRecords: "1" });
  const res = await airtable(`https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${tableId}?${qs}`);
  if (!res.ok) throw new Error(`LOOKUP_${tableId}_${res.status}_${JSON.stringify(res.json?.error || {})}`);
  return res.json.records?.[0] || null;
}

async function createRecord(tableId, fields) {
  return airtable(`https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${tableId}`, {
    method: "POST",
    body: JSON.stringify({ fields, typecast: false }),
  });
}

async function getRecord(tableId, recordId) {
  return airtable(`https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${tableId}/${recordId}`);
}

function choicesFor(meta, tableId, fieldName) {
  const table = (meta.tables || []).find((t) => t.id === tableId);
  const field = (table?.fields || []).find((f) => f.name === fieldName);
  return (field?.options?.choices || []).map((c) => c.name);
}

function pickChoice(choices, preferred) {
  return preferred.find((p) => choices.includes(p)) || null;
}

function isoOrNull(value) {
  return /^\d{4}-\d{2}-\d{2}$/.test(String(value || "")) ? value : null;
}

const ledger = {
  asOf: new Date().toISOString(),
  mode: APPLY ? "APPLY" : "DRY_RUN",
  publicationState: "CUSTOMER_BLOCKED",
  stream: "TRACK_B",
  researchCostUsd: 0,
  operations: [],
  readBack: [],
  staged: [],
  summary: {
    creates: 0,
    noops: 0,
    conflicts: 0,
    staged: 0,
    failures: 0,
    inventory: 0,
    findings: 0,
    contracts: 0,
    milestones: 0,
    organizations: 0,
    relationships: 0,
    evidence: 0,
  },
};

const metaRes = await airtable(`https://api.airtable.com/v0/meta/bases/${GRAPH_BASE_ID_CANONICAL}/tables`);
if (!metaRes.ok) throw new Error("META_FETCH_FAILED");
const meta = metaRes.json;
const orgTypes = choicesFor(meta, FOUNDATION_DESTINATIONS.organizations.tableId, "Organization Type");
const orgVerification = choicesFor(meta, FOUNDATION_DESTINATIONS.organizations.tableId, "Verification State");
const orgConfidence = choicesFor(meta, FOUNDATION_DESTINATIONS.organizations.tableId, "Confidence");
const relTypes = choicesFor(meta, FOUNDATION_DESTINATIONS.hotelOrgRelationships.tableId, "Relationship Type");
const findingFieldNames = choicesFor(meta, FOUNDATION_DESTINATIONS.discoveryFindings.tableId, "Field Name");
const evidenceEntityTypes = choicesFor(meta, FOUNDATION_DESTINATIONS.graphEvidence.tableId, "Entity Type");
ledger.selectChoicesObserved = {
  organizationType: orgTypes,
  relationshipType: relTypes,
  evidenceEntityType: evidenceEntityTypes,
};

const orgTypeChoice = pickChoice(orgTypes, ["Hotel Operator", "Operator", "Manager", "Management Company", "Owner", "Brand"]);
function relationshipChoiceFor(dossier) {
  if (dossier.agreementType === "LEASE") return pickChoice(relTypes, ["Lessee", "Tenant", "Landlord", "Operator"]);
  if (dossier.agreementType === "MANAGEMENT") return pickChoice(relTypes, ["Operator", "Manager", "Management Company"]);
  if (dossier.agreementType === "FRANCHISE") return pickChoice(relTypes, ["Franchisor", "Franchisee", "Brand"]);
  return null;
}
const evidenceEntityChoice = pickChoice(evidenceEntityTypes, [
  "Contract",
  "Hotel Contract",
  "Discovery Finding",
  "Finding",
  "Management Agreement",
]);

const invByDiscovery = new Map();
const contractById = new Map();
const orgByName = new Map();

for (const d of batch.dossiers || []) {
  if (!d.persistEligible) {
    ledger.summary.staged += 1;
    ledger.staged.push({ id: d.discoveryPropertyId, reason: d.identityClass || "NOT_PERSIST_ELIGIBLE" });
    continue;
  }
  if (isTrackAReservedId(d.discoveryPropertyId) || isTrackAReservedId(d.findingId)) {
    ledger.operations.push({ id: d.discoveryPropertyId, action: "SKIP_TRACK_A_RESERVED" });
    continue;
  }

  const existingInv = await findByFormula(
    FOUNDATION_DESTINATIONS.discoveryInventory.tableId,
    `{${MAP_DISCOVERY_INVENTORY.discoveryPropertyId}}="${d.discoveryPropertyId}"`,
  );
  await sleep(160);

  const hpcStatus =
    d.hpcMatchStatus === "MATCHED_UNIQUE_NAME_AND_COUNTRY"
      ? "MATCHED"
      : d.hpcMatchStatus === "AMBIGUOUS_MULTIPLE"
        ? "AMBIGUOUS"
        : "DISCOVERED_NOT_IN_HPC";

  if (existingInv) {
    ledger.summary.noops += 1;
    ledger.operations.push({ id: d.discoveryPropertyId, action: "NOOP_EXISTING", airtableId: existingInv.id, table: "inventory" });
    invByDiscovery.set(d.discoveryPropertyId, existingInv.id);
  } else if (!APPLY) {
    ledger.summary.creates += 1;
    ledger.summary.inventory += 1;
    ledger.operations.push({ id: d.discoveryPropertyId, action: "PLAN_CREATE", table: "inventory" });
  } else {
    const fields = buildDiscoveryInventoryFields({
      discoveryPropertyId: d.discoveryPropertyId,
      hotelName: d.hotelName,
      hotelNameNormalized: d.hotelNameNormalized,
      city: d.city,
      stateProvince: d.stateProvince,
      physicalCountry: d.physicalCountry,
      address: d.address,
      currentBrand: d.currentBrand,
      historicalBrand: d.historicalBrand,
      identityClass: d.identityClass,
      evidenceConfidence: d.evidenceConfidence,
      hpcMatchStatus: hpcStatus,
      sourceUrl: d.sourceUrl,
      discoveryMethod: "TRACK_B_GLOBAL_HISTORICAL_WAVES",
      lifecycleStatus: hpcStatus === "MATCHED" ? "HPC_MATCHED" : "IDENTITY_VERIFIED",
      notes: `CUSTOMER_BLOCKED | Track B global waves | ${d.wave} | status=${d.reconciliation?.currentStatusClass || ""} | agreement=${d.agreementType || "UNRESOLVED"}`,
    });
    if (hpcStatus === "MATCHED" && d.hpcRecordId) fields[MAP_DISCOVERY_INVENTORY.hpcHotel] = [d.hpcRecordId];
    const created = await createRecord(FOUNDATION_DESTINATIONS.discoveryInventory.tableId, fields);
    await sleep(180);
    if (!created.ok) {
      ledger.summary.failures += 1;
      ledger.operations.push({ id: d.discoveryPropertyId, action: "CREATE_FAIL", table: "inventory", error: created.json?.error || created.json });
    } else {
      ledger.summary.creates += 1;
      ledger.summary.inventory += 1;
      invByDiscovery.set(d.discoveryPropertyId, created.json.id);
      const rb = await getRecord(FOUNDATION_DESTINATIONS.discoveryInventory.tableId, created.json.id);
      ledger.readBack.push({
        table: "inventory",
        id: created.json.id,
        ok: rb.ok,
        discoveryPropertyId: rb.json?.fields?.[MAP_DISCOVERY_INVENTORY.discoveryPropertyId],
        publicationState: rb.json?.fields?.[MAP_DISCOVERY_INVENTORY.publicationState],
        country: rb.json?.fields?.[MAP_DISCOVERY_INVENTORY.country],
      });
      ledger.operations.push({ id: d.discoveryPropertyId, action: "CREATE", table: "inventory", airtableId: created.json.id });
    }
  }

  const existingFinding = await findByFormula(
    FOUNDATION_DESTINATIONS.discoveryFindings.tableId,
    `{${MAP_DISCOVERY_FINDINGS.findingId}}="${d.findingId}"`,
  );
  await sleep(160);
  const linkId = invByDiscovery.get(d.discoveryPropertyId) || existingInv?.id || null;
  if (existingFinding) {
    ledger.summary.noops += 1;
    ledger.operations.push({ id: d.findingId, action: "NOOP_EXISTING", airtableId: existingFinding.id, table: "findings" });
  } else if (!APPLY) {
    ledger.summary.creates += 1;
    ledger.summary.findings += 1;
    ledger.operations.push({ id: d.findingId, action: "PLAN_CREATE", table: "findings" });
  } else if (!linkId) {
    ledger.summary.failures += 1;
    ledger.operations.push({ id: d.findingId, action: "CREATE_FAIL", table: "findings", error: "NO_INVENTORY_LINK" });
  } else {
    const fields = buildCommercialFindingFields(
      {
        findingId: d.findingId,
        commercialCategory: d.commercialCategory,
        commercialThesis: d.evidenceSnippet,
        decisionEvidence: d.evidenceSnippet,
        nextDecisionDate: isoOrNull(d.milestoneDate || d.expirationDate),
        sourceUrl: d.sourceUrl,
        evidenceConfidence: d.evidenceConfidence,
        verificationStatus: "EVIDENCE_SUPPORTED",
        notes: `CUSTOMER_BLOCKED | ${d.wave} | ${d.reconciliation?.currentStatusClass || ""} | ${d.supersessionGap || ""}`.slice(0, 8000),
      },
      linkId,
    );
    if (fields["Field Name"] && findingFieldNames.length && !findingFieldNames.includes(fields["Field Name"])) {
      delete fields["Field Name"];
    }
    const created = await createRecord(FOUNDATION_DESTINATIONS.discoveryFindings.tableId, fields);
    await sleep(180);
    if (!created.ok) {
      ledger.summary.failures += 1;
      ledger.operations.push({ id: d.findingId, action: "CREATE_FAIL", table: "findings", error: created.json?.error || created.json });
    } else {
      ledger.summary.creates += 1;
      ledger.summary.findings += 1;
      const rb = await getRecord(FOUNDATION_DESTINATIONS.discoveryFindings.tableId, created.json.id);
      ledger.readBack.push({
        table: "findings",
        id: created.json.id,
        ok: rb.ok,
        findingId: rb.json?.fields?.[MAP_DISCOVERY_FINDINGS.findingId],
        findingType: rb.json?.fields?.[MAP_DISCOVERY_FINDINGS.findingType],
      });
      ledger.operations.push({ id: d.findingId, action: "CREATE", table: "findings", airtableId: created.json.id });
    }
  }

  if (contractPersistAllowed(d) && d.contractId) {
    const existingContract = await findByFormula(
      FOUNDATION_DESTINATIONS.hotelContracts.tableId,
      `{Contract ID}="${d.contractId}"`,
    );
    await sleep(160);
    const inventoryId = invByDiscovery.get(d.discoveryPropertyId) || existingInv?.id || null;
    if (existingContract) {
      ledger.summary.noops += 1;
      contractById.set(d.contractId, existingContract.id);
      ledger.operations.push({ id: d.contractId, action: "NOOP_EXISTING", airtableId: existingContract.id, table: "contracts" });
    } else if (!APPLY) {
      ledger.summary.creates += 1;
      ledger.summary.contracts += 1;
      ledger.operations.push({ id: d.contractId, action: "PLAN_CREATE", table: "contracts", agreementType: d.agreementType });
    } else if (!inventoryId) {
      ledger.summary.staged += 1;
      ledger.staged.push({ id: d.contractId, reason: "CONTRACT_WAITING_ON_INVENTORY" });
    } else {
      const fields = {
        "Contract ID": d.contractId,
        "Discovery Property": [inventoryId],
        "Agreement Type": d.agreementType,
        "Agreement Role": d.agreementRole || "ORIGINAL",
        "Contract Status": d.contractStatus || "HISTORICALLY_VERIFIED",
        "Hotel Country": d.physicalCountry,
        "Hotel City": d.city || undefined,
        "Hotel State / Region": d.stateProvince || undefined,
        "Brand Label": d.currentBrand || d.historicalBrand || undefined,
        "Counterparty Name Raw": d.counterpartyName || d.operator || d.priorOperator || undefined,
        "Primary Source URL": d.sourceUrl,
        "Evidence Confidence": d.evidenceConfidence === "HIGH" ? "HIGH" : "MEDIUM",
        "Publication State": "CUSTOMER_BLOCKED",
        "Geographic Processing Class": "GLOBAL_RESEARCH_STANDARD",
        "Schema Version": "hif-track-b-global-historical-waves-v1",
        "Notes / Quarantine": `CUSTOMER_BLOCKED | ${d.evidenceSnippet || ""}`.slice(0, 8000),
      };
      if (hpcStatus === "MATCHED" && d.hpcRecordId) fields["HPC Hotel"] = [d.hpcRecordId];
      const commencement = isoOrNull(d.commencementDate);
      const expiration = isoOrNull(d.expirationDate);
      if (commencement) fields["Commencement Date"] = commencement;
      if (expiration) fields["Scheduled Expiration Date"] = expiration;
      if (d.terminationSummary) fields["Termination Rights Summary"] = d.terminationSummary;
      for (const k of Object.keys(fields)) if (fields[k] == null || fields[k] === "") delete fields[k];
      const created = await createRecord(FOUNDATION_DESTINATIONS.hotelContracts.tableId, fields);
      await sleep(180);
      if (!created.ok) {
        ledger.summary.failures += 1;
        ledger.operations.push({ id: d.contractId, action: "CREATE_FAIL", table: "contracts", error: created.json?.error || created.json });
      } else {
        ledger.summary.creates += 1;
        ledger.summary.contracts += 1;
        contractById.set(d.contractId, created.json.id);
        const rb = await getRecord(FOUNDATION_DESTINATIONS.hotelContracts.tableId, created.json.id);
        ledger.readBack.push({
          table: "contracts",
          id: created.json.id,
          ok: rb.ok,
          contractId: rb.json?.fields?.["Contract ID"],
          agreementType: rb.json?.fields?.["Agreement Type"],
          publicationState: rb.json?.fields?.["Publication State"],
        });
        ledger.operations.push({ id: d.contractId, action: "CREATE", table: "contracts", airtableId: created.json.id });
      }
    }

    if (d.milestoneId && isoOrNull(d.milestoneDate)) {
      const existingMs = await findByFormula(
        FOUNDATION_DESTINATIONS.contractMilestones.tableId,
        `{Milestone ID}="${d.milestoneId}"`,
      );
      await sleep(160);
      const contractRecId = contractById.get(d.contractId) || existingContract?.id || null;
      if (existingMs) {
        ledger.summary.noops += 1;
        ledger.operations.push({ id: d.milestoneId, action: "NOOP_EXISTING", airtableId: existingMs.id, table: "milestones" });
      } else if (!APPLY) {
        ledger.summary.creates += 1;
        ledger.summary.milestones += 1;
        ledger.operations.push({ id: d.milestoneId, action: "PLAN_CREATE", table: "milestones" });
      } else if (!contractRecId) {
        ledger.summary.staged += 1;
        ledger.staged.push({ id: d.milestoneId, reason: "MILESTONE_WAITING_ON_CONTRACT" });
      } else {
        const fields = {
          "Milestone ID": d.milestoneId,
          Contract: [contractRecId],
          "Discovery Property": inventoryId ? [inventoryId] : undefined,
          "Milestone Type": d.milestoneType || "OTHER",
          "Milestone Date": d.milestoneDate,
          "Date Meaning Precision": "EXACT_DAY",
          "Current Status Class": d.contractStatus || "HISTORICALLY_VERIFIED",
          "Status Corroborated": true,
          "Source URL": d.sourceUrl,
          "Evidence Snippet": (d.evidenceSnippet || "").slice(0, 8000),
          "Publication State": "CUSTOMER_BLOCKED",
          "Schema Version": "hif-track-b-global-historical-waves-v1",
          Notes: "CUSTOMER_BLOCKED | exact date stated in source | not a commercial-availability flag",
        };
        if (hpcStatus === "MATCHED" && d.hpcRecordId) fields["HPC Hotel"] = [d.hpcRecordId];
        for (const k of Object.keys(fields)) if (fields[k] == null) delete fields[k];
        const created = await createRecord(FOUNDATION_DESTINATIONS.contractMilestones.tableId, fields);
        await sleep(180);
        if (!created.ok) {
          ledger.summary.failures += 1;
          ledger.operations.push({ id: d.milestoneId, action: "CREATE_FAIL", table: "milestones", error: created.json?.error || created.json });
        } else {
          ledger.summary.creates += 1;
          ledger.summary.milestones += 1;
          const rb = await getRecord(FOUNDATION_DESTINATIONS.contractMilestones.tableId, created.json.id);
          ledger.readBack.push({
            table: "milestones",
            id: created.json.id,
            ok: rb.ok,
            milestoneId: rb.json?.fields?.["Milestone ID"],
            milestoneDate: rb.json?.fields?.["Milestone Date"],
            publicationState: rb.json?.fields?.["Publication State"],
          });
          ledger.operations.push({ id: d.milestoneId, action: "CREATE", table: "milestones", airtableId: created.json.id });
        }
      }
    }
  } else if (d.agreementType == null || d.contractEligible === false) {
    ledger.summary.staged += 1;
    ledger.staged.push({
      id: d.findingId,
      reason: d.dateConflict ? "DATE_CONFLICT_NO_CONTRACT_ROW" : "AGREEMENT_TYPE_UNRESOLVED_OR_NOT_A_CONTRACT",
    });
  }

  if (d.counterpartyName && d.hpcRecordId && hpcStatus === "MATCHED") {
    const orgKey = d.counterpartyName.toLowerCase();
    let org = orgByName.get(orgKey);
    if (!org) {
      org = await findByFormula(
        FOUNDATION_DESTINATIONS.organizations.tableId,
        `{Legal Name}="${d.counterpartyName.replace(/"/g, "")}"`,
      );
      await sleep(160);
    }
    if (!org && !orgTypeChoice) {
      ledger.summary.staged += 1;
      ledger.staged.push({ id: d.counterpartyName, reason: "ORG_TYPE_CHOICE_NOT_IN_SCHEMA", choices: orgTypes });
    } else if (!org && !APPLY) {
      ledger.summary.creates += 1;
      ledger.summary.organizations += 1;
      ledger.operations.push({ id: d.counterpartyName, action: "PLAN_CREATE", table: "organizations" });
    } else if (!org && APPLY) {
      const fields = {
        "Organization ID": `org_tb_${orgKey.replace(/[^a-z0-9]+/g, "_").slice(0, 40)}`,
        "Legal Name": d.counterpartyName,
        "Organization Type": orgTypeChoice,
        "Dedupe Key": orgKey,
        "Publication State": "CUSTOMER_BLOCKED",
        "Provenance Summary": `CUSTOMER_BLOCKED | named counterparty on ${d.sourceUrl}`,
        "Schema Version": "hif-track-b-global-historical-waves-v1",
      };
      const v = pickChoice(orgVerification, ["SOURCE_SUPPORTED", "UNVERIFIED", "VERIFIED"]);
      const c = pickChoice(orgConfidence, ["MEDIUM", "HIGH"]);
      if (v) fields["Verification State"] = v;
      if (c) fields.Confidence = c;
      const created = await createRecord(FOUNDATION_DESTINATIONS.organizations.tableId, fields);
      await sleep(180);
      if (!created.ok) {
        ledger.summary.failures += 1;
        ledger.operations.push({ id: d.counterpartyName, action: "CREATE_FAIL", table: "organizations", error: created.json?.error || created.json });
      } else {
        org = { id: created.json.id };
        ledger.summary.creates += 1;
        ledger.summary.organizations += 1;
        ledger.operations.push({ id: d.counterpartyName, action: "CREATE", table: "organizations", airtableId: created.json.id });
      }
    } else if (org) {
      ledger.summary.noops += 1;
      ledger.operations.push({ id: d.counterpartyName, action: "NOOP_EXISTING", table: "organizations", airtableId: org.id });
    }
    if (org?.id) orgByName.set(orgKey, org);
    const relTypeChoice = relationshipChoiceFor(d);

    if (org?.id && relTypeChoice) {
      const relId = `rel_tb_${d.discoveryPropertyId}`;
      const existingRel = await findByFormula(
        FOUNDATION_DESTINATIONS.hotelOrgRelationships.tableId,
        `{Relationship ID}="${relId}"`,
      );
      await sleep(140);
      if (existingRel) {
        ledger.summary.noops += 1;
      } else if (!APPLY) {
        ledger.summary.creates += 1;
        ledger.summary.relationships += 1;
        ledger.operations.push({ id: relId, action: "PLAN_CREATE", table: "relationships" });
      } else {
        const created = await createRecord(FOUNDATION_DESTINATIONS.hotelOrgRelationships.tableId, {
          "Relationship ID": relId,
          "HPC Hotel ID": d.hpcRecordId,
          "Organization ID": org.id,
          "Relationship Type": relTypeChoice,
          "Publication State": "CUSTOMER_BLOCKED",
          "Schema Version": "hif-track-b-global-historical-waves-v1",
          Notes: `CUSTOMER_BLOCKED | instrument=${d.agreementType} | relationship select=${relTypeChoice} | Current left unset`,
        });
        await sleep(160);
        if (!created.ok) {
          ledger.summary.failures += 1;
          ledger.operations.push({ id: relId, action: "CREATE_FAIL", table: "relationships", error: created.json?.error || created.json });
        } else {
          ledger.summary.creates += 1;
          ledger.summary.relationships += 1;
          ledger.operations.push({ id: relId, action: "CREATE", table: "relationships", airtableId: created.json.id });
        }
      }
    } else if (org?.id && !relTypeChoice) {
      ledger.summary.staged += 1;
      ledger.staged.push({ id: d.discoveryPropertyId, reason: "RELATIONSHIP_TYPE_CHOICE_NOT_CONFIRMED", choices: relTypes });
    }

    if (!evidenceEntityChoice) {
      ledger.summary.staged += 1;
      ledger.staged.push({ id: d.discoveryPropertyId, reason: "EVIDENCE_ENTITY_TYPE_NOT_CONFIRMED", choices: evidenceEntityTypes });
    }
  } else if (d.hpcRecordId == null) {
    ledger.staged.push({ id: d.discoveryPropertyId, reason: "GRAPH_EVIDENCE_AND_RELATIONSHIP_REQUIRE_HPC_MATCH" });
  }
}

for (const s of batch.staged || []) {
  ledger.summary.staged += 1;
  ledger.staged.push({ id: s.discoveryPropertyId, reason: s.persistReason, table: "staged_not_written" });
}

const fileName = APPLY ? "PERSISTENCE_LEDGER_APPLY.json" : "PERSISTENCE_LEDGER_DRY_RUN.json";
fs.writeFileSync(path.join(OUT, fileName), JSON.stringify(ledger, null, 2));
console.log(JSON.stringify({ ok: true, mode: ledger.mode, summary: ledger.summary, failures: ledger.operations.filter((o) => o.action === "CREATE_FAIL").length }, null, 2));
void HPC_TABLE_ID;
