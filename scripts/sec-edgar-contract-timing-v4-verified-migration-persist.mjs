#!/usr/bin/env node
/**
 * V4 verified historical contract migration (Research Mode only).
 *
 *   node scripts/sec-edgar-contract-timing-v4-verified-migration-persist.mjs --reconcile
 *   node scripts/sec-edgar-contract-timing-v4-verified-migration-persist.mjs --dry-run --batch=0
 *   HIF_APPLY_RESEARCH=1 node scripts/sec-edgar-contract-timing-v4-verified-migration-persist.mjs --apply --batch=0
 *   HIF_APPLY_RESEARCH=1 node scripts/... --apply --batch=0 --idempotency-rerun
 *
 * Batches of ≤50. VERIFIED agreements only. No HPC writes. CUSTOMER_BLOCKED.
 */
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import {
  GRAPH_BASE_ID_CANONICAL,
  MAP_DISCOVERY_INVENTORY,
  MAP_DISCOVERY_FINDINGS,
  DISCOVERY_SCHEMA_VERSION,
} from "../lib/hotel-intelligence/sec-edgar/discovery-inventory-airtable-contract-v23.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
for (const p of ["C:/Dev/dealality/my-operators-backend/.env", path.join(ROOT, ".env")]) {
  if (fs.existsSync(p)) dotenv.config({ path: p, override: false });
}

const OUT_DIR = path.join(ROOT, "reports/sec-edgar-contract-timing-v4");
const AUDIT = path.join(OUT_DIR, "MIGRATION_QUALITY_AUDIT.json");
const IDS = path.join(OUT_DIR, "SCHEMA_TABLE_IDS.json");
const RECON = path.join(OUT_DIR, "VERIFIED_MIGRATION_RECONCILE.json");
const CHECKPOINT = path.join(OUT_DIR, "VERIFIED_MIGRATION_CHECKPOINT.json");
const BATCH_SIZE = 50;

const APPLY = process.argv.includes("--apply");
const DRY = !APPLY || process.argv.includes("--dry-run");
const RECONCILE_ONLY = process.argv.includes("--reconcile");
const IDEMPOTENCY = process.argv.includes("--idempotency-rerun");
const batchArg = process.argv.find((a) => a.startsWith("--batch="));
const BATCH_INDEX = batchArg ? Number(batchArg.split("=")[1]) : 0;

function sha(s) {
  return crypto.createHash("sha1").update(String(s)).digest("hex").slice(0, 16);
}
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
function token() {
  return (process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || "").trim();
}
function scrub(fields) {
  for (const k of Object.keys(fields)) {
    if (fields[k] == null || fields[k] === "") delete fields[k];
  }
  return fields;
}
function norm(s) {
  return String(s || "")
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}
function hotelMatchKey(hotelName, city, state, agreementType) {
  return [norm(hotelName), norm(city), norm(state), String(agreementType || "FRANCHISE").toUpperCase()].join("|");
}

async function at(tableId, method, body, qs = "") {
  const res = await fetch(`https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${tableId}${qs}`, {
    method,
    headers: {
      Authorization: `Bearer ${token()}`,
      ...(body ? { "Content-Type": "application/json" } : {}),
    },
    body: body ? JSON.stringify(body) : undefined,
  });
  return { ok: res.ok, status: res.status, json: await res.json().catch(() => ({})) };
}

async function listAll(tableId, fields = []) {
  const out = [];
  let offset = null;
  do {
    const params = new URLSearchParams({ pageSize: "100" });
    for (const f of fields) params.append("fields[]", f);
    if (offset) params.set("offset", offset);
    const r = await at(tableId, "GET", null, `?${params}`);
    if (!r.ok) throw new Error(`LIST_FAILED ${tableId}: ${JSON.stringify(r.json)}`);
    out.push(...(r.json.records || []));
    offset = r.json.offset || null;
    await sleep(120);
  } while (offset);
  return out;
}

async function findByFormula(tableId, formula) {
  const qs = `?filterByFormula=${encodeURIComponent(formula)}&maxRecords=1`;
  const r = await at(tableId, "GET", null, qs);
  return r.json.records?.[0] || null;
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
    for (const f of rows) if (f.findingId) byId.set(f.findingId, f);
  }
  return byId;
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

async function reconcile(schemaIds, audit) {
  const verified = (audit.agreements || []).filter((a) => a.quality === "VERIFIED");
  const milestones = (audit.milestones || []).filter((m) => m.quality === "VERIFIED");
  const msByContract = new Map();
  for (const m of milestones) {
    if (!msByContract.has(m.contractId)) msByContract.set(m.contractId, []);
    msByContract.get(m.contractId).push(m);
  }

  const liveContracts = await listAll(schemaIds.contractsTableId, [
    "Contract ID",
    "Hotel City",
    "Hotel State / Region",
    "Agreement Type",
    "Primary Source URL",
    "Publication State",
    "Notes / Quarantine",
  ]);
  // Notes may contain hotel name; also try to match via Contract ID
  const liveByContractId = new Map();
  for (const rec of liveContracts) {
    const cid = rec.fields?.["Contract ID"];
    if (cid) liveByContractId.set(cid, rec);
  }

  // Inventory by Discovery Property ID (same algorithm as Research Mode persist)
  const liveInv = await listAll(schemaIds.inventoryTableId, [
    MAP_DISCOVERY_INVENTORY.discoveryPropertyId,
    MAP_DISCOVERY_INVENTORY.hotelNameNormalized,
    MAP_DISCOVERY_INVENTORY.city,
    MAP_DISCOVERY_INVENTORY.stateRegion,
  ]);
  const invByDiscoveryId = new Map();
  const invByHotel = new Map();
  for (const rec of liveInv) {
    const did = rec.fields?.[MAP_DISCOVERY_INVENTORY.discoveryPropertyId];
    if (did) invByDiscoveryId.set(did, rec);
    const k = hotelMatchKey(
      rec.fields?.[MAP_DISCOVERY_INVENTORY.hotelNameNormalized],
      rec.fields?.[MAP_DISCOVERY_INVENTORY.city],
      rec.fields?.[MAP_DISCOVERY_INVENTORY.stateRegion],
      "FRANCHISE",
    );
    invByHotel.set(k, rec);
  }

  // Contracts by linked Discovery Property record id
  const liveContractsFull = await listAll(schemaIds.contractsTableId, [
    "Contract ID",
    "Discovery Property",
    "Hotel City",
    "Hotel State / Region",
    "Agreement Type",
    "Primary SEC Accession",
    "Notes / Quarantine",
    "Publication State",
  ]);
  const contractsByInvRecId = new Map();
  for (const rec of liveContractsFull) {
    for (const invId of rec.fields?.["Discovery Property"] || []) {
      if (!contractsByInvRecId.has(invId)) contractsByInvRecId.set(invId, []);
      contractsByInvRecId.get(invId).push(rec);
    }
  }

  const alreadyPersisted = [];
  const pending = [];
  for (const a of verified) {
    const matchKey = hotelMatchKey(a.hotelName, a.city, a.stateRegion, a.agreementType);
    const propertyKey = `${norm(a.hotelName)}|${norm(a.city)}|${norm(a.stateRegion)}`;
    const discoveryPropertyId = `disc_ctim_v4_${sha(propertyKey)}`;

    let matched = liveByContractId.get(a.contractId) || null;
    let matchMethod = matched ? "CONTRACT_ID" : null;
    let idAlias = null;

    // Pilot used a different contract-ID hash + simpler propertyKey (toLowerCase, keeps punctuation)
    if (!matched) {
      const pilotPropertyKey = `${String(a.hotelName).toLowerCase()}|${String(a.city).toLowerCase()}|${String(a.stateRegion).toLowerCase()}`;
      const pilotDiscoveryId = `disc_ctim_v4_${sha(pilotPropertyKey)}`;
      const inv =
        invByDiscoveryId.get(discoveryPropertyId) ||
        invByDiscoveryId.get(pilotDiscoveryId) ||
        invByHotel.get(matchKey) ||
        null;
      if (inv) {
        const linked = contractsByInvRecId.get(inv.id) || [];
        if (linked.length === 1) {
          matched = linked[0];
          matchMethod = "DISCOVERY_PROPERTY_LINK";
          idAlias = {
            auditContractId: a.contractId,
            liveContractId: matched.fields?.["Contract ID"] || null,
          };
        } else if (linked.length > 1) {
          const byNotes = linked.find((r) =>
            norm(r.fields?.["Notes / Quarantine"] || "").includes(norm(a.hotelName).slice(0, 16)),
          );
          if (byNotes) {
            matched = byNotes;
            matchMethod = "DISCOVERY_PROPERTY_NOTES";
            idAlias = {
              auditContractId: a.contractId,
              liveContractId: matched.fields?.["Contract ID"] || null,
            };
          }
        }
      }
      // Last resort: notes quarantine contains hotel name (pilot rows)
      if (!matched) {
        const byNotes = liveContractsFull.find((r) =>
          norm(r.fields?.["Notes / Quarantine"] || "").includes(norm(a.hotelName).slice(0, 18)),
        );
        if (byNotes) {
          matched = byNotes;
          matchMethod = "NOTES_HOTEL_NAME";
          idAlias = {
            auditContractId: a.contractId,
            liveContractId: matched.fields?.["Contract ID"] || null,
          };
        }
      }
    }

    const ms = msByContract.get(a.contractId) || [];
    const entry = {
      contractId: a.contractId,
      hotelName: a.hotelName,
      city: a.city,
      stateRegion: a.stateRegion,
      agreementType: a.agreementType,
      brandFamily: a.brandFamily,
      sourceUrls: a.sourceUrls,
      sourceAccessions: a.sourceAccessions,
      findingIds: a.findingIds,
      evidenceSnippets: a.evidenceSnippets,
      milestones: ms,
      matchKey,
      alreadyPersisted: Boolean(matched),
      liveContractRecordId: matched?.id || null,
      liveContractId: matched?.fields?.["Contract ID"] || null,
      matchMethod,
      discoveryPropertyId,
      idAlias,
    };
    if (matched) alreadyPersisted.push(entry);
    else pending.push(entry);
  }

  // Stable sort pending for deterministic batches
  pending.sort(
    (a, b) =>
      String(a.hotelName).localeCompare(String(b.hotelName)) ||
      String(a.city).localeCompare(String(b.city)) ||
      String(a.contractId).localeCompare(String(b.contractId)),
  );

  const batches = [];
  for (let i = 0; i < pending.length; i += BATCH_SIZE) {
    batches.push({
      batchIndex: batches.length,
      size: Math.min(BATCH_SIZE, pending.length - i),
      contractIds: pending.slice(i, i + BATCH_SIZE).map((x) => x.contractId),
    });
  }

  const report = {
    module: "V4_VERIFIED_MIGRATION_RECONCILE",
    at: new Date().toISOString(),
    verifiedAgreements: verified.length,
    verifiedMilestones: milestones.length,
    liveContractsListed: liveContracts.length,
    alreadyPersisted: alreadyPersisted.length,
    pending: pending.length,
    batchSize: BATCH_SIZE,
    batchCount: batches.length,
    batches,
    alreadyPersistedSample: alreadyPersisted.slice(0, 5).map((x) => ({
      hotelName: x.hotelName,
      auditContractId: x.contractId,
      liveContractId: x.liveContractId,
      matchMethod: x.matchMethod,
    })),
    pendingSample: pending.slice(0, 5).map((x) => ({
      hotelName: x.hotelName,
      contractId: x.contractId,
      city: x.city,
      stateRegion: x.stateRegion,
    })),
    researchOnlyRetained: (audit.agreements || []).filter((a) => a.quality === "RESEARCH_ONLY").length,
    conflictedRetained: (audit.agreements || []).filter((a) => a.quality === "CONFLICTED").length,
    note: "RESEARCH_ONLY/CONFLICTED/REJECTED not migrated as verified. Pilot rows matched by Contract ID or city/state heuristics.",
  };

  fs.writeFileSync(RECON, JSON.stringify({ ...report, pending, alreadyPersisted }, null, 2));
  // Also write slim without full arrays for console
  console.log(
    JSON.stringify(
      {
        out: RECON,
        verifiedAgreements: report.verifiedAgreements,
        alreadyPersisted: report.alreadyPersisted,
        pending: report.pending,
        batchCount: report.batchCount,
        expectedNewMax: report.pending,
      },
      null,
      2,
    ),
  );
  return { report, pending, alreadyPersisted, msByContract };
}

async function persistBatch(schemaIds, batchRows, findings, opts = {}) {
  const results = [];
  const rejected = [];
  let failClosed = false;

  // Index milestones by linked Contract record id (ARRAYJOIN formulas return names, not ids)
  let milestonesByContractRecId = opts.milestonesByContractRecId || null;
  if (!milestonesByContractRecId) {
    milestonesByContractRecId = new Map();
    const allMs = await listAll(schemaIds.milestonesTableId, ["Milestone ID", "Contract", "Publication State"]);
    for (const rec of allMs) {
      for (const cid of rec.fields?.Contract || []) {
        if (!milestonesByContractRecId.has(cid)) milestonesByContractRecId.set(cid, []);
        milestonesByContractRecId.get(cid).push(rec);
      }
    }
  }

  for (const row of batchRows) {
    const f = findings.get(row.findingIds?.[0]) || null;
    const sourceUrl = row.sourceUrls?.[0] || f?.source?.documentUrl || null;
    const accession = row.sourceAccessions?.[0] || f?.source?.accessionNumber || null;
    const ms = (row.milestones || [])[0] || null;
    const primaryDate = ms?.primaryDate || f?.primaryDate || null;
    const dateMeaning = ms?.dateMeaning || f?.dateMeaning || "FRANCHISE_EXPIRATION";

    const failReasons = [];
    if (!row.hotelName) failReasons.push("missing_hotel");
    if (!row.city || !row.stateRegion) failReasons.push("missing_geo");
    if (!sourceUrl || !accession) failReasons.push("missing_provenance");
    if (!primaryDate) failReasons.push("missing_date");
    if (!ms?.milestoneId) failReasons.push("missing_milestone");
    if (failReasons.length) {
      rejected.push({ contractId: row.contractId, hotelName: row.hotelName, failReasons });
      results.push({ contractId: row.contractId, action: "REJECTED", failReasons });
      continue;
    }

    const propertyKey = `${norm(row.hotelName)}|${norm(row.city)}|${norm(row.stateRegion)}`;
    const discoveryPropertyId = `disc_ctim_v4_${sha(propertyKey)}`;
    const contractId = row.contractId;
    const milestoneId = ms.milestoneId;
    const airtableFindingId = `find_ctim_v4_${sha([discoveryPropertyId, dateMeaning, primaryDate, accession].join("|"))}`;

    // Prefer live IDs from reconcile (pilot may use different Contract ID hash)
    const liveContractId = row.liveContractId || row.idAlias?.liveContractId || null;
    const pilotPropertyKey = `${String(row.hotelName).toLowerCase()}|${String(row.city).toLowerCase()}|${String(row.stateRegion).toLowerCase()}`;
    const pilotDiscoveryId = `disc_ctim_v4_${sha(pilotPropertyKey)}`;

    let invRec =
      (await findByFormula(
        schemaIds.inventoryTableId,
        `{${MAP_DISCOVERY_INVENTORY.discoveryPropertyId}}='${discoveryPropertyId}'`,
      )) ||
      (await findByFormula(
        schemaIds.inventoryTableId,
        `{${MAP_DISCOVERY_INVENTORY.discoveryPropertyId}}='${pilotDiscoveryId}'`,
      ));

    let contractRec =
      (liveContractId
        ? await findByFormula(schemaIds.contractsTableId, `{Contract ID}='${liveContractId}'`)
        : null) ||
      (await findByFormula(schemaIds.contractsTableId, `{Contract ID}='${contractId}'`)) ||
      (row.liveContractRecordId
        ? (await at(schemaIds.contractsTableId, "GET", null, `/${row.liveContractRecordId}`)).json
        : null);

    let milestoneRec = await findByFormula(
      schemaIds.milestonesTableId,
      `{Milestone ID}='${milestoneId}'`,
    );
    // Pilot milestones used different IDs — prefer any milestone already linked to this contract
    if (!milestoneRec && contractRec?.id) {
      const linked = milestonesByContractRecId.get(contractRec.id) || [];
      if (linked.length >= 1) milestoneRec = linked[0];
    }

    let findingRec = await findByFormula(
      schemaIds.findingsTableId,
      `{${MAP_DISCOVERY_FINDINGS.findingId}}='${airtableFindingId}'`,
    );

    const rowResult = {
      contractId,
      milestoneId,
      hotelName: row.hotelName,
      discoveryPropertyId,
      airtableFindingId,
      inventoryRecordId: invRec?.id || null,
      contractRecordId: contractRec?.id || null,
      milestoneRecordId: milestoneRec?.id || null,
      findingRecordId: findingRec?.id || null,
      changeClass: contractRec ? "UNCHANGED" : "NEW",
      actions: [],
      readBack: null,
    };

    if (DRY) {
      rowResult.actions.push("DRY_RUN");
      results.push(rowResult);
      await sleep(60);
      continue;
    }

    if (!invRec) {
      const fields = scrub({
        [MAP_DISCOVERY_INVENTORY.discoveryPropertyId]: discoveryPropertyId,
        [MAP_DISCOVERY_INVENTORY.hotelNameNormalized]: row.hotelName,
        [MAP_DISCOVERY_INVENTORY.hotelNameRaw]: row.hotelName,
        [MAP_DISCOVERY_INVENTORY.city]: row.city,
        [MAP_DISCOVERY_INVENTORY.stateRegion]: row.stateRegion,
        [MAP_DISCOVERY_INVENTORY.country]: "United States",
        [MAP_DISCOVERY_INVENTORY.brandFamily]: row.brandFamily || f?.brandFamily || null,
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
        [MAP_DISCOVERY_INVENTORY.discoveryMethod]: "SEC_CMBS_VERIFIED_MIGRATION_V4",
        [MAP_DISCOVERY_INVENTORY.propertyKey]: propertyKey,
        [MAP_DISCOVERY_INVENTORY.notesQuarantine]: `V4 verified migration ${contractId}`,
        "Geographic Processing Class": "PRIORITY_HPC_ENRICHMENT",
        "Continent / Macro Region": "NORTH_AMERICA",
      });
      const created = await at(schemaIds.inventoryTableId, "POST", { fields, typecast: true });
      if (!created.ok) {
        rowResult.actions.push("INVENTORY_FAIL");
        rowResult.error = created.json;
        results.push(rowResult);
        failClosed = true;
        break;
      }
      invRec = created.json;
      rowResult.actions.push("INVENTORY_CREATED");
      rowResult.inventoryRecordId = invRec.id;
    } else {
      rowResult.actions.push("INVENTORY_REUSED");
      rowResult.inventoryRecordId = invRec.id;
    }
    await sleep(140);

    if (!contractRec) {
      const fields = scrub({
        "Contract ID": contractId,
        "Discovery Property": invRec?.id ? [invRec.id] : undefined,
        "Agreement Type": row.agreementType || "FRANCHISE",
        "Agreement Role": "ORIGINAL",
        "Brand Family": row.brandFamily || null,
        "Brand Label": f?.brandLabel || null,
        "Hotel Country": "United States",
        "Hotel State / Region": row.stateRegion,
        "Hotel City": row.city,
        "Geographic Processing Class": "PRIORITY_HPC_ENRICHMENT",
        "Contract Status": ms?.currentStatus || "EXPIRED_STATUS_UNCONFIRMED",
        "Scheduled Expiration Date":
          primaryDate && /^\d{4}-\d{2}-\d{2}/.test(primaryDate) ? primaryDate.slice(0, 10) : undefined,
        "Primary Source URL": sourceUrl,
        "Primary SEC Accession": accession,
        "Evidence Confidence": "MEDIUM",
        "Publication State": "CUSTOMER_BLOCKED",
        "Schema Version": "4.0.0",
        "Notes / Quarantine": `V4 verified migration · ${row.hotelName} · ${dateMeaning}`,
      });
      const created = await at(schemaIds.contractsTableId, "POST", { fields, typecast: true });
      if (!created.ok) {
        rowResult.actions.push("CONTRACT_FAIL");
        rowResult.error = created.json;
        results.push(rowResult);
        failClosed = true;
        break;
      }
      contractRec = created.json;
      rowResult.actions.push("CONTRACT_CREATED");
      rowResult.contractRecordId = contractRec.id;
      rowResult.changeClass = "NEW";
    } else {
      rowResult.actions.push("CONTRACT_REUSED");
      rowResult.contractRecordId = contractRec.id;
      rowResult.changeClass = "UNCHANGED";
    }
    await sleep(140);

    if (!milestoneRec) {
      // Fail closed rather than create a second milestone under an existing contract during idempotency
      if (opts.forbidMilestoneCreateIfContractExists && contractRec?.id) {
        rowResult.actions.push("MILESTONE_MISSING_FAIL_CLOSED");
        rowResult.error = {
          note: "Contract exists but no linked milestone found; refusing to create duplicate during verify",
        };
        results.push(rowResult);
        failClosed = true;
        break;
      }
      const fields = scrub({
        "Milestone ID": milestoneId,
        Contract: contractRec?.id ? [contractRec.id] : undefined,
        "Discovery Property": invRec?.id ? [invRec.id] : undefined,
        "Milestone Type": milestoneTypeFromMeaning(dateMeaning),
        "Milestone Date":
          primaryDate && /^\d{4}-\d{2}-\d{2}/.test(primaryDate) ? primaryDate.slice(0, 10) : undefined,
        "Date Meaning Precision": f?.datePrecision || "YEAR_ONLY",
        "Current Status Class": ms?.currentStatus || "EXPIRED_STATUS_UNCONFIRMED",
        "Source URL": sourceUrl,
        "SEC Accession": accession,
        "Evidence Snippet": String(row.evidenceSnippets?.[0] || f?.snippet || "").slice(0, 4000),
        "Publication State": "CUSTOMER_BLOCKED",
        "Schema Version": "4.0.0",
        Notes: `V4 verified migration · historicalApplicability=${ms?.historicalApplicability || "HISTORICALLY_VERIFIED"}`,
      });
      const created = await at(schemaIds.milestonesTableId, "POST", { fields, typecast: true });
      if (!created.ok) {
        rowResult.actions.push("MILESTONE_FAIL");
        rowResult.error = created.json;
        results.push(rowResult);
        failClosed = true;
        break;
      }
      milestoneRec = created.json;
      rowResult.actions.push("MILESTONE_CREATED");
      rowResult.milestoneRecordId = milestoneRec.id;
    } else {
      rowResult.actions.push(
        milestoneRec.fields?.["Milestone ID"] === milestoneId
          ? "MILESTONE_REUSED"
          : "MILESTONE_REUSED_VIA_CONTRACT_LINK",
      );
      rowResult.milestoneRecordId = milestoneRec.id;
      rowResult.liveMilestoneId = milestoneRec.fields?.["Milestone ID"] || null;
    }
    await sleep(140);

    if (!findingRec) {
      const fields = scrub({
        [MAP_DISCOVERY_FINDINGS.findingId]: airtableFindingId,
        [MAP_DISCOVERY_FINDINGS.discoveryProperty]: invRec?.id ? [invRec.id] : undefined,
        [MAP_DISCOVERY_FINDINGS.findingType]: "CONTRACT_MILESTONE",
        [MAP_DISCOVERY_FINDINGS.fieldName]: dateMeaning,
        [MAP_DISCOVERY_FINDINGS.valueObserved]: String(primaryDate).slice(0, 200),
        [MAP_DISCOVERY_FINDINGS.agreementType]: row.agreementType || "FRANCHISE",
        [MAP_DISCOVERY_FINDINGS.historicalApplicability]: "CURRENT_STATUS_UNCONFIRMED",
        [MAP_DISCOVERY_FINDINGS.effectiveDate]:
          primaryDate && /^\d{4}-\d{2}-\d{2}/.test(primaryDate) ? primaryDate.slice(0, 10) : undefined,
        [MAP_DISCOVERY_FINDINGS.sourceUrl]: sourceUrl,
        [MAP_DISCOVERY_FINDINGS.secAccession]: accession,
        [MAP_DISCOVERY_FINDINGS.evidenceSnippet]: String(
          row.evidenceSnippets?.[0] || f?.snippet || "",
        ).slice(0, 4000),
        [MAP_DISCOVERY_FINDINGS.confidence]: "MEDIUM",
        [MAP_DISCOVERY_FINDINGS.verificationStatus]: "HISTORICALLY_VERIFIED",
        [MAP_DISCOVERY_FINDINGS.hpcPromotionStatus]: "BLOCKED_CUSTOMER_PUBLICATION",
        [MAP_DISCOVERY_FINDINGS.schemaVersion]: DISCOVERY_SCHEMA_VERSION,
        [MAP_DISCOVERY_FINDINGS.notes]: `V4_VERIFIED_MIGRATION|${contractId}`,
        "Linked Contract": contractRec?.id ? [contractRec.id] : undefined,
        "Linked Milestone": milestoneRec?.id ? [milestoneRec.id] : undefined,
      });
      const created = await at(schemaIds.findingsTableId, "POST", { fields, typecast: true });
      if (!created.ok) {
        rowResult.actions.push("FINDING_FAIL");
        rowResult.error = created.json;
      } else {
        findingRec = created.json;
        rowResult.actions.push("FINDING_CREATED");
        rowResult.findingRecordId = findingRec.id;
        if (milestoneRec?.id) {
          await at(schemaIds.milestonesTableId, "PATCH", {
            records: [{ id: milestoneRec.id, fields: { "Related Finding": [findingRec.id] } }],
            typecast: true,
          });
          rowResult.actions.push("MILESTONE_FINDING_LINKED");
        }
      }
    } else {
      rowResult.actions.push("FINDING_REUSED");
      rowResult.findingRecordId = findingRec.id;
    }

    // Read-back
    const rbC = await at(schemaIds.contractsTableId, "GET", null, `/${contractRec.id}`);
    const rbM = await at(schemaIds.milestonesTableId, "GET", null, `/${milestoneRec.id}`);
    const rbI = await at(schemaIds.inventoryTableId, "GET", null, `/${invRec.id}`);
    const cf = rbC.json?.fields || {};
    const mf = rbM.json?.fields || {};
    const iff = rbI.json?.fields || {};
    rowResult.readBack = {
      ok: Boolean(rbC.ok && rbM.ok && rbI.ok),
      contractId: cf["Contract ID"],
      milestoneId: mf["Milestone ID"],
      contractPublication: cf["Publication State"],
      milestonePublication: mf["Publication State"],
      inventoryPublication: iff[MAP_DISCOVERY_INVENTORY.publicationState],
      milestoneLinkedContract: (mf.Contract || []).length > 0,
      hpcOnContract: (cf["HPC Hotel"] || []).length,
      hpcOnMilestone: (mf["HPC Hotel"] || []).length,
    };
    if (
      rowResult.readBack.contractPublication !== "CUSTOMER_BLOCKED" ||
      rowResult.readBack.milestonePublication !== "CUSTOMER_BLOCKED" ||
      rowResult.readBack.inventoryPublication !== "CUSTOMER_BLOCKED" ||
      !rowResult.readBack.milestoneLinkedContract
    ) {
      rowResult.readBack.ok = false;
      rowResult.actions.push("READBACK_FAIL");
      results.push(rowResult);
      failClosed = true;
      break;
    }

    results.push(rowResult);
    await sleep(160);
  }

  return { results, rejected, failClosed };
}

async function main() {
  if (!token()) {
    console.log(JSON.stringify({ ok: false, error: "NO_AIRTABLE_TOKEN" }));
    process.exitCode = 1;
    return;
  }
  if (APPLY && String(process.env.HIF_APPLY_RESEARCH || "") !== "1") {
    console.log(JSON.stringify({ ok: false, error: "HIF_APPLY_RESEARCH_ENV_REQUIRED" }));
    process.exitCode = 2;
    return;
  }
  if (!fs.existsSync(AUDIT) || !fs.existsSync(IDS)) {
    throw new Error("Missing audit or SCHEMA_TABLE_IDS");
  }
  const audit = JSON.parse(fs.readFileSync(AUDIT, "utf8"));
  const schemaIds = JSON.parse(fs.readFileSync(IDS, "utf8"));
  const findings = loadFindingsIndex();

  const { report, pending } = await reconcile(schemaIds, audit);
  if (RECONCILE_ONLY) return;

  const VERIFY_IDEMP = process.argv.includes("--verify-idempotency");
  const limitArg = process.argv.find((a) => a.startsWith("--limit="));
  const LIMIT = limitArg ? Number(limitArg.split("=")[1]) : 50;

  if (VERIFY_IDEMP) {
    // Re-process already-persisted VERIFIED rows; expect CONTRACT_REUSED / MILESTONE_REUSED
    const reconFull = JSON.parse(fs.readFileSync(RECON, "utf8"));
    const sample = (reconFull.alreadyPersisted || []).slice(0, LIMIT);
    console.log(JSON.stringify({ mode: "VERIFY_IDEMPOTENCY", sample: sample.length }));
    const { results, rejected, failClosed } = await persistBatch(schemaIds, sample, findings, {
      forbidMilestoneCreateIfContractExists: true,
    });
    const milestoneReused = (r) =>
      r.actions?.includes("MILESTONE_REUSED") ||
      r.actions?.includes("MILESTONE_REUSED_VIA_CONTRACT_LINK");
    const summary = {
      module: "V4_VERIFIED_MIGRATION_IDEMPOTENCY_VERIFY",
      at: new Date().toISOString(),
      attempted: sample.length,
      createdContracts: results.filter((r) => r.actions?.includes("CONTRACT_CREATED")).length,
      reusedContracts: results.filter((r) => r.actions?.includes("CONTRACT_REUSED")).length,
      createdMilestones: results.filter((r) => r.actions?.includes("MILESTONE_CREATED")).length,
      reusedMilestones: results.filter((r) => milestoneReused(r)).length,
      readBackOk: results.filter((r) => r.readBack?.ok).length,
      rejected: rejected.length,
      failClosed,
      hpcRecordsCreated: 0,
      ok:
        results.every(
          (r) => r.actions?.includes("CONTRACT_REUSED") && milestoneReused(r) && r.readBack?.ok,
        ) &&
        results.every((r) => !r.actions?.includes("CONTRACT_CREATED")) &&
        results.every((r) => !r.actions?.includes("MILESTONE_CREATED")) &&
        !failClosed,
      results,
    };
    const out = path.join(OUT_DIR, "VERIFIED_MIGRATION_IDEMPOTENCY_VERIFY.json");
    fs.writeFileSync(out, JSON.stringify(summary, null, 2));
    console.log(
      JSON.stringify(
        {
          out,
          ok: summary.ok,
          reusedContracts: summary.reusedContracts,
          createdContracts: summary.createdContracts,
          readBackOk: summary.readBackOk,
        },
        null,
        2,
      ),
    );
    if (!summary.ok) process.exitCode = 3;
    return;
  }

  if (BATCH_INDEX < 0 || BATCH_INDEX >= report.batchCount) {
    if (report.batchCount === 0) {
      console.log(
        JSON.stringify({
          ok: true,
          note: "NO_PENDING_VERIFIED_ALL_RECONCILED",
          alreadyPersisted: report.alreadyPersisted,
          pending: 0,
        }),
      );
      return;
    }
    throw new Error(`Invalid --batch=${BATCH_INDEX}; batchCount=${report.batchCount}`);
  }

  const batchMeta = report.batches[BATCH_INDEX];
  const batchRows = pending.filter((p) => batchMeta.contractIds.includes(p.contractId));
  console.log(
    JSON.stringify({
      batchIndex: BATCH_INDEX,
      batchSize: batchRows.length,
      dryRun: DRY,
      idempotency: IDEMPOTENCY,
    }),
  );

  const { results, rejected, failClosed } = await persistBatch(schemaIds, batchRows, findings);

  const summary = {
    module: "V4_VERIFIED_MIGRATION_BATCH",
    at: new Date().toISOString(),
    batchIndex: BATCH_INDEX,
    dryRun: DRY,
    apply: APPLY && !DRY,
    idempotencyRerun: IDEMPOTENCY,
    attempted: batchRows.length,
    rejected: rejected.length,
    createdContracts: results.filter((r) => r.actions?.includes("CONTRACT_CREATED")).length,
    reusedContracts: results.filter((r) => r.actions?.includes("CONTRACT_REUSED")).length,
    createdMilestones: results.filter((r) => r.actions?.includes("MILESTONE_CREATED")).length,
    reusedMilestones: results.filter((r) => r.actions?.includes("MILESTONE_REUSED")).length,
    createdFindings: results.filter((r) => r.actions?.includes("FINDING_CREATED")).length,
    readBackOk: results.filter((r) => r.readBack?.ok).length,
    failClosed,
    hpcRecordsCreated: 0,
    hpcFieldsModified: 0,
    publicationState: "CUSTOMER_BLOCKED",
    rejected,
    results,
  };

  const outName = `VERIFIED_MIGRATION_BATCH_${BATCH_INDEX}_${IDEMPOTENCY ? "IDEMP" : DRY ? "DRY" : "APPLY"}.json`;
  fs.writeFileSync(path.join(OUT_DIR, outName), JSON.stringify(summary, null, 2));

  const cp = fs.existsSync(CHECKPOINT) ? JSON.parse(fs.readFileSync(CHECKPOINT, "utf8")) : { batches: {} };
  cp.batches[String(BATCH_INDEX)] = {
    at: summary.at,
    mode: IDEMPOTENCY ? "IDEMP" : DRY ? "DRY" : "APPLY",
    createdContracts: summary.createdContracts,
    reusedContracts: summary.reusedContracts,
    createdMilestones: summary.createdMilestones,
    readBackOk: summary.readBackOk,
    failClosed,
  };
  cp.updatedAt = summary.at;
  fs.writeFileSync(CHECKPOINT, JSON.stringify(cp, null, 2));

  console.log(
    JSON.stringify(
      {
        out: path.join(OUT_DIR, outName),
        createdContracts: summary.createdContracts,
        reusedContracts: summary.reusedContracts,
        createdMilestones: summary.createdMilestones,
        readBackOk: summary.readBackOk,
        failClosed,
      },
      null,
      2,
    ),
  );
  if (failClosed) process.exitCode = 3;
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
